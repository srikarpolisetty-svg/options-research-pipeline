# ibkr_execution_engine.py
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import duckdb
import exchange_calendars as ecals
from ib_insync import IB, Contract, MarketOrder, StopOrder, Order
from policy.expiration import is_in_third_friday_week
from policy.risk import (
    ENTRY_QTY,
    MAX_OPEN_ORDERS,
    MIN_ORDER_AGE_SECONDS,
    PER_DAY_RISK_PCT,
    PER_TRADE_RISK_PCT,
    TRAIL_PCT,
    TRAIL_TIF,
)


# =========================
# Config
# =========================
HOST = "127.0.0.1"
PORT = 4002
DB_PATH = "options_data.db"

# Global kill switch (entries + exits)
EXECUTE_TRADES_DEFAULT = True

# Optional: allow exits even when kill switch is off (recommended safety behavior)
ALLOW_EXITS_WHEN_KILLED = True


@dataclass
class RiskConfig:
    per_trade_risk_pct: float = PER_TRADE_RISK_PCT
    per_day_risk_pct: float = PER_DAY_RISK_PCT
    max_open_orders: int = MAX_OPEN_ORDERS
    min_order_age_seconds: int = MIN_ORDER_AGE_SECONDS
    trail_pct: float = TRAIL_PCT
    trail_tif: str = TRAIL_TIF
    entry_qty: int = ENTRY_QTY


class IBKRExecutionEngine:
    """
    Execution-only engine:
      - Uses conId from DuckDB for entries (no qualification)
      - Places server-side IB trailing stops (no Python trailing loop)
      - Entries can be disabled by gates; exits/management can still run (configurable)
      - Dedupe entries per run (no repeated buys for same conId)
      - Avoids re-placing BE/Trail if open orders already exist
    """

    def __init__(
        self,
        client_id: int,
        host=HOST,
        port=PORT,
        db_path=DB_PATH,
        execute_trades_default=EXECUTE_TRADES_DEFAULT,
        allow_exits_when_killed: bool = ALLOW_EXITS_WHEN_KILLED,
        risk: RiskConfig = RiskConfig(),
    ):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.db_path = db_path
        self.execute_trades_default = execute_trades_default
        self.allow_exits_when_killed = allow_exits_when_killed
        self.risk = risk

        self.ib = IB()
        self.NY_TZ = ZoneInfo("America/New_York")
        self.XNYS = ecals.get_calendar("XNYS")

        # Track submit times so you can enforce the 15-min gate
        self.order_submit_time: dict[int, datetime] = {}

    # -------------------------
    # Simple logger (max visibility)
    # -------------------------
    def log(self, event: str, **fields):
        try:
            ts = datetime.now(self.NY_TZ).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        extras = ""
        if fields:
            parts = []
            for k, v in fields.items():
                try:
                    parts.append(f"{k}={v}")
                except Exception:
                    parts.append(f"{k}=?")
            extras = " " + " ".join(parts)

        print(f"[EXEC][{event}] {ts}{extras}", flush=True)

    # -------------------------
    # Connection
    # -------------------------
    def connect(self):
        if not self.ib.isConnected():
            self.ib.connect(self.host, self.port, clientId=self.client_id)

    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()

    # -------------------------
    # Contract from conId (DB -> IB)
    # -------------------------
    def opt_contract_from_conid(self, conid: int) -> Contract:
        return Contract(conId=int(conid), secType="OPT", exchange="SMART", currency="USD")

    # -------------------------
    # Minimal mark price (ONLY for managing OPEN positions)
    # -------------------------
    def get_mark_price_snapshot(self, contract: Contract, wait_s: float = 0.8) -> float | None:
        """
        Only used for exits/management logic (PnL/return %).
        Entries do NOT request market data.
        """
        t = self.ib.reqMktData(contract, "", snapshot=True, regulatorySnapshot=False)
        self.ib.sleep(wait_s)

        bid = t.bid if (t.bid and t.bid > 0) else None
        ask = t.ask if (t.ask and t.ask > 0) else None
        last = t.last if (t.last and t.last > 0) else None
        close = t.close if (t.close and t.close > 0) else None

        if bid is not None and ask is not None:
            return float((bid + ask) / 2.0)
        if last is not None:
            return float(last)
        if close is not None:
            return float(close)
        return None

    # -------------------------
    # Positions / PnL (options only)
    # -------------------------
    def get_positions(self):
        return list(self.ib.positions())

    def _entry_from_position(self, p) -> float | None:
        if p.avgCost is None:
            return None
        try:
            ac = float(p.avgCost)
        except Exception:
            return None
        if ac <= 0:
            return None

        if ac > 20.0:
            return ac / 100.0
        return ac

    def compute_unrealized_pnl_options(self) -> float:
        pnl = 0.0
        for p in self.get_positions():
            if p.contract.secType != "OPT" or p.position == 0:
                continue

            mark = self.get_mark_price_snapshot(p.contract)
            if mark is None:
                continue

            entry = self._entry_from_position(p)
            if entry is None or entry <= 0:
                continue

            pnl += (mark - entry) * float(p.position) * 100.0

        return float(pnl)

    def position_return_pct(self, p, mark: float) -> float | None:
        entry = self._entry_from_position(p)
        if entry is None or entry <= 0:
            return None
        return (mark - entry) / entry * 100.0

    # -------------------------
    # Orders
    # -------------------------
    def _track_trade(self, trade):
        try:
            oid = trade.order.orderId
            self.order_submit_time[oid] = datetime.now(timezone.utc)
        except Exception:
            pass

    def place_market(self, contract: Contract, side: str, qty: int, allow_orders: bool):
        if not allow_orders:
            return None
        trade = self.ib.placeOrder(contract, MarketOrder(side.upper(), int(qty)))
        self._track_trade(trade)
        self.ib.sleep(0.2)
        return trade

    def place_stop_close_sell(self, contract: Contract, qty: int, stop_price: float, allow_orders: bool):
        if not allow_orders:
            return None
        trade = self.ib.placeOrder(contract, StopOrder("SELL", int(qty), float(stop_price)))
        self._track_trade(trade)
        self.ib.sleep(0.2)
        return trade

    def place_trailing_stop_pct_sell(self, contract: Contract, qty: int, trailing_pct: float, allow_orders: bool):
        if not allow_orders:
            return None

        o = Order(
            action="SELL",
            orderType="TRAIL",
            totalQuantity=int(qty),
            trailingPercent=float(trailing_pct),
            tif=str(self.risk.trail_tif),
        )
        trade = self.ib.placeOrder(contract, o)
        self._track_trade(trade)
        self.ib.sleep(0.2)
        return trade

    # -------------------------
    # DB helpers
    # -------------------------
    def load_recent_signal_rows(self, symbol: str, limit: int = 6):
        con = duckdb.connect(self.db_path, read_only=True)
        df = con.execute(
            """
            SELECT *
            FROM option_snapshots_enriched
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            [symbol, limit],
        ).df()
        con.close()
        return df

    def _row_est_cost(self, row, qty: int) -> float | None:
        for name in ("mid", "price", "option_price", "last", "close"):
            v = getattr(row, name, None)
            if v is None:
                continue
            try:
                px = float(v)
                if px > 0:
                    return px * 100.0 * qty
            except Exception:
                pass

        bid = getattr(row, "bid", None)
        ask = getattr(row, "ask", None)
        try:
            bidf = float(bid) if bid is not None else None
            askf = float(ask) if ask is not None else None
            if bidf is not None and askf is not None and bidf > 0 and askf > 0:
                return ((bidf + askf) / 2.0) * 100.0 * qty
        except Exception:
            pass

        return None

    # -------------------------
    # Existing orders / dedupe
    # -------------------------
    def _open_orders_for_conid(self, conid: int):
        working_status = {"Submitted", "PreSubmitted", "ApiPending"}
        out = []
        for t in self.ib.trades():
            try:
                if t.contract and int(t.contract.conId) == int(conid) and t.orderStatus.status in working_status:
                    out.append(t)
            except Exception:
                continue
        return out

    def _has_working_trailing_sell(self, conid: int) -> bool:
        for t in self._open_orders_for_conid(conid):
            try:
                if t.order and t.order.action == "SELL" and t.order.orderType == "TRAIL":
                    return True
            except Exception:
                pass
        return False

    def _has_working_stop_sell(self, conid: int) -> bool:
        for t in self._open_orders_for_conid(conid):
            try:
                if t.order and t.order.action == "SELL" and t.order.orderType in {"STP", "STOP"}:
                    return True
            except Exception:
                pass
        return False

    def _already_long_conid(self, conid: int) -> bool:
        for p in self.get_positions():
            try:
                if p.contract.secType == "OPT" and int(p.contract.conId) == int(conid) and float(p.position) > 0:
                    return True
            except Exception:
                continue
        return False

    # -------------------------
    # Cancel + Liquidate (prints added)
    # -------------------------
    def cancel_all_option_orders(self):
        self.log("CANCEL_ALL_OPT_ORDERS_START")

        working_status = {"Submitted", "PreSubmitted", "ApiPending"}
        cancelled = 0
        for t in self.ib.trades():
            try:
                if (
                    t.contract is not None
                    and t.contract.secType == "OPT"
                    and t.orderStatus is not None
                    and t.orderStatus.status in working_status
                ):
                    oid = getattr(getattr(t, "order", None), "orderId", None)
                    conid = getattr(getattr(t, "contract", None), "conId", None)
                    self.log("CANCEL_OPT_ORDER", orderId=oid, conid=conid, status=t.orderStatus.status)
                    self.ib.cancelOrder(t.order)
                    cancelled += 1
            except Exception as e:
                self.log("CANCEL_OPT_ORDER_ERR", err=str(e))
                continue

        self.ib.sleep(0.2)
        self.log("CANCEL_ALL_OPT_ORDERS_END", cancelled=cancelled)

    def close_all_option_positions_market(self, allow_exits: bool):
        if not allow_exits:
            self.log("LIQUIDATE_SKIP_EXITS_DISABLED")
            return

        self.log("LIQUIDATE_START")

        sold = 0
        for p in self.get_positions():
            try:
                if p.contract is None or p.contract.secType != "OPT":
                    continue

                qty = float(p.position)
                if qty <= 0:
                    continue

                conid = getattr(p.contract, "conId", None)
                self.log("LIQUIDATE_SELL_MKT_PLACING", conid=conid, qty=int(qty))
                self.place_market(p.contract, "SELL", int(qty), allow_orders=True)
                sold += 1
            except Exception as e:
                self.log("LIQUIDATE_ERR", err=str(e))
                continue

        self.log("LIQUIDATE_END", positions_sold=sold)

    def enforce_daily_loss_forced_liquidation(self, max_day_risk: float, allow_exits: bool) -> bool:
        daily_pnl = float(self.compute_unrealized_pnl_options())
        self.log("DAILY_PNL", daily_unreal_pnl=round(float(daily_pnl), 2), max_day_risk=round(float(max_day_risk), 2))

        if daily_pnl < 0 and abs(daily_pnl) >= float(max_day_risk):
            self.log("DAILY_LOSS_BREACH", daily_unreal_pnl=round(float(daily_pnl), 2), max_day_risk=round(float(max_day_risk), 2), action="LIQUIDATE")

            self.cancel_all_option_orders()
            self.close_all_option_positions_market(allow_exits=allow_exits)

            self.log("DAILY_LOSS_KILL_DONE")
            return False

        return True

    # -------------------------
    # ConId resolve (FIXED reqContractDetails call)
    # -------------------------
    def create_conid_from_fields(
        self,
        symbol: str,
        expiration_date,
        strike: float,
        call_put: str,
        multiplier: str = "100",
    ) -> int | None:
        try:
            if hasattr(expiration_date, "strftime"):
                exp = expiration_date.strftime("%Y%m%d")
            else:
                s = str(expiration_date).strip()
                if "-" in s and len(s) >= 10:
                    exp = s.replace("-", "")[:8]
                else:
                    exp = s[:8]

            right = str(call_put).upper()
            if right not in {"C", "P"}:
                self.log("CONID_RESOLVE_BAD_RIGHT", symbol=symbol, call_put=call_put)
                return None

            c = Contract(
                symbol=str(symbol).upper(),
                secType="OPT",
                exchange="SMART",
                currency="USD",
                lastTradeDateOrContractMonth=exp,
                strike=float(strike),
                right=right,
                multiplier=str(multiplier),
            )

            # ✅ FIX: ib_insync uses reqContractDetails(contract) (no self.ib.)
            cds = self.ib.reqContractDetails(c)
            if not cds:
                self.log("CONID_RESOLVE_FAIL", symbol=symbol, exp=exp, strike=strike, right=right)
                return None

            conid = int(cds[0].contract.conId)
            self.log("CONID_RESOLVED", symbol=symbol, exp=exp, strike=strike, right=right, conid=conid)
            return conid

        except Exception as e:
            self.log("CONID_RESOLVE_ERR", symbol=symbol, strike=strike, call_put=call_put, err=str(e))
            return None

    # -------------------------
    # Entry-only order filter (minimal fix)
    # -------------------------
    def _is_entry_order_trade(self, t) -> bool:
        try:
            o = getattr(t, "order", None)
            if o is None:
                return False
            if getattr(o, "action", None) != "BUY":
                return False
            # Only exclude TRAIL (protective). STOP/STP shouldn’t ever be BUY in your system.
            if getattr(o, "orderType", None) == "TRAIL":
                return False
            return True
        except Exception:
            return False

    # -------------------------
    # Debug snapshots (unchanged)
    # -------------------------
    def _print_positions_snapshot(self, where: str):
        try:
            pos = self.get_positions()
        except Exception as e:
            self.log("POS_SNAPSHOT_ERR", where=where, err=str(e))
            return

        opt_pos = []
        for p in pos:
            try:
                if p.contract and p.contract.secType == "OPT" and float(p.position) != 0:
                    opt_pos.append(p)
            except Exception:
                continue

        self.log("POS_SNAPSHOT", where=where, opt_positions=len(opt_pos))

        if not opt_pos:
            return

        for p in opt_pos:
            try:
                conid = int(p.contract.conId)
                qty = float(p.position)
                avgCost = getattr(p, "avgCost", None)
                sym = getattr(p.contract, "symbol", None)
                self.log("POS", where=where, symbol=sym, conid=conid, qty=qty, avgCost=avgCost)
            except Exception as e:
                self.log("POS_ERR", where=where, err=str(e))

    def _print_open_orders_snapshot(self, where: str):
        working_status = {"Submitted", "PreSubmitted", "ApiPending"}
        try:
            trades = list(self.ib.trades())
        except Exception as e:
            self.log("ORD_SNAPSHOT_ERR", where=where, err=str(e))
            return

        working = []
        for t in trades:
            try:
                st = getattr(getattr(t, "orderStatus", None), "status", None)
                if st in working_status:
                    working.append(t)
            except Exception:
                continue

        self.log("ORD_SNAPSHOT", where=where, working=len(working))

        for t in working:
            try:
                o = getattr(t, "order", None)
                c = getattr(t, "contract", None)
                self.log(
                    "ORD",
                    where=where,
                    orderId=getattr(o, "orderId", None),
                    status=getattr(getattr(t, "orderStatus", None), "status", None),
                    action=getattr(o, "action", None),
                    orderType=getattr(o, "orderType", None),
                    qty=getattr(o, "totalQuantity", None),
                    conid=getattr(c, "conId", None),
                    symbol=getattr(c, "symbol", None),
                )
            except Exception as e:
                self.log("ORD_ERR", where=where, err=str(e))

    # -------------------------
    # Main loop (same structure, extra logs only)
    # -------------------------
    def run(self, symbol: str):
        self.log("RUN_START", symbol=symbol, client_id=self.client_id, port=self.port)

        today = datetime.now(self.NY_TZ).date()
        is_tf_week, tf_date = is_in_third_friday_week(today)
        self.log("GATE_CAL", symbol=symbol, today=today, is_third_friday_week=is_tf_week, third_friday=tf_date)

        if is_tf_week:
            self.log("SKIP_THIRD_FRIDAY_WEEK", symbol=symbol, today=today, third_friday=tf_date)
            return

        now = datetime.now(self.NY_TZ)
        is_open = bool(self.XNYS.is_open_on_minute(now, ignore_breaks=True))
        self.log("GATE_MARKET_HOURS", symbol=symbol, now=str(now), is_open=is_open)

        if not is_open:
            self.log("SKIP_MARKET_CLOSED", symbol=symbol, now=str(now))
            return

        self.connect()
        self.log("CONNECTED", symbol=symbol, isConnected=self.ib.isConnected())

        allow_orders = bool(self.execute_trades_default)
        allow_exits = bool(allow_orders or self.allow_exits_when_killed)
        allow_entries = bool(allow_orders)
        self.log("GATE_ALLOW_FLAGS", symbol=symbol, allow_orders=allow_orders, allow_entries=allow_entries, allow_exits=allow_exits)

        self._print_positions_snapshot(where="run_start")
        self._print_open_orders_snapshot(where="run_start")

        acct = {}
        for r in self.ib.accountSummary():
            try:
                v = r.value
                if v is None or v == "":
                    continue
                acct[r.tag] = float(str(v).replace(",", ""))
            except Exception:
                continue

        buying_power = acct.get("BuyingPower") or acct.get("AvailableFunds") or 0.0
        max_trade_risk = float(buying_power) * self.risk.per_trade_risk_pct
        max_day_risk = float(buying_power) * self.risk.per_day_risk_pct

        self.log(
            "BUDGETS",
            symbol=symbol,
            buying_power=buying_power,
            per_trade_risk_pct=self.risk.per_trade_risk_pct,
            per_day_risk_pct=self.risk.per_day_risk_pct,
            max_trade_risk=max_trade_risk,
            max_day_risk=max_day_risk,
        )

        open_trades = [
            t for t in self.ib.trades()
            if getattr(getattr(t, "orderStatus", None), "status", None) in {"Submitted", "PreSubmitted", "ApiPending"}
        ]
        open_entry_trades = [t for t in open_trades if self._is_entry_order_trade(t)]
        self.log("GATE_OPEN_ENTRY_TRADES", symbol=symbol, open_entry_trades=len(open_entry_trades), max_open_orders=self.risk.max_open_orders)

        if len(open_entry_trades) >= self.risk.max_open_orders:
            allow_entries = False
            self.log("GATE_MAX_OPEN_ORDERS_HIT", symbol=symbol, open_entry_trades=len(open_entry_trades), max_open_orders=self.risk.max_open_orders)

        now_utc = datetime.now(timezone.utc)
        min_age_triggered = False
        min_age_seconds = int(self.risk.min_order_age_seconds)

        for t in open_entry_trades:
            try:
                oid = t.order.orderId
            except Exception:
                continue
            ts = self.order_submit_time.get(oid)
            if ts:
                age_s = (now_utc - ts).total_seconds()
                if age_s <= min_age_seconds:
                    allow_entries = False
                    min_age_triggered = True
                    self.log("GATE_MIN_ORDER_AGE_HIT", symbol=symbol, orderId=oid, age_s=int(age_s), min_age_s=min_age_seconds)
                    break

        self.log("GATE_MIN_ORDER_AGE", symbol=symbol, triggered=min_age_triggered, min_age_s=min_age_seconds)

        self.log("DAILY_LOSS_CHECK", symbol=symbol, max_day_risk=max_day_risk, allow_exits=allow_exits)
        allow_entries_before = bool(allow_entries)
        allow_entries = bool(allow_entries) and self.enforce_daily_loss_forced_liquidation(
            max_day_risk=max_day_risk,
            allow_exits=allow_exits
        )
        self.log("DAILY_LOSS_RESULT", symbol=symbol, allow_entries_before=allow_entries_before, allow_entries_after=allow_entries)

        df = self.load_recent_signal_rows(symbol)
        self.log("DB_ROWS_LOADED", symbol=symbol, rows=int(len(df)))

        signal_cols = [
            "atm_call_signal", "atm_put_signal",
            "otm1_call_signal", "otm1_put_signal",
            "otm2_call_signal", "otm2_put_signal",
        ]

        seen_conids: set[int] = set()

        for row in df.itertuples():
            row_ts = getattr(row, "timestamp", None)
            fired = []
            for c in signal_cols:
                try:
                    if bool(getattr(row, c, False)):
                        fired.append(c)
                except Exception:
                    pass

            if not fired:
                continue

            self.log("SIGNAL_ROW", symbol=symbol, row_ts=row_ts, fired=",".join(fired))

            conid = self.create_conid_from_fields(
                symbol=row.symbol,
                expiration_date=row.expiration_date,
                strike=row.strike,
                call_put=row.call_put,
            )
            if conid is None:
                self.log("SIGNAL_SKIP_NO_CONID", symbol=symbol, row_ts=row_ts)
                continue

            if conid in seen_conids:
                self.log("SIGNAL_SKIP_DUP_CONID", symbol=symbol, conid=conid, row_ts=row_ts)
                continue
            seen_conids.add(conid)

            if self._already_long_conid(conid):
                self.log("ENTRY_SKIP_ALREADY_LONG", symbol=symbol, conid=conid)
                continue

            contract = self.opt_contract_from_conid(conid)

            qty = int(self.risk.entry_qty)
            est_cost = self._row_est_cost(row, qty=qty)
            if est_cost is None:
                self.log("ENTRY_SKIP_NO_EST_COST", symbol=symbol, conid=conid, qty=qty, row_ts=row_ts)
                continue

            passed_risk = bool(est_cost <= max_trade_risk)

            self.log(
                "ENTRY_EVAL",
                symbol=symbol,
                conid=conid,
                qty=qty,
                est_cost=round(float(est_cost), 2),
                max_trade_risk=round(float(max_trade_risk), 2),
                passed_risk=passed_risk,
                allow_entries=allow_entries,
            )

            if not allow_entries:
                self.log("ENTRY_SKIP_ALLOW_ENTRIES_FALSE", symbol=symbol, conid=conid)
                continue
            if not passed_risk:
                self.log("ENTRY_SKIP_RISK_TOO_HIGH", symbol=symbol, conid=conid, est_cost=round(float(est_cost), 2), max_trade_risk=round(float(max_trade_risk), 2))
                continue

            self.log("ORDER_BUY_PLACING", symbol=symbol, conid=conid, qty=qty)
            tr = self.place_market(contract, "BUY", qty, allow_orders=allow_entries)
            if tr is not None:
                try:
                    oid = tr.order.orderId
                except Exception:
                    oid = None
                self.log("ORDER_BUY_PLACED", symbol=symbol, conid=conid, qty=qty, orderId=oid)

                self._print_positions_snapshot(where="after_buy")
                self._print_open_orders_snapshot(where="after_buy")

                if not self._has_working_trailing_sell(conid):
                    self.log("ORDER_TRAIL_PLACING", symbol=symbol, conid=conid, qty=qty, trail_pct=float(self.risk.trail_pct * 100.0))
                    tr2 = self.place_trailing_stop_pct_sell(
                        contract=contract,
                        qty=qty,
                        trailing_pct=self.risk.trail_pct * 100.0,
                        allow_orders=allow_exits,
                    )
                    if tr2 is not None:
                        try:
                            oid2 = tr2.order.orderId
                        except Exception:
                            oid2 = None
                        self.log("ORDER_TRAIL_PLACED", symbol=symbol, conid=conid, qty=qty, orderId=oid2)

                        self._print_positions_snapshot(where="after_trail")
                        self._print_open_orders_snapshot(where="after_trail")
                else:
                    self.log("ORDER_TRAIL_SKIP_EXISTS", symbol=symbol, conid=conid)

        for p in self.get_positions():
            if p.contract.secType != "OPT" or p.position <= 0:
                continue

            conid = int(p.contract.conId)
            qty = int(abs(p.position))

            mark = self.get_mark_price_snapshot(p.contract)
            if mark is None:
                self.log("PM_SKIP_NO_MARK", symbol=symbol, conid=conid, qty=qty)
                continue

            ret = self.position_return_pct(p, mark)
            if ret is None:
                self.log("PM_SKIP_NO_RET", symbol=symbol, conid=conid, qty=qty, mark=mark)
                continue

            self.log("PM_STATE", symbol=symbol, conid=conid, qty=qty, mark=round(float(mark), 4), ret_pct=round(float(ret), 2))

            if ret >= 25 and not self._has_working_stop_sell(conid):
                entry = self._entry_from_position(p)
                if entry and entry > 0:
                    self.log("ORDER_BE_STOP_PLACING", symbol=symbol, conid=conid, qty=qty, stop_price=round(float(entry), 4), ret_pct=round(float(ret), 2))
                    tr3 = self.place_stop_close_sell(p.contract, qty, float(entry), allow_orders=allow_exits)
                    if tr3 is not None:
                        try:
                            oid3 = tr3.order.orderId
                        except Exception:
                            oid3 = None
                        self.log("ORDER_BE_STOP_PLACED", symbol=symbol, conid=conid, qty=qty, orderId=oid3)

                        self._print_positions_snapshot(where="after_be_stop")
                        self._print_open_orders_snapshot(where="after_be_stop")
            elif ret >= 25:
                self.log("ORDER_BE_STOP_SKIP_EXISTS", symbol=symbol, conid=conid, ret_pct=round(float(ret), 2))

            if ret >= 50 and qty >= 2:
                has_working_sell_mkt = any(
                    (t.order.action == "SELL" and t.order.orderType == "MKT")
                    for t in self._open_orders_for_conid(conid)
                    if getattr(t, "order", None) is not None
                )
                if not has_working_sell_mkt:
                    self.log("ORDER_TP1_PLACING", symbol=symbol, conid=conid, sell_qty=1, ret_pct=round(float(ret), 2))
                    tr4 = self.place_market(p.contract, "SELL", 1, allow_orders=allow_exits)
                    if tr4 is not None:
                        try:
                            oid4 = tr4.order.orderId
                        except Exception:
                            oid4 = None
                        self.log("ORDER_TP1_PLACED", symbol=symbol, conid=conid, sell_qty=1, orderId=oid4)

                        self._print_positions_snapshot(where="after_tp1")
                        self._print_open_orders_snapshot(where="after_tp1")
                else:
                    self.log("ORDER_TP1_SKIP_WORKING_SELL_MKT", symbol=symbol, conid=conid, ret_pct=round(float(ret), 2))

            if not self._has_working_trailing_sell(conid):
                self.log("ORDER_TRAIL_ENSURE_PLACING", symbol=symbol, conid=conid, qty=qty, trail_pct=float(self.risk.trail_pct * 100.0))
                tr5 = self.place_trailing_stop_pct_sell(
                    contract=p.contract,
                    qty=qty,
                    trailing_pct=self.risk.trail_pct * 100.0,
                    allow_orders=allow_exits,
                )
                if tr5 is not None:
                    try:
                        oid5 = tr5.order.orderId
                    except Exception:
                        oid5 = None
                    self.log("ORDER_TRAIL_ENSURE_PLACED", symbol=symbol, conid=conid, qty=qty, orderId=oid5)

                    self._print_positions_snapshot(where="after_trail_ensure")
                    self._print_open_orders_snapshot(where="after_trail_ensure")
            else:
                self.log("ORDER_TRAIL_ENSURE_SKIP_EXISTS", symbol=symbol, conid=conid)

        self.log("RUN_END", symbol=symbol)


def open_connection(client_id: int) -> IBKRExecutionEngine:
    eng = IBKRExecutionEngine(client_id=client_id)
    eng.connect()
    return eng


def main_execution(client_id: int, symbols):
    eng = open_connection(client_id)
    try:
        symbols = [str(s).upper() for s in symbols if s]
        for i, symbol in enumerate(symbols, start=1):
            try:
                print(f"[EXEC] ({i}/{len(symbols)}) {symbol}", flush=True)
                eng.run(symbol)
            except Exception as e:
                print(f"[EXEC] skip {symbol}: {e}", flush=True)
            finally:
                time.sleep(0.15)
    finally:
        eng.disconnect()
