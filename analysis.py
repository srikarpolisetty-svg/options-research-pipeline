import duckdb
from message import send_text
from analysis_functions import load_all_groups, get_option_metrics, update_signal
from policy.expiration import is_in_third_friday_week
from policy.signals import SIGNAL_THRESHOLDS

import sys
from datetime import datetime
from zoneinfo import ZoneInfo
import exchange_calendars as ecals


def run_option_signals(symbol: str):
    NY_TZ = ZoneInfo("America/New_York")
    XNYS = ecals.get_calendar("XNYS")

    now = datetime.now(NY_TZ)

    # Skip the entire 3rd-Friday week (monthly expiration week)
    is_tf_week, tf_date = is_in_third_friday_week(now.date())
    if is_tf_week:
        print(
            f"[SKIP] Third-Friday week detected. third_friday={tf_date} "
            f"today={now.date()} (NY). Exiting.",
            flush=True,
        )
        sys.exit(0)

    if not XNYS.is_open_on_minute(now, ignore_breaks=True):
        print(f"Market closed (holiday/after-hours) — skipping insert. now={now}")
        sys.exit(0)

    now1 = datetime.now()
    print(f"Run time: {now1.strftime('%Y-%m-%d %H:%M')}")

    con = duckdb.connect("options_data.db")

    groups = load_all_groups(con, symbol)
    if groups is None:
        con.close()
        return f"no data {symbol}"

    def M(key: str):
        return get_option_metrics(groups, key)

    def gt(x, thr: float) -> bool:
        try:
            if x is None:
                return False
            if x != x:  # NaN
                return False
            return x > thr
        except Exception:
            return False

    def handle_bucket(bucket: str):
        thr_cfg = SIGNAL_THRESHOLDS[bucket]

        t5_price = thr_cfg["5w"]["price"]
        t5_vol   = thr_cfg["5w"]["volume"]
        t5_iv    = thr_cfg["5w"]["iv"]
        t3_price = thr_cfg["3d"]["price"]
        t3_vol   = thr_cfg["3d"]["volume"]
        t3_iv    = thr_cfg["3d"]["iv"]

        call_m = M(f"{bucket}_CALL")
        put_m  = M(f"{bucket}_PUT")

        call_signal = False
        put_signal  = False

        if call_m is not None:
            call_signal = (
                gt(call_m.get("z_price_5w"),  t5_price) and
                gt(call_m.get("z_volume_5w"), t5_vol)   and
                gt(call_m.get("z_iv_5w"),     t5_iv)    and
                gt(call_m.get("z_price_3d"),  t3_price) and
                gt(call_m.get("z_volume_3d"), t3_vol)   and
                gt(call_m.get("z_iv_3d"),     t3_iv)
            )

        if put_m is not None:
            put_signal = (
                gt(put_m.get("z_price_5w"),  t5_price) and
                gt(put_m.get("z_volume_5w"), t5_vol)   and
                gt(put_m.get("z_iv_5w"),     t5_iv)    and
                gt(put_m.get("z_price_3d"),  t3_price) and
                gt(put_m.get("z_volume_3d"), t3_vol)   and
                gt(put_m.get("z_iv_3d"),     t3_iv)
            )

        if call_signal and not put_signal and call_m is not None:
            send_text(
                f"🚀 STRONG {bucket} CALL SIGNAL\n\n"
                f"Symbol: {call_m['symbol']}\n"
                f"Strike: {call_m['strike']}\n"
                f"Option Price (mid): {call_m['price']}\n\n"
                f"Thresholds: 5w(price={t5_price},vol={t5_vol},iv={t5_iv}) "
                f"3d(price={t3_price},vol={t3_vol},iv={t3_iv})"
            )
            print(f"ALERT SENT ({bucket} CALL)")

            update_signal(
                con,
                symbol=call_m["symbol"],
                snapshot_id=call_m["snapshot_id"],
                call_put="C",
                bucket=bucket,
                signal_column=f"{bucket.lower()}_call_signal",
            )

        elif put_signal and not call_signal and put_m is not None:
            send_text(
                f"⚠️ STRONG {bucket} PUT SIGNAL\n\n"
                f"Symbol: {put_m['symbol']}\n"
                f"Strike: {put_m['strike']}\n"
                f"Option Price (mid): {put_m['price']}\n\n"
                f"Thresholds: 5w(price={t5_price},vol={t5_vol},iv={t5_iv}) "
                f"3d(price={t3_price},vol={t3_vol},iv={t3_iv})"
            )
            print(f"ALERT SENT ({bucket} PUT)")

            update_signal(
                con,
                symbol=put_m["symbol"],
                snapshot_id=put_m["snapshot_id"],
                call_put="P",
                bucket=bucket,
                signal_column=f"{bucket.lower()}_put_signal",
            )

        elif call_signal and put_signal:
            print(f"{bucket} CALL & PUT both elevated → volatility spike, no directional {bucket} signal.")
        else:
            print(
                f"No {bucket} directional signal. "
                f"Needed > 5w(price={t5_price},vol={t5_vol},iv={t5_iv}) "
                f"and > 3d(price={t3_price},vol={t3_vol},iv={t3_iv})."
            )

    handle_bucket("ATM")
    handle_bucket("OTM_1")
    handle_bucket("OTM_2")

    con.close()

