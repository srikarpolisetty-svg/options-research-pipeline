import os
import subprocess
import datetime as dt
import duckdb

DUCKDB_PATH = "live_state.duckdb"

# stale thresholds (seconds)
MAX_QUOTE_AGE   = 90
MAX_VOL_AGE     = 180
MAX_STRIKES_AGE = 25 * 60

# require N consecutive failures before restart
FAILS_REQUIRED = 3
STREAK_FILE = "/tmp/live_streamer_failstreak.txt"

# restart command (kill + relaunch)
RESTART_CMD = (
    "pkill -f 'live_ingest_latest.py' || true; "
    "nohup ./run_streamer.sh >/dev/null 2>&1 &"
)


def now_utc_naive() -> dt.datetime:
    return dt.datetime.utcnow()


def max_ts(con, table: str, col: str):
    return con.execute(f"SELECT MAX({col}) FROM {table}").fetchone()[0]


def age_seconds(ts) -> float | None:
    if ts is None:
        return None
    return (now_utc_naive() - ts).total_seconds()


def load_streak() -> int:
    try:
        return int(open(STREAK_FILE).read().strip())
    except Exception:
        return 0


def save_streak(n: int) -> None:
    with open(STREAK_FILE, "w") as f:
        f.write(str(n))


def bump_streak_and_maybe_restart(reason: str) -> int:
    streak = load_streak() + 1
    save_streak(streak)
    print(f"[health] DEAD streak={streak} reason={reason}")

    if streak >= FAILS_REQUIRED:
        print("[health] restarting streamer...")
        subprocess.call(["bash", "-lc", RESTART_CMD])
        save_streak(0)

    return 1


def main() -> int:
    # 1) DB exists?
    if not os.path.exists(DUCKDB_PATH):
        return bump_streak_and_maybe_restart("missing_db")

    # 2) Read max timestamps
    try:
        con = duckdb.connect(DUCKDB_PATH, read_only=True)
        ts_q = max_ts(con, "live_quotes_latest", "ts_event")
        ts_v = max_ts(con, "live_vol10m_latest", "ts_calc")
        ts_s = max_ts(con, "live_strikes6_latest", "ts_refresh")
        con.close()
    except Exception as e:
        return bump_streak_and_maybe_restart(f"db_error={e}")

    # 3) Convert to ages
    age_q = age_seconds(ts_q)
    age_v = age_seconds(ts_v)
    age_s = age_seconds(ts_s)

    # 4) Decide dead: quotes is the primary signal
    if age_q is None or age_q > MAX_QUOTE_AGE:
        return bump_streak_and_maybe_restart(f"quotes_age={age_q}")

    # Optional extra info (doesn't trigger restart by itself)
    extra = []
    if age_v is None or age_v > MAX_VOL_AGE:
        extra.append(f"vol_age={age_v}")
    if age_s is None or age_s > MAX_STRIKES_AGE:
        extra.append(f"strikes_age={age_s}")

    # 5) Healthy → reset streak
    save_streak(0)
    msg = f"[health] OK quotes_age={age_q:.1f}s"
    if age_v is not None:
        msg += f" vol_age={age_v:.1f}s"
    if age_s is not None:
        msg += f" strikes_age={age_s:.1f}s"
    if extra:
        msg += " WARN(" + ", ".join(extra) + ")"
    print(msg)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())