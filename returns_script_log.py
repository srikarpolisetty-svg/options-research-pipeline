from returns_script_functions import fill_return_label
from returns_script_functions import fill_return_label_executionsignals
from policy.expiration import is_in_third_friday_week

import datetime
import sys
from zoneinfo import ZoneInfo
import exchange_calendars as ecals


NY_TZ = ZoneInfo("America/New_York")
XNYS = ecals.get_calendar("XNYS")  # NYSE

now_ny = datetime.datetime.now(NY_TZ)

# -------------------------
# Skip the entire 3rd-Friday week (monthly expiration week)
# -------------------------
is_tf_week, tf_date = is_in_third_friday_week(now_ny.date())
if is_tf_week:
    print(
        f"[SKIP] Third-Friday week detected. third_friday={tf_date} "
        f"today={now_ny.date()} (NY). Exiting.",
        flush=True
    )
    sys.exit(0)

# True only if the exchange is actually open right now (includes holidays/early closes)
if not XNYS.is_open_on_minute(now_ny, ignore_breaks=True):
    print(f"Market closed (holiday/after-hours) — skipping insert. now={now_ny}")
    sys.exit(0)


# db 2 (single database now)
fill_return_label(
    "opt_ret_10m",
    "f.timestamp >= base.timestamp + INTERVAL 10 MINUTE"
)

fill_return_label(
    "opt_ret_1h",
    "f.timestamp >= base.timestamp + INTERVAL 1 HOUR"
)

fill_return_label(
    "opt_ret_eod",
    "DATE(f.timestamp) = DATE(base.timestamp)",
    "AND DATE(base.timestamp) < CURRENT_DATE",
    order_dir="DESC"
)

fill_return_label(
    "opt_ret_next_open",
    "DATE(f.timestamp) > DATE(base.timestamp)"
)

fill_return_label(
    "opt_ret_1d",
    "f.timestamp >= base.timestamp + INTERVAL 1 DAY"
)

fill_return_label(
    "opt_ret_exp",
    "DATE(f.timestamp) = base.expiration_date",
    "AND base.expiration_date <= CURRENT_DATE",
    order_dir="DESC"
)


# db 3 (execution signals table in same database)
fill_return_label_executionsignals(
    "opt_ret_10m",
    "f.timestamp >= base.timestamp + INTERVAL 10 MINUTE"
)

fill_return_label_executionsignals(
    "opt_ret_1h",
    "f.timestamp >= base.timestamp + INTERVAL 1 HOUR"
)

fill_return_label_executionsignals(
    "opt_ret_eod",
    "DATE(f.timestamp) = DATE(base.timestamp)",
    "AND DATE(base.timestamp) < CURRENT_DATE",
    order_dir="DESC"
)

fill_return_label_executionsignals(
    "opt_ret_next_open",
    "DATE(f.timestamp) > DATE(base.timestamp)"
)

fill_return_label_executionsignals(
    "opt_ret_next_open",
    "DATE(f.timestamp) > DATE(base.timestamp)"
)

fill_return_label_executionsignals(
    "opt_ret_1d",
    "f.timestamp >= base.timestamp + INTERVAL 1 DAY"
)

fill_return_label_executionsignals(
    "opt_ret_exp",
    "DATE(f.timestamp) = base.expiration_date",
    "AND base.expiration_date <= CURRENT_DATE",
    order_dir="DESC"
)

now = datetime.datetime.now()
print(now.strftime("%Y-%m-%d %H:%M"))
