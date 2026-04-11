from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterable

LOOKAHEAD_DAYS_DEFAULT = 4
EXCLUDE_THIRD_FRIDAY_DEFAULT = True


def is_third_friday(d: date) -> bool:
    return d.weekday() == 4 and 15 <= d.day <= 21


def third_friday_of_month(d: date) -> date:
    first = d.replace(day=1)
    days_to_friday = (4 - first.weekday()) % 7
    first_friday = first + timedelta(days=days_to_friday)
    return first_friday + timedelta(days=14)


def is_in_third_friday_week(d: date) -> tuple[bool, date]:
    tf = third_friday_of_month(d)
    week_start = tf - timedelta(days=tf.weekday())
    week_end = week_start + timedelta(days=6)
    return week_start <= d <= week_end, tf


def is_eligible_friday_expiration(
    expiration: date,
    now_date: date,
    *,
    lookahead_days: int = LOOKAHEAD_DAYS_DEFAULT,
    exclude_third_friday: bool = EXCLUDE_THIRD_FRIDAY_DEFAULT,
) -> bool:
    if expiration.weekday() != 4:
        return False

    days_out = (expiration - now_date).days
    if days_out < 0 or days_out > lookahead_days:
        return False

    if exclude_third_friday and is_third_friday(expiration):
        return False

    return True


def find_first_eligible_friday(
    expirations: Iterable[str],
    now_date: date,
    *,
    lookahead_days: int = LOOKAHEAD_DAYS_DEFAULT,
    exclude_third_friday: bool = EXCLUDE_THIRD_FRIDAY_DEFAULT,
    date_format: str = "%Y%m%d",
) -> str | None:
    for exp in sorted(expirations):
        d = datetime.strptime(exp, date_format).date()
        if is_eligible_friday_expiration(
            d,
            now_date,
            lookahead_days=lookahead_days,
            exclude_third_friday=exclude_third_friday,
        ):
            return exp
    return None


def has_any_eligible_weekly_friday(
    expirations: Iterable[str],
    *,
    exclude_third_friday: bool = EXCLUDE_THIRD_FRIDAY_DEFAULT,
    date_format: str = "%Y%m%d",
) -> bool:
    for exp in expirations:
        d = datetime.strptime(exp, date_format).date()
        if d.weekday() != 4:
            continue
        if exclude_third_friday and is_third_friday(d):
            continue
        return True
    return False
