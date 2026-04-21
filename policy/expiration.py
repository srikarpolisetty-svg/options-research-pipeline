from __future__ import annotations

from functools import lru_cache
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


def _nth_weekday_of_month(year: int, month: int, weekday: int, nth: int) -> date:
    first = date(year, month, 1)
    days_until_weekday = (weekday - first.weekday()) % 7
    return first + timedelta(days=days_until_weekday + (nth - 1) * 7)


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    if month == 12:
        cursor = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        cursor = date(year, month + 1, 1) - timedelta(days=1)

    while cursor.weekday() != weekday:
        cursor -= timedelta(days=1)
    return cursor


def _observed_fixed_holiday(year: int, month: int, day: int) -> date:
    holiday = date(year, month, day)
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


@lru_cache(maxsize=None)
def _nyse_holidays(year: int) -> frozenset[date]:
    holidays = {
        _observed_fixed_holiday(year, 1, 1),
        _nth_weekday_of_month(year, 1, 0, 3),
        _nth_weekday_of_month(year, 2, 0, 3),
        _easter_sunday(year) - timedelta(days=2),
        _last_weekday_of_month(year, 5, 0),
        _observed_fixed_holiday(year, 6, 19),
        _observed_fixed_holiday(year, 7, 4),
        _nth_weekday_of_month(year, 9, 0, 1),
        _nth_weekday_of_month(year, 11, 3, 4),
        _observed_fixed_holiday(year, 12, 25),
    }

    next_new_year_observed = _observed_fixed_holiday(year + 1, 1, 1)
    if next_new_year_observed.year == year:
        holidays.add(next_new_year_observed)

    return frozenset(holidays)


def is_nyse_market_holiday(d: date) -> bool:
    return d in _nyse_holidays(d.year)


def weekly_expiration_anchor(expiration: date) -> date | None:
    if expiration.weekday() == 4:
        return expiration

    if expiration.weekday() == 3:
        friday = expiration + timedelta(days=1)
        if friday.weekday() == 4 and is_nyse_market_holiday(friday):
            return friday

    return None


def is_eligible_friday_expiration(
    expiration: date,
    now_date: date,
    *,
    lookahead_days: int = LOOKAHEAD_DAYS_DEFAULT,
    exclude_third_friday: bool = EXCLUDE_THIRD_FRIDAY_DEFAULT,
) -> bool:
    anchor = weekly_expiration_anchor(expiration)
    if anchor is None:
        return False

    days_out = (expiration - now_date).days
    if days_out < 0 or days_out > lookahead_days:
        return False

    if exclude_third_friday and is_third_friday(anchor):
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


def find_first_eligible_friday_with_reason(
    expirations: Iterable[str],
    now_date: date,
    *,
    lookahead_days: int = LOOKAHEAD_DAYS_DEFAULT,
    exclude_third_friday: bool = EXCLUDE_THIRD_FRIDAY_DEFAULT,
    date_format: str = "%Y%m%d",
) -> tuple[str | None, str | None]:
    parsed = [
        datetime.strptime(exp, date_format).date()
        for exp in sorted(expirations)
    ]
    weekly_expirations = [d for d in parsed if weekly_expiration_anchor(d) is not None]
    if not weekly_expirations:
        return None, "no weekly expirations"

    weekly_non_third = [
        d for d in weekly_expirations
        if not (exclude_third_friday and is_third_friday(weekly_expiration_anchor(d)))
    ]
    if not weekly_non_third:
        return None, "only third-Friday expirations"

    eligible = [
        d for d in weekly_non_third
        if 0 <= (d - now_date).days <= lookahead_days
    ]
    if eligible:
        return eligible[0].strftime(date_format), None

    in_window_expirations = [
        d for d in weekly_expirations
        if 0 <= (d - now_date).days <= lookahead_days
    ]
    if exclude_third_friday and in_window_expirations and all(
        is_third_friday(weekly_expiration_anchor(d)) for d in in_window_expirations
    ):
        return None, "only third-Friday within lookahead"

    future_weeklies = [d for d in weekly_non_third if d >= now_date]
    if not future_weeklies:
        return None, "no future weekly expirations"

    return None, f"no weekly expiration within {lookahead_days}d"


def has_any_eligible_weekly_friday(
    expirations: Iterable[str],
    *,
    exclude_third_friday: bool = EXCLUDE_THIRD_FRIDAY_DEFAULT,
    date_format: str = "%Y%m%d",
) -> bool:
    for exp in expirations:
        d = datetime.strptime(exp, date_format).date()
        anchor = weekly_expiration_anchor(d)
        if anchor is None:
            continue
        if exclude_third_friday and is_third_friday(anchor):
            continue
        return True
    return False
