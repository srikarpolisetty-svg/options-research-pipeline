import datetime as dt

import duckdb


RAW_SYMBOLS_DB_PATH = "rawsymbols.db"
OPTIONS_DB_PATH = "options_data.db"
MAX_FALLBACK_DAYS_TO_CHECK = 8


def query_stats(con, table_name, value_column, parent_symbol, side, grouping, time_decay_bucket, since_dt):
    row = con.execute(f"""
        SELECT
            COUNT({value_column}) AS sample_count,
            AVG({value_column}) AS mean_value,
            STDDEV_SAMP({value_column}) AS std_value
        FROM {table_name}
        WHERE parent_symbol = ?
          AND side = ?
          AND grouping = ?
          AND time_decay_bucket = ?
          AND timestamp >= ?
    """, [
        parent_symbol,
        side,
        grouping,
        time_decay_bucket,
        since_dt,
    ]).fetchone()

    return {
        "count": row[0],
        "mean": row[1],
        "std": row[2],
    }


def query_stats_for_day(
    con,
    table_name,
    value_column,
    parent_symbol,
    side,
    grouping,
    time_decay_bucket,
    target_date,
):
    row = con.execute(f"""
        SELECT
            COUNT({value_column}) AS sample_count,
            AVG({value_column}) AS mean_value,
            STDDEV_SAMP({value_column}) AS std_value
        FROM {table_name}
        WHERE parent_symbol = ?
          AND side = ?
          AND grouping = ?
          AND time_decay_bucket = ?
          AND CAST(timestamp AS DATE) = ?
    """, [
        parent_symbol,
        side,
        grouping,
        time_decay_bucket,
        target_date,
    ]).fetchone()

    return {
        "count": row[0],
        "mean": row[1],
        "std": row[2],
        "source": f"last_full_same_decay:{target_date}",
    }


def has_usable_stats(stats):
    return (
        stats["count"] >= 2
        and stats["mean"] is not None
        and stats["std"] is not None
        and stats["std"] > 0
    )


def query_recent_stats_with_same_decay_fallback(
    con,
    table_name,
    value_column,
    parent_symbol,
    side,
    grouping,
    time_decay_bucket,
    since_dt,
    today_date,
):
    recent_stats = query_stats(
        con,
        table_name,
        value_column,
        parent_symbol,
        side,
        grouping,
        time_decay_bucket,
        since_dt,
    )
    recent_stats["source"] = "recent_3d_same_decay"
    if has_usable_stats(recent_stats):
        return recent_stats

    fallback_days = con.execute(f"""
        SELECT CAST(timestamp AS DATE) AS sample_date
        FROM {table_name}
        WHERE parent_symbol = ?
          AND side = ?
          AND grouping = ?
          AND time_decay_bucket = ?
          AND CAST(timestamp AS DATE) < ?
        GROUP BY sample_date
        ORDER BY sample_date DESC
        LIMIT {MAX_FALLBACK_DAYS_TO_CHECK}
    """, [
        parent_symbol,
        side,
        grouping,
        time_decay_bucket,
        today_date,
    ]).fetchall()

    for (fallback_day,) in fallback_days:
        fallback_stats = query_stats_for_day(
            con,
            table_name,
            value_column,
            parent_symbol,
            side,
            grouping,
            time_decay_bucket,
            fallback_day,
        )
        if has_usable_stats(fallback_stats):
            return fallback_stats

    return recent_stats


def load_historical_baselines():
    raw_con = duckdb.connect(RAW_SYMBOLS_DB_PATH, read_only=True)
    hist_con = duckdb.connect(OPTIONS_DB_PATH, read_only=True)
    now_dt = dt.datetime.now(dt.timezone.utc)
    today_date = now_dt.date()
    precomputed_stats = {}

    try:
        metadata_rows = raw_con.execute("""
            SELECT DISTINCT
                parent_symbol,
                side,
                grouping,
                decay_bucket
            FROM raw_symbols
            ORDER BY parent_symbol, side, grouping, decay_bucket
        """).fetchall()

        for parent_symbol, side, grouping, decay_bucket in metadata_rows:
            mid_35d = query_stats(
                hist_con,
                "option_snapshots_raw",
                "mid",
                parent_symbol,
                side,
                grouping,
                decay_bucket,
                now_dt - dt.timedelta(days=35),
            )
            mid_3d = query_recent_stats_with_same_decay_fallback(
                hist_con,
                "option_snapshots_raw",
                "mid",
                parent_symbol,
                side,
                grouping,
                decay_bucket,
                now_dt - dt.timedelta(days=3),
                today_date,
            )
            iv_35d = query_stats(
                hist_con,
                "option_snapshots_raw",
                "iv",
                parent_symbol,
                side,
                grouping,
                decay_bucket,
                now_dt - dt.timedelta(days=35),
            )
            iv_3d = query_recent_stats_with_same_decay_fallback(
                hist_con,
                "option_snapshots_raw",
                "iv",
                parent_symbol,
                side,
                grouping,
                decay_bucket,
                now_dt - dt.timedelta(days=3),
                today_date,
            )
            vol_35d = query_stats(
                hist_con,
                "rolling_volume_history",
                "rolling_volume_10m",
                parent_symbol,
                side,
                grouping,
                decay_bucket,
                now_dt - dt.timedelta(days=35),
            )
            vol_3d = query_recent_stats_with_same_decay_fallback(
                hist_con,
                "rolling_volume_history",
                "rolling_volume_10m",
                parent_symbol,
                side,
                grouping,
                decay_bucket,
                now_dt - dt.timedelta(days=3),
                today_date,
            )

            precomputed_stats[(parent_symbol, side, grouping, decay_bucket)] = {
                "mean_mid_35d": mid_35d["mean"],
                "std_mid_35d": mid_35d["std"],
                "mean_mid_3d": mid_3d["mean"],
                "std_mid_3d": mid_3d["std"],
                "source_mid_3d": mid_3d.get("source"),
                "mean_iv_35d": iv_35d["mean"],
                "std_iv_35d": iv_35d["std"],
                "mean_iv_3d": iv_3d["mean"],
                "std_iv_3d": iv_3d["std"],
                "source_iv_3d": iv_3d.get("source"),
                "mean_vol_35d": vol_35d["mean"],
                "std_vol_35d": vol_35d["std"],
                "mean_vol_3d": vol_3d["mean"],
                "std_vol_3d": vol_3d["std"],
                "source_vol_3d": vol_3d.get("source"),
            }
    finally:
        hist_con.close()
        raw_con.close()

    return precomputed_stats
