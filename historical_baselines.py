import datetime as dt

import duckdb


RAW_SYMBOLS_DB_PATH = "rawsymbols.db"
OPTIONS_DB_PATH = "options_data.db"


def query_stats(con, table_name, value_column, parent_symbol, side, grouping, time_decay_bucket, since_dt):
    row = con.execute(f"""
        SELECT
            COUNT(*) AS sample_count,
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


def load_historical_baselines():
    raw_con = duckdb.connect(RAW_SYMBOLS_DB_PATH, read_only=True)
    hist_con = duckdb.connect(OPTIONS_DB_PATH, read_only=True)
    now_dt = dt.datetime.now(dt.timezone.utc)
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
            mid_3d = query_stats(
                hist_con,
                "option_snapshots_raw",
                "mid",
                parent_symbol,
                side,
                grouping,
                decay_bucket,
                now_dt - dt.timedelta(days=3),
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
            iv_3d = query_stats(
                hist_con,
                "option_snapshots_raw",
                "iv",
                parent_symbol,
                side,
                grouping,
                decay_bucket,
                now_dt - dt.timedelta(days=3),
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
            vol_3d = query_stats(
                hist_con,
                "rolling_volume_history",
                "rolling_volume_10m",
                parent_symbol,
                side,
                grouping,
                decay_bucket,
                now_dt - dt.timedelta(days=3),
            )

            precomputed_stats[(parent_symbol, side, grouping, decay_bucket)] = {
                "mean_mid_35d": mid_35d["mean"],
                "std_mid_35d": mid_35d["std"],
                "mean_mid_3d": mid_3d["mean"],
                "std_mid_3d": mid_3d["std"],
                "mean_iv_35d": iv_35d["mean"],
                "std_iv_35d": iv_35d["std"],
                "mean_iv_3d": iv_3d["mean"],
                "std_iv_3d": iv_3d["std"],
                "mean_vol_35d": vol_35d["mean"],
                "std_vol_35d": vol_35d["std"],
                "mean_vol_3d": vol_3d["mean"],
                "std_vol_3d": vol_3d["std"],
            }
    finally:
        hist_con.close()
        raw_con.close()

    return precomputed_stats
