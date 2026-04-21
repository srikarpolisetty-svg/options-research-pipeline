def get_latest_snapshot(
    con,
    table: str,
    symbol: str,
    call_put: str,
    moneyness_bucket: str
):
    """
    Grab the latest snapshot row for a given table / symbol / call_put / moneyness bucket.
    Returns a 1-row DataFrame.
    """
    query = f"""
        SELECT *
        FROM {table}
        WHERE symbol = ?
          AND call_put = ?
          AND moneyness_bucket = ?
        ORDER BY snapshot_id DESC
        LIMIT 1
    """
    return con.execute(query, [symbol, call_put, moneyness_bucket]).df()


def load_all_groups(con, symbol: str, table: str = "option_snapshots_enriched"):
    buckets = ["ATM", "OTM_1", "OTM_2"]
    sides = ["C", "P"]

    data = {}

    for bucket in buckets:
        for side in sides:
            key = f"{bucket}_{'CALL' if side=='C' else 'PUT'}"

            try:
                df = get_latest_snapshot(con, table, symbol, side, bucket)
            except Exception:
                df = None

            if df is not None and df.empty:
                df = None

            data[key] = df

    return data


def get_option_metrics(groups, key: str):
    """
    groups: dict from load_all_groups()
    key: e.g. "ATM_PUT", "ATM_CALL", "OTM_1_CALL", "OTM_2_PUT", etc.
    """

    if key not in groups:
        return None

    df = groups[key]
    if df is None or df.empty:
        return None

    row = df.iloc[0]

    return {
        "z_price_3d":   row.get("mid_z_3d"),
        "z_volume_3d":  row.get("volume_z_3d"),
        "z_iv_3d":      row.get("iv_z_3d"),

        "z_price_5w":   row.get("mid_z_5w"),
        "z_volume_5w":  row.get("volume_z_5w"),
        "z_iv_5w":      row.get("iv_z_5w"),

        "strike":       row.get("strike"),
        "price":        row.get("mid"),
        "symbol":       row.get("symbol"),
        "snapshot_id":  row.get("snapshot_id"),
        "call_put":     row.get("call_put"),
        "bucket":       row.get("moneyness_bucket"),
    }


def update_signal(
    con,
    symbol: str,
    snapshot_id,
    call_put,
    bucket,
    signal_column,
    table: str = "option_snapshots_execution_signals",
):
    con.execute(f"""
        UPDATE {table}
        SET {signal_column} = TRUE
        WHERE snapshot_id = ?
          AND symbol = ?
          AND call_put = ?
          AND moneyness_bucket = ?;
    """, [snapshot_id, symbol, call_put, bucket])
