import duckdb


con = duckdb.connect("options_data.db")



def fill_return_label(label_name, time_condition, extra_where="", order_dir=None):
    """
    label_name: column to update, e.g. 'opt_ret_10m'
    time_condition: SQL condition for selecting the future snapshot
    extra_where: optional SQL filter for the UPDATE WHERE clause
    order_dir: None (default) or 'ASC' / 'DESC'
    """

    order_clause = f"ORDER BY f.timestamp {order_dir}" if order_dir else "ORDER BY f.timestamp"

    con.execute(f"""
        UPDATE option_snapshots_enriched base
        SET {label_name} = (
            SELECT (f.mid - base.mid) / base.mid
            FROM option_snapshots_enriched f
            WHERE f.symbol            = base.symbol
              AND f.call_put          = base.call_put
              AND f.expiration_date   = base.expiration_date
              AND f.moneyness_bucket  = base.moneyness_bucket
              AND {time_condition}
            {order_clause}
            LIMIT 1
        )
        WHERE {label_name} IS NULL
        {extra_where};
    """)












def fill_return_label_executionsignals(label_name, time_condition, extra_where="", order_dir=None):
    """
    Same logic as fill_return_label, but updates option_snapshots_execution_signals.

    order_dir:
      - None (default): earliest matching snapshot (ORDER BY f.timestamp)
      - "DESC": latest matching snapshot (ORDER BY f.timestamp DESC) for payoff/EOD labels
    """
    order_clause = f"ORDER BY f.timestamp {order_dir}" if order_dir else "ORDER BY f.timestamp"

    con.execute(f"""
        UPDATE option_snapshots_execution_signals base
        SET {label_name} = (
            SELECT (f.mid - base.mid) / base.mid
            FROM option_snapshots_execution_signals f
            WHERE f.symbol            = base.symbol
              AND f.call_put          = base.call_put
              AND f.expiration_date   = base.expiration_date
              AND f.moneyness_bucket  = base.moneyness_bucket
              AND {time_condition}
            {order_clause}
            LIMIT 1
        )
        WHERE {label_name} IS NULL
        {extra_where};
    """)













