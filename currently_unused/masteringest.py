from databasefunctions import master_ingest
import duckdb
import argparse
from datetime import datetime

DB_PATH = "/home/ubuntu/supreme-options-bot-prekafka/options_data.db"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()

    print(f"[MASTER] start {datetime.now()} run_id={args.run_id}", flush=True)

    with duckdb.connect(DB_PATH) as con:

        # ============================================================
        # CREATE TABLES (ALL)
        # ============================================================

        # =========================
        # RAW
        # =========================
        con.execute("""
            CREATE TABLE IF NOT EXISTS option_snapshots_raw (
                snapshot_id TEXT,
                timestamp TIMESTAMP,
                symbol TEXT,
                underlying_price DOUBLE,
                strike DOUBLE,
                call_put TEXT,
                days_to_expiry INTEGER,
                expiration_date DATE,
                moneyness_bucket TEXT,
                bid DOUBLE,
                ask DOUBLE,
                mid DOUBLE,
                volume INTEGER,
                open_interest INTEGER,
                iv DOUBLE,
                spread DOUBLE,
                spread_pct DOUBLE,
                time_decay_bucket TEXT
            );
        """)

        # =========================
        # ENRICHED
        # =========================
        con.execute("""
            CREATE TABLE IF NOT EXISTS option_snapshots_enriched (
                snapshot_id TEXT,
                timestamp TIMESTAMP,
                symbol TEXT,
                underlying_price DOUBLE,
                strike DOUBLE,
                call_put TEXT,
                days_to_expiry INTEGER,
                expiration_date DATE,
                moneyness_bucket TEXT,
                bid DOUBLE,
                ask DOUBLE,
                mid DOUBLE,
                volume INTEGER,
                open_interest INTEGER,
                iv DOUBLE,
                spread DOUBLE,
                spread_pct DOUBLE,
                time_decay_bucket TEXT,

                -- 3D z-scores
                mid_z_3d DOUBLE,
                volume_z_3d DOUBLE,
                iv_z_3d DOUBLE,

                -- 5W z-scores
                mid_z_5w DOUBLE,
                volume_z_5w DOUBLE,
                iv_z_5w DOUBLE,

                opt_ret_10m DOUBLE,
                opt_ret_1h DOUBLE,
                opt_ret_eod DOUBLE,
                opt_ret_next_open DOUBLE,
                opt_ret_1d DOUBLE,
                opt_ret_exp DOUBLE
            );
        """)

        # =========================
        # EXECUTION SIGNALS
        # =========================
        con.execute("""
            CREATE TABLE IF NOT EXISTS option_snapshots_execution_signals (
                snapshot_id TEXT,
                timestamp TIMESTAMP,
                symbol TEXT,
                underlying_price DOUBLE,
                strike DOUBLE,
                call_put TEXT,
                days_to_expiry INTEGER,
                expiration_date DATE,
                moneyness_bucket TEXT,
                bid DOUBLE,
                ask DOUBLE,
                mid DOUBLE,
                volume INTEGER,
                open_interest INTEGER,
                iv DOUBLE,
                spread DOUBLE,
                spread_pct DOUBLE,
                time_decay_bucket TEXT,

                -- 3D z-scores
                mid_z_3d DOUBLE,
                volume_z_3d DOUBLE,
                iv_z_3d DOUBLE,

                -- 5W z-scores
                mid_z_5w DOUBLE,
                volume_z_5w DOUBLE,
                iv_z_5w DOUBLE,

                opt_ret_10m DOUBLE,
                opt_ret_1h DOUBLE,
                opt_ret_eod DOUBLE,
                opt_ret_next_open DOUBLE,
                opt_ret_1d DOUBLE,
                opt_ret_exp DOUBLE,

                atm_call_signal BOOLEAN,
                atm_put_signal BOOLEAN,
                otm1_call_signal BOOLEAN,
                otm1_put_signal BOOLEAN,
                otm2_call_signal BOOLEAN,
                otm2_put_signal BOOLEAN
            );
        """)

        # ============================================================
        # INGEST
        # ============================================================
        master_ingest(run_id=args.run_id)

        # ============================================================
        # CLEANUP (ALL)
        # ============================================================

        con.execute("""
            DELETE FROM option_snapshots_raw
            WHERE timestamp < NOW() - INTERVAL '35 days';
        """)

        con.execute("""
            DELETE FROM option_snapshots_enriched
            WHERE timestamp < NOW() - INTERVAL '35 days';
        """)

        con.execute("""
            DELETE FROM option_snapshots_execution_signals
            WHERE timestamp < NOW() - INTERVAL '35 days';
        """)
