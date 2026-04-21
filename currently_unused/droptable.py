import duckdb

DB_PATH = "options_data.db"

TABLES = [
    "option_snapshots_raw",
    "option_snapshots_enriched",
    "option_snapshots_execution_signals",
]

def main():
    con = duckdb.connect(DB_PATH)
    try:
        # ---- DROP TABLES ----
        for table in TABLES:
            print(f"Dropping table: {table}")
            con.execute(f"DROP TABLE IF EXISTS {table}")
        print("✅ All tables dropped successfully.")

        # ---- CREATE RAW TABLE ----
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
        print("✅ Table created: option_snapshots_raw")

    finally:
        con.close()

if __name__ == "__main__":
    main()

