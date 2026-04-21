from execution_functions import get_all_symbols
import duckdb
from IBKR_execution import main_execution

DB_PATH = "/home/ubuntu/supreme-options-bot-prekafka/options_data.db"
CLIENT_ID = 9001   # pick a fixed, unused client id

con = duckdb.connect(DB_PATH, read_only=True)

symbols = get_all_symbols(con)

con.close()

# Run execution ONCE with all symbols
main_execution(
    client_id=CLIENT_ID,
    symbols=symbols,
)
