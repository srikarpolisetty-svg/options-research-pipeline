from execution_functions import get_all_symbols
import duckdb
from analysis import run_option_signals

con = duckdb.connect("options_data.db")


symbols = get_all_symbols(con)


for symbol in symbols:
    run_option_signals(symbol)




con.close()