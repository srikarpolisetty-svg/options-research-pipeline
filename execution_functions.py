import requests


from dotenv import load_dotenv
import os
from policy.risk import TRAIL_PCT

load_dotenv()   # ← once, at startup

SECRET_KEY = os.getenv("SECRET_KEY")

ACCOUNT_ID = os.getenv("ACCOUNT_ID")

EXECUTE_TRADES = False   # 🔒 global kill switch



import requests

def get_access_token(secret_key, validity_minutes=120):
    url = "https://api.public.com/userapiauthservice/personal/access-tokens"

    headers = {
        "Content-Type": "application/json"
    }

    request_body = {
        "validityInMinutes": validity_minutes,
        "secret": secret_key   # ✅ use the argument
    }

    response = requests.post(url, headers=headers, json=request_body)

    if response.headers.get("Content-Type", "").startswith("application/json"):
        return response.json()
    else:
        return {
            "status_code": response.status_code,
            "text": response.text
        }






















def preflight_single_leg_option(
    account_id,
    access_token,
    symbol,
    strike,
    expiration_date,   # "YYYY-MM-DD"
    call_put,          # "C" or "P"
    quantity="1"
):
    url = f"https://api.public.com/userapigateway/trading/{account_id}/preflight/single-leg"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    request_body = {
        "instrument": {
            "symbol": symbol,
            "type": "OPTION",
            "strikePrice": str(strike),
            "expirationDate": expiration_date,
            "callPut": "CALL" if call_put.upper() == "C" else "PUT"
        },
        "orderSide": "BUY",
        "orderType": "MARKET",
        "quantity": str(quantity),
        "expiration": {
            "timeInForce": "DAY"
        },
        "equityMarketSession": "CORE",
        "openCloseIndicator": "OPEN"
    }

    response = requests.post(url, headers=headers, json=request_body)

    if not response.headers.get("Content-Type", "").startswith("application/json"):
        return {
            "status": response.status_code,
            "error": response.text
        }

    return response.json()

































import requests

import requests

EXECUTE_TRADES = False  # global safety switch



import requests

import requests
import uuid

def place_equity_order(
    account_id,
    access_token,
    symbol,
    side,          # "BUY" or "SELL"
    quantity,
    execute=True   # safety switch
):
    if not execute:
        return {
            "status": "DRY_RUN",
            "symbol": symbol,
            "side": side,
            "quantity": quantity
        }

    url = f"https://api.public.com/userapigateway/trading/{account_id}/order"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "orderId": str(uuid.uuid4()),   # REQUIRED
        "instrument": {
            "symbol": symbol,
            "type": "OPTION"
        },
        "orderSide": side,
        "orderType": "MARKET",
        "expiration": {                 # REQUIRED (per your docs)
            "timeInForce": "DAY"
        },
        "quantity": quantity,
        "openCloseIndicator": "OPEN"
    }

    response = requests.post(url, headers=headers, json=payload)
    return response.status_code, response.json()




     







# get order status



import requests

def get_order_status(
    account_id,
    access_token,
    order_id
):
    url = f"https://api.public.com/userapigateway/trading/{account_id}/order/{order_id}"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)

    if response.headers.get("Content-Type", "").startswith("application/json"):
        return response.json()
    else:
        return {
            "status_code": response.status_code,
            "text": response.text
        }











#converting into a number
def to_float(x, default=0.0):
    try:
        return float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return default
    





# get portfolio buying power 

import requests

def get_portfolio(account_id, api_key):
    url = f"https://api.public.com/userapigateway/trading/{account_id}/portfolio/v2"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()







def get_daily_unrealized_pnl_options(portfolio_data):
    daily_unrealized_pnl = 0.0

    for pos in portfolio_data.get("positions", []):
        # only option positions
        is_option = (
            pos.get("instrument", {}).get("type") == "OPTION"
            or any(
                leg.get("instrument", {}).get("type") == "OPTION"
                for leg in pos.get("legs", [])
            )
        )

        if not is_option:
            continue

        daily_gain = (pos.get("positionDailyGain") or {}).get("gainValue")
        daily_unrealized_pnl += to_float(daily_gain)

    return daily_unrealized_pnl






# get max prices for position

max_price_tracker = {}

def update_max_prices(data):
    for pos in data.get("positions", []):
        # ✅ only option positions
        if (pos.get("instrument") or {}).get("type") != "OPTION":
            continue

        symbol = (pos.get("instrument") or {}).get("symbol")
        if not symbol:
            continue

        current_price = to_float((pos.get("lastPrice") or {}).get("lastPrice"))
        if current_price is None:
            continue

        prev_max = max_price_tracker.get(symbol)

        # If we haven't stored a max yet, or the new price is higher, update it
        if prev_max is None or current_price > prev_max:
            max_price_tracker[symbol] = current_price

    return max_price_tracker




        
        













def get_instrument(
    access_token: str,
    symbol: str,
    instrument_type: str,   # "EQUITY" or "OPTION"
    timeout: int = 20
):
    """
    Fetch and validate a single instrument by symbol + type.
    Use this right before placing an order.
    """

    url = "https://api.public.com/userapigateway/trading/instruments"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    params = {
        "symbol": symbol,
        "type": instrument_type
    }

    response = requests.get(url, headers=headers, params=params, timeout=timeout)

    if not response.ok:
        return {
            "status": "error",
            "http_status": response.status_code,
            "text": response.text
        }

    data = response.json()

    # Defensive check
    instruments = data.get("instruments", [])
    if not instruments:
        return {
            "status": "not_found",
            "symbol": symbol,
            "type": instrument_type
        }

    return {
        "status": "ok",
        "instrument": instruments[0]
    }




def get_all_symbolsTester(con, table="option_snapshots_raw"):
    df = con.execute(
        f"SELECT DISTINCT symbol FROM {table}"
    ).df()
    return df["symbol"].tolist()



def get_all_symbols(con, table="option_snapshots_enriched"):
    df = con.execute(
        f"SELECT DISTINCT symbol FROM {table}"
    ).df()
    return df["symbol"].tolist()










def trail_exit_signals(data, token_response, trail_pct=TRAIL_PCT):
    """
    Trailing stop exit for OPTION positions only.
    trail_pct means: exit if price drops trail_pct from max.
    """

    update_max_prices(data)

    for pos in data.get("positions", []):
        # ✅ only option positions
        if (pos.get("instrument") or {}).get("type") != "OPTION":
            continue

        symbol = (pos.get("instrument") or {}).get("symbol")
        if not symbol:
            continue

        current_price = to_float((pos.get("lastPrice") or {}).get("lastPrice"))
        if current_price is None:
            continue

        peak = max_price_tracker.get(symbol)
        if peak is None:
            continue

        stop_level = peak * (1 - trail_pct)

        if current_price <= stop_level:
            place_close_order(
                ACCOUNT_ID,
                token_response,
                symbol=symbol,
                quantity=pos["quantity"]
            )

           




def place_close_order(
    account_id,
    token,
    symbol,
    quantity
):
    url = f"https://api.public.com/userapigateway/trading/{account_id}/order"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "orderId": str(uuid.uuid4()),
        "instrument": {
            "symbol": symbol,
            "type": "OPTION"
        },
        "orderSide": "SELL",              # BUY or SELL
        "orderType": "MARKET",         # MARKET or LIMIT
        "quantity": quantity,
        "openCloseIndicator": "CLOSE"
    }

    response = requests.post(url, headers=headers, json=payload)
    return response.status_code, response.json()





def place_stop_close_order(
    account_id,
    token,
    symbol,
    side,        # SELL to close long, BUY to close short
    quantity,
    stop_price
):
    url = f"https://api.public.com/userapigateway/trading/{account_id}/order"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "orderId": str(uuid.uuid4()),
        "instrument": {
            "symbol": symbol,
            "type": "OPTION"
        },
        "orderSide": side,
        "orderType": "STOP",
        "stopPrice": stop_price,
        "quantity": quantity,
        "openCloseIndicator": "CLOSE"
    }

    response = requests.post(url, headers=headers, json=payload)
    return response.status_code, response.json()








import uuid

def generate_order_id():
    return str(uuid.uuid4())
