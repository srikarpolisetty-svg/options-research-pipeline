from __future__ import annotations


UNSUPPORTED_OPTION_CHAIN_SYMBOLS = {
    "NVR",
}


def normalize_equity_symbol(symbol: str) -> str:
    return str(symbol).strip().upper()


def filter_supported_option_chain_symbols(symbols: list[str]) -> list[str]:
    filtered_symbols: list[str] = []
    seen: set[str] = set()

    for symbol in symbols:
        if not symbol or not isinstance(symbol, str):
            continue

        normalized = normalize_equity_symbol(symbol)
        if not normalized or normalized in seen:
            continue
        if normalized in UNSUPPORTED_OPTION_CHAIN_SYMBOLS:
            continue

        seen.add(normalized)
        filtered_symbols.append(normalized)

    return filtered_symbols


def databento_symbol_key(symbol: str) -> str:
    return normalize_equity_symbol(symbol).replace("-", "").replace(".", "")


def databento_parent_symbol(symbol: str) -> str:
    return f"{databento_symbol_key(symbol)}.OPT"
