from __future__ import annotations

STRIKE_TARGET_MULTIPLIERS: dict[str, float] = {
    "ATM": 1.000,
    "C1": 1.015,
    "P1": 0.985,
    "C2": 1.035,
    "P2": 0.965,
}


def closest_strike(target: float, strikes: list[float]) -> float:
    return float(min(strikes, key=lambda s: abs(float(s) - float(target))))


def build_strike_map(underlying_price: float, strikes: list[float]) -> dict[str, float]:
    atm = float(underlying_price) * STRIKE_TARGET_MULTIPLIERS["ATM"]
    return {
        "ATM": closest_strike(atm, strikes),
        "C1": closest_strike(float(underlying_price) * STRIKE_TARGET_MULTIPLIERS["C1"], strikes),
        "P1": closest_strike(float(underlying_price) * STRIKE_TARGET_MULTIPLIERS["P1"], strikes),
        "C2": closest_strike(float(underlying_price) * STRIKE_TARGET_MULTIPLIERS["C2"], strikes),
        "P2": closest_strike(float(underlying_price) * STRIKE_TARGET_MULTIPLIERS["P2"], strikes),
    }
