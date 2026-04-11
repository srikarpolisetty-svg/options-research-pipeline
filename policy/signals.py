from __future__ import annotations

SIGNAL_THRESHOLDS: dict[str, dict[str, dict[str, float]]] = {
    "ATM": {
        "5w": {"price": 1.5, "volume": 1.5, "iv": 0.5},
        "3d": {"price": 1.5, "volume": 1.5, "iv": 0.5},
    },
    "OTM_1": {
        "5w": {"price": 1.5, "volume": 1.5, "iv": 0.5},
        "3d": {"price": 1.5, "volume": 1.5, "iv": 0.5},
    },
    "OTM_2": {
        "5w": {"price": 1.5, "volume": 1.5, "iv": 0.5},
        "3d": {"price": 1.5, "volume": 1.5, "iv": 0.5},
    },
}
