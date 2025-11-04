from __future__ import annotations
import pandas as pd
import numpy as np

def join_book_odds(df: pd.DataFrame, market: str) -> pd.DataFrame:
    # Placeholder: user can wire real odds fetchers; keep columns if present
    # Expect columns like: odds_over_2_5, odds_1p, odds_2p, odds_fgs
    return df.copy()

def edge_vs_book(prob: float, price_decimal: float) -> float:
    # positive if our prob > implied book prob
    if price_decimal <= 1.0:
        return 0.0
    book_prob = 1.0 / price_decimal
    return prob - book_prob
