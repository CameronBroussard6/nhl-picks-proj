from __future__ import annotations
import pandas as pd

def load_odds_csv(path: str) -> pd.DataFrame:
    """Generic CSV odds loader.
    Expected columns per market (examples, extend as needed):
      - player_id, market, price, line
        where market in ['SOG', 'PTS1', 'PTS2', 'FGS']
        price is decimal (e.g., 2.10), line for SOG like 2.5, 3.5
    """
    df = pd.read_csv(path)
    # minimal normalization
    if 'player_id' not in df or 'market' not in df or 'price' not in df:
        raise ValueError("CSV must have at least columns: player_id, market, price")
    return df

def best_price(odds: pd.DataFrame) -> pd.DataFrame:
    """Collapse multiple books to a single best (highest) price per player/market/line."""
    key = ['player_id','market']
    if 'line' in odds.columns:
        key.append('line')
    idx = odds.groupby(key)['price'].idxmax()
    return odds.loc[idx].reset_index(drop=True)
