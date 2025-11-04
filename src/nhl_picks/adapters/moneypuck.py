"""MoneyPuck adapter (xG, goalie GSAx, etc.).
Scraping is discouraged; use published CSVs when allowed, or mirror to your storage.
"""
from __future__ import annotations
import pandas as pd

def fetch_goalie_metrics(date_str: str) -> pd.DataFrame:
    """Return columns: team, starter_name, gsax60, sv"""
    raise NotImplementedError("Wire MoneyPuck goalie dataset here.")
