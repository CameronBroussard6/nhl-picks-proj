"""Daily Faceoff adapter for lines/PP and projected starters.
Keep within their ToS; consider manual CSV exports or a partner API.
"""
from __future__ import annotations
import pandas as pd

def fetch_lines_and_goalies(date_str: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (lines_df, goalies_df).
    lines_df columns: team, line, player_id, pp_unit
    goalies_df columns: team, starter_name, gsax60, sv (you may join gsax later)
    """
    raise NotImplementedError("Wire Daily Faceoff (or your preferred source).")
