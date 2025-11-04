"""Thin NHL API adapter (game schedule, teams, skater stats).

Notes:
- The public NHL Stats API is undocumented and subject to change.
- For production, add retries and caching.
- This file shows the *shape* we expect; we leave implementation commented for now.
"""
from __future__ import annotations
import pandas as pd

def fetch_daily(date_str: str) -> dict:
    """Return dict with keys: teams, players, lines, goalies, team_rates, player_rates.
    Implement using NHL endpoints or other sources you prefer.
    """
    raise NotImplementedError("Wire your NHL API calls here (schedule, skaters, teams, goalies).")
