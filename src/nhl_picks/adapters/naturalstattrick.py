"""Natural Stat Trick (NST) adapter.
For ToS compliance and stability, we recommend exporting CSVs you host, or using a paid data service.
This module defines expected outputs to map into the model.
"""
from __future__ import annotations
import pandas as pd

def fetch_player_rates(date_str: str) -> pd.DataFrame:
    """Return columns: player_id, team, pos, ev_minutes, pp_minutes, ev_sog60, pp_sog60, ev_g60, pp_g60, a1_60, a2_60"""
    raise NotImplementedError("Wire NST or your preferred source.")
