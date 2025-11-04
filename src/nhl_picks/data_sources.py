from __future__ import annotations
from dataclasses import dataclass
import pandas as pd

@dataclass
class DataBundle:
    players: pd.DataFrame
    teams: pd.DataFrame
    lines: pd.DataFrame
    goalies: pd.DataFrame
    team_rates: pd.DataFrame
    player_rates: pd.DataFrame
    opp_map: dict

def fetch_bundle(*, games_date: str, last_n: int = 7, w_recent: float = 0.55) -> DataBundle:
    """Live NHL.com only."""
    from .adapters.nhl_api import fetch_daily
    d = fetch_daily(games_date, last_n=last_n, w_recent=w_recent)
    return DataBundle(
        players=d["players"],
        teams=d["teams"],
        lines=d["lines"],
        goalies=d["goalies"],
        team_rates=d["team_rates"],
        player_rates=d["player_rates"],
        opp_map=d["opp_map"],
    )
