from __future__ import annotations
from dataclasses import dataclass
import pandas as pd

from .adapters.nhl_web import build_bundle

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
    players, lines, player_rates, team_rates, goalies, teams_df, opp_map = build_bundle(
        games_date, last_n=last_n, w_recent=w_recent
    )
    return DataBundle(
        players=players,
        teams=teams_df,
        lines=lines,
        goalies=goalies,
        team_rates=team_rates,
        player_rates=player_rates,
        opp_map=opp_map,
    )
