from __future__ import annotations
from dataclasses import dataclass
import pandas as pd

from .adapters.slate_espn import fetch_slate
from .adapters.nhl_stats import build_bundle_for_slate

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
    slate = fetch_slate(games_date)              # ESPN slate & opponent map
    teams_df = slate["teams_df"]
    opp_map  = slate["opp_map"]
    slate_teams = teams_df["team"].tolist()

    players, lines, player_rates, team_rates, goalies = build_bundle_for_slate(
        games_date, slate_teams, last_n=last_n, w_recent=w_recent
    )

    # Keep only players on slate teams (defensive)
    players = players[players["team"].isin(slate_teams)].reset_index(drop=True)
    player_rates = player_rates[player_rates["team"].isin(slate_teams)].reset_index(drop=True)
    team_rates = team_rates[team_rates["team"].isin(slate_teams)].reset_index(drop=True)

    return DataBundle(
        players=players,
        teams=teams_df,
        lines=lines,
        goalies=goalies,
        team_rates=team_rates,
        player_rates=player_rates,
        opp_map=opp_map,
    )
