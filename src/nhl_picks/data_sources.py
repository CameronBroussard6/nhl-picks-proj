from __future__ import annotations
from dataclasses import dataclass
import pandas as pd

from .adapters.slate_espn import fetch_slate
from .adapters.moneypuck import load_money_puck, build_player_rates, build_team_rates, build_players_table

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
    slate = fetch_slate(games_date)                      # ESPN slate/opponents
    skaters, teams = load_money_puck(games_date)         # MoneyPuck season folder for this date

    player_rates = build_player_rates(skaters, last_n=last_n, w_recent=w_recent)
    team_rates   = build_team_rates(teams)

    # Filter to slate teams only
    teams_df = slate["teams_df"]
    opp_map  = slate["opp_map"]
    team_set = set(teams_df["team"])
    player_rates = player_rates[player_rates["team"].isin(team_set)].reset_index(drop=True)
    team_rates   = team_rates[team_rates["team"].isin(team_set)].reset_index(drop=True)

    players = build_players_table(player_rates)
    lines = players[["team", "player_id"]].copy()
    lines["line"] = "NA"
    lines["pp_unit"] = "none"

    goalies = teams_df.copy()
    goalies.columns = ["team"]
    goalies["starter_name"] = ""
    goalies["gsax60"] = 0.0
    goalies["sv"] = 0.905

    return DataBundle(
        players=players,
        teams=teams_df,
        lines=lines,
        goalies=goalies,
        team_rates=team_rates,
        player_rates=player_rates,
        opp_map=opp_map,
    )
