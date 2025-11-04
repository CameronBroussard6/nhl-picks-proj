from __future__ import annotations
import pandas as pd
import numpy as np
from datetime import date

# ----- Public interfaces -----

class DataBundle:
    """Container for all raw tables used by the model for a given slate."""
    def __init__(self, players, teams, lines, goalies, team_rates, player_rates):
        self.players = players        # df: player_id, name, team, pos, handed, role, is_pp1
        self.teams = teams            # df: team, conf, div
        self.lines = lines            # df: team, line, player_id, pp_unit
        self.goalies = goalies        # df: team, starter_name, gsax60, sv
        self.team_rates = team_rates  # df: team, ev_cf60, ev_sog_for60, ev_sog_against60, ev_gf60, ev_xga60, pk_sog_against60, pk_xga60
        self.player_rates = player_rates  # df: player_id, team, pos, ev_minutes, pp_minutes, ev_sog60, pp_sog60, ev_g60, pp_g60, a1_60, a2_60

# ----- Mock adapters (runnable offline) -----

def mock_bundle(games_date: str | None = None) -> DataBundle:
    rng = np.random.default_rng(0)
    teams = pd.DataFrame({
        "team": ["BOS","NYI","COL","TBL","BUF","UTA"],
        "conf": ["E","E","W","E","E","W"],
        "div":  ["A","A","C","A","A","C"],
    })
    players = []
    lines = []
    player_rates = []
    for t in teams.team:
        # 9 forwards + 3 D simplified
        for i in range(1,13):
            pid = f"{t}-{i:02d}"
            pos = "F" if i<=9 else "D"
            role = "top6" if i<=6 else ("bottom6" if i<=9 else "top4D")
            is_pp1 = 1 if (i in [1,2,3,10]) else 0
            players.append({
                "player_id": pid, "name": f"P{pid}", "team": t, "pos": pos, "handed": "R", "role": role, "is_pp1": is_pp1
            })
            lines.append({"team": t, "line": "L1" if i in [1,2,3] else ("L2" if i in [4,5,6] else "D"), "player_id": pid, "pp_unit": "PP1" if is_pp1 else "none"})
            ev_minutes = rng.uniform(250, 900)
            pp_minutes = rng.uniform(0, 200) if is_pp1 else rng.uniform(0, 80)
            ev_sog60 = rng.normal(7.0 if pos=="F" else 4.0, 1.0)
            pp_sog60 = rng.normal(10.0 if is_pp1 else 4.0, 2.0)
            ev_g60   = rng.normal(1.0 if pos=="F" else 0.4, 0.3)
            pp_g60   = rng.normal(1.8 if is_pp1 else 0.6, 0.5)
            a1_60    = rng.normal(0.9 if pos=="F" else 0.5, 0.2)
            a2_60    = rng.normal(0.5 if pos=="F" else 0.3, 0.15)
            player_rates.append({
                "player_id": pid, "team": t, "pos": pos,
                "ev_minutes": max(30, ev_minutes), "pp_minutes": max(0, pp_minutes),
                "ev_sog60": max(0.1, ev_sog60), "pp_sog60": max(0.1, pp_sog60),
                "ev_g60": max(0.01, ev_g60), "pp_g60": max(0.01, pp_g60),
                "a1_60": max(0.0, a1_60), "a2_60": max(0.0, a2_60)
            })
    goalies = pd.DataFrame({
        "team": teams.team.values,
        "starter_name": [f"G_{t}" for t in teams.team.values],
        "gsax60": [0.2, -0.1, 0.1, 0.05, -0.2, 0.0],
        "sv": [0.915,0.902,0.918,0.910,0.900,0.905]
    })
    team_rates = pd.DataFrame({
        "team": teams.team.values,
        "ev_cf60": [60, 52, 63, 58, 55, 50],
        "ev_sog_for60": [32,28,35,31,30,27],
        "ev_sog_against60": [28,31,30,29,33,34],
        "ev_gf60": [3.2,2.7,3.5,3.0,3.0,2.8],
        "ev_xga60": [2.6,2.9,2.5,2.7,3.1,3.2],
        "pk_sog_against60": [90,105,85,95,110,108],
        "pk_xga60": [7.0,7.8,6.8,7.2,8.5,8.0]
    })
    players = pd.DataFrame(players)
    lines = pd.DataFrame(lines)
    player_rates = pd.DataFrame(player_rates)
    return DataBundle(players, teams, lines, goalies, team_rates, player_rates)

def fetch_bundle(use_mock: bool = True, games_date: str | None = None) -> DataBundle:
    if use_mock:
        return mock_bundle(games_date)

    from .adapters.nhl_api import fetch_daily
    bundle = fetch_daily(games_date)
    if bundle is None:
        raise RuntimeError(f"No NHL games found for {games_date}")
    return DataBundle(
        players=bundle["players"],
        teams=bundle["teams"],
        lines=bundle["lines"],
        goalies=bundle["goalies"],
        team_rates=bundle["team_rates"],
        player_rates=bundle["player_rates"],
    )

