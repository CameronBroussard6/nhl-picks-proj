from __future__ import annotations
from typing import Dict, List, Tuple
from datetime import datetime

import pandas as pd

from ..net import get_json

ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard"
NHL_WEB_BASE = "https://api-web.nhle.com/v1"


def season_code(date_iso: str) -> str:
    d = datetime.fromisoformat(date_iso)
    if d.month < 7:
        return f"{d.year-1}{d.year}"
    return f"{d.year}{d.year+1}"


def fetch_slate(date_iso: str) -> Tuple[List[str], Dict[str, str]]:
    """ESPN slate & opponents (team abbreviations)."""
    yyyymmdd = date_iso.replace("-", "")
    js = get_json(ESPN_SCOREBOARD, params={"dates": yyyymmdd}, allow_proxy=False)
    events = js.get("events", [])
    teams: List[str] = []
    opp: Dict[str, str] = {}
    for ev in events:
        comps = ev.get("competitions", [])
        if not comps:
            continue
        cteams = comps[0].get("competitors", [])
        if len(cteams) != 2:
            continue
        a, b = cteams[0], cteams[1]
        ta = a["team"]["abbreviation"].upper()
        tb = b["team"]["abbreviation"].upper()
        teams.extend([ta, tb])
        opp[ta] = tb
        opp[tb] = ta
    # de-dup/order
    teams = sorted(pd.unique(pd.Series(teams)))
    return teams, opp


def fetch_roster(team_abbr: str, season: str) -> pd.DataFrame:
    """NHL web roster: returns player_id, name, pos for skaters only."""
    js = get_json(f"{NHL_WEB_BASE}/roster/{team_abbr}/{season}", allow_proxy=False)
    rows = []
    for p in js.get("forwards", []) + js.get("defensemen", []):
        pid = str(p.get("id") or p.get("playerId"))
        name = p.get("firstName", "") + " " + p.get("lastName", "")
        pos = "F" if "forwards" in p.get("type", "forwards").lower() else "D"
        if not pid or name.strip() == "":
            # Some payloads nest differently; try common fields
            pid = pid or str(p.get("player", {}).get("id") or "")
            fullname = p.get("player", {}).get("fullName")
            if fullname:
                name = fullname
        if not pid or not name:
            continue
        rows.append({"player_id": pid, "name": name.strip(), "pos": pos})
    return pd.DataFrame(rows)


def fetch_player_recent(player_id: str, season: str, last_n: int = 7) -> Dict[str, float]:
    """
    NHL web game log: compute last_n averages for SOG/goals/points.
    If fewer than N games exist, average whatever is available.
    """
    js = get_json(
        f"{NHL_WEB_BASE}/player/{player_id}/game-log/{season}/2",
        params={"site": "en_nhl"},
        allow_proxy=False,
    )
    gl = js.get("gameLog", [])
    if not gl:
        return {"sog": 0.0, "g": 0.0, "pts": 0.0}
    gl = gl[:last_n]
    sog = sum(int(g.get("shots", 0) or 0) for g in gl) / len(gl)
    goals = sum(int(g.get("goals", 0) or 0) for g in gl) / len(gl)
    pts = sum(
        int(g.get("goals", 0) or 0) + int(g.get("assists", 0) or 0)
        for g in gl
    ) / len(gl)
    return {"sog": sog, "g": goals, "pts": pts}


def build_bundle(date_iso: str, last_n: int, w_recent: float):
    """
    Complete live bundle using ESPN (slate) + NHL web (roster + logs).
    Returns: players, lines, player_rates, team_rates, goalies, teams_df, opp_map
    """
    slate_teams, opp_map = fetch_slate(date_iso)
    season = season_code(date_iso)

    # team rates: use simple per-game proxies via roster depth (fallback); refine later by team web endpoints
    team_rows = []
    players_rows, lines_rows, pr_rows = [], [], []

    TOI_EV = {"F": 17.5, "D": 21.0}

    for team in slate_teams:
        ros = fetch_roster(team, season)
        if ros.empty:
            continue

        # crude team rates placeholders (you can replace with a team endpoint later)
        team_rows.append({
            "team": team,
            "ev_cf60": 55.0,
            "ev_sog_for60": 30.0,
            "ev_sog_against60": 30.0,
            "ev_gf60": 3.0,
            "ev_xga60": 3.0,
            "pk_sog_against60": 90.0,
            "pk_xga60": 7.2,
        })

        for _, p in ros.iterrows():
            rec = fetch_player_recent(p["player_id"], season, last_n=last_n)

            # Season-wide splits (shots/goals/points PG) aren’t exposed here; blend recency with a neutral baseline.
            sog_pg = w_recent * rec["sog"] + (1 - w_recent) * 2.3
            g_pg   = w_recent * rec["g"]   + (1 - w_recent) * 0.3
            pts_pg = w_recent * rec["pts"] + (1 - w_recent) * 0.7

            toi = TOI_EV[p["pos"]]
            per60 = 60.0 / toi

            players_rows.append({
                "player_id": p["player_id"], "name": p["name"], "team": team, "pos": p["pos"], "is_pp1": False
            })
            lines_rows.append({"team": team, "line": "NA", "player_id": p["player_id"], "pp_unit": "none"})
            pr_rows.append({
                "player_id": p["player_id"], "team": team, "pos": p["pos"],
                "ev_minutes": 600, "pp_minutes": 60,
                "ev_sog60": sog_pg * per60, "pp_sog60": sog_pg * per60,
                "ev_g60": g_pg * per60,     "pp_g60": g_pg * per60,
                "a1_60": max(0.0, (pts_pg - g_pg) * 0.6) * per60,
                "a2_60": max(0.0, (pts_pg - g_pg) * 0.4) * per60,
            })

    players = pd.DataFrame(players_rows)
    lines = pd.DataFrame(lines_rows)
    player_rates = pd.DataFrame(pr_rows)
    team_rates = pd.DataFrame(team_rows)
    teams_df = pd.DataFrame({"team": slate_teams})

    goalies = teams_df.copy()
    goalies["starter_name"] = ""
    goalies["gsax60"] = 0.0
    goalies["sv"] = 0.905

    return players, lines, player_rates, team_rates, goalies, teams_df, opp_map
