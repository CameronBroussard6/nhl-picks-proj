from __future__ import annotations
from typing import Dict, List, Tuple
from datetime import datetime

import pandas as pd

from ..net import get_json

BASE = "https://statsapi.web.nhl.com/api/v1"

def _season_code(date_iso: str) -> str:
    d = datetime.fromisoformat(date_iso)
    if d.month < 7:
        return f"{d.year-1}{d.year}"   # e.g., 20242025
    return f"{d.year}{d.year+1}"

def fetch_team_maps(season_code: str) -> Tuple[Dict[str,int], pd.DataFrame]:
    """
    Returns:
      abbr_to_id: {'BOS': 6, ...}
      team_stats_df: ['team','ev_sog_for60','ev_sog_against60','ev_gf60','ev_xga60','pk_sog_against60','pk_xga60']
    """
    js = get_json(f"{BASE}/teams", params={"expand":"team.stats","season":season_code})
    abbr_to_id: Dict[str,int] = {}
    rows = []
    for t in js.get("teams", []):
        abbr = t.get("abbreviation") or t["name"][:3].upper()
        abbr_to_id[abbr] = t["id"]
        # team stats
        spl = (t.get("teamStats") or [{}])[0].get("splits") or [{}]
        stat = spl[0].get("stat") or {}
        shots_for  = float(stat.get("shotsPerGame", 30.0))
        shots_against = float(stat.get("shotsAllowedPerGame", 30.0))
        goals_for  = float(stat.get("goalsPerGame", 3.0))
        goals_against = float(stat.get("goalsAgainstPerGame", 3.0))
        rows.append({
            "team": abbr,
            "ev_cf60": 55.0,
            "ev_sog_for60": shots_for,
            "ev_sog_against60": shots_against,
            "ev_gf60": goals_for,
            "ev_xga60": goals_against,
            "pk_sog_against60": shots_against * 3.0,
            "pk_xga60": goals_against * 2.4,
        })
    return abbr_to_id, pd.DataFrame(rows)

def fetch_roster_for_team(team_id: int, season_code: str) -> pd.DataFrame:
    js = get_json(f"{BASE}/teams/{team_id}/roster", params={"season": season_code})
    rows = []
    for r in js.get("roster", []):
        typ = r["position"]["type"]
        if typ not in ("Forward", "Defenseman"):
            continue
        rows.append({
            "player_id": str(r["person"]["id"]),
            "name": r["person"]["fullName"],
            "pos": "F" if typ == "Forward" else "D",
        })
    return pd.DataFrame(rows)

def fetch_player_stats(player_id: str, season_code: str, last_n: int = 7) -> Dict[str,float]:
    # Season totals
    js = get_json(f"{BASE}/people/{player_id}/stats", params={"stats":"statsSingleSeason","season":season_code})
    splits = (js.get("stats") or [{}])[0].get("splits") or []
    gp = sog_pg = g_pg = pts_pg = 0.0
    if splits:
        stat = splits[0]["stat"]
        gp = float(stat.get("games", 0))
        sog_pg = float(stat.get("shots", 0))/max(1.0, gp)
        g_pg   = float(stat.get("goals", 0))/max(1.0, gp)
        pts_pg = float(stat.get("points",0))/max(1.0, gp)

    # Recent logs
    js2 = get_json(f"{BASE}/people/{player_id}/stats", params={"stats":"gameLog","season":season_code})
    gl = (js2.get("stats") or [{}])[0].get("splits") or []
    gl = gl[:last_n]
    if gl:
        sog_recent = sum(int(x["stat"].get("shots",0)) for x in gl)/len(gl)
        g_recent   = sum(int(x["stat"].get("goals",0)) for x in gl)/len(gl)
        pts_recent = sum(int(x["stat"].get("goals",0))+int(x["stat"].get("assists",0)) for x in gl)/len(gl)
    else:
        sog_recent = g_recent = pts_recent = 0.0

    return dict(sog_pg=sog_pg, g_pg=g_pg, pts_pg=pts_pg,
                sog_recent=sog_recent, g_recent=g_recent, pts_recent=pts_recent)

def build_bundle_for_slate(date_iso: str, slate_teams: List[str], last_n: int, w_recent: float):
    season_code = _season_code(date_iso)
    abbr_to_id, team_rates = fetch_team_maps(season_code)

    # filter to teams on slate
    team_rates = team_rates[team_rates["team"].isin(slate_teams)].reset_index(drop=True)

    players_rows, lines_rows, pr_rows = [], [], []

    TOI_EV = {"F": 17.5, "D": 21.0}
    for abbr in slate_teams:
        tid = abbr_to_id.get(abbr)
        if tid is None:
            continue
        ros = fetch_roster_for_team(tid, season_code)
        for _, p in ros.iterrows():
            stats = fetch_player_stats(p["player_id"], season_code, last_n=last_n)
            sog_pg = w_recent*stats["sog_recent"] + (1-w_recent)*stats["sog_pg"]
            g_pg   = w_recent*stats["g_recent"]   + (1-w_recent)*stats["g_pg"]
            pts_pg = w_recent*stats["pts_recent"] + (1-w_recent)*stats["pts_pg"]

            toi = TOI_EV[p["pos"]]
            per60 = 60.0/max(1e-6, toi)

            players_rows.append({
                "player_id": p["player_id"],
                "name": p["name"],
                "team": abbr,
                "pos": p["pos"],
                "is_pp1": False,
            })
            lines_rows.append({"team": abbr, "line":"NA", "player_id": p["player_id"], "pp_unit":"none"})
            pr_rows.append({
                "player_id": p["player_id"], "team": abbr, "pos": p["pos"],
                "ev_minutes": 600, "pp_minutes": 60,
                "ev_sog60": sog_pg*per60, "pp_sog60": sog_pg*per60,
                "ev_g60": g_pg*per60,     "pp_g60": g_pg*per60,
                "a1_60": max(0.0, (pts_pg - g_pg)*0.6)*per60,
                "a2_60": max(0.0, (pts_pg - g_pg)*0.4)*per60,
            })

    players = pd.DataFrame(players_rows)
    lines   = pd.DataFrame(lines_rows)
    player_rates = pd.DataFrame(pr_rows)

    # goalies placeholder (neutral)
    goalies = pd.DataFrame({"team": slate_teams})
    goalies["starter_name"] = ""
    goalies["gsax60"] = 0.0
    goalies["sv"] = 0.905

    return players, lines, player_rates, team_rates, goalies
