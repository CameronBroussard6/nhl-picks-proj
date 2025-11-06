from __future__ import annotations
from typing import Dict, List, Tuple
from datetime import datetime

import pandas as pd

from ..net import get_json

BASE = "https://statsapi.web.nhl.com/api/v1"


def _season_code(date_iso: str) -> str:
    """
    NHL Stats API season code is like '20242025'.
    Season rolls over on July 1.
    """
    d = datetime.fromisoformat(date_iso)
    if d.month < 7:
        return f"{d.year-1}{d.year}"
    return f"{d.year}{d.year+1}"


def fetch_team_maps(season_code: str) -> Tuple[Dict[str, int], pd.DataFrame]:
    """
    Returns:
      abbr_to_id: {'BOS': 6, ...}
      team_stats_df: ['team','ev_sog_for60','ev_sog_against60','ev_gf60','ev_xga60',
                      'pk_sog_against60','pk_xga60','ev_cf60']
    """
    js = get_json(
        f"{BASE}/teams",
        params={"expand": "team.stats", "season": season_code},
        allow_proxy=False,  # <— IMPORTANT: no proxy fallback for NHL Stats
    )

    abbr_to_id: Dict[str, int] = {}
    rows: List[dict] = []

    for t in js.get("teams", []):
        abbr = (t.get("abbreviation") or t.get("name", "")[:3]).upper()
        if not abbr:
            continue
        abbr_to_id[abbr] = t["id"]

        # Team-level stats (per-game) from the first split, if present
        spl = (t.get("teamStats") or [{}])[0].get("splits") or [{}]
        stat = spl[0].get("stat") or {}
        shots_for = float(stat.get("shotsPerGame", 30.0))
        shots_against = float(stat.get("shotsAllowedPerGame", 30.0))
        goals_for = float(stat.get("goalsPerGame", 3.0))
        goals_against = float(stat.get("goalsAgainstPerGame", 3.0))

        rows.append({
            "team": abbr,
            "ev_cf60": 55.0,  # neutral baseline; refine later if desired
            "ev_sog_for60": shots_for,
            "ev_sog_against60": shots_against,
            "ev_gf60": goals_for,
            "ev_xga60": goals_against,
            "pk_sog_against60": shots_against * 3.0,
            "pk_xga60": goals_against * 2.4,
        })

    return abbr_to_id, pd.DataFrame(rows)


def fetch_roster_for_team(team_id: int, season_code: str) -> pd.DataFrame:
    """
    Returns a DataFrame with columns: ['player_id','name','pos'] for skaters only (F/D).
    """
    js = get_json(
        f"{BASE}/teams/{team_id}/roster",
        params={"season": season_code},
        allow_proxy=False,
    )
    rows: List[dict] = []
    for r in js.get("roster", []):
        pos_type = r.get("position", {}).get("type", "")
        if pos_type not in ("Forward", "Defenseman"):
            continue
        rows.append({
            "player_id": str(r["person"]["id"]),
            "name": r["person"]["fullName"],
            "pos": "F" if pos_type == "Forward" else "D",
        })
    return pd.DataFrame(rows)


def fetch_player_stats(player_id: str, season_code: str, last_n: int = 7) -> Dict[str, float]:
    """
    Returns per-game season + recent (last_n) averages:
      {'sog_pg','g_pg','pts_pg','sog_recent','g_recent','pts_recent'}
    """
    # Season totals
    js = get_json(
        f"{BASE}/people/{player_id}/stats",
        params={"stats": "statsSingleSeason", "season": season_code},
        allow_proxy=False,
    )
    splits = (js.get("stats") or [{}])[0].get("splits") or []
    gp = sog_pg = g_pg = pts_pg = 0.0
    if splits:
        stat = splits[0].get("stat", {})
        gp = float(stat.get("games", 0) or 0)
        shots = float(stat.get("shots", 0) or 0)
        goals = float(stat.get("goals", 0) or 0)
        assists = float(stat.get("assists", 0) or 0)
        sog_pg = shots / max(1.0, gp)
        g_pg = goals / max(1.0, gp)
        pts_pg = (goals + assists) / max(1.0, gp)

    # Recent game logs
    js2 = get_json(
        f"{BASE}/people/{player_id}/stats",
        params={"stats": "gameLog", "season": season_code},
        allow_proxy=False,
    )
    gl = (js2.get("stats") or [{}])[0].get("splits") or []
    gl = gl[:last_n]
    if gl:
        sog_recent = sum(int(x.get("stat", {}).get("shots", 0) or 0) for x in gl) / len(gl)
        g_recent = sum(int(x.get("stat", {}).get("goals", 0) or 0) for x in gl) / len(gl)
        pts_recent = sum(
            (int(x.get("stat", {}).get("goals", 0) or 0) + int(x.get("stat", {}).get("assists", 0) or 0))
            for x in gl
        ) / len(gl)
    else:
        sog_recent = g_recent = pts_recent = 0.0

    return {
        "sog_pg": sog_pg,
        "g_pg": g_pg,
        "pts_pg": pts_pg,
        "sog_recent": sog_recent,
        "g_recent": g_recent,
        "pts_recent": pts_recent,
    }


def build_bundle_for_slate(date_iso: str, slate_teams: List[str], last_n: int, w_recent: float):
    """
    Build players, lines, per-60 player rates, team rates, and neutral goalies for the slate.

    Returns:
      players (player_id, name, team, pos, is_pp1)
      lines   (team, line, player_id, pp_unit)
      player_rates (ev/pp sog60, g60, a1_60, a2_60, minutes)
      team_rates   (team-level rates)
      goalies      (neutral placeholders)
    """
    season_code = _season_code(date_iso)
    abbr_to_id, team_rates = fetch_team_maps(season_code)

    # keep only slate teams
    team_rates = team_rates[team_rates["team"].isin(slate_teams)].reset_index(drop=True)

    players_rows: List[dict] = []
    lines_rows: List[dict] = []
    pr_rows: List[dict] = []

    # simple EV TOI assumptions (can refine later)
    TOI_EV = {"F": 17.5, "D": 21.0}

    for abbr in slate_teams:
        tid = abbr_to_id.get(abbr)
        if tid is None:
            continue

        roster = fetch_roster_for_team(tid, season_code)
        if roster.empty:
            continue

        for _, p in roster.iterrows():
            stats = fetch_player_stats(p["player_id"], season_code, last_n=last_n)

            sog_pg = w_recent * stats["sog_recent"] + (1 - w_recent) * stats["sog_pg"]
            g_pg   = w_recent * stats["g_recent"]   + (1 - w_recent) * stats["g_pg"]
            pts_pg = w_recent * stats["pts_recent"] + (1 - w_recent) * stats["pts_pg"]

            toi = TOI_EV[p["pos"]]
            per60 = 60.0 / max(1e-6, toi)

            players_rows.append({
                "player_id": p["player_id"],
                "name": p["name"],
                "team": abbr,
                "pos": p["pos"],
                "is_pp1": False,  # placeholder
            })
            lines_rows.append({
                "team": abbr,
                "line": "NA",
                "player_id": p["player_id"],
                "pp_unit": "none",
            })
            pr_rows.append({
                "player_id": p["player_id"],
                "team": abbr,
                "pos": p["pos"],
                "ev_minutes": 600,
                "pp_minutes": 60,
                "ev_sog60": sog_pg * per60,
                "pp_sog60": sog_pg * per60,
                "ev_g60": g_pg * per60,
                "pp_g60": g_pg * per60,
                "a1_60": max(0.0, (pts_pg - g_pg) * 0.6) * per60,
                "a2_60": max(0.0, (pts_pg - g_pg) * 0.4) * per60,
            })

    players = pd.DataFrame(players_rows)
    lines = pd.DataFrame(lines_rows)
    player_rates = pd.DataFrame(pr_rows)

    # neutral goalie placeholders
    goalies = pd.DataFrame({"team": slate_teams})
    goalies["starter_name"] = ""
    goalies["gsax60"] = 0.0
    goalies["sv"] = 0.905

    return players, lines, player_rates, team_rates, goalies
