from __future__ import annotations
import math
from datetime import datetime
from typing import Dict, List

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://statsapi.web.nhl.com/api/v1"

# ---------- HTTP session with retries ----------
_session = None
def _session_with_retries() -> requests.Session:
    global _session
    if _session is not None:
        return _session
    s = requests.Session()
    s.headers.update({
        "User-Agent": "nhl-picks/1.0 (+https://github.com)",
        "Accept": "application/json",
    })
    retry = Retry(
        total=6,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    _session = s
    return s

def _get(path: str, **params):
    s = _session_with_retries()
    r = s.get(f"{BASE}{path}", params=params, timeout=25)
    r.raise_for_status()
    return r.json()

# ---------- helpers ----------
def _season_for_date(date_iso: str) -> str:
    dt = datetime.fromisoformat(date_iso)
    y = dt.year
    if dt.month < 7:  # Jan–Jun: season started previous year
        return f"{y-1}{y}"
    return f"{y}{y+1}"

def _abbr(team_obj) -> str:
    # NHL API doesn't always include abbreviation; fall back to first 3 letters.
    return team_obj.get("abbreviation") or team_obj["name"][:3].upper()

# ---------- fetchers ----------
def fetch_schedule(date_iso: str) -> pd.DataFrame:
    js = _get("/schedule", date=date_iso)
    games = []
    for d in js.get("dates", []):
        for g in d.get("games", []):
            a = g["teams"]["away"]["team"]
            h = g["teams"]["home"]["team"]
            games.append({
                "away_id": a["id"], "home_id": h["id"],
                "away": a["name"], "home": h["name"],
                "away_abbr": _abbr(a), "home_abbr": _abbr(h),
            })
    return pd.DataFrame(games)

def fetch_team_stats() -> pd.DataFrame:
    js = _get("/teams", expand="team.stats")
    rows = []
    for t in js["teams"]:
        stats = t.get("teamStats", [{}])[0].get("splits", [{}])[0].get("stat", {})
        rows.append({
            "team_id": t["id"],
            "team": _abbr(t),
            "shotsPerGame": float(stats.get("shotsPerGame", 30.0)),
            "shotsAllowedPerGame": float(stats.get("shotsAllowedPerGame", 30.0)),
            "goalsPerGame": float(stats.get("goalsPerGame", 3.0)),
            "goalsAgainstPerGame": float(stats.get("goalsAgainstPerGame", 3.0)),
        })
    return pd.DataFrame(rows)

def fetch_roster(team_id: int) -> pd.DataFrame:
    js = _get(f"/teams/{team_id}/roster")
    rows = []
    for p in js.get("roster", []):
        if p["position"]["type"] not in ("Forward", "Defenseman"):
            continue
        rows.append({
            "player_id": str(p["person"]["id"]),
            "name": p["person"]["fullName"],
            "pos": "F" if p["position"]["type"] == "Forward" else "D",
        })
    return pd.DataFrame(rows)

def fetch_player_rates(player_id: str, season: str, last_n: int = 7) -> dict:
    """Return season + recent per-game rates for SOG, Goals, Points."""
    js = _get(f"/people/{player_id}/stats", stats="statsSingleSeason", season=season)
    spl = js.get("stats",[{}])[0].get("splits",[])
    gp = 0; sog_pg = g_pg = pts_pg = 0.0
    if spl:
        s = spl[0]["stat"]
        gp = int(s.get("games", 0))
        sog_pg = float(s.get("shots", 0))/max(1, gp)
        g_pg   = float(s.get("goals", 0))/max(1, gp)
        pts_pg = float(s.get("points",0))/max(1, gp)

    js2 = _get(f"/people/{player_id}/stats", stats="gameLog", season=season)
    gl = js2.get("stats",[{}])[0].get("splits",[])[:last_n]
    if gl:
        sog_recent = sum(int(x["stat"].get("shots",0)) for x in gl)/len(gl)
        g_recent   = sum(int(x["stat"].get("goals",0)) for x in gl)/len(gl)
        pts_recent = sum(int(x["stat"].get("goals",0))+int(x["stat"].get("assists",0)) for x in gl)/len(gl)
    else:
        sog_recent = g_recent = pts_recent = 0.0

    return dict(
        gp=gp,
        sog_pg_season=sog_pg, g_pg_season=g_pg, pts_pg_season=pts_pg,
        sog_pg_recent=sog_recent, g_pg_recent=g_recent, pts_pg_recent=pts_recent
    )

# ---------- public: assemble daily bundle ----------
def fetch_daily(date_iso: str, *, last_n: int = 7, w_recent: float = 0.55) -> dict:
    """
    Returns dict with:
      players, teams, lines, goalies, team_rates, player_rates, opp_map
    All DataFrames are ready for the existing pipeline.
    """
    sched = fetch_schedule(date_iso)
    if sched.empty:
        raise RuntimeError(f"No NHL games found for {date_iso}")

    # Map opponents (abbr -> abbr) for the slate
    opp_map: Dict[str, str] = {}
    for _, g in sched.iterrows():
        a = g["away_abbr"]; h = g["home_abbr"]
        opp_map[a] = h
        opp_map[h] = a

    team_stats = fetch_team_stats()
    team_ids = pd.unique(pd.concat([sched["home_id"], sched["away_id"]]))
    teams_df = pd.DataFrame({"team_id": team_ids}).merge(team_stats, on="team_id", how="left")

    abbr_by_id = dict(zip(teams_df["team_id"], teams_df["team"]))

    # League averages for opponent adjustments (kept for projectors)
    # Build minimal 'team_rates' with columns used by projectors
    team_rates = pd.DataFrame({
        "team": teams_df["team"],
        "ev_cf60": 55.0,                                # placeholder pace (league-ish)
        "ev_sog_for60": teams_df["shotsPerGame"],
        "ev_sog_against60": teams_df["shotsAllowedPerGame"],
        "ev_gf60": teams_df["goalsPerGame"],
        "ev_xga60": teams_df["goalsAgainstPerGame"],    # proxy for xGA
        "pk_sog_against60": teams_df["shotsAllowedPerGame"]*3.0,  # coarse proxy
        "pk_xga60": teams_df["goalsAgainstPerGame"]*2.4,
    })

    season = _season_for_date(date_iso)

    players_rows: List[dict] = []
    lines_rows: List[dict] = []
    pr_rows: List[dict] = []

    # Convert per-game to per-60 using approximate TOI; forward/defense split
    TOI_EV = {"F": 17.5, "D": 21.0}
    for _, g in sched.iterrows():
        for tid in (g["home_id"], g["away_id"]):
            abbr = abbr_by_id.get(tid, "")
            ros = fetch_roster(int(tid))
            if ros.empty:
                continue
            for _, p in ros.iterrows():
                rates = fetch_player_rates(p["player_id"], season, last_n)
                sog_pg = w_recent*rates["sog_pg_recent"] + (1-w_recent)*rates["sog_pg_season"]
                g_pg   = w_recent*rates["g_pg_recent"]   + (1-w_recent)*rates["g_pg_season"]
                pts_pg = w_recent*rates["pts_pg_recent"] + (1-w_recent)*rates["pts_pg_season"]

                toi = TOI_EV[p["pos"]]
                per60 = 60.0 / max(1e-6, toi)

                players_rows.append({
                    "player_id": p["player_id"],
                    "name": p["name"],
                    "team": abbr,
                    "pos": p["pos"],
                    "is_pp1": False,  # unknown via NHL.com; the pipeline tolerates False
                })
                lines_rows.append({
                    "team": abbr, "line": "NA", "player_id": p["player_id"], "pp_unit": "none"
                })
                # Supply the columns expected by transforms/projectors
                pr_rows.append({
                    "player_id": p["player_id"], "team": abbr, "pos": p["pos"],
                    "ev_minutes": 600, "pp_minutes": 60,
                    "ev_sog60": sog_pg * per60, "pp_sog60": sog_pg * per60,
                    "ev_g60":   g_pg   * per60, "pp_g60":   g_pg   * per60,
                    "a1_60": max(0.0, (pts_pg - g_pg) * 0.6 * per60),
                    "a2_60": max(0.0, (pts_pg - g_pg) * 0.4 * per60),
                })

    players = pd.DataFrame(players_rows)
    lines   = pd.DataFrame(lines_rows)
    player_rates = pd.DataFrame(pr_rows)

    # shallow goalie table (no starters from NHL.com; add later if needed)
    goalies = teams_df[["team"]].copy()
    goalies["starter_name"] = ""
    goalies["gsax60"] = 0.0
    goalies["sv"] = 0.905

    return dict(
        players=players,
        teams=teams_df[["team"]],
        lines=lines,
        goalies=goalies,
        team_rates=team_rates,
        player_rates=player_rates,
        opp_map=opp_map,
    )
