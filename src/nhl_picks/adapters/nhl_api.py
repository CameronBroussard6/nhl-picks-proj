from __future__ import annotations
import requests, math
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

BASE = "https://statsapi.web.nhl.com/api/v1"

import requests, math
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://statsapi.web.nhl.com/api/v1"

_session = None
def _session_with_retries():
    global _session
    if _session is not None:
        return _session
    s = requests.Session()
    s.headers.update({
        "User-Agent": "nhl-picks-bot/1.0 (+https://github.com)",  # polite UA
        "Accept": "application/json",
    })
    retry = Retry(
        total=6,                # up to 6 tries
        backoff_factor=0.6,     # 0.6, 1.2, 2.4, ...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    _session = s
    return s

def _get(url, **params):
    s = _session_with_retries()
    r = s.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def _season_for_date(d: str) -> str:
    # NHL season string like "20242025"
    dt = datetime.fromisoformat(d)
    yr = dt.year
    if dt.month < 7:  # Jan-Jun => season started last year
        return f"{yr-1}{yr}"
    return f"{yr}{yr+1}"

def fetch_schedule(date_str: str):
    js = _get(f"{BASE}/schedule", date=date_str)
    games = []
    for d in js.get("dates", []):
        for g in d.get("games", []):
            a = g["teams"]["away"]["team"]
            h = g["teams"]["home"]["team"]
            games.append({"away_id": a["id"], "home_id": h["id"], "away": a["name"], "home": h["name"]})
    return pd.DataFrame(games)

def fetch_teams_stats():
    js = _get(f"{BASE}/teams", expand="team.stats")
    rows = []
    for t in js["teams"]:
        tid = t["id"]; name = t["name"]; abbr = t.get("abbreviation", name[:3].upper())
        stats = t.get("teamStats", [{}])[0].get("splits", [{}])[0].get("stat", {})
        rows.append({
            "team_id": tid,
            "team": abbr,
            "shotsAllowedPerGame": float(stats.get("shotsAllowedPerGame", 30.0)),
            "goalsAgainstPerGame": float(stats.get("goalsAgainstPerGame", 3.0)),
            "shotsPerGame": float(stats.get("shotsPerGame", 30.0)),
            "goalsPerGame": float(stats.get("goalsPerGame", 3.0)),
        })
    return pd.DataFrame(rows)

def fetch_roster(team_id: int):
    js = _get(f"{BASE}/teams/{team_id}/roster")
    rows = []
    for p in js.get("roster", []):
        if p["position"]["type"] not in ("Forward","Defenseman"):
            continue
        rows.append({"player_id": p["person"]["id"], "name": p["person"]["fullName"]})
    return pd.DataFrame(rows)

def fetch_player_rates(player_id: int, season: str, last_n: int = 7):
    """Return season + recent per-game rates for SOG, Goals, Points."""
    # season totals
    js = _get(f"{BASE}/people/{player_id}/stats", stats="statsSingleSeason", season=season)
    splits = js.get("stats",[{}])[0].get("splits",[])
    sog_pg = g_pg = pts_pg = 0.0
    gp = 0
    if splits:
        s = splits[0]["stat"]
        gp = int(s.get("games", 0))
        sog_pg = float(s.get("shots",0))/max(1,gp)
        g_pg   = float(s.get("goals",0))/max(1,gp)
        pts_pg = float(s.get("points",0))/max(1,gp)
    # recent N
    js2 = _get(f"{BASE}/people/{player_id}/stats", stats="gameLog", season=season)
    gl = js2.get("stats",[{}])[0].get("splits",[])[:last_n]
    if gl:
        sog_recent = sum(int(x["stat"].get("shots",0)) for x in gl)/len(gl)
        g_recent   = sum(int(x["stat"].get("goals",0)) for x in gl)/len(gl)
        pts_recent = sum(int(x["stat"].get("goals",0))+int(x["stat"].get("assists",0)) for x in gl)/len(gl)
    else:
        sog_recent = g_recent = pts_recent = 0.0
    return {
        "gp": gp,
        "sog_pg_season": sog_pg, "g_pg_season": g_pg, "pts_pg_season": pts_pg,
        "sog_pg_recent": sog_recent, "g_pg_recent": g_recent, "pts_pg_recent": pts_recent
    }

def fetch_daily(date_str: str, last_n: int = 7, w_recent: float = 0.55):
    """Assemble a minimal bundle for projections using NHL.com only."""
    sched = fetch_schedule(date_str)
    if sched.empty:
        return None

    team_stats = fetch_teams_stats()

    teams_df = pd.DataFrame({
        "team_id": pd.unique(pd.concat([sched["home_id"], sched["away_id"]])),
    })
    teams_df = teams_df.merge(team_stats, on="team_id", how="left")
    # make simple abbr map
    teams_df["team"] = teams_df["team"]
    abbr_map = dict(zip(teams_df["team_id"], teams_df["team"]))

    # League averages for opponent adjustments
    lg_sog_allowed = teams_df["shotsAllowedPerGame"].mean()
    lg_ga = teams_df["goalsAgainstPerGame"].mean()

    players = []
    lines = []
    player_rates = []

    season = _season_for_date(date_str)

    for _, row in sched.iterrows():
        for tid in (row["home_id"], row["away_id"]):
            ros = fetch_roster(tid)
            if ros.empty:
                continue
            for _, p in ros.iterrows():
                rates = fetch_player_rates(int(p["player_id"]), season, last_n)
                # blended per-game rates
                sog_pg = w_recent*rates["sog_pg_recent"] + (1-w_recent)*rates["sog_pg_season"]
                g_pg   = w_recent*rates["g_pg_recent"]   + (1-w_recent)*rates["g_pg_season"]
                pts_pg = w_recent*rates["pts_pg_recent"] + (1-w_recent)*rates["pts_pg_season"]
                players.append({
                    "player_id": str(p["player_id"]),
                    "name": p["name"],
                    "team_id": tid,
                    "team": abbr_map.get(tid,"")
                })
                lines.append({
                    "team": abbr_map.get(tid,""),
                    "line": "NA", "player_id": str(p["player_id"]), "pp_unit": "none"
                })
                player_rates.append({
                    "player_id": str(p["player_id"]),
                    "team": abbr_map.get(tid,""),
                    "pos": "F",               # NHL API doesn't mark here; ok for now
                    "ev_minutes": 500,        # placeholders for shrinkage math (not used heavily here)
                    "pp_minutes": 60,
                    "ev_sog60": sog_pg*60/18, # approx convert pg -> per60 using ~18 TOI
                    "pp_sog60": sog_pg*60/18,
                    "ev_g60": g_pg*60/18,
                    "pp_g60": g_pg*60/18,
                    "a1_60": max(0.0, (pts_pg - g_pg)*0.6*60/18),
                    "a2_60": max(0.0, (pts_pg - g_pg)*0.4*60/18)
                })

    # goalies/opp adjustments from team stats only (placeholder GSAx=0)
    goalies = teams_df[["team"]].copy()
    goalies["starter_name"] = ""
    goalies["gsax60"] = 0.0
    goalies["sv"] = 0.905

    # team_rates: map shots allowed / xga stand-ins
    team_rates = pd.DataFrame({
        "team": teams_df["team"],
        "ev_cf60": 55,
        "ev_sog_for60": teams_df["shotsPerGame"],
        "ev_sog_against60": teams_df["shotsAllowedPerGame"],
        "ev_gf60": teams_df["goalsPerGame"],
        "ev_xga60": teams_df["goalsAgainstPerGame"],     # using GA as proxy for xGA
        "pk_sog_against60": teams_df["shotsAllowedPerGame"]*3.0, # rough proxy
        "pk_xga60": teams_df["goalsAgainstPerGame"]*2.4
    })

    # Packages for our existing pipeline
    players_df = pd.DataFrame(players)
    lines_df = pd.DataFrame(lines)
    player_rates_df = pd.DataFrame(player_rates)

    return {
        "players": players_df,
        "teams": team_rates[["team"]],
        "lines": lines_df,
        "goalies": goalies,
        "team_rates": team_rates,
        "player_rates": player_rates_df
    }
