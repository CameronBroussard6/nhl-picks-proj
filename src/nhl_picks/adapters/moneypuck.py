from __future__ import annotations
from typing import Tuple
import io

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# MoneyPuck public CSVs (seasonal). These paths have been stable for years,
# but if they change, only edit these two URLs.
PLAYER_GBG = "https://moneypuck.com/moneypuck/playerData/seasonSummary/2025/skaters.csv"
TEAM_GBG   = "https://moneypuck.com/moneypuck/teamData/seasonSummary/2025/teams.csv"

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "nhl-picks/1.0 (+https://github.com)"})
    retry = Retry(total=6, backoff_factor=0.6,
                  status_forcelist=(429,500,502,503,504),
                  allowed_methods=frozenset(["GET"]))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

def _read_csv(url: str) -> pd.DataFrame:
    s = _session()
    r = s.get(url, timeout=30)
    r.raise_for_status()
    return pd.read_csv(io.BytesIO(r.content))

def load_money_puck() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      skaters: season summary with shots, goals, points per game
      teams  : team summary for allowed stats (shotsAllowedPerGame, goalsAgainstPerGame)
    """
    skaters = _read_csv(PLAYER_GBG)
    teams   = _read_csv(TEAM_GBG)

    # Normalize team abbreviations to 3-letter ESPN style when possible
    if "team" in skaters.columns:
        skaters["team"] = skaters["team"].str.upper()
    if "team" in teams.columns:
        teams["team"] = teams["team"].str.upper()

    return skaters, teams

def build_player_rates(skaters: pd.DataFrame, last_n: int, w_recent: float) -> pd.DataFrame:
    """
    Construct per-60 rates compatible with our pipeline from MoneyPuck season stats.
    MoneyPuck season summary doesn’t contain "last N" directly; we approximate by
    blending season rates with a lightweight recency bump if available columns exist.
    """
    df = skaters.copy()

    # Columns that typically exist on MoneyPuck summaries:
    # 'shots', 'goals', 'assists', 'games_played' (names can vary slightly)
    # We’ll defensively look for alternatives.
    shots = df.filter(regex="(?i)^shots").iloc[:,0]
    goals = df.filter(regex="(?i)^goals(?!Against)").iloc[:,0]
    assists = df.filter(regex="(?i)^assists").iloc[:,0]
    gp = df.filter(regex="(?i)games").iloc[:,0]

    sog_pg = shots.divide(gp.clip(lower=1)).astype(float)
    g_pg   = goals.divide(gp.clip(lower=1)).astype(float)
    pts_pg = (goals + assists).divide(gp.clip(lower=1)).astype(float)

    # If MoneyPuck exposes a last-10 or recent rolling metric in this file,
    # try to find it; otherwise keep season-only.
    recent_cols = df.filter(regex="(?i)(last|rolling|recent).*shots").columns
    if len(recent_cols) > 0:
        sog_recent = df[recent_cols[0]].astype(float)
    else:
        sog_recent = sog_pg

    recent_cols_g = df.filter(regex="(?i)(last|rolling|recent).*goals").columns
    g_recent = df[recent_cols_g[0]].astype(float) if len(recent_cols_g) else g_pg

    recent_cols_p = df.filter(regex="(?i)(last|rolling|recent).*points").columns
    pts_recent = df[recent_cols_p[0]].astype(float) if len(recent_cols_p) else pts_pg

    sog_pg_blend = w_recent * sog_recent + (1 - w_recent) * sog_pg
    g_pg_blend   = w_recent * g_recent   + (1 - w_recent) * g_pg
    pts_pg_blend = w_recent * pts_recent + (1 - w_recent) * pts_pg

    # Approximate per-60 with typical EV ice time
    def per60(pg, pos):
        toi = 17.5 if pos == "F" else 21.0
        return pg * (60.0 / toi)

    pos = df.get("position", "F").astype(str).str[0].str.upper().where(lambda s: s.isin(["F","D"]), "F")

    out = pd.DataFrame({
        "player_id": df["playerId"].astype(str) if "playerId" in df.columns else df["playerid"].astype(str),
        "team": df["team"],
        "pos": pos,
        "ev_minutes": 600, "pp_minutes": 60,
        "ev_sog60": [per60(x, p) for x,p in zip(sog_pg_blend, pos)],
        "pp_sog60": [per60(x, p) for x,p in zip(sog_pg_blend, pos)],
        "ev_g60":   [per60(x, p) for x,p in zip(g_pg_blend,   pos)],
        "pp_g60":   [per60(x, p) for x,p in zip(g_pg_blend,   pos)],
        "a1_60":    [per60(max(y - z, 0.0)*0.6, p) for y,z,p in zip(pts_pg_blend, g_pg_blend, pos)],
        "a2_60":    [per60(max(y - z, 0.0)*0.4, p) for y,z,p in zip(pts_pg_blend, g_pg_blend, pos)],
        "name": df["player"].astype(str) if "player" in df.columns else df["name"].astype(str),
    })

    return out

def build_team_rates(teams: pd.DataFrame) -> pd.DataFrame:
    # Try common MoneyPuck column names, with safe fallbacks
    sa = teams.filter(regex="(?i)shots.*against.*per.*game").iloc[:,0] if not teams.filter(regex="(?i)shots.*against.*per.*game").empty else teams.get("shotsAgainstPerGame", 30.0)
    ga = teams.filter(regex="(?i)goals.*against.*per.*game").iloc[:,0] if not teams.filter(regex="(?i)goals.*against.*per.*game").empty else teams.get("goalsAgainstPerGame", 3.0)
    sf = teams.filter(regex="(?i)shots.*per.*game$").iloc[:,0] if not teams.filter(regex="(?i)shots.*per.*game$").empty else teams.get("shotsPerGame", 30.0)
    gf = teams.filter(regex="(?i)goals.*per.*game$").iloc[:,0] if not teams.filter(regex="(?i)goals.*per.*game$").empty else teams.get("goalsPerGame", 3.0)

    out = pd.DataFrame({
        "team": teams["team"].str.upper(),
        "ev_cf60": 55.0,
        "ev_sog_for60": sf,
        "ev_sog_against60": sa,
        "ev_gf60": gf,
        "ev_xga60": ga,
        "pk_sog_against60": sa * 3.0,
        "pk_xga60": ga * 2.4,
    })
    return out

def build_players_table(player_rates: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "player_id": player_rates["player_id"],
        "name": player_rates["name"],
        "team": player_rates["team"],
        "pos": player_rates["pos"],
        "is_pp1": False,
    })
