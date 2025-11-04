from __future__ import annotations
from typing import Tuple, List
import io
from datetime import datetime

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "nhl-picks/1.0 (+https://github.com)"})
    retry = Retry(total=6, backoff_factor=0.6,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=frozenset(["GET"]))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

def _season_folder(date_iso: str) -> str:
    """
    MoneyPuck folders are like '2024-2025'.
    Season changes on July 1 (roughly). For dates Jan–Jun -> prevYear-thisYear.
    """
    d = datetime.fromisoformat(date_iso)
    if d.month < 7:   # Jan–Jun
        return f"{d.year-1}-{d.year}"
    return f"{d.year}-{d.year+1}"

def _try_urls(urls: List[str]) -> pd.DataFrame:
    s = _session()
    last_err = None
    for u in urls:
        try:
            r = s.get(u, timeout=30)
            r.raise_for_status()
            # Some MP endpoints return CSV directly; keep bytes robustly
            return pd.read_csv(io.BytesIO(r.content))
        except Exception as e:
            last_err = e
            continue
    raise last_err or RuntimeError("No MoneyPuck URL succeeded")

def load_money_puck(date_iso: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      skaters (season summary), teams (season summary)
    Tries a couple of known MoneyPuck paths for current season folder.
    """
    folder = _season_folder(date_iso)

    player_urls = [
        f"https://moneypuck.com/moneypuck/playerData/seasonSummary/{folder}/skaters.csv",
        # historical alt names (keep as fallback in case MP changes)
        f"https://moneypuck.com/moneypuck/playerData/seasonSummary/{folder}/skatersSummary.csv",
    ]
    team_urls = [
        f"https://moneypuck.com/moneypuck/teamData/seasonSummary/{folder}/teams.csv",
        f"https://moneypuck.com/moneypuck/teamData/seasonSummary/{folder}/teamSummary.csv",
    ]

    skaters = _try_urls(player_urls)
    teams   = _try_urls(team_urls)

    # Normalize team abbreviations
    if "team" in skaters.columns:
        skaters["team"] = skaters["team"].astype(str).str.upper()
    if "team" in teams.columns:
        teams["team"] = teams["team"].astype(str).str.upper()

    return skaters, teams

def build_player_rates(skaters: pd.DataFrame, last_n: int, w_recent: float) -> pd.DataFrame:
    """
    Construct per-60 rates compatible with our pipeline from MoneyPuck season stats.
    If MP exposes a recent metric, blend with w_recent; else season only.
    """
    df = skaters.copy()

    # Flexible column grabs (names vary slightly year-to-year)
    shots   = df.filter(regex=r"(?i)^shots(?!.*against)").iloc[:, 0]
    goals   = df.filter(regex=r"(?i)^goals(?!.*against)").iloc[:, 0]
    assists = df.filter(regex=r"(?i)^assists").iloc[:, 0]
    gp      = df.filter(regex=r"(?i)games").iloc[:, 0]

    sog_pg = shots.divide(gp.clip(lower=1)).astype(float)
    g_pg   = goals.divide(gp.clip(lower=1)).astype(float)
    pts_pg = (goals + assists).divide(gp.clip(lower=1)).astype(float)

    # Optional “recent” columns
    sog_recent = df.filter(regex=r"(?i)(last|rolling|recent).*shots").iloc[:,0] if not df.filter(regex=r"(?i)(last|rolling|recent).*shots").empty else sog_pg
    g_recent   = df.filter(regex=r"(?i)(last|rolling|recent).*goals").iloc[:,0] if not df.filter(regex=r"(?i)(last|rolling|recent).*goals").empty else g_pg
    pts_recent = df.filter(regex=r"(?i)(last|rolling|recent).*points").iloc[:,0] if not df.filter(regex=r"(?i)(last|rolling|recent).*points").empty else pts_pg

    sog_pg_blend = w_recent * sog_recent + (1 - w_recent) * sog_pg
    g_pg_blend   = w_recent * g_recent   + (1 - w_recent) * g_pg
    pts_pg_blend = w_recent * pts_recent + (1 - w_recent) * pts_pg

    def per60(pg, pos):
        toi = 17.5 if pos == "F" else 21.0
        return pg * (60.0 / toi)

    pos = df.get("position", "F").astype(str).str[0].str.upper().where(lambda s: s.isin(["F","D"]), "F")

    out = pd.DataFrame({
        "player_id": (df["playerId"] if "playerId" in df.columns else df["playerid"]).astype(str),
        "team": df["team"],
        "pos": pos,
        "ev_minutes": 600, "pp_minutes": 60,
        "ev_sog60": [per60(x, p) for x,p in zip(sog_pg_blend, pos)],
        "pp_sog60": [per60(x, p) for x,p in zip(sog_pg_blend, pos)],
        "ev_g60":   [per60(x, p) for x,p in zip(g_pg_blend,   pos)],
        "pp_g60":   [per60(x, p) for x,p in zip(g_pg_blend,   pos)],
        "a1_60":    [per60(max(y - z, 0.0)*0.6, p) for y,z,p in zip(pts_pg_blend, g_pg_blend, pos)],
        "a2_60":    [per60(max(y - z, 0.0)*0.4, p) for y,z,p in zip(pts_pg_blend, g_pg_blend, pos)],
        "name": (df["player"] if "player" in df.columns else df["name"]).astype(str),
    })

    return out

def build_team_rates(teams: pd.DataFrame) -> pd.DataFrame:
    sa = teams.filter(regex=r"(?i)shots.*against.*per.*game").iloc[:,0] if not teams.filter(regex=r"(?i)shots.*against.*per.*game").empty else teams.get("shotsAgainstPerGame", 30.0)
    ga = teams.filter(regex=r"(?i)goals.*against.*per.*game").iloc[:,0]  if not teams.filter(regex=r"(?i)goals.*against.*per.*game").empty  else teams.get("goalsAgainstPerGame", 3.0)
    sf = teams.filter(regex=r"(?i)shots.*per.*game$").iloc[:,0]          if not teams.filter(regex=r"(?i)shots.*per.*game$").empty          else teams.get("shotsPerGame", 30.0)
    gf = teams.filter(regex=r"(?i)goals.*per.*game$").iloc[:,0]          if not teams.filter(regex=r"(?i)goals.*per.*game$").empty          else teams.get("goalsPerGame", 3.0)

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
