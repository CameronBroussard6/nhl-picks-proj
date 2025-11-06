from __future__ import annotations
from typing import Tuple, List, Optional
from datetime import datetime
import re

import pandas as pd

from ..net import read_csv_safely, get_bytes

BASE = "https://moneypuck.com/moneypuck"

# ------------------ season folder discovery ------------------

def _season_folder_from_date(date_iso: str) -> str:
    d = datetime.fromisoformat(date_iso)
    if d.month < 7:
        return f"{d.year-1}-{d.year}"
    return f"{d.year}-{d.year+1}"

def _discover_latest_folder(kind: str) -> Optional[str]:
    """
    Try to discover a valid folder under:
      /playerData/seasonSummary/ or /teamData/seasonSummary/
    Returns a folder string like '2025-2026', or None.
    """
    assert kind in ("playerData", "teamData")
    idx_url = f"{BASE}/{kind}/seasonSummary/"
    try:
        html = get_bytes(idx_url).decode("utf-8", errors="ignore")
        # Look for '/seasonSummary/####-####/skaters.csv' or '/teams.csv'
        m = re.search(r"/seasonSummary/(\d{4}-\d{4})/(?:skaters|teams)\.csv", html, re.I)
        if m:
            return m.group(1)
        # fallback: capture any seasonSummary folder name
        m2 = re.search(r"/seasonSummary/(\d{4}-\d{4})/", html, re.I)
        return m2.group(1) if m2 else None
    except Exception:
        return None

def _candidate_player_urls(folder: str) -> List[str]:
    return [
        f"{BASE}/playerData/seasonSummary/{folder}/skaters.csv",
        f"{BASE}/playerData/seasonSummary/{folder}/skatersSummary.csv",
    ]

def _candidate_team_urls(folder: str) -> List[str]:
    return [
        f"{BASE}/teamData/seasonSummary/{folder}/teams.csv",
        f"{BASE}/teamData/seasonSummary/{folder}/teamSummary.csv",
    ]

def _first_ok_csv(urls: List[str]) -> pd.DataFrame:
    last_err = None
    for u in urls:
        try:
            return read_csv_safely(u)
        except Exception as e:
            last_err = e
            continue
    if last_err:
        raise last_err
    raise RuntimeError("No MoneyPuck URL succeeded")

def load_money_puck(date_iso: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Try expected folder from date; if not there, discover from index
    folder = _season_folder_from_date(date_iso)

    def load_pair(fold: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        skaters = _first_ok_csv(_candidate_player_urls(fold))
        teams   = _first_ok_csv(_candidate_team_urls(fold))
        return skaters, teams

    try:
        skaters, teams = load_pair(folder)
    except Exception:
        # Discover a valid folder by scanning MoneyPuck listing (via proxy)
        discovered_p = _discover_latest_folder("playerData")
        discovered_t = _discover_latest_folder("teamData")
        # prefer matching folders; else fall back to whichever we found
        chosen = discovered_p or discovered_t or folder
        skaters, teams = load_pair(chosen)

    # Normalize team abbreviations (best effort)
    if "team" in skaters.columns:
        skaters["team"] = skaters["team"].astype(str).str.upper()
    if "team" in teams.columns:
        teams["team"] = teams["team"].astype(str).str.upper()

    return skaters, teams

# ------------------ flexible column getters ------------------

def _first_col(df: pd.DataFrame, patterns: List[str]) -> Optional[pd.Series]:
    for pat in patterns:
        cols = df.filter(regex=pat, axis=1)
        if cols.shape[1] > 0:
            return cols.iloc[:, 0].astype(float, errors="ignore")
    return None

def _first_name(df: pd.DataFrame, patterns: List[str]) -> Optional[pd.Series]:
    for pat in patterns:
        cols = df.filter(regex=pat, axis=1)
        if cols.shape[1] > 0:
            return cols.iloc[:, 0].astype(str)
    return None

def _first_id(df: pd.DataFrame, patterns: List[str]) -> Optional[pd.Series]:
    for pat in patterns:
        cols = df.filter(regex=pat, axis=1)
        if cols.shape[1] > 0:
            return cols.iloc[:, 0].astype(str)
    return None

# ------------------ builders ------------------

def build_player_rates(skaters: pd.DataFrame, last_n: int, w_recent: float) -> pd.DataFrame:
    df = skaters.copy()

    # IDs, names, team, position
    player_id = _first_id(df, [r"(?i)playerid", r"(?i)player_id"])
    name      = _first_name(df, [r"(?i)player$", r"(?i)name$"])
    team      = _first_name(df, [r"(?i)^team$"])
    pos_raw   = _first_name(df, [r"(?i)^position$", r"(?i)pos"])

    if player_id is None or name is None or team is None:
        cols = list(df.columns)
        raise RuntimeError(f"MoneyPuck skaters.csv missing id/name/team columns. Columns seen: {cols[:25]}...")

    pos = (pos_raw if pos_raw is not None else pd.Series(["F"] * len(df))).str.strip().str[0].str.upper()
    pos = pos.where(pos.isin(["F", "D"]), "F")

    # core stats
    shots   = _first_col(df, [r"(?i)^shots(?!.*against)"])
    goals   = _first_col(df, [r"(?i)^goals(?!.*against)"])
    assists = _first_col(df, [r"(?i)^assists"])
    gp      = _first_col(df, [r"(?i)games"])

    if shots is None or goals is None or assists is None or gp is None:
        cols = list(df.columns)
        raise RuntimeError(f"MoneyPuck skaters.csv missing shots/goals/assists/games columns. Columns seen: {cols[:25]}...")

    # per-game season
    gp_safe = gp.clip(lower=1)
    sog_pg = (shots / gp_safe).astype(float)
    g_pg   = (goals / gp_safe).astype(float)
    pts_pg = ((goals + assists) / gp_safe).astype(float)

    # optional “recent” metrics
    sog_recent = _first_col(df, [r"(?i)(last|rolling|recent).*shots"]) or sog_pg
    g_recent   = _first_col(df, [r"(?i)(last|rolling|recent).*goals"])  or g_pg
    pts_recent = _first_col(df, [r"(?i)(last|rolling|recent).*points"]) or pts_pg

    sog_pg_blend = w_recent * sog_recent + (1 - w_recent) * sog_pg
    g_pg_blend   = w_recent * g_recent   + (1 - w_recent) * g_pg
    pts_pg_blend = w_recent * pts_recent + (1 - w_recent) * pts_pg

    def per60(pg, p):
        toi = 17.5 if p == "F" else 21.0
        return pg * (60.0 / toi)

    ev_sog60 = [per60(x, p) for x, p in zip(sog_pg_blend, pos)]
    pp_sog60 = ev_sog60
    ev_g60   = [per60(x, p) for x, p in zip(g_pg_blend, pos)]
    pp_g60   = ev_g60
    a1_60    = [per60(max(pt - g, 0.0)*0.6, p) for pt, g, p in zip(pts_pg_blend, g_pg_blend, pos)]
    a2_60    = [per60(max(pt - g, 0.0)*0.4, p) for pt, g, p in zip(pts_pg_blend, g_pg_blend, pos)]

    out = pd.DataFrame({
        "player_id": player_id.values,
        "team": team.str.upper().values,
        "pos": pos.values,
        "ev_minutes": 600, "pp_minutes": 60,
        "ev_sog60": ev_sog60, "pp_sog60": pp_sog60,
        "ev_g60": ev_g60, "pp_g60": pp_g60,
        "a1_60": a1_60, "a2_60": a2_60,
        "name": name.values,
    })
    return out

def build_team_rates(teams: pd.DataFrame) -> pd.DataFrame:
    df = teams.copy()

    def grab(regexes: List[str], default: float) -> pd.Series:
        for r in regexes:
            cols = df.filter(regex=r, axis=1)
            if cols.shape[1] > 0:
                return cols.iloc[:, 0].astype(float, errors="ignore")
        return pd.Series([default] * len(df), index=df.index, dtype=float)

    sa = grab([r"(?i)shots.*against.*per.*game", r"(?i)shotsAgainstPerGame"], 30.0)
    ga = grab([r"(?i)goals.*against.*per.*game", r"(?i)goalsAgainstPerGame"], 3.0)
    sf = grab([r"(?i)shots.*per.*game$", r"(?i)shotsPerGame$"], 30.0)
    gf = grab([r"(?i)goals.*per.*game$", r"(?i)goalsPerGame$"], 3.0)

    team = _first_name(df, [r"(?i)^team$"])
    if team is None:
        cols = list(df.columns)
        raise RuntimeError(f"MoneyPuck teams.csv missing 'team' column. Columns seen: {cols[:25]}...")

    out = pd.DataFrame({
        "team": team.str.upper().values,
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
