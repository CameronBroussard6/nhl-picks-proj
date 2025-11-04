from __future__ import annotations
from typing import Tuple, List
from datetime import datetime
import pandas as pd

from ..net import read_csv_safely

def _season_folder(date_iso: str) -> str:
    d = datetime.fromisoformat(date_iso)
    if d.month < 7:
        return f"{d.year-1}-{d.year}"
    return f"{d.year}-{d.year+1}"

def _first_ok(urls: List[str]) -> pd.DataFrame:
    last_err = None
    for u in urls:
        try:
            return read_csv_safely(u)
        except Exception as e:
            last_err = e
            continue
    raise last_err or RuntimeError("No MoneyPuck URL succeeded")

def load_money_puck(date_iso: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    folder = _season_folder(date_iso)
    player_urls = [
        f"https://moneypuck.com/moneypuck/playerData/seasonSummary/{folder}/skaters.csv",
        f"https://moneypuck.com/moneypuck/playerData/seasonSummary/{folder}/skatersSummary.csv",
    ]
    team_urls = [
        f"https://moneypuck.com/moneypuck/teamData/seasonSummary/{folder}/teams.csv",
        f"https://moneypuck.com/moneypuck/teamData/seasonSummary/{folder}/teamSummary.csv",
    ]
    skaters = _first_ok(player_urls)
    teams   = _first_ok(team_urls)

    if "team" in skaters.columns:
        skaters["team"] = skaters["team"].astype(str).str.upper()
    if "team" in teams.columns:
        teams["team"] = teams["team"].astype(str).str.upper()
    return skaters, teams

def build_player_rates(skaters: pd.DataFrame, last_n: int, w_recent: float) -> pd.DataFrame:
    df = skaters.copy()
    shots   = df.filter(regex=r"(?i)^shots(?!.*against)").iloc[:, 0]
    goals   = df.filter(regex=r"(?i)^goals(?!.*against)").iloc[:, 0]
    assists = df.filter(regex=r"(?i)^assists").iloc[:, 0]
    gp      = df.filter(regex=r"(?i)games").iloc[:, 0]

    sog_pg = shots.divide(gp.clip(lower=1)).astype(float)
    g_pg   = goals.divide(gp.clip(lower=1)).astype(float)
    pts_pg = (goals + assists).divide(gp.clip(lower=1)).astype(float)

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
