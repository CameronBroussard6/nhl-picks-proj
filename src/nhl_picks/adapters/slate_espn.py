from __future__ import annotations
from typing import Dict, List
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ✅ Correct ESPN scoreboard endpoint
SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard"

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "nhl-picks/1.0 (+https://github.com)"})
    retry = Retry(
        total=6, backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

def fetch_slate(date_iso: str) -> dict:
    """
    Returns:
      teams_df: DataFrame ['team'] with ESPN abbreviations (e.g., BOS, NYI)
      opp_map : dict team -> opponent
    """
    yyyymmdd = date_iso.replace("-", "")
    s = _session()
    r = s.get(SCOREBOARD, params={"dates": yyyymmdd}, timeout=25)
    r.raise_for_status()
    js = r.json()

    events = js.get("events", [])
    teams: List[str] = []
    opp_map: Dict[str, str] = {}

    for ev in events:
        comps = ev.get("competitions", [])
        if not comps: 
            continue
        comp = comps[0]
        cteams = comp.get("competitors", [])
        if len(cteams) != 2:
            continue
        # ESPN uses "homeAway" and "team": {"abbreviation": "..."}
        a = cteams[0]
        b = cteams[1]
        ta = a["team"]["abbreviation"].upper()
        tb = b["team"]["abbreviation"].upper()
        teams.extend([ta, tb])
        opp_map[ta] = tb
        opp_map[tb] = ta

    teams_df = pd.DataFrame({"team": sorted(pd.unique(pd.Series(teams)))})
    if teams_df.empty:
        raise RuntimeError(f"No ESPN slate for {date_iso}")
    return {"teams_df": teams_df, "opp_map": opp_map}
