from __future__ import annotations
from typing import Dict, List
import pandas as pd

from ..net import get_json

SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard"

def fetch_slate(date_iso: str) -> dict:
    """
    Returns:
      teams_df: DataFrame ['team'] with ESPN abbreviations (e.g., BOS, NYI)
      opp_map : dict team -> opponent
    """
    yyyymmdd = date_iso.replace("-", "")
    js = get_json(SCOREBOARD, params={"dates": yyyymmdd})
    events = js.get("events", [])
    teams: List[str] = []
    opp_map: Dict[str, str] = {}

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
        opp_map[ta] = tb
        opp_map[tb] = ta

    teams_df = pd.DataFrame({"team": sorted(pd.unique(pd.Series(teams)))})
    if teams_df.empty:
        raise RuntimeError(f"No ESPN slate for {date_iso}")
    return {"teams_df": teams_df, "opp_map": opp_map}
