from __future__ import annotations
import argparse
import os
from datetime import datetime, timedelta

import yaml
from zoneinfo import ZoneInfo

from .data_sources import fetch_bundle
from .transforms import stabilize_rates
from .projectors import (
    expected_toi,
    sog_projection,
    points_projection,
    first_goal_projection,
)
from .report import write_site


def load_config(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def choose_slate_date() -> str:
    """If after 11pm Central, use tomorrow's date; else use today."""
    now_ct = datetime.now(ZoneInfo("America/Chicago"))
    if now_ct.hour >= 23:
        return (now_ct + timedelta(days=1)).date().isoformat()
    return now_ct.date().isoformat()

def run_daily(cfg):
    games_date = choose_slate_date()

    # fetch live bundle (NHL.com only)
    bundle = fetch_bundle(
        games_date=games_date,
        last_n=int(cfg.get("nhl", {}).get("last_n", 7)),
        w_recent=float(cfg.get("nhl", {}).get("w_recent", 0.55)),
    )
    notice = f"Slate date: {games_date} • Source: NHL.com live"

    players = bundle.players
    team_rates = bundle.team_rates
    goalies = bundle.goalies
    lines = bundle.lines
    opp_map = bundle.opp_map  # <- real team→opponent mapping from schedule

    # stabilize per-60 rates with priors
    player_star = stabilize_rates(
        players, bundle.player_rates, cfg["priors"], cfg["shrinkage"]
    )

    # forecast minutes
    toi_df = expected_toi(players, lines)

    # projections
    sog_over_line = int(cfg.get("report", {}).get("sog_over_line", 3))
    sog_df = sog_projection(
        player_star,
        toi_df,
        team_rates,
        opp_map,
        cfg["pace"]["use_geometric_mean"],
        prob_threshold=sog_over_line,
    )
    pts_df = points_projection(
        player_star,
        toi_df,
        team_rates,
        goalies,
        opp_map,
        cfg["goalie"]["beta_gsax"],
        cfg["pace"]["use_geometric_mean"],
    )
    fgs_df = first_goal_projection(
        player_star, toi_df, team_rates, goalies, cfg["pace"]["use_geometric_mean"]
    )

    write_site(
        site_dir="site",
        site_title=cfg["publish"]["site_title"],
        players=players,
        sog_df=sog_df,
        pts_df=pts_df,
        fgs_df=fgs_df,
        updated=datetime.utcnow().isoformat() + "Z",
        top_n=cfg["report"]["top_n"],
        sog_line=sog_over_line,
        notice=notice,
    )
    print("Generated site/index.html and site/picks.json")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("command", choices=["run-daily"])
    args = ap.parse_args()
    cfg = load_config(args.config)
    if args.command == "run-daily":
        run_daily(cfg)

if __name__ == "__main__":
    main()
