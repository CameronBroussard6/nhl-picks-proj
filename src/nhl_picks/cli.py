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


def infer_pairs(teams_df):
    """
    TEMP pairing for mock/demo data.
    When the NHL.com schedule adapter is fully wired, replace this
    with schedule-derived mappings (team -> opponent) for the slate.
    """
    t = set(teams_df.team.tolist())
    pairs = []
    if {"BOS", "NYI"} <= t:
        pairs.append(("BOS", "NYI"))
    if {"COL", "TBL"} <= t:
        pairs.append(("COL", "TBL"))
    if {"BUF", "UTA"} <= t:
        pairs.append(("BUF", "UTA"))
    opp = {}
    for a, b in pairs:
        opp[a] = b
        opp[b] = a
    return opp


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
    # pick the slate date (today or tomorrow based on time)
    games_date = choose_slate_date()

    # fetch data bundle (adapter may fallback to mock on network error)
    bundle = fetch_bundle(
        use_mock=cfg["data_sources"]["use_mock"], games_date=games_date
    )
    source_label = getattr(bundle, "_source_label", "unknown source")
    notice = f"Slate date: {games_date} • Source: {source_label}"

    players = bundle.players
    team_rates = bundle.team_rates
    goalies = bundle.goalies
    lines = bundle.lines

    # stabilize per-60 rates with priors
    player_star = stabilize_rates(
        players, bundle.player_rates, cfg["priors"], cfg["shrinkage"]
    )

    # forecast minutes
    toi_df = expected_toi(players, lines)

    # opponent map (for mock/demo); replace with schedule-derived mapping in adapter
    opp_map = infer_pairs(bundle.teams)

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

    # write site artifacts
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
    ap.add_argument("command", choices=["run-daily", "backtest"])
    args = ap.parse_args()
    cfg = load_config(args.config)

    if args.command == "run-daily":
        run_daily(cfg)
    elif args.command == "backtest":
        from .backtest import make_fake_history, calibration
        import matplotlib.pyplot as plt

        hist = make_fake_history()
        pts = hist[hist["market"] == "PTS1"].rename(
            columns={"prob_1p": "prob", "hit": "hit"}
        )
        cal = calibration(pts, "prob", "hit", bins=10)
        os.makedirs("site", exist_ok=True)
        plt.figure()
        plt.plot(cal["pred"], cal["emp"], marker="o")
        plt.plot([0, 1], [0, 1])
        plt.title("Calibration: Points 1+ (mock data)")
        plt.xlabel("Predicted")
        plt.ylabel("Empirical")
        plt.savefig("site/calibration_points1.png", dpi=120, bbox_inches="tight")
        cal.to_csv("site/calibration_points1.csv", index=False)
        print("Backtest artifacts written to site/")


if __name__ == "__main__":
    main()
