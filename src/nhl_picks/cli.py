from __future__ import annotations
import argparse, yaml, os
from datetime import datetime
import pandas as pd

from .data_sources import fetch_bundle
from .transforms import stabilize_rates
from .projectors import expected_toi, sog_projection, points_projection, first_goal_projection
from .report import write_site

def infer_pairs(teams_df):
    # For the demo, pair BOS-NYI, COL-TBL, BUF-UTA if they exist.
    t = set(teams_df.team.tolist())
    pairs = []
    if {'BOS','NYI'} <= t: pairs.append(('BOS','NYI'))
    if {'COL','TBL'} <= t: pairs.append(('COL','TBL'))
    if {'BUF','UTA'} <= t: pairs.append(('BUF','UTA'))
    # Build mapping team -> opp (single opp per team in this simplistic slate)
    opp = {}
    for a,b in pairs:
        opp[a]=b; opp[b]=a
    return opp

def load_config(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def run_daily(cfg):
    bundle = fetch_bundle(use_mock=cfg['data_sources']['use_mock'])
    players = bundle.players
    team_rates = bundle.team_rates
    goalies = bundle.goalies
    lines = bundle.lines
    player_star = stabilize_rates(players, bundle.player_rates, cfg['priors'], cfg['shrinkage'])
    toi_df = expected_toi(players, lines)
    opp_map = infer_pairs(bundle.teams)
    sog_df = sog_projection(
    player_star, toi_df, team_rates, opp_map,
    cfg['pace']['use_geometric_mean'],
    prob_threshold=int(cfg['report'].get('sog_over_line', 3))
)
    pts_df = points_projection(player_star, toi_df, team_rates, goalies, opp_map, cfg['goalie']['beta_gsax'], cfg['pace']['use_geometric_mean'])
    fgs_df = first_goal_projection(player_star, toi_df, team_rates, goalies, cfg['pace']['use_geometric_mean'])
    write_site(site_dir='site', site_title=cfg['publish']['site_title'], players=players, sog_df=sog_df, pts_df=pts_df, fgs_df=fgs_df, updated=datetime.utcnow().isoformat()+'Z', top_n=cfg['report']['top_n'])
    print("Generated site/index.md and site/picks.json")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("command", choices=["run-daily","backtest"])
    args = ap.parse_args()
    cfg = load_config(args.config)
    if args.command == "run-daily":
        run_daily(cfg)
    elif args.command == "backtest":
        from .backtest import make_fake_history, calibration
        hist = make_fake_history()
        # Example calibration for Points 1+
        pts = hist[hist['market']=="PTS1"].rename(columns={'prob_1p':'prob','hit':'hit'})
        cal = calibration(pts, 'prob', 'hit', bins=10)
        import matplotlib.pyplot as plt
        plt.figure()
        plt.plot(cal['pred'], cal['emp'], marker='o')
        plt.plot([0,1],[0,1])
        plt.title("Calibration: Points 1+ (mock data)")
        plt.xlabel("Predicted")
        plt.ylabel("Empirical")
        os.makedirs('site', exist_ok=True)
        plt.savefig('site/calibration_points1.png', dpi=120, bbox_inches='tight')
        cal.to_csv('site/calibration_points1.csv', index=False)
        print("Backtest artifacts written to site/")

if __name__ == "__main__":
    main()
