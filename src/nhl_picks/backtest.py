from __future__ import annotations
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from .data_sources import mock_bundle
from .transforms import stabilize_rates
from .projectors import expected_toi, sog_projection, points_projection, first_goal_projection
from .models import fair_odds

def make_fake_history(days: int = 60, seed: int = 7):
    """Generate mock slates over a number of days to test calibration/ROI plumbing."""
    rng = np.random.default_rng(seed)
    dates = [datetime.utcnow().date() - timedelta(days=d) for d in range(days, 0, -1)]
    history = []
    for d in dates:
        bundle = mock_bundle(str(d))
        players = bundle.players
        team_rates = bundle.team_rates
        goalies = bundle.goalies
        lines = bundle.lines
        player_star = stabilize_rates(players, bundle.player_rates, 
                                      {'sog_per60_forward':7.2,'sog_per60_defense':4.1,
                                       'g_per60_forward':1.0,'g_per60_defense':0.4,
                                       'a1_per60_forward':0.9,'a1_per60_defense':0.5,
                                       'a2_per60_forward':0.5,'a2_per60_defense':0.3},
                                      {'tau_ev':400,'tau_pp':120})
        toi_df = expected_toi(players, lines)
        opp_map = {'BOS':'NYI','NYI':'BOS','COL':'TBL','TBL':'COL','BUF':'UTA','UTA':'BUF'}
        sog_df = sog_projection(player_star, toi_df, team_rates, opp_map, True)
        pts_df = points_projection(player_star, toi_df, team_rates, goalies, opp_map, -0.35, True)
        fgs_df = first_goal_projection(player_star, toi_df, team_rates, goalies, True)
        # Simulate outcomes from the projected means
        sog_df['actual_sog'] = rng.poisson(sog_df['proj_sog_mean'].values)
        pts_df['actual_pts'] = rng.poisson(pts_df['proj_points_mean'].values)
        # First goal: draw a winner according to prob_first_goal across all players
        fgs_probs = fgs_df['prob_first_goal'].values
        fgs_probs = fgs_probs / fgs_probs.sum()
        winner_idx = rng.choice(len(fgs_probs), p=fgs_probs)
        fgs_df['is_first'] = 0
        fgs_df.loc[fgs_df.index[winner_idx], 'is_first'] = 1
        for m in ['SOG','PTS1','FGS']:
            if m == 'SOG':
                # evaluate over 2.5 line as an example
                picks = sog_df.assign(prob_over_2_5 = 1 - np.exp(-sog_df['proj_sog_mean']) * (1 + sog_df['proj_sog_mean'] + (sog_df['proj_sog_mean']**2)/2.0))
                picks['hit'] = (picks['actual_sog'] >= 3).astype(int)
                picks['market'] = 'SOG'
                picks['date'] = d
                history.append(picks[['player_id','team','opp','proj_sog_mean','prob_over_2_5','actual_sog','hit','market','date']])
            elif m == 'PTS1':
                picks = pts_df.assign(prob_1p = 1 - np.exp(-pts_df['proj_points_mean']))
                picks['hit'] = (picks['actual_pts'] >= 1).astype(int)
                picks['market'] = 'PTS1'
                picks['date'] = d
                history.append(picks[['player_id','team','opp','proj_points_mean','prob_1p','actual_pts','hit','market','date']])
            else:
                picks = fgs_df.copy()
                picks['hit'] = picks['is_first']
                picks['market'] = 'FGS'
                picks['date'] = d
                history.append(picks[['player_id','team','prob_first_goal','hit','market','date']])
    return pd.concat(history, ignore_index=True)

def calibration(df: pd.DataFrame, prob_col: str, hit_col: str, bins: int = 10):
    df = df.copy()
    df['bin'] = pd.qcut(df[prob_col].clip(1e-6, 1-1e-6), bins, duplicates='drop')
    grp = df.groupby('bin').agg(pred=('{}' .format(prob_col),'mean'),
                                emp=(hit_col,'mean'),
                                n=(hit_col,'size')).reset_index(drop=True)
    return grp
