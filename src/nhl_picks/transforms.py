from __future__ import annotations
import pandas as pd
import numpy as np

def compute_weights(minutes: float, tau: float) -> float:
    return float(minutes / (minutes + tau)) if minutes >= 0 else 0.0

def stabilize_rates(players: pd.DataFrame, player_rates: pd.DataFrame, priors: dict, shrinkage: dict) -> pd.DataFrame:
    df = player_rates.merge(players[['player_id','pos','is_pp1']], on='player_id', how='left')
    out = []
    for _, r in df.iterrows():
        pos = r['pos']
        sog_prior_ev = priors['sog_per60_forward'] if pos=='F' else priors['sog_per60_defense']
        g_prior_ev   = priors['g_per60_forward'] if pos=='F' else priors['g_per60_defense']
        a1_prior     = priors['a1_per60_forward'] if pos=='F' else priors['a1_per60_defense']
        a2_prior     = priors['a2_per60_forward'] if pos=='F' else priors['a2_per60_defense']
        w_ev = compute_weights(r['ev_minutes'], shrinkage['tau_ev'])
        w_pp = compute_weights(r['pp_minutes'], shrinkage['tau_pp'])
        ev_sog60 = w_ev*r['ev_sog60'] + (1-w_ev)*sog_prior_ev
        pp_sog60 = w_pp*r['pp_sog60'] + (1-w_pp)*max(sog_prior_ev, 6.5)  # PP prior a bit higher
        ev_g60   = w_ev*r['ev_g60'] + (1-w_ev)*g_prior_ev
        pp_g60   = w_pp*r['pp_g60'] + (1-w_pp)*(g_prior_ev*1.5)
        a1_60    = w_ev*r['a1_60'] + (1-w_ev)*a1_prior
        a2_60    = w_ev*r['a2_60'] + (1-w_ev)*a2_prior
        out.append({
            "player_id": r['player_id'], "team": r['team'], "pos": pos, "is_pp1": r['is_pp1'],
            "ev_minutes": r['ev_minutes'], "pp_minutes": r['pp_minutes'],
            "ev_sog60_star": ev_sog60, "pp_sog60_star": pp_sog60,
            "ev_g60_star": ev_g60, "pp_g60_star": pp_g60,
            "a1_60_star": a1_60, "a2_60_star": a2_60
        })
    return pd.DataFrame(out)
