from __future__ import annotations
import pandas as pd
import numpy as np

from .models import prob_at_least, fair_odds, clamp

def expected_toi(players: pd.DataFrame, lines: pd.DataFrame) -> pd.DataFrame:
    # Simple heuristic using role + pp1
    merged = players[['player_id','team','pos','role','is_pp1']].merge(lines[['player_id','line','pp_unit']], on='player_id', how='left')
    est = []
    for _, r in merged.iterrows():
        ev = 15.5 if r['role']=='top6' else (12.0 if r['pos']=='F' else 18.0 if r['role']=='top4D' else 14.0)
        pp = 3.2 if r['pp_unit']=='PP1' else (1.2 if r['pos']=='F' else 0.8)
        est.append({"player_id": r['player_id'], "exp_toi_ev": ev, "exp_toi_pp": pp})
    return pd.DataFrame(est)

def pace_factor(team_rates: pd.DataFrame, team_a: str, team_b: str, use_geo=True) -> float:
    a = float(team_rates.loc[team_rates.team==team_a, 'ev_cf60'])
    b = float(team_rates.loc[team_rates.team==team_b, 'ev_cf60'])
    lg = float(team_rates['ev_cf60'].mean())
    if use_geo:
        return float(np.sqrt((a/lg) * (b/lg)))
    return float((a/lg + b/lg)/2.0)

def sog_projection(player_star: pd.DataFrame, toi_df: pd.DataFrame, team_rates: pd.DataFrame, teams_opp: dict, use_geo=True) -> pd.DataFrame:
    df = player_star.merge(toi_df, on='player_id', how='left')
    rows = []
    lg_sog_against = float(team_rates['ev_sog_against60'].mean())
    lg_pk_sog_against = float(team_rates['pk_sog_against60'].mean())
    for _, r in df.iterrows():
        team = r['team']
        opp = teams_opp.get(team, None)
        if opp is None: 
            continue
        pf = pace_factor(team_rates, team, opp, use_geo)
        opp_ev = float(team_rates.loc[team_rates.team==opp,'ev_sog_against60'])
        opp_pk = float(team_rates.loc[team_rates.team==opp,'pk_sog_against60'])
        mu = r['ev_sog60_star']*(r['exp_toi_ev']/60.0)*pf*(opp_ev/lg_sog_against)            + r['pp_sog60_star']*(r['exp_toi_pp']/60.0)*(opp_pk/lg_pk_sog_against)
        rows.append({"player_id": r['player_id'], "team": team, "opp": opp, "proj_sog_mean": max(0.01, mu)})
    return pd.DataFrame(rows)

def points_projection(player_star: pd.DataFrame, toi_df: pd.DataFrame, team_rates: pd.DataFrame, goalies: pd.DataFrame, teams_opp: dict, beta_gsax: float, use_geo=True) -> pd.DataFrame:
    df = player_star.merge(toi_df, on='player_id', how='left')
    rows = []
    lg_xga = float(team_rates['ev_xga60'].mean())
    lg_pk_xga = float(team_rates['pk_xga60'].mean())
    for _, r in df.iterrows():
        team = r['team']
        opp = teams_opp.get(team, None)
        if opp is None: continue
        pf = pace_factor(team_rates, team, opp, use_geo)
        xga_ev = float(team_rates.loc[team_rates.team==opp,'ev_xga60'])
        xga_pk = float(team_rates.loc[team_rates.team==opp,'pk_xga60'])
        gsax60 = float(goalies.loc[goalies.team==opp,'gsax60'])
        goalie_factor = np.exp(beta_gsax * gsax60)
        mu_g = (r['ev_g60_star']*(r['exp_toi_ev']/60.0)*(xga_ev/lg_xga)*pf*goalie_factor)              + (r['pp_g60_star']*(r['exp_toi_pp']/60.0)*(xga_pk/lg_pk_xga)*goalie_factor)
        mu_a = (r['a1_60_star'] + r['a2_60_star'])*((r['exp_toi_ev'] + 0.6*r['exp_toi_pp'])/60.0)
        mu_p = max(0.01, mu_g + mu_a)
        p1p = 1.0 - np.exp(-mu_p)
        p2p = 1.0 - np.exp(-mu_p)*(1+mu_p)
        rows.append({
            "player_id": r['player_id'], "team": team, "opp": opp,
            "proj_points_mean": mu_p, "prob_1p": clamp(p1p, 0, 1), "prob_2p": clamp(p2p, 0, 1),
            "fair_odds_1p": fair_odds(p1p), "fair_odds_2p": fair_odds(p2p)
        })
    return pd.DataFrame(rows)

def first_goal_projection(player_star: pd.DataFrame, toi_df: pd.DataFrame, team_rates: pd.DataFrame, goalies: pd.DataFrame, use_geo=True) -> pd.DataFrame:
    # compute team first-goal probs from gf60 vs opp xga + goalie, then allocate to players by early-usage share
    rows = []
    teams = team_rates.team.unique()
    lg = float(team_rates['ev_cf60'].mean())
    # Build pairings by a simple round-robin mock (BOS vs NYI, COL vs TBL, BUF vs UTA)
    pairs = [("BOS","NYI"),("COL","TBL"),("BUF","UTA")]
    team_for_rate = {}
    for a,b in pairs:
        gsax_b = float(goalies.loc[goalies.team==b,'gsax60'])
        gsax_a = float(goalies.loc[goalies.team==a,'gsax60'])
        gf_a = float(team_rates.loc[team_rates.team==a,'ev_gf60'])
        gf_b = float(team_rates.loc[team_rates.team==b,'ev_gf60'])
        xga_b = float(team_rates.loc[team_rates.team==b,'ev_xga60'])
        xga_a = float(team_rates.loc[team_rates.team==a,'ev_xga60'])
        pf = np.sqrt((float(team_rates.loc[team_rates.team==a,'ev_cf60'])/lg) * (float(team_rates.loc[team_rates.team==b,'ev_cf60'])/lg))
        # goalie factor on opponent
        team_for_rate[a] = gf_a * (xga_b/np.mean(team_rates.ev_xga60)) * np.exp(-0.3*gsax_b) * pf
        team_for_rate[b] = gf_b * (xga_a/np.mean(team_rates.ev_xga60)) * np.exp(-0.3*gsax_a) * pf
        total = team_for_rate[a] + team_for_rate[b]
        p_a_first = team_for_rate[a]/total if total>0 else 0.5
        p_b_first = 1 - p_a_first
        # Allocation: use first-10-min TOI proxy (EV 8 + PP 2 if PP1)
        team_players = player_star.merge(toi_df, on='player_id', how='left')
        for team, p_team in [(a, p_a_first),(b, p_b_first)]:
            tp = team_players[team_players.team==team].copy()
            tp['first10'] = 8.0 + 2.0*tp['is_pp1']
            tp['share'] = (tp['ev_g60_star'] * tp['first10']) / (tp['ev_g60_star'] * tp['first10']).sum()
            for _, r in tp.iterrows():
                rows.append({
                    "player_id": r['player_id'], "team": team, "prob_first_goal": p_team * float(r['share']),
                    "fair_odds_fgs": 1.0 / (p_team * float(r['share']) + 1e-9)
                })
    return pd.DataFrame(rows)
