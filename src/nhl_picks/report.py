from __future__ import annotations
import pandas as pd
from jinja2 import Template
import os

TABLE_TMPL = Template("""
### {{ title }}
| Player | Team | Opp | Metric | Value |
|---|---|---|---|---:|
{% for r in rows -%}
| {{ r.name }} | {{ r.team }} | {{ r.opp }} | {{ r.metric }} | {{ '%.3f'|format(r.value) }} |
{% endfor %}
""")

INDEX_TMPL = Template("""
# {{ site_title }}

_Last updated: {{ updated }}_
  
## Top Shots on Goal (by edge / mean)
{{ sog_table }}

## Top Points (1+) by fair probability
{{ points1_table }}

## Top First Goalscorer (by fair odds)
{{ fgs_table }}
""")

def build_tables(players, sog_df, pts_df, fgs_df, top_n=10):
    pmap = players.set_index('player_id')['name'].to_dict()

    def top_rows_sog():
        s = sog_df.sort_values('proj_sog_mean', ascending=False).head(top_n)
        rows = []
        for _, r in s.iterrows():
            rows.append({"name": pmap.get(r.player_id, r.player_id), "team": r.team, "opp": r.opp, "metric": "SOG mean", "value": float(r.proj_sog_mean)})
        return TABLE_TMPL.render(title=f"Top {top_n} SOG Means", rows=rows)

    def top_rows_pts1():
        s = pts_df.sort_values('prob_1p', ascending=False).head(top_n)
        rows = []
        for _, r in s.iterrows():
            rows.append({"name": pmap.get(r.player_id, r.player_id), "team": r.team, "opp": r.opp, "metric": "Pr(1+ pt)", "value": float(r.prob_1p)})
        return TABLE_TMPL.render(title=f"Top {top_n} Points (1+) Probabilities", rows=rows)

    def top_rows_fgs():
        s = fgs_df.sort_values('prob_first_goal', ascending=False).head(top_n)
        rows = []
        for _, r in s.iterrows():
            rows.append({"name": pmap.get(r.player_id, r.player_id), "team": r.team, "opp": getattr(r, 'opp', ''), "metric": "Pr(First Goal)", "value": float(r.prob_first_goal)})
        return TABLE_TMPL.render(title=f"Top {top_n} First Goalscorer Probabilities", rows=rows)

    return top_rows_sog(), top_rows_pts1(), top_rows_fgs()

def write_site(site_dir: str, site_title: str, players: pd.DataFrame, sog_df: pd.DataFrame, pts_df: pd.DataFrame, fgs_df: pd.DataFrame, updated: str, top_n: int = 10):
    os.makedirs(site_dir, exist_ok=True)
    sog_table, points1_table, fgs_table = build_tables(players, sog_df, pts_df, fgs_df, top_n)
    content = INDEX_TMPL.render(site_title=site_title, updated=updated, sog_table=sog_table, points1_table=points1_table, fgs_table=fgs_table)
    with open(os.path.join(site_dir, "index.md"), "w", encoding="utf-8") as f:
        f.write(content)
    # also dump a machine-readable file
    out = {
        "generated_at": updated,
        "top_sog": sog_df.sort_values('proj_sog_mean', ascending=False).head(top_n).to_dict(orient="records"),
        "top_points": pts_df.sort_values('prob_1p', ascending=False).head(top_n).to_dict(orient="records"),
        "top_fgs": fgs_df.sort_values('prob_first_goal', ascending=False).head(top_n).to_dict(orient="records"),
    }
    import json
    with open(os.path.join(site_dir, "picks.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
