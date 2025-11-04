from __future__ import annotations
import pandas as pd
from jinja2 import Template
import os, json
from datetime import datetime

HTML_TMPL = Template("""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{{ site_title }}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif; margin: 24px; }
    h1 { margin-bottom: 0.2rem; }
    .sub { color: #555; margin-top: 0; }
    table { border-collapse: collapse; width: 100%; margin: 12px 0 28px 0; }
    th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
    th { background: #f5f5f5; }
    .num { text-align: right; font-variant-numeric: tabular-nums; }
    .section { margin-top: 28px; }
    .foot { color: #777; font-size: 0.9rem; margin-top: 24px; }
    a { color: #0b6; text-decoration: none; }
    a:hover { text-decoration: underline; }
  </style>
</head>
<body>
  <h1>{{ site_title }}</h1>
  <p class="sub">Last updated: {{ updated }}</p>

  <div class="section">
    <h2>Top Shots on Goal (SOG) — by projected mean</h2>
    <table>
      <thead><tr><th>Player</th><th>Team</th><th>Opp</th><th>Metric</th><th class="num">Value</th></tr></thead>
      <tbody>
      {% for r in sog -%}
        <tr>
          <td>{{ r.name }}</td><td>{{ r.team }}</td><td>{{ r.opp }}</td>
          <td>SOG mean</td><td class="num">{{ '%.3f'|format(r.value) }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>

  <div class="section">
    <h2>Top Points (1+) — by fair probability</h2>
    <table>
      <thead><tr><th>Player</th><th>Team</th><th>Opp</th><th>Metric</th><th class="num">Value</th></tr></thead>
      <tbody>
      {% for r in pts1 -%}
        <tr>
          <td>{{ r.name }}</td><td>{{ r.team }}</td><td>{{ r.opp }}</td>
          <td>Pr(1+ point)</td><td class="num">{{ '%.3f'|format(r.value) }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>

  <div class="section">
    <h2>Top First Goalscorer — by probability</h2>
    <table>
      <thead><tr><th>Player</th><th>Team</th><th>Metric</th><th class="num">Value</th></tr></thead>
      <tbody>
      {% for r in fgs -%}
        <tr>
          <td>{{ r.name }}</td><td>{{ r.team }}</td>
          <td>Pr(First Goal)</td><td class="num">{{ '%.4f'|format(r.value) }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>

  <p class="foot">JSON feed: <a href="./picks.json">picks.json</a></p>
</body>
</html>
""")

def _top_rows(players, df, cols, key, top_n):
    # cols: (team, opp, valuecol)
    pmap = players.set_index('player_id')['name'].to_dict()
    s = df.sort_values(key, ascending=False).head(top_n)
    rows = []
    for _, r in s.iterrows():
        rows.append({
            "name": pmap.get(r.player_id, r.player_id),
            "team": getattr(r, cols[0]),
            "opp": getattr(r, cols[1]) if cols[1] else "",
            "value": float(getattr(r, cols[2])),
        })
    return rows

def write_site(site_dir: str, site_title: str, players: pd.DataFrame,
               sog_df: pd.DataFrame, pts_df: pd.DataFrame, fgs_df: pd.DataFrame,
               updated: str, top_n: int = 10):
    os.makedirs(site_dir, exist_ok=True)

    sog_rows  = _top_rows(players, sog_df, ('team','opp','proj_sog_mean'), 'proj_sog_mean', top_n)
    pts1_rows = _top_rows(players, pts_df, ('team','opp','prob_1p'),       'prob_1p',       top_n)
    fgs_rows  = _top_rows(players, fgs_df, ('team',None,'prob_first_goal'), 'prob_first_goal', top_n)

    html = HTML_TMPL.render(site_title=site_title, updated=updated,
                            sog=sog_rows, pts1=pts1_rows, fgs=fgs_rows)

    with open(os.path.join(site_dir, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)

    # JSON feed
    out = {
        "generated_at": updated,
        "top_sog":   sog_df.sort_values('proj_sog_mean', ascending=False).head(top_n).to_dict(orient="records"),
        "top_points":pts_df.sort_values('prob_1p',       ascending=False).head(top_n).to_dict(orient="records"),
        "top_fgs":   fgs_df.sort_values('prob_first_goal', ascending=False).head(top_n).to_dict(orient="records"),
    }
    with open(os.path.join(site_dir, "picks.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
