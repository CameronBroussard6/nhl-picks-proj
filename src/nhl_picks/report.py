from __future__ import annotations
import os
import json
import pandas as pd
from jinja2 import Template

HTML_TMPL = Template("""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{{ site_title }}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root { --bg:#fff; --fg:#111; --muted:#666; --line:#e5e5e5; }
    html,body { background: var(--bg); color: var(--fg); }
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif; margin: 24px; }
    h1 { margin-bottom: .25rem; }
    .sub { color: var(--muted); margin-top: 0; }
    table { border-collapse: collapse; width: 100%; margin: 12px 0 28px 0; }
    th, td { border: 1px solid var(--line); padding: 8px; text-align: left; }
    th { background: #f7f7f7; }
    .num { text-align: right; font-variant-numeric: tabular-nums; }
    .section { margin-top: 28px; }
    .foot { color: var(--muted); font-size: .9rem; margin-top: 24px; }
    .empty { color: var(--muted); font-style: italic; margin: 8px 0 24px; }
    a { color: #0b6; text-decoration: none; }
    a:hover { text-decoration: underline; }
  </style>
</head>
<body>
  <h1>{{ site_title }}</h1>
  <p class="sub">Last updated: {{ updated }}</p>
  {% if notice %}
    <p class="sub"><strong>Note:</strong> {{ notice }}</p>
  {% endif %}

  <div class="section">
    <h2>Top Shots on Goal (SOG)</h2>
    {% if sog|length == 0 %}
      <p class="empty">No SOG picks available.</p>
    {% else %}
    <table>
      <thead>
        <tr>
          <th>Player</th><th>Team</th><th>Opp</th>
          <th class="num">Projected SOG (mean)</th>
          <th class="num">Prob ≥ {{ sog_line }}</th>
        </tr>
      </thead>
      <tbody>
      {% for r in sog -%}
        <tr>
          <td>{{ r.name }}</td><td>{{ r.team }}</td><td>{{ r.opp }}</td>
          <td class="num">{{ '%.3f'|format(r.mu) }}</td>
          <td class="num">{{ '%.1f%%'|format(100*r.prob) }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
    {% endif %}
  </div>

  <div class="section">
    <h2>Top Points (1+) — by fair probability</h2>
    {% if pts1|length == 0 %}
      <p class="empty">No points picks available.</p>
    {% else %}
    <table>
      <thead><tr><th>Player</th><th>Team</th><th>Opp</th><th class="num">Pr(1+ point)</th></tr></thead>
      <tbody>
      {% for r in pts1 -%}
        <tr>
          <td>{{ r.name }}</td><td>{{ r.team }}</td><td>{{ r.opp }}</td>
          <td class="num">{{ '%.3f'|format(r.value) }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
    {% endif %}
  </div>

  <div class="section">
    <h2>Top First Goalscorer — by probability</h2>
    {% if fgs|length == 0 %}
      <p class="empty">No FGS picks available.</p>
    {% else %}
    <table>
      <thead><tr><th>Player</th><th>Team</th><th class="num">Pr(First Goal)</th></tr></thead>
      <tbody>
      {% for r in fgs -%}
        <tr>
          <td>{{ r.name }}</td><td>{{ r.team }}</td>
          <td class="num">{{ '%.4f'|format(r.value) }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
    {% endif %}
  </div>

  <p class="foot">JSON feed: <a href="./picks.json">picks.json</a></p>
</body>
</html>
""")

def _safe_top(df: pd.DataFrame, sort_cols, ascending=False, n=10):
    if df is None or df.empty:
        return pd.DataFrame()
    cols = [c for c in sort_cols if c in df.columns]
    if not cols:
        return df.head(n)
    return df.sort_values(cols, ascending=ascending).head(n)

def _top_rows_sog(players: pd.DataFrame, sog_df: pd.DataFrame, top_n: int):
    pmap = players.set_index('player_id')['name'].to_dict() if not players.empty else {}
    s = _safe_top(sog_df, ['prob_over', 'proj_sog_mean'], ascending=False, n=top_n)
    rows = []
    for _, r in s.iterrows():
        rows.append({
            "name": pmap.get(r.get('player_id'), r.get('player_id', '')),
            "team": r.get('team',''),
            "opp": r.get('opp',''),
            "mu": float(r.get('proj_sog_mean', 0.0)),
            "prob": float(r.get('prob_over', 0.0)),
        })
    return rows

def _top_rows_pts1(players: pd.DataFrame, pts_df: pd.DataFrame, top_n: int):
    pmap = players.set_index('player_id')['name'].to_dict() if not players.empty else {}
    s = _safe_top(pts_df, ['prob_1p'], ascending=False, n=top_n)
    rows = []
    for _, r in s.iterrows():
        rows.append({
            "name": pmap.get(r.get('player_id'), r.get('player_id', '')),
            "team": r.get('team',''),
            "opp": r.get('opp',''),
            "value": float(r.get('prob_1p', 0.0)),
        })
    return rows

def _top_rows_fgs(players: pd.DataFrame, fgs_df: pd.DataFrame, top_n: int):
    pmap = players.set_index('player_id')['name'].to_dict() if not players.empty else {}
    s = _safe_top(fgs_df, ['prob_first_goal'], ascending=False, n=top_n)
    rows = []
    for _, r in s.iterrows():
        rows.append({
            "name": pmap.get(r.get('player_id'), r.get('player_id', '')),
            "team": r.get('team',''),
            "value": float(r.get('prob_first_goal', 0.0)),
        })
    return rows

def write_site(
    site_dir: str,
    site_title: str,
    players: pd.DataFrame,
    sog_df: pd.DataFrame,
    pts_df: pd.DataFrame,
    fgs_df: pd.DataFrame,
    updated: str,
    top_n: int = 10,
    sog_line: int = 3,
    notice: str | None = None,
):
    os.makedirs(site_dir, exist_ok=True)

    sog_rows  = _top_rows_sog(players, sog_df, top_n)
    pts1_rows = _top_rows_pts1(players, pts_df, top_n)
    fgs_rows  = _top_rows_fgs(players, fgs_df, top_n)

    html = HTML_TMPL.render(
        site_title=site_title,
        updated=updated,
        sog=sog_rows,
        sog_line=sog_line,
        pts1=pts1_rows,
        fgs=fgs_rows,
        notice=notice,
    )
    with open(os.path.join(site_dir, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)

    out = {
        "generated_at": updated,
        "sog_line": sog_line,
        "top_sog":  (sog_df if sog_df is not None else pd.DataFrame()).to_dict(orient="records"),
        "top_points": (pts_df if pts_df is not None else pd.DataFrame()).to_dict(orient="records"),
        "top_fgs":   (fgs_df if fgs_df is not None else pd.DataFrame()).to_dict(orient="records"),
        "notice": notice,
    }
    with open(os.path.join(site_dir, "picks.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
