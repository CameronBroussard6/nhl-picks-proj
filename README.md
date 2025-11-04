# nhl-picks-proj

Daily automated **NHL** projections & picks (Shots on Goal, Points, First Goalscorer) with a GitHub Actions workflow
that builds a markdown page (and JSON) you can publish with GitHub Pages.

## Quick start

```bash
# create and activate a venv (optional)
python -m venv .venv && . .venv/bin/activate  # on Windows: .venv\Scripts\activate

pip install -r requirements.txt

# run end-to-end locally
python -m nhl_picks.cli --config config.yaml run-daily
```

Outputs land in `site/` as `index.md` and `picks.json`.

### Config

Edit `config.yaml` to switch data sources, windows, and schedule options.
This skeleton ships with **mock adapters** so the code runs without keys.
Swap them out for real sources when ready.

### Repo layout

```
nhl-picks-proj/
  ├─ src/nhl_picks/
  │   ├─ __init__.py
  │   ├─ data_sources.py
  │   ├─ transforms.py
  │   ├─ models.py
  │   ├─ projectors.py
  │   ├─ report.py
  │   ├─ optics.py
  │   └─ cli.py
  ├─ config.yaml
  ├─ requirements.txt
  ├─ .github/workflows/daily.yml
  └─ site/   (generated)
```

## Publishing

Enable **GitHub Pages** (Settings → Pages → deploy from `gh-pages`), or commit `site/` to your website repo.
The provided workflow commits to `gh-pages` automatically.


## Next steps wired
- **Adapters stubs** for NHL API / NST / MoneyPuck / DailyFaceoff in `src/nhl_picks/adapters/`.
- **Odds ingestion** from CSV in `src/nhl_picks/odds.py` (collapse to best price).
- **Backtest**: `python -m nhl_picks.cli backtest` outputs a calibration plot and CSV (mock data).

### Wiring real data
1. Implement fetchers in `adapters/` and flip `data_sources.use_mock` to `false` in `config.yaml`.
2. Map columns to the expected schemas documented in `data_sources.py` docstrings.
3. (Optional) Add secrets as repository `ACTION` secrets (API keys) and use them inside the workflow.
