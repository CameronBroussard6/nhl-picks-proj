import os
from datetime import date
from nhl_picks.adapters.nhl_web import build_bundle

if __name__ == "__main__":
    d = os.getenv("GAMES_DATE") or date.today().isoformat()
    players, lines, pr, tr, goalies, teams_df, opp = build_bundle(d, last_n=5, w_recent=0.65)
    print("Teams on slate:", teams_df["team"].tolist())
    print("Players sample:", players.head(5).to_dict(orient="records"))
    print("Player rates sample:", pr.head(5).to_dict(orient="records"))
    print("Team rates sample:", tr.head(5).to_dict(orient="records"))
