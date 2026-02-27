"""
HSL Hockey Super League - Spartan Hockey Academy Stats Scraper

Scans game IDs across THREE divisions:
  - 23371 (2014 Major) - primary division, keep all games
  - 23373 (2015 Major) - team played here early season, Spartan games only
  - 32765 (2018 Major) - second son's division, Spartan games only

SPARTAN_TEAM_IDS:
  - 294811 (2014/2015 Major Spartan team)
  - 294868 (2018 Major Spartan team)

For each player, stats are combined across all divisions they played
for a Spartan team, but stats from non-Spartan teams are excluded.

Incremental mode: already-processed game IDs are cached in docs/game_cache.json.
"""

import requests
from bs4 import BeautifulSoup
import json
import re
import time
import os
from datetime import datetime

# All divisions to scan
DIVISION_IDS     = ["23371", "23373", "32765"]

# Our Spartan team IDs — one per division
SPARTAN_TEAM_IDS = {"294811", "294868"}

# Primary division for standings
PRIMARY_DIVISION = "23371"

BASE_URL = "https://hockeysuperleague.ca"
HEADERS  = {"User-Agent": "Mozilla/5.0 (compatible; HSL-stats-bot/1.0)"}

GAME_ID_START = 1620000
GAME_ID_END   = 1627500

CACHE_FILE  = "docs/game_cache.json"
OUTPUT_FILE = "docs/data.json"


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def load_cache():
    if not os.path.exists(CACHE_FILE):
        return set(), []
    with open(CACHE_FILE) as f:
        data = json.load(f)
    return set(data.get("processed_ids", [])), data.get("games", [])


def save_cache(processed_ids, games):
    os.makedirs("docs", exist_ok=True)
    with open(CACHE_FILE, "w") as f:
        json.dump({
            "processed_ids": sorted(processed_ids),
            "games": games
        }, f, indent=2)


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

def get_game_ids_to_scan(processed_ids):
    all_ids = set(range(GAME_ID_START, GAME_ID_END + 1))
    new_ids = sorted(all_ids - processed_ids)
    print(f"Total range: {len(all_ids)} IDs | Already processed: {len(processed_ids)} | New to scan: {len(new_ids)}")
    return new_ids


def parse_game(game_id):
    """
    Try fetching the game under each division URL.
    Accept the first one where player links inside the stat tables
    actually match that division ID.

    Rules:
    - 23371 (2014 Major): keep ALL games
    - 23373 (2015 Major): only keep if a Spartan team is playing
    - 32765 (2018 Major): only keep if a Spartan team is playing
    """
    for div_id in DIVISION_IDS:
        url = f"{BASE_URL}/division/0/{div_id}/game/view/{game_id}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
        except Exception:
            continue

        soup   = BeautifulSoup(resp.text, "html.parser")
        tables = soup.find_all("table")
        if not tables:
            continue

        # Find player stat tables
        player_tables = []
        for table in tables:
            hr = table.find("tr")
            if not hr:
                continue
            cols = [th.get_text(strip=True).upper() for th in hr.find_all(["th", "td"])]
            if "G" in cols and "A" in cols and "PTS" in cols:
                player_tables.append(table)

        if len(player_tables) < 2:
            continue

        # Confirm game belongs to this division via player links inside tables
        division_confirmed = False
        for table in player_tables:
            for link in table.find_all("a", href=True):
                if f"/0/{div_id}/" in link["href"]:
                    division_confirmed = True
                    break
            if division_confirmed:
                break

        if not division_confirmed:
            continue

        # Must be Final
        full_text   = soup.get_text()
        score_match = re.search(r'(\d+)\s*[-–]\s*(\d+)\s*Final', full_text)
        if not score_match:
            break  # page is the same regardless of div_id, no point retrying

        home_score = int(score_match.group(1))
        away_score = int(score_match.group(2))

        date_match = re.search(
            r'(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday),\s+(\w+ \d+,\s+\d{4})',
            full_text
        )
        game_date = date_match.group(0) if date_match else ""

        game_info = {
            "game_url":     url,
            "division_id":  div_id,
            "date":         game_date,
            "home_score":   home_score,
            "away_score":   away_score,
            "home_team":    "",
            "home_team_id": "",
            "away_team":    "",
            "away_team_id": "",
            "players":      []
        }

        skip_headings = {"Scoring", "Shots", "Scoring Summary", "Penalty Summary", "Staff"}

        for i, table in enumerate(player_tables[:2]):
            team_name = f"Team {i+1}"
            for sibling in table.find_all_previous(["h1", "h2", "h3", "h4"]):
                text = sibling.get_text(strip=True)
                if text and text not in skip_headings:
                    team_name = text
                    break

            if i == 0:
                game_info["home_team"] = team_name
            else:
                game_info["away_team"] = team_name

            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) < 6:
                    continue

                jersey = cols[0].get_text(strip=True)
                name   = cols[1].get_text(strip=True)
                if not name:
                    continue

                player_id = ""
                team_id   = ""
                link = cols[1].find("a", href=True)
                if link:
                    m = re.search(r'/team/\d+/0/(\d+)/(\d+)/player/(\d+)', link["href"])
                    if m:
                        if m.group(1) != div_id:
                            continue  # skip players from wrong division
                        team_id   = m.group(2)
                        player_id = m.group(3)

                if team_id:
                    if i == 0 and not game_info["home_team_id"]:
                        game_info["home_team_id"] = team_id
                    elif i == 1 and not game_info["away_team_id"]:
                        game_info["away_team_id"] = team_id

                try:
                    g   = int(cols[2].get_text(strip=True) or 0)
                    a   = int(cols[3].get_text(strip=True) or 0)
                    pts = int(cols[4].get_text(strip=True) or 0)
                    pim = int(cols[5].get_text(strip=True) or 0)
                except ValueError:
                    continue

                game_info["players"].append({
                    "name": name, "player_id": player_id,
                    "team": team_name, "team_id": team_id,
                    "jersey": jersey,
                    "g": g, "a": a, "pts": pts, "pim": pim,
                })

        if not game_info["players"]:
            return None

        # For non-primary divisions, only keep games involving a Spartan team
        team_ids_in_game = {game_info["home_team_id"], game_info["away_team_id"]}
        if div_id != PRIMARY_DIVISION and not (SPARTAN_TEAM_IDS & team_ids_in_game):
            return None

        return game_info

    return None


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def build_leaders(games):
    """
    League leaders across all games.
    player_id+team_id key keeps multi-team players separate.
    """
    players = {}
    for game in games:
        for p in game["players"]:
            key = f"{p['player_id']}_{p['team_id']}" if p["player_id"] else f"{p['name']}_{p['team_id']}"
            if key not in players:
                players[key] = {
                    "name": p["name"], "player_id": p["player_id"],
                    "team": p["team"], "team_id": p["team_id"],
                    "jersey": p["jersey"],
                    "g": 0, "a": 0, "pts": 0, "pim": 0, "gp": 0
                }
            players[key]["g"]   += p["g"]
            players[key]["a"]   += p["a"]
            players[key]["pts"] += p["pts"]
            players[key]["pim"] += p["pim"]
            players[key]["gp"]  += 1

    return sorted(players.values(), key=lambda x: (x["pts"], x["g"]), reverse=True)


def build_spartan_leaders(games, spartan_team_ids):
    """
    Spartan Leaders: combine each player's stats across ALL divisions
    but ONLY when they were playing for a Spartan team.
    Stats from other teams are excluded.
    Keyed by player_id only so cross-division games combine correctly.
    """
    players = {}
    for game in games:
        for p in game["players"]:
            if p["team_id"] not in spartan_team_ids:
                continue
            key = p["player_id"] if p["player_id"] else p["name"]
            if key not in players:
                players[key] = {
                    "name": p["name"], "player_id": p["player_id"],
                    "team": p["team"], "team_id": p["team_id"],
                    "jersey": p["jersey"],
                    "g": 0, "a": 0, "pts": 0, "pim": 0, "gp": 0
                }
            players[key]["g"]   += p["g"]
            players[key]["a"]   += p["a"]
            players[key]["pts"] += p["pts"]
            players[key]["pim"] += p["pim"]
            players[key]["gp"]  += 1

    return sorted(players.values(), key=lambda x: (x["pts"], x["g"]), reverse=True)


def build_standings(games):
    """Standings from primary division (2014 Major) only."""
    teams = {}
    for game in games:
        if game.get("division_id") != PRIMARY_DIVISION:
            continue

        home    = game.get("home_team", "")
        away    = game.get("away_team", "")
        home_id = game.get("home_team_id", "")
        away_id = game.get("away_team_id", "")
        hs      = game.get("home_score", 0)
        as_     = game.get("away_score", 0)

        for tid, tname in [(home_id, home), (away_id, away)]:
            if not tname:
                continue
            if tid not in teams:
                teams[tid] = {
                    "team": tname, "team_id": tid,
                    "gp": 0, "w": 0, "l": 0, "otl": 0,
                    "gf": 0, "ga": 0, "pts": 0
                }

        if home and away:
            teams[home_id]["gp"] += 1
            teams[away_id]["gp"] += 1
            teams[home_id]["gf"] += hs
            teams[home_id]["ga"] += as_
            teams[away_id]["gf"] += as_
            teams[away_id]["ga"] += hs

            if hs > as_:
                teams[home_id]["w"]   += 1
                teams[home_id]["pts"] += 2
                teams[away_id]["l"]   += 1
            elif as_ > hs:
                teams[away_id]["w"]   += 1
                teams[away_id]["pts"] += 2
                teams[home_id]["l"]   += 1
            else:
                teams[home_id]["otl"] += 1
                teams[away_id]["otl"] += 1
                teams[home_id]["pts"] += 1
                teams[away_id]["pts"] += 1

    return sorted(teams.values(), key=lambda x: (x["pts"], x["gf"] - x["ga"]), reverse=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=== HSL Spartan Hockey Stats Scraper (Incremental) ===")
    print(f"Divisions: {DIVISION_IDS} | Spartan teams: {SPARTAN_TEAM_IDS}")

    processed_ids, cached_games = load_cache()
    print(f"Loaded cache: {len(cached_games)} games already stored")

    new_ids = get_game_ids_to_scan(processed_ids)

    new_games       = []
    newly_processed = set()

    for i, game_id in enumerate(new_ids):
        if i % 100 == 0 and i > 0:
            print(f"  Progress: {i}/{len(new_ids)} scanned | {len(new_games)} new games found")

        game = parse_game(game_id)
        newly_processed.add(game_id)

        if game:
            new_games.append(game)
            team_ids_in_game = {game.get("home_team_id"), game.get("away_team_id")}
            flag = "⭐" if SPARTAN_TEAM_IDS & team_ids_in_game else ""
            print(f"  ✓{flag} [div {game.get('division_id')}] [{game.get('date','')}] "
                  f"{game.get('home_team','?')} {game.get('home_score','?')} - "
                  f"{game.get('away_score','?')} {game.get('away_team','?')} "
                  f"| {len(game['players'])} players")

        time.sleep(0.3)

    all_games     = cached_games + new_games
    all_processed = processed_ids | newly_processed
    save_cache(all_processed, all_games)
    print(f"\nCache updated: {len(all_processed)} IDs processed, {len(all_games)} total games")

    leaders         = build_leaders(all_games)
    standings       = build_standings(all_games)
    spartan_players = build_spartan_leaders(all_games, SPARTAN_TEAM_IDS)

    print(f"\nSpartan leaders found: {len(spartan_players)}")
    for p in spartan_players[:5]:
        print(f"  {p['name']} — {p['g']}G {p['a']}A {p['pts']}PTS (team {p['team_id']})")

    # Spartan team IDs as list for JSON output
    output = {
        "updated":          datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "division":         "2014 Major",
        "spartan_team_ids": list(SPARTAN_TEAM_IDS),
        "games_processed":  len(all_games),
        "leaders":          leaders,
        "spartan_leaders":  spartan_players,
        "standings":        standings,
        "games": [
            {
                "date":       g["date"],
                "home_team":  g.get("home_team", ""),
                "away_team":  g.get("away_team", ""),
                "home_score": g.get("home_score", 0),
                "away_score": g.get("away_score", 0),
                "game_url":   g["game_url"],
            }
            for g in all_games
        ]
    }

    os.makedirs("docs", exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Wrote {OUTPUT_FILE}")
    print(f"  {len(leaders)} players | {len(standings)} teams | {len(all_games)} games")


if __name__ == "__main__":
    main()
