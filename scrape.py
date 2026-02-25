"""
HSL Hockey Super League - 2014 Major Division Stats Scraper
Scans game IDs, totals up G/A/PTS/PIM per player,
and writes results to docs/data.json for the website to display.

Incremental mode: already-processed game IDs are cached in docs/game_cache.json
so re-runs only fetch new games instead of scanning the full ID range.
"""

import requests
from bs4 import BeautifulSoup
import json
import re
import time
import os
from datetime import datetime

DIVISION_ID = "23371"
SPARTAN_TEAM_ID = "350513"
BASE_URL = "https://hockeysuperleague.ca"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; HSL-stats-bot/1.0)"
}

GAME_ID_START = 1625000
GAME_ID_END   = 1627500

CACHE_FILE = "docs/game_cache.json"
OUTPUT_FILE = "docs/data.json"


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def load_cache():
    """
    Returns (processed_ids, cached_games) where:
      processed_ids = set of game ID ints we've already tried (hit or miss)
      cached_games  = list of game dicts that were successfully parsed
    """
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
    Fetch and parse a single game page.
    Returns a game dict on success, or None if the game should be skipped.

    Key fixes vs. old version:
      1. Division check uses resp.url (after redirects) not page text.
      2. team_id is extracted from each player's OWN link, not just the first link in the table.
    """
    url = f"{BASE_URL}/division/0/{DIVISION_ID}/game/view/{game_id}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception:
        return None  # 404 or network error — skip silently

    # --- FIX 1: Division filter via final URL after any redirects ---
    # If HSL redirects us to a different division, the URL will change.
    final_url = resp.url
    if f"/0/{DIVISION_ID}/" not in final_url:
        return None  # Redirected to a different division

    soup = BeautifulSoup(resp.text, "html.parser")

    # Must have tables
    tables = soup.find_all("table")
    if not tables:
        return None

    full_text = soup.get_text()

    # Must be a Final game
    score_match = re.search(r'(\d+)\s*[-–]\s*(\d+)\s*Final', full_text)
    if not score_match:
        return None

    home_score = int(score_match.group(1))
    away_score = int(score_match.group(2))

    # Date
    date_match = re.search(
        r'(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday),\s+(\w+ \d+,\s+\d{4})',
        full_text
    )
    game_date = date_match.group(0) if date_match else ""

    # --- Find player stat tables (columns: # Name G A PTS PIM) ---
    player_tables = []
    for table in tables:
        header_row = table.find("tr")
        if not header_row:
            continue
        col_names = [th.get_text(strip=True).upper() for th in header_row.find_all(["th", "td"])]
        if "G" in col_names and "A" in col_names and "PTS" in col_names:
            player_tables.append(table)

    if len(player_tables) < 2:
        return None

    game_info = {
        "game_url": final_url,
        "date": game_date,
        "home_score": home_score,
        "away_score": away_score,
        "home_team": "",
        "home_team_id": "",
        "away_team": "",
        "away_team_id": "",
        "players": []
    }

    skip_headings = {"Scoring", "Shots", "Scoring Summary", "Penalty Summary", "Staff"}

    for i, table in enumerate(player_tables[:2]):  # only first 2 tables = home & away
        # Find closest preceding heading for team name
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

        # Parse player rows
        rows = table.find_all("tr")[1:]  # skip header
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 6:
                continue

            jersey = cols[0].get_text(strip=True)
            name = cols[1].get_text(strip=True)
            if not name:
                continue

            # --- FIX 2: Extract team_id from THIS player's own link ---
            player_id = ""
            team_id = ""
            link = cols[1].find("a", href=True)
            if link:
                href = link["href"]
                # href pattern: /team/11823/0/23371/350513/player/4042777
                m = re.search(r'/team/\d+/0/\d+/(\d+)/player/(\d+)', href)
                if m:
                    team_id = m.group(1)
                    player_id = m.group(2)

            # Update team_id on game_info from first player in each table
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
                "name": name,
                "player_id": player_id,
                "team": team_name,
                "team_id": team_id,   # now per-player, not table-level
                "jersey": jersey,
                "g": g, "a": a, "pts": pts, "pim": pim,
            })

    return game_info


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def build_leaders(games):
    players = {}
    for game in games:
        for p in game["players"]:
            key = p["player_id"] if p["player_id"] else p["name"]
            if key not in players:
                players[key] = {
                    "name": p["name"],
                    "player_id": p["player_id"],
                    "team": p["team"],
                    "team_id": p["team_id"],
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
    teams = {}
    for game in games:
        home, away = game.get("home_team", ""), game.get("away_team", "")
        home_id, away_id = game.get("home_team_id", ""), game.get("away_team_id", "")
        hs, as_ = game.get("home_score", 0), game.get("away_score", 0)

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
    print("=== HSL 2014 Major Stats Scraper (Incremental) ===")

    processed_ids, cached_games = load_cache()
    print(f"Loaded cache: {len(cached_games)} games already stored")

    new_ids = get_game_ids_to_scan(processed_ids)

    new_games = []
    newly_processed = set()

    for i, game_id in enumerate(new_ids):
        if i % 100 == 0 and i > 0:
            print(f"  Progress: {i}/{len(new_ids)} scanned | {len(new_games)} new games found")

        game = parse_game(game_id)
        newly_processed.add(game_id)

        if game:
            new_games.append(game)
            print(f"  ✓ [{game.get('date','')}] "
                  f"{game.get('home_team','?')} {game.get('home_score','?')} - "
                  f"{game.get('away_score','?')} {game.get('away_team','?')} "
                  f"| home_id={game.get('home_team_id','')} away_id={game.get('away_team_id','')} "
                  f"| {len(game['players'])} players")

        time.sleep(0.3)

    # Merge and save cache
    all_games = cached_games + new_games
    all_processed = processed_ids | newly_processed
    save_cache(all_processed, all_games)
    print(f"\nCache updated: {len(all_processed)} IDs processed, {len(all_games)} total games")

    # Build output
    leaders   = build_leaders(all_games)
    standings = build_standings(all_games)
    spartan_players = [p for p in leaders if p["team_id"] == SPARTAN_TEAM_ID]

    print(f"\nSpartan leaders found: {len(spartan_players)}")
    for p in spartan_players[:5]:
        print(f"  {p['name']} — {p['g']}G {p['a']}A {p['pts']}PTS (team_id={p['team_id']})")

    output = {
        "updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "division": "2014 Major",
        "spartan_team_id": SPARTAN_TEAM_ID,
        "games_processed": len(all_games),
        "leaders": leaders,
        "spartan_leaders": spartan_players,
        "standings": standings,
        "games": [
            {
                "date": g["date"],
                "home_team": g.get("home_team", ""),
                "away_team": g.get("away_team", ""),
                "home_score": g.get("home_score", 0),
                "away_score": g.get("away_score", 0),
                "game_url": g["game_url"],
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
