"""
HSL Hockey Super League - 2014 Major Division Stats Scraper
Scans game IDs directly, totals up G/A/PTS/PIM per player,
and writes results to docs/data.json for the website to display.
"""

import requests
from bs4 import BeautifulSoup
import json
import re
import time
from datetime import datetime

DIVISION_ID = "23371"
SPARTAN_TEAM_ID = "350513"
BASE_URL = "https://hockeysuperleague.ca"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; HSL-stats-bot/1.0)"
}

# Game IDs for 2014 Major division games this season.
# We scan a range and skip any that aren't Final or don't belong to this division.
# Based on known game IDs: 1626620, 1626651, 1626652, 1626653
# We'll scan a broad range to catch all games.
GAME_ID_START = 1625000
GAME_ID_END   = 1627500

def get_game_links():
    """Return list of all game URLs to try."""
    urls = [
        f"{BASE_URL}/division/0/{DIVISION_ID}/game/view/{gid}"
        for gid in range(GAME_ID_START, GAME_ID_END + 1)
    ]
    print(f"Will scan {len(urls)} game IDs from {GAME_ID_START} to {GAME_ID_END}")
    return urls

def parse_game(url):
    """
    Parse a single game page. Returns:
      {
        "game_url": url,
        "home_team": str,
        "away_team": str,
        "home_score": int,
        "away_score": int,
        "date": str,
        "players": [ {name, team, team_id, jersey, g, a, pts, pim}, ... ]
      }
    or None if the game has no stats yet.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        # 404 or other HTTP error — game ID doesn't exist, skip silently
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Make sure this page belongs to our division by checking for our division ID
    # in the page links (team links contain the division ID)
    page_text = resp.text
    if f"/0/{DIVISION_ID}/" not in page_text:
        return None  # Wrong division or not a game page

    # Check if there are any player stat tables
    tables = soup.find_all("table")
    if not tables:
        return None

    # --- Game info ---
    game_info = {"game_url": url, "players": []}

    # Try to find team names and scores
    # The page has team names in headings above each table
    headings = soup.find_all(["h1", "h2", "h3", "h4"])
    team_names = []
    for h in headings:
        text = h.get_text(strip=True)
        # Skip generic headings
        if text in ("Scoring", "Shots", "Scoring Summary", "Penalty Summary", "Staff") or not text:
            continue
        if len(text) > 3 and text not in team_names:
            team_names.append(text)

    # Score: look for pattern like "3 - 1 Final"
    full_text = soup.get_text()
    score_match = re.search(r'(\d+)\s*[-–]\s*(\d+)\s*Final', full_text)
    if score_match:
        game_info["home_score"] = int(score_match.group(1))
        game_info["away_score"] = int(score_match.group(2))
    else:
        # Game not final yet
        return None

    # Date
    date_match = re.search(r'(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday),\s+(\w+ \d+,\s+\d{4})', full_text)
    if date_match:
        game_info["date"] = date_match.group(0)
    else:
        game_info["date"] = ""

    # --- Player tables ---
    # Each team has a table with columns: # | Name | G | A | PTS | PIM
    # The team name is in an <h1> or <h2> immediately before the table (or nearby)
    # We'll find all tables that have the right columns

    player_tables = []
    for table in tables:
        headers_row = table.find("tr")
        if not headers_row:
            continue
        headers_text = [th.get_text(strip=True).upper() for th in headers_row.find_all(["th", "td"])]
        if "G" in headers_text and "A" in headers_text and "PTS" in headers_text:
            player_tables.append(table)

    if len(player_tables) < 2:
        return None

    # Find team name for each table by walking backwards through siblings
    for i, table in enumerate(player_tables):
        # Find the closest preceding heading
        team_name = f"Team {i+1}"
        team_id = ""
        for sibling in table.find_all_previous(["h1", "h2", "h3", "h4"]):
            text = sibling.get_text(strip=True)
            if text and text not in ("Scoring", "Shots", "Scoring Summary", "Penalty Summary"):
                team_name = text
                break

        # Also try to extract team_id from player links in the table
        first_player_link = table.find("a", href=re.compile(r"/team/\d+/0/\d+/(\d+)/player/"))
        if first_player_link:
            m = re.search(r'/team/\d+/0/\d+/(\d+)/player/', first_player_link["href"])
            if m:
                team_id = m.group(1)

        if i == 0:
            game_info["home_team"] = team_name
            game_info["home_team_id"] = team_id
        else:
            game_info["away_team"] = team_name
            game_info["away_team_id"] = team_id

        # Parse rows
        rows = table.find_all("tr")[1:]  # skip header
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 6:
                continue
            jersey = cols[0].get_text(strip=True)
            name = cols[1].get_text(strip=True)
            if not name:
                continue

            # Extract player_id from link if available
            player_id = ""
            link = cols[1].find("a", href=True)
            if link:
                pm = re.search(r'/player/(\d+)', link["href"])
                if pm:
                    player_id = pm.group(1)

            try:
                g = int(cols[2].get_text(strip=True) or 0)
                a = int(cols[3].get_text(strip=True) or 0)
                pts = int(cols[4].get_text(strip=True) or 0)
                pim = int(cols[5].get_text(strip=True) or 0)
            except ValueError:
                continue

            game_info["players"].append({
                "name": name,
                "player_id": player_id,
                "team": team_name,
                "team_id": team_id,
                "jersey": jersey,
                "g": g,
                "a": a,
                "pts": pts,
                "pim": pim,
            })

    return game_info

def build_leaders(games):
    """Aggregate per-player stats across all games."""
    players = {}  # player_id -> stats dict

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
            players[key]["g"] += p["g"]
            players[key]["a"] += p["a"]
            players[key]["pts"] += p["pts"]
            players[key]["pim"] += p["pim"]
            players[key]["gp"] += 1

    return sorted(players.values(), key=lambda x: (x["pts"], x["g"]), reverse=True)

def build_standings(games):
    """Build team standings from game results."""
    teams = {}

    for game in games:
        home = game.get("home_team", "")
        away = game.get("away_team", "")
        home_id = game.get("home_team_id", "")
        away_id = game.get("away_team_id", "")
        hs = game.get("home_score", 0)
        as_ = game.get("away_score", 0)

        for tid, tname in [(home_id, home), (away_id, away)]:
            if not tname:
                continue
            if tid not in teams:
                teams[tid] = {"team": tname, "team_id": tid, "gp": 0, "w": 0, "l": 0, "otl": 0, "gf": 0, "ga": 0, "pts": 0}

        if home and away:
            teams[home_id]["gp"] += 1
            teams[away_id]["gp"] += 1
            teams[home_id]["gf"] += hs
            teams[home_id]["ga"] += as_
            teams[away_id]["gf"] += as_
            teams[away_id]["ga"] += hs

            if hs > as_:
                teams[home_id]["w"] += 1
                teams[home_id]["pts"] += 2
                teams[away_id]["l"] += 1
            elif as_ > hs:
                teams[away_id]["w"] += 1
                teams[away_id]["pts"] += 2
                teams[home_id]["l"] += 1
            else:
                teams[home_id]["otl"] += 1
                teams[away_id]["otl"] += 1
                teams[home_id]["pts"] += 1
                teams[away_id]["pts"] += 1

    return sorted(teams.values(), key=lambda x: (x["pts"], x["gf"] - x["ga"]), reverse=True)

def main():
    print("=== HSL 2014 Major Stats Scraper ===")

    game_links = get_game_links()

    games = []
    for i, url in enumerate(game_links):
        if i % 100 == 0:
            print(f"  Scanning ID {GAME_ID_START + i}... ({len(games)} games found so far)")
        game = parse_game(url)
        if game:
            games.append(game)
            print(f"  ✓ [{game.get('date','')}] {game.get('home_team','?')} {game.get('home_score','?')} - {game.get('away_score','?')} {game.get('away_team','?')} ({len(game['players'])} players)")
        time.sleep(0.3)  # be polite to the server

    print(f"\nProcessed {len(games)} completed games")

    leaders = build_leaders(games)
    standings = build_standings(games)

    spartan_players = [p for p in leaders if p["team_id"] == SPARTAN_TEAM_ID]

    output = {
        "updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "division": "2014 Major",
        "spartan_team_id": SPARTAN_TEAM_ID,
        "games_processed": len(games),
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
            for g in games
        ]
    }

    import os
    os.makedirs("docs", exist_ok=True)
    with open("docs/data.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"✓ Wrote docs/data.json")
    print(f"  {len(leaders)} players | {len(standings)} teams | {len(games)} games")

if __name__ == "__main__":
    main()
