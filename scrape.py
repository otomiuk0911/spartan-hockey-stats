"""
HSL Hockey Super League - Spartan Hockey Academy Stats Scraper
=============================================================

Regular Season  (hockeysuperleague.ca)
  23371  2014 Major  — primary division, all games + standings
  23373  2015 Major  — Spartan-only (moved up mid-season)
  32765  2018 Major  — Spartan-only

Playoffs  (hslchampionship.ca, /gamesheet/ URL pattern)
  22196  2014 Major playoffs — all games + standings
  36713  2018 Major playoffs — Spartan-only

Spartan team IDs
  Regular season : 294811 (2014/2015 Major), 294868 (2018 Major)
  Playoffs       : 350513 (2014 Major – listed in championship nav), 294868 (2018 Major)

Output → docs/data.json
  { regular_season: { spartan_leaders, leaders, standings, games },
    playoffs:       { spartan_leaders, leaders, standings, games },
    updated, spartan_team_ids_rs, spartan_team_ids_po }
"""

import requests
from bs4 import BeautifulSoup
import json, re, time, os
from datetime import datetime

# ── Regular-season config ──────────────────────────────────────────────────
RS_BASE   = "https://hockeysuperleague.ca"
RS_DIVS   = ["23371", "23373", "32765"]
RS_PRIME  = "23371"          # standings come from here
RS_START  = 1_620_000
RS_END    = 1_627_500
RS_SPARTAN = {"294811", "294868"}

# ── Playoff config ─────────────────────────────────────────────────────────
PO_BASE   = "https://hslchampionship.ca"
PO_DIVS   = ["22196", "36713"]
PO_PRIME  = "22196"
PO_START  = 1_828_000
PO_END    = 1_835_000
PO_SPARTAN = {"350513", "294868"}

# ── Shared ─────────────────────────────────────────────────────────────────
HEADERS    = {"User-Agent": "Mozilla/5.0 (compatible; HSL-stats-bot/1.0)"}
CACHE_FILE = "docs/game_cache.json"
OUT_FILE   = "docs/data.json"


# ── Cache ──────────────────────────────────────────────────────────────────
def load_cache():
    if not os.path.exists(CACHE_FILE):
        return set(), [], set(), []
    d = json.load(open(CACHE_FILE))
    return (set(d.get("rs_ids", [])), d.get("rs_games", []),
            set(d.get("po_ids", [])), d.get("po_games", []))

def save_cache(rs_ids, rs_games, po_ids, po_games):
    os.makedirs("docs", exist_ok=True)
    json.dump({"rs_ids": sorted(rs_ids), "rs_games": rs_games,
               "po_ids": sorted(po_ids), "po_games": po_games},
              open(CACHE_FILE, "w"), indent=2)


# ── Game parser ────────────────────────────────────────────────────────────
def parse_game(gid, base, divs, spartan_ids, prime_div, url_tmpl):
    """
    url_tmpl: e.g. "division/0/{div}/game/view/{gid}"
               or   "division/0/{div}/gamesheet/{gid}"
    """
    for div in divs:
        url = f"{base}/{url_tmpl.format(div=div, gid=gid)}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
        except Exception:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # find stat tables (G / A / PTS columns)
        ptables = []
        for tbl in soup.find_all("table"):
            hr = tbl.find("tr")
            if not hr: continue
            cols = [c.get_text(strip=True).upper()
                    for c in hr.find_all(["th","td"])]
            if "G" in cols and "A" in cols and "PTS" in cols:
                ptables.append(tbl)
        if len(ptables) < 2:
            continue

        # confirm division via player links inside the tables
        confirmed = any(f"/0/{div}/" in a["href"]
                        for tbl in ptables
                        for a in tbl.find_all("a", href=True))
        if not confirmed:
            continue

        txt = soup.get_text()
        sm  = re.search(r'(\d+)\s*[-–]\s*(\d+)\s*Final', txt)
        if not sm:
            break          # not final; same page regardless of div
        hs, as_ = int(sm.group(1)), int(sm.group(2))

        dm = re.search(
            r'(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday)'
            r',\s+(\w+ \d+,\s+\d{4})', txt)
        date = dm.group(0) if dm else ""

        gi = {"game_url": url, "division_id": div, "date": date,
              "home_score": hs, "away_score": as_,
              "home_team": "", "home_team_id": "",
              "away_team": "", "away_team_id": "",
              "players": []}

        SKIP = {"Scoring","Shots","Scoring Summary","Penalty Summary","Staff"}

        for i, tbl in enumerate(ptables[:2]):
            tname = f"Team {i+1}"
            for sib in tbl.find_all_previous(["h1","h2","h3","h4"]):
                t = sib.get_text(strip=True)
                if t and t not in SKIP:
                    tname = t; break
            if i == 0: gi["home_team"] = tname
            else:      gi["away_team"] = tname

            for row in tbl.find_all("tr")[1:]:
                cells = row.find_all("td")
                if len(cells) < 6: continue
                jersey = cells[0].get_text(strip=True)
                name   = cells[1].get_text(strip=True)
                if not name: continue

                pid, tid = "", ""
                lnk = cells[1].find("a", href=True)
                if lnk:
                    m = re.search(r'/team/\d+/0/(\d+)/(\d+)/player/(\d+)',
                                  lnk["href"])
                    if m:
                        if m.group(1) != div: continue
                        tid, pid = m.group(2), m.group(3)

                if tid:
                    if i == 0 and not gi["home_team_id"]: gi["home_team_id"] = tid
                    elif i == 1 and not gi["away_team_id"]: gi["away_team_id"] = tid

                try:
                    g, a, pts, pim = (int(cells[j].get_text(strip=True) or 0)
                                      for j in (2,3,4,5))
                except ValueError:
                    continue

                gi["players"].append(
                    {"name": name, "player_id": pid, "team": tname,
                     "team_id": tid, "jersey": jersey,
                     "g": g, "a": a, "pts": pts, "pim": pim})

        if not gi["players"]:
            return None

        both = {gi["home_team_id"], gi["away_team_id"]}
        if div != prime_div and not (spartan_ids & both):
            return None

        return gi
    return None


# ── Aggregation ────────────────────────────────────────────────────────────
def build_leaders(games):
    p = {}
    for g in games:
        for pl in g["players"]:
            k = f"{pl['player_id']}_{pl['team_id']}" if pl["player_id"] \
                else f"{pl['name']}_{pl['team_id']}"
            if k not in p:
                p[k] = {**pl, "g":0,"a":0,"pts":0,"pim":0,"gp":0}
            p[k]["g"]+=pl["g"]; p[k]["a"]+=pl["a"]
            p[k]["pts"]+=pl["pts"]; p[k]["pim"]+=pl["pim"]; p[k]["gp"]+=1
    return sorted(p.values(), key=lambda x:(x["pts"],x["g"]), reverse=True)

def build_spartan_leaders(games, spartan_ids):
    p = {}
    for g in games:
        for pl in g["players"]:
            if pl["team_id"] not in spartan_ids: continue
            k = pl["player_id"] if pl["player_id"] else pl["name"]
            if k not in p:
                p[k] = {**pl, "g":0,"a":0,"pts":0,"pim":0,"gp":0}
            p[k]["g"]+=pl["g"]; p[k]["a"]+=pl["a"]
            p[k]["pts"]+=pl["pts"]; p[k]["pim"]+=pl["pim"]; p[k]["gp"]+=1
    return sorted(p.values(), key=lambda x:(x["pts"],x["g"]), reverse=True)

def build_standings(games, prime):
    t = {}
    for g in games:
        if g["division_id"] != prime: continue
        hid,aid = g["home_team_id"],g["away_team_id"]
        hn,an   = g["home_team"],g["away_team"]
        hs,as_  = g["home_score"],g["away_score"]
        for tid,tnm in [(hid,hn),(aid,an)]:
            if not tid: continue
            t.setdefault(tid,{"team":tnm,"team_id":tid,
                               "gp":0,"w":0,"l":0,"otl":0,
                               "gf":0,"ga":0,"pts":0})
        if hid and aid:
            t[hid]["gp"]+=1; t[aid]["gp"]+=1
            t[hid]["gf"]+=hs; t[hid]["ga"]+=as_
            t[aid]["gf"]+=as_; t[aid]["ga"]+=hs
            if hs > as_:
                t[hid]["w"]+=1; t[hid]["pts"]+=2; t[aid]["l"]+=1
            elif as_ > hs:
                t[aid]["w"]+=1; t[aid]["pts"]+=2; t[hid]["l"]+=1
            else:
                t[hid]["otl"]+=1; t[aid]["otl"]+=1
                t[hid]["pts"]+=1; t[aid]["pts"]+=1
    return sorted(t.values(),
                  key=lambda x:(x["pts"],x["gf"]-x["ga"]), reverse=True)

def to_list(games):
    return [{"date":g["date"],"home_team":g["home_team"],
             "away_team":g["away_team"],"home_score":g["home_score"],
             "away_score":g["away_score"],"game_url":g["game_url"],
             "division_id":g["division_id"]} for g in games]


# ── Main ───────────────────────────────────────────────────────────────────
def scan(label, ids_range, processed, old_games,
         base, divs, spartan_ids, prime, url_tmpl):
    new_ids = sorted(set(range(*ids_range)) - processed)
    print(f"\n[{label}] {len(new_ids)} IDs to scan")
    new_games, newly = [], set()
    for i, gid in enumerate(new_ids):
        if i % 100 == 0 and i:
            print(f"  {i}/{len(new_ids)} scanned | {len(new_games)} found")
        game = parse_game(gid, base, divs, spartan_ids, prime, url_tmpl)
        newly.add(gid)
        if game:
            star = "⭐" if spartan_ids & {game["home_team_id"],
                                          game["away_team_id"]} else " "
            print(f"  ✓{star}[{game['division_id']}] "
                  f"{game['home_team']} {game['home_score']}-"
                  f"{game['away_score']} {game['away_team']}")
            new_games.append(game)
        time.sleep(0.3)
    return old_games + new_games, processed | newly

def main():
    print("=== HSL Spartan Stats Scraper ===")
    rs_proc, rs_games, po_proc, po_games = load_cache()

    rs_games, rs_proc = scan(
        "Regular Season", (RS_START, RS_END+1), rs_proc, rs_games,
        RS_BASE, RS_DIVS, RS_SPARTAN, RS_PRIME,
        "division/0/{div}/game/view/{gid}")

    po_games, po_proc = scan(
        "Playoffs", (PO_START, PO_END+1), po_proc, po_games,
        PO_BASE, PO_DIVS, PO_SPARTAN, PO_PRIME,
        "division/0/{div}/gamesheet/{gid}")

    save_cache(rs_proc, rs_games, po_proc, po_games)

    out = {
        "updated":             datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "spartan_team_ids_rs": list(RS_SPARTAN),
        "spartan_team_ids_po": list(PO_SPARTAN),
        "regular_season": {
            "games_processed": len(rs_games),
            "spartan_leaders": build_spartan_leaders(rs_games, RS_SPARTAN),
            "leaders":         build_leaders(rs_games),
            "standings":       build_standings(rs_games, RS_PRIME),
            "games":           to_list(rs_games),
        },
        "playoffs": {
            "games_processed": len(po_games),
            "spartan_leaders": build_spartan_leaders(po_games, PO_SPARTAN),
            "leaders":         build_leaders(po_games),
            "standings":       build_standings(po_games, PO_PRIME),
            "games":           to_list(po_games),
        },
    }
    os.makedirs("docs", exist_ok=True)
    json.dump(out, open(OUT_FILE,"w"), indent=2)
    print(f"\n✓ {OUT_FILE} written — RS {len(rs_games)} games, PO {len(po_games)} games")

if __name__ == "__main__":
    main()
