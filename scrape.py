"""
HSL Spartan Hockey Stats Scraper
=================================
Usage:
  python scrape.py --mode rs      # Regular season only
  python scrape.py --mode po      # Playoffs only
  python scrape.py                # Both

Strategy:
  1. Try to discover game IDs from schedule pages (fast, ~20 requests)
  2. If too few found (<10), fall back to windowed ID scan around known anchors
  3. Incremental cache means reruns only fetch new games

Regular Season  (hockeysuperleague.ca)
  23371  2014 Major  — all games + standings
  23373  2015 Major  — Spartan games only
  32765  2018 Major  — Spartan games only
  Anchor: 1626651  →  scan 1623000–1628000

Playoffs  (hslchampionship.ca  /gamesheet/ URLs)
  22196  2014 Major playoffs — all games + standings
  36713  2018 Major playoffs — Spartan games only
  Anchor: 1830371  →  scan 1828000–1834000

Spartan team IDs
  RS : 294811 (2014/2015 Major),  294868 (2018 Major)
  PO : 350513 (2014 Major champ), 294868 (2018 Major champ)
"""

import argparse
import requests
from bs4 import BeautifulSoup
import json, re, time, os
from datetime import datetime

# ── Regular season ─────────────────────────────────────────────────────────
RS_BASE    = "https://hockeysuperleague.ca"
RS_DIVS    = ["23371", "23373", "32765"]
RS_PRIME   = "23371"
RS_SPARTAN = {"294811", "294868"}
RS_SCAN    = (1623000, 1628000)   # anchored around known ID 1626651

# ── Playoffs ───────────────────────────────────────────────────────────────
PO_BASE    = "https://hslchampionship.ca"
PO_DIVS    = ["22196", "36713"]
PO_PRIME   = "22196"
PO_SPARTAN = {"350513", "294868"}
PO_SCAN    = (1828000, 1834000)   # anchored around known IDs 1830371, 1830410

# ── Shared ─────────────────────────────────────────────────────────────────
HEADERS    = {"User-Agent": "Mozilla/5.0 (compatible; HSL-stats-bot/1.0)"}
CACHE_FILE = "docs/game_cache.json"
OUT_FILE   = "docs/data.json"
SLEEP      = 0.2


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


# ── Discover game IDs from schedule pages ──────────────────────────────────
def discover_ids(base, div, link_pattern):
    ids = set()
    for path in ["masterschedule", "games"]:
        url = f"{base}/division/0/{div}/{path}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            for m in re.finditer(link_pattern, r.text):
                ids.add(int(m.group(1)))
        except Exception as e:
            print(f"  [{div}/{path}] failed: {e}")
        time.sleep(0.2)
    return ids


# ── Parse one game ─────────────────────────────────────────────────────────
def parse_game(gid, base, div, url_tmpl):
    url = f"{base}/{url_tmpl.format(div=div, gid=gid)}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    txt  = soup.get_text()

    sm = re.search(r'(\d+)\s*[-\u2013]\s*(\d+)\s*Final', txt)
    if not sm:
        return None

    ptables = []
    for tbl in soup.find_all("table"):
        hr = tbl.find("tr")
        if not hr: continue
        cols = [c.get_text(strip=True).upper() for c in hr.find_all(["th","td"])]
        if "G" in cols and "A" in cols and "PTS" in cols:
            ptables.append(tbl)
    if len(ptables) < 2:
        return None

    if not any(f"/0/{div}/" in a["href"]
               for tbl in ptables for a in tbl.find_all("a", href=True)):
        return None

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
                m2 = re.search(r'/team/\d+/0/(\d+)/(\d+)/player/(\d+)', lnk["href"])
                if m2:
                    if m2.group(1) != div: continue
                    tid, pid = m2.group(2), m2.group(3)

            if tid:
                if i == 0 and not gi["home_team_id"]:   gi["home_team_id"] = tid
                elif i == 1 and not gi["away_team_id"]: gi["away_team_id"] = tid

            try:
                g, a, pts, pim = (int(cells[j].get_text(strip=True) or 0)
                                  for j in (2,3,4,5))
            except ValueError:
                continue

            gi["players"].append({
                "name": name, "player_id": pid, "team": tname,
                "team_id": tid, "jersey": jersey,
                "g": g, "a": a, "pts": pts, "pim": pim})

    return gi if gi["players"] else None


# ── Aggregation ────────────────────────────────────────────────────────────
def build_leaders(games):
    p = {}
    for g in games:
        for pl in g["players"]:
            k = (f"{pl['player_id']}_{pl['team_id']}" if pl["player_id"]
                 else f"{pl['name']}_{pl['team_id']}")
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
        hid, aid = g["home_team_id"], g["away_team_id"]
        hn,  an  = g["home_team"],    g["away_team"]
        hs,  as_ = g["home_score"],   g["away_score"]
        for tid, tnm in [(hid,hn),(aid,an)]:
            if not tid: continue
            t.setdefault(tid, {"team":tnm,"team_id":tid,
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


# ── Core scan ──────────────────────────────────────────────────────────────
def run_scan(label, base, divs, prime, spartan_ids,
             url_tmpl, link_pattern, scan_range,
             processed_ids, cached_games):

    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")

    # Step 1: try schedule pages
    candidate_ids = set()
    for div in divs:
        candidate_ids |= discover_ids(base, div, link_pattern)

    # Step 2: if schedule pages didn't yield enough, fall back to window scan
    print(f"\n  Schedule pages found {len(candidate_ids)} IDs", end="")
    if len(candidate_ids) < 50:
        print(f" — too few, falling back to window scan {scan_range}")
        candidate_ids = set(range(scan_range[0], scan_range[1] + 1))
    else:
        print()

    new_ids = sorted(candidate_ids - processed_ids)
    print(f"  {len(candidate_ids)} total · {len(processed_ids)} cached · "
          f"{len(new_ids)} to fetch\n")

    new_games  = []
    newly_proc = set()

    for i, gid in enumerate(new_ids):
        if i > 0 and i % 200 == 0:
            print(f"  ... {i}/{len(new_ids)} scanned, {len(new_games)} games found so far")

        game = None
        for div in divs:
            g = parse_game(gid, base, div, url_tmpl)
            if not g:
                continue
            both = {g["home_team_id"], g["away_team_id"]}
            if g["division_id"] != prime and not (spartan_ids & both):
                break
            game = g
            break

        newly_proc.add(gid)

        if game:
            star = "⭐" if spartan_ids & {game["home_team_id"],
                                          game["away_team_id"]} else " "
            print(f"  ✓{star}[{game['division_id']}] "
                  f"{game['home_team']} {game['home_score']}-"
                  f"{game['away_score']} {game['away_team']}  ({game['date']})")
            new_games.append(game)

        time.sleep(SLEEP)

    all_games = cached_games + new_games
    all_proc  = processed_ids | newly_proc
    print(f"\n  {label} done: {len(new_games)} new · {len(all_games)} total")
    return all_games, all_proc


# ── Write output ───────────────────────────────────────────────────────────
def write_output(rs_games, po_games):
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
    json.dump(out, open(OUT_FILE, "w"), indent=2)
    print(f"\n✓ Wrote {OUT_FILE}  "
          f"(RS {len(rs_games)} games · PO {len(po_games)} games)")


# ── Entry point ────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["rs","po","both"], default="both")
    args = parser.parse_args()

    print(f"=== HSL Spartan Stats Scraper  [mode={args.mode}] ===")
    rs_proc, rs_games, po_proc, po_games = load_cache()
    print(f"Cache: {len(rs_games)} RS games · {len(po_games)} PO games")

    if args.mode in ("rs", "both"):
        rs_games, rs_proc = run_scan(
            "REGULAR SEASON",
            RS_BASE, RS_DIVS, RS_PRIME, RS_SPARTAN,
            "division/0/{div}/game/view/{gid}",
            r'/game/view/(\d+)',
            RS_SCAN,
            rs_proc, rs_games)

    if args.mode in ("po", "both"):
        po_games, po_proc = run_scan(
            "PLAYOFFS",
            PO_BASE, PO_DIVS, PO_PRIME, PO_SPARTAN,
            "division/0/{div}/gamesheet/{gid}",
            r'/gamesheet/(\d+)',
            PO_SCAN,
            po_proc, po_games)

    save_cache(rs_proc, rs_games, po_proc, po_games)
    write_output(rs_games, po_games)


if __name__ == "__main__":
    main()
