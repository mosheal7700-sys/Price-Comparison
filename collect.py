#!/usr/bin/env python3
"""Collect Israeli supermarket prices and write static JSON files for the site.

Usage:
    python collect.py --out site/data [--config config.json] [--prev-base URL]

For every city in config.json the script picks a few branches per chain,
downloads each branch's latest full price file (price-transparency law),
lines products up by barcode and writes:

    <out>/index.json          cities, run time and per-chain status
    <out>/city-<key>.json     branches and prices for one city

If a chain cannot be reached, the last published data for its branches is
taken from --prev-base (the live site) and marked as stale, so one failing
portal never empties the page. The script exits 0 even when portals fail;
check index.json -> chains for the status of each chain.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import threading
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import israeli_prices as ilp

RETRY_DELAY = float(os.environ.get("COLLECT_RETRY_DELAY", "4"))
WORKERS = int(os.environ.get("COLLECT_WORKERS", "4"))


def log(msg: str) -> None:
    print(msg, flush=True)


def retry(fn, tries: int = 3):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as exc:  # portals fail in many different ways
            last = exc
            if i < tries - 1:
                time.sleep(RETRY_DELAY * (i + 1))
    raise last


def _age_hours(published) -> float | None:
    """Hours since a portal file was published (file names carry Israel local time)."""
    if published is None:
        return None
    try:
        if published.tzinfo is None:
            try:
                from zoneinfo import ZoneInfo
                published = published.replace(tzinfo=ZoneInfo("Asia/Jerusalem"))
            except Exception:
                published = published.replace(tzinfo=timezone(timedelta(hours=2)))
        return (datetime.now(timezone.utc) - published).total_seconds() / 3600
    except Exception:
        return None


def fetch_fresh(adapter, ftype, store_id, max_age_h: float, tries: int, wait: float):
    """Newest file of a type, asking the portal again when what it lists looks old.

    Chains' portals sometimes answer a listing with a stale or truncated view (the run
    after it is fine). If the newest file is older than max_age_h, list again a few
    times and keep the newest one seen. Returns (FileRef, bytes-parsed-later, age_h).
    """
    best, best_age = None, None
    for i in range(max(1, tries)):
        ref = retry(lambda: adapter.latest(ftype, store_id=store_id))
        age = _age_hours(ref.published_at)
        if best is None or (ref.published_at and (best.published_at is None or ref.published_at > best.published_at)):
            best, best_age = ref, age
        if age is None or age <= max_age_h:
            break
        if i < tries - 1:
            time.sleep(wait * (i + 1))
    return best, best_age


def fetch_prev(base: str | None, name: str):
    """Previously published JSON from the live site, or None."""
    if not base:
        return None
    try:
        with urllib.request.urlopen(f"{base.rstrip('/')}/{name}", timeout=30) as resp:
            return json.load(resp)
    except Exception:
        return None


def chain_name(slug: str, infos: dict) -> str:
    info = infos.get(slug)
    if not info:
        return slug
    return info.name_he or info.name or slug


def cnorm(s: str) -> str:
    """Normalize Hebrew place text: no quotes or hyphens, one spelling of קרית."""
    s = (s or "").replace('"', "").replace("'", "").replace("׳", "").replace("״", "")
    s = re.sub(r"[-־,.()/]+", " ", s)
    s = s.replace("קריית", "קרית")
    return " ".join(s.split())


def has_term(text: str, terms) -> bool:
    """True if one of the terms appears in the text as whole words."""
    hay = f" {cnorm(text)} "
    return any(f" {cnorm(t)} " in hay for t in terms)


_LOOSE_STOP = {"קג", "קילוגרם", "גרם", "גר", "יח", "יחידה", "יחידות", "ליטר", "מל", "לק", "טרי", "טריה", "טרייה",
               "טריים", "טריות", "ארוז", "ארוזה", "ארוזים", "ארוזות", "במשקל", "משקל", "קילו",
               "ישראלי", "ישראלית", "מובחר", "מובחרים", "מובחרות", "איכותי"}

_FINALS = str.maketrans("ךםןףץ", "כמנפצ")


def _stem(tok: str) -> str:
    """One Hebrew word without final letters, plural endings and a trailing ה, so that
    "מלפפון" and "מלפפונים", or "עגבניה" and "עגבניות", become the same key."""
    t = tok.translate(_FINALS)
    if len(t) > 4 and (t.endswith("ימ") or t.endswith("ות")):
        t = t[:-2]
    elif len(t) >= 4 and t.endswith("ה"):
        t = t[:-1]
    return t


_STOP_STEMS = {_stem(w) for w in _LOOSE_STOP}


def norm_name(name: str, keep_numbers: bool = False) -> str:
    """Matching key for products without a barcode: name without units, filler words,
    final letters and plural endings. Numbers (pack sizes) are dropped for items sold by weight
    and kept for fixed-size packs, so an 800 g pack is never merged with a 400 g pack."""
    s = re.sub(r'["\'׳״]', "", name)
    if keep_numbers:
        s = re.sub(r"[\-־()*/,+:]+", " ", s)
    else:
        s = re.sub(r"[\d\-־()*/,.%+:]+", " ", s)
    toks = [_stem(t) for t in s.split()]
    return " ".join(t for t in toks if t and t not in _STOP_STEMS)


def item_name(it) -> str:
    """The item's name; when a chain cut it short (many portals stop at 20 characters) and the
    manufacturer description carries the same text in full, use that."""
    nm = (it.name or "").strip()
    md = (getattr(it, "manufacturer_description", None) or "").strip()
    if md and len(md) > len(nm) and md.startswith(nm):
        return md
    return nm


def best_name(votes: Counter) -> str:
    """Most common spelling, where a name that is only the beginning of a longer one counts for the longer one
    (one chain's cut-off 'סנו סושי - שקיות אוכ' supports another chain's full 'סנו סושי - שקיות אוכל')."""
    names = list(votes)
    score = {n: votes[n] for n in names}
    for a in names:
        for b in names:
            if len(b) > len(a) and b.startswith(a):
                score[b] += votes[a]
    return max(names, key=lambda n: (score[n], len(n)))


def complete_names(out: Path, index_cities: list, longest: dict) -> int:
    """Some branches publish product names cut short (often at 20 characters) while another branch of the
    same chain, in another city, publishes the full text for the same barcode. After every city is built,
    replace a cut name with the longer one that starts with it."""
    fixed = 0
    for c in index_cities:
        path = out / c["file"]
        data = json.loads(path.read_text(encoding="utf-8"))
        changed = False
        for p in data.get("products", []):
            full = longest.get(p["id"])
            if full and len(full) > len(p["n"]) and full.startswith(p["n"]):
                p["n"] = full
                changed = True
                fixed += 1
        if changed:
            path.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    return fixed


def _is_code(v) -> bool:
    return bool(re.fullmatch(r"\d+", str(v or "").strip())) and str(v).strip().lstrip("0") != ""


def pick_stores(stores, terms, limit, fuzzy=False, codes=None):
    """Branches whose city, address or name contains one of the city terms,
    or whose city field is one of the city's numeric (CBS) codes."""
    has = fuzzy_has if fuzzy else has_term
    scored = []
    seen = set()
    for s in stores:
        if s.store_id in seen:
            continue
        in_city = (bool(s.city) and has(s.city, terms)) or bool(codes and _is_code(s.city) and str(s.city).strip().lstrip('0') in codes)
        text = " ".join(x for x in (s.address, s.name) if x)
        in_text = has(text, terms)
        if in_city or in_text:
            seen.add(s.store_id)
            scored.append((0 if in_city else 1, s))
    scored.sort(key=lambda x: x[0])
    return [s for _, s in scored[:limit]]


# Spellings and abbreviations chains use for city names. Applied only as a fallback,
# when an exact match finds no branch of a chain in a city.
_CITY_ALIASES = {
    "ראשון לציון": ["ראשלצ", "ראשל צ"],
    "תל אביב יפו": ["תל אביב", "תא", "ת א"],
    "תל אביב": ["תל אביב יפו", "תא", "ת א"],
    "ירושלים": ["ירושלם"],
    "פתח תקווה": ["פתח תקוה", "פת", "פ ת"],
    "באר שבע": ["בש", "ב ש", "באר שבע"],
    "רמת גן": ["רג", "ר ג"],
    "קריית גת": ["קרית גת"],
    "בני ברק": ["ב ב"],
    "נס ציונה": ["נס ציונא"],
    "הוד השרון": ["הוד השרון"],
    "כפר סבא": ["כפר סבא"],
}


def _lev1(a: str, b: str) -> bool:
    """True if a and b differ by at most one inserted, deleted or replaced letter."""
    if a == b:
        return True
    if abs(len(a) - len(b)) > 1:
        return False
    if len(a) > len(b):
        a, b = b, a
    i = 0
    while i < len(a) and a[i] == b[i]:
        i += 1
    if len(a) == len(b):
        return a[i + 1:] == b[i + 1:]
    return a[i:] == b[i + 1:]


def fuzzy_has(text: str, terms) -> bool:
    """Like has_term, but a word of five letters or more may be misspelled by one letter,
    and known abbreviations of the city are accepted."""
    toks = cnorm(text).split()
    for t in terms:
        alts = [cnorm(t)] + [cnorm(x) for x in _CITY_ALIASES.get(cnorm(t), [])]
        for alt in alts:
            w = alt.split()
            if not w:
                continue
            for i in range(len(toks) - len(w) + 1):
                if all(toks[i + k] == w[k] or (len(w[k]) >= 5 and _lev1(toks[i + k], w[k])) for k in range(len(w))):
                    return True
    return False


def size_label(item) -> str:
    try:
        if item.quantity is not None and item.unit_qty:
            q = format(Decimal(item.quantity).normalize(), "f")
            return f"{q} {item.unit_qty}".strip()
    except Exception:
        pass
    return ""


def branch_label(store) -> str:
    """Branch name; when the name says nothing but the city (or is empty or a bare number), the street address is added."""
    name = (store.name or "").strip()
    addr = (store.address or "").strip()
    city = (getattr(store, "city", "") or "").strip()
    rest = cnorm(name)
    for w in cnorm(city).split():
        rest = rest.replace(w, " ")
    only_city = not re.sub(r"[\W_]+", "", rest)
    bare = not re.sub(r"[\d\W_]+|סניף|חנות|מספר", "", rest)
    if addr and (only_city or not name):
        label = addr
    elif addr and bare:
        label = f"{name}, {addr}"
    else:
        label = name or store.store_id or ""
    return label.strip()[:60]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--out", default="data")
    ap.add_argument("--prev-base", default=os.environ.get("PREV_BASE"))
    args = ap.parse_args()

    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    chains_cfg = cfg["chains"]
    per_chain = int(cfg.get("stores_per_chain", 2))
    # branches downloaded per chain and city; the fullest ones become the columns, the rest only fill gaps
    cand_cfg = int(cfg.get("candidate_stores", 6))
    cand = 10**6 if cand_cfg <= 0 else max(per_chain, cand_cfg)  # 0 = every branch in the city
    fill_gaps = bool(cfg.get("fill_gaps", True))
    max_age_h = float(cfg.get("max_file_age_hours", 36))   # older price files are re-listed (portals sometimes serve a stale view)
    age_tries = int(cfg.get("stale_retries", 3))
    age_wait = float(cfg.get("stale_retry_wait", 20))
    fuzzy = bool(cfg.get("fuzzy_city_match", True))  # retry with spelling variants when a chain has no branch in a city
    min_chains = int(cfg.get("min_chains", 1))
    min_city_chains = int(cfg.get("min_city_chains", 1))   # a city is listed on the site only when at least this many chains have a branch there
    hidden_cities: list[str] = []
    max_products = int(cfg.get("max_products", 0)) or 10**9  # 0 = no limit
    exclude_names = [str(x).strip() for x in cfg.get("exclude_names", ["דמי משלוח"]) if str(x).strip()]   # fees and other non-products
    include_loose = bool(cfg.get("include_unbarcoded", True))
    loose_min_chains = int(cfg.get("loose_min_chains", 2))
    max_loose = int(cfg.get("max_loose", 0)) or 10**9  # 0 = no limit
    # product names or item codes to trace in the log: what was found, and whether it made it into the site
    watch = [str(w).strip() for w in cfg.get("watch", []) if str(w).strip()]
    watch_f = [(w, w.translate(_FINALS)) for w in watch]
    cities_cfg = [c for c in cfg["cities"] if c.get("enabled", True)]
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    infos = {c.slug: c for c in ilp.list_chains()}
    status: dict[str, dict] = {}

    # 1. branch lists
    def stores_task(slug):
        if slug not in infos:
            return slug, None, "unknown chain"
        try:
            ad = ilp.get_adapter(slug)
            ref, age = fetch_fresh(ad, ilp.FileType.STORES, None, max_age_h * 2, age_tries, age_wait)
            if age is not None and age > max_age_h * 2:
                log(f"[fresh] {slug}: branch list file is {age:.0f}h old even after {age_tries} tries")
            return slug, ilp.parse(retry(lambda: ad.download(ref))).stores, None
        except Exception as exc:
            return slug, None, str(exc)[:200]

    store_lists: dict[str, list] = {}
    with ThreadPoolExecutor(WORKERS) as ex:
        for slug, stores, err in ex.map(stores_task, chains_cfg):
            status[slug] = {"name": chain_name(slug, infos), "ok": err is None}
            if err:
                status[slug]["error"] = err
                log(f"[stores] {slug}: FAILED {err}")
            else:
                store_lists[slug] = stores
                log(f"[stores] {slug}: {len(stores)} branches")
                # how the chain spells its cities: shows why a branch is or is not matched to a city
                cc: dict[str, int] = {}
                for st_ in stores:
                    k_ = (getattr(st_, "city", "") or "").strip() or "(no city)"
                    cc[k_] = cc.get(k_, 0) + 1
                top_ = sorted(cc.items(), key=lambda kv: -kv[1])[:120]
                log(f"[cities] {slug}: " + ", ".join(f"{k}({n})" for k, n in top_))

    # 2. which branches serve which city
    prev_cache: dict[str, dict | None] = {}

    def get_prev(key):
        if key not in prev_cache:
            prev_cache[key] = fetch_prev(args.prev_base, f"city-{key}.json")
        return prev_cache[key]

    # Some chains (Rami Levy, Osher Ad) record a branch's city as a numeric CBS code (3000 = Jerusalem),
    # and their branch names often omit the city. Codes come from config ("code") and are also learned:
    # a code seen on at least two branches already matched to the city by name or address belongs to it.
    city_codes: dict[str, set] = {}
    for city in cities_cfg:
        cfg_code = city.get("code")
        codes = {str(x).strip().lstrip("0") for x in (cfg_code if isinstance(cfg_code, list) else [cfg_code]) if x}
        votes: dict[str, int] = {}
        for slug_, sts_ in store_lists.items():
            for st_ in pick_stores(sts_, city["match"], 10**6):
                if _is_code(st_.city):
                    k_ = str(st_.city).strip().lstrip("0")
                    votes[k_] = votes.get(k_, 0) + 1
        tot_ = sum(votes.values())
        learned = {k for k, n in votes.items() if n >= 2 and n >= 0.6 * tot_}
        if learned and codes and not (learned & codes):
            log(f"[codes] {city['name']}: config code {sorted(codes)} disagrees with codes seen on matched branches {votes}; using both")
        city_codes[city["key"]] = codes | learned
        if city_codes[city["key"]]:
            log(f"[codes] {city['name']}: {sorted(city_codes[city['key']])}" + (f" (learned {sorted(learned)})" if learned else ""))

    plans = []  # (city, entries)
    jobs: dict[tuple, None] = {}
    for city in cities_cfg:
        entries = []
        match_counts: dict[str, int] = {}
        for slug in chains_cfg:
            if slug in store_lists:
                pool = pick_stores(store_lists[slug], city["match"], 10**6, codes=city_codes.get(city["key"]))
                if not pool and fuzzy:
                    pool = pick_stores(store_lists[slug], city["match"], 10**6, fuzzy=True, codes=city_codes.get(city["key"]))
                    if pool:
                        log(f"[match] {city['name']}/{slug}: no exact match, {len(pool)} branches found by spelling variants")

                def _find(term):
                    """'id:34' = the branch whose store number is 34 (searched in the whole chain);
                    anything else = a word in the branch name or address (searched in the city)."""
                    if not term:
                        return []
                    if str(term).startswith("id:"):
                        want = str(term)[3:].strip().lstrip("0")
                        return [x for x in store_lists[slug] if str(x.store_id).strip().lstrip("0") == want]
                    return [x for x in pool if has_term(" ".join(y for y in (x.address, x.name) if y), [term])]

                def _list_pool():
                    for x in pool[:40]:
                        log(f'[pin]   {slug} branch {x.store_id}: name="{x.name}" address="{x.address}" city="{getattr(x, "city", "")}"')

                match_counts[slug] = len(pool)
                pin = (city.get("branches") or {}).get(slug)
                pinned = _find(pin)
                if pin and not pinned:
                    log(f'[pin] {city["name"]}/{slug}: no branch matches "{pin}", choosing by size')
                    _list_pool()
                xterm = (city.get("extra_branches") or {}).get(slug)
                extra = [x for x in _find(xterm) if x not in pinned]
                if xterm and not extra:
                    log(f'[pin] {city["name"]}/{slug}: no branch matches extra "{xterm}"')
                    _list_pool()
                rest = [x for x in pool if x not in pinned and x not in extra]
                for s in pinned + extra + rest[:cand]:
                    custom = (city.get("labels") or {}).get(f"{slug}:{str(s.store_id).strip().lstrip('0')}")
                    entries.append({"chain": slug, "store_id": s.store_id, "label": custom or branch_label(s),
                                    "pin": s in pinned, "extra": s in extra})
                    jobs[(slug, s.store_id)] = None
            else:
                prev = get_prev(city["key"])
                if prev:
                    for st in prev.get("stores", []):
                        if st["chain"] == slug:
                            sid = st["id"].split(":", 1)[1]
                            entries.append({"chain": slug, "store_id": sid, "label": st.get("b", "")})
        if match_counts:
            miss = [k for k, n in match_counts.items() if n == 0]
            log(f"[match] {city['name']}: " + " ".join(f"{k}={n}" for k, n in match_counts.items())
                + (f"  <-- no branch found for: {', '.join(miss)}" if miss else ""))
        plans.append((city, entries))

    # 3. one city at a time: download its price files, write the city file, free the memory.
    # (Holding every parsed price file at once would not fit in the memory of a GitHub runner.)
    # When a portal answers "no PriceFull file for store N" for most stores of a chain (a flaky or truncated
    # listing, often when many sessions ask at once), the chain is listed once, one request at a time, and the
    # stores are looked up in that single listing.
    chain_lists: dict[str, list] = {}
    chain_lock = threading.Lock()
    diag_done: set[str] = set()

    def chain_listing(chain, ad):
        with chain_lock:
            if chain not in chain_lists:
                refs = []
                for i in range(3):
                    try:
                        refs = list(ad.iter_files(file_type=ilp.FileType.PRICE_FULL))
                    except Exception as exc:
                        log(f"[listing] {chain}: attempt {i + 1} failed: {str(exc)[:120]}")
                        refs = []
                    if refs:
                        break
                    time.sleep(RETRY_DELAY * (i + 1))
                chain_lists[chain] = refs
                log(f"[listing] {chain}: one full listing, {len(refs)} PriceFull files for {len({r.store_id for r in refs})} stores")
            return chain_lists[chain]

    def from_listing(chain, sid, ad):
        if not hasattr(ad, "iter_files"):
            return None
        from israeli_prices.chains.base import same_store
        best = None
        for r in chain_listing(chain, ad):
            if same_store(r.store_id, sid) and (best is None or (r.published_at and (best.published_at is None or r.published_at > best.published_at))):
                best = r
        return best

    def price_task(job):
        chain, sid = job
        try:
            ad = ilp.get_adapter(chain)
            try:
                ref, age = fetch_fresh(ad, ilp.FileType.PRICE_FULL, sid, max_age_h, age_tries, age_wait)
            except Exception as exc:
                if "no PriceFull" not in str(exc):
                    raise
                ref = from_listing(chain, sid, ad)
                if ref is None:
                    with chain_lock:
                        first = chain not in diag_done
                        diag_done.add(chain)
                    if first and hasattr(ad, "iter_files"):
                        try:
                            names = [r.name for r in ad.iter_files(store_id=sid)][:6]
                        except Exception as e2:
                            names = [f"(listing failed: {str(e2)[:80]})"]
                        log(f"[listing] {chain}/{sid}: no PriceFull in the portal; files listed for this store: {names or 'none'}")
                    raise
                age = _age_hours(ref.published_at)
                log(f"[listing] {chain}/{sid}: found in the chain-wide listing after the per-store listing missed it")
            pf = ilp.parse(retry(lambda: ad.download(ref)))
            file_ts[job] = ref.published_at
            if age is not None and age > max_age_h:
                log(f"[fresh] {chain}/{sid}: newest price file is {age:.0f}h old even after {age_tries} tries")
            return job, pf, None
        except Exception as exc:
            return job, None, str(exc)[:200]

    # barcode -> the longest spelling of its name. Seeded from the last published run, so a full name learned once
    # (e.g. in a city whose branch spells it out) is kept even when a later run covers fewer cities.
    learned_prev = fetch_prev(args.prev_base, "names-full.json")
    longest: dict[str, str] = {k: v for k, v in (learned_prev or {}).items() if isinstance(k, str) and isinstance(v, str)}
    learned: dict[str, str] = dict(longest)     # what gets published for the next run
    file_ts: dict[tuple, object] = {}   # (chain, store id) -> when the portal published the file we used
    counters = {slug: {"ok": 0, "failed": 0, "error": None} for slug in chains_cfg}
    pool = ThreadPoolExecutor(WORKERS)
    started = time.time()

    # 4. build one file per city
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    index_cities = []
    for n, (city, entries) in enumerate(plans, 1):
        files: dict[tuple, object] = {}
        city_jobs = list({(e["chain"], e["store_id"]) for e in entries if e["chain"] in store_lists})
        for job, pf, err in pool.map(price_task, city_jobs):
            if err:
                counters[job[0]]["failed"] += 1
                counters[job[0]]["error"] = counters[job[0]]["error"] or err
                log(f"[prices] {job[0]}/{job[1]}: FAILED {err}")
            else:
                counters[job[0]]["ok"] += 1
                files[job] = pf
        log(f"[{n}/{len(plans)}] {city['name']}: {len(files)}/{len(city_jobs)} files "
            f"({int(time.time() - started)}s elapsed)")
        # keep, per chain, the branches with the most items (a full-range branch carries frozen, fish, deli...);
        # the other downloaded branches of the chain only fill items the kept branches lack
        by_chain: dict[str, list] = {}
        for e in entries:
            by_chain.setdefault(e["chain"], []).append(e)
        entries, extras = [], {}
        # a branch that failed to download this time is shown from the last published data (marked stale), so that
        # a chain with two columns (e.g. an extra_branches branch) never silently drops to one
        failed_any = any((e["chain"], e["store_id"]) not in files for es_ in by_chain.values() for e in es_)
        prev = get_prev(city["key"]) if failed_any else None
        prev_idx = {}
        if prev:
            prev_idx = {s["id"]: i for i, s in enumerate(prev.get("stores", []))}
        for ch, es in by_chain.items():
            got = [e for e in es if (e["chain"], e["store_id"]) in files]
            if got:
                got.sort(key=lambda e: (not e.get("pin"), -len(files[(e["chain"], e["store_id"])].items)))
                add = [e for e in got if e.get("extra")]          # extra_branches: always a column of their own
                main = [e for e in got if not e.get("extra")]
                kept = main[:per_chain] + add
                if fill_gaps:
                    extras[ch] = [files[(e["chain"], e["store_id"])] for e in main[per_chain:]]
            else:
                kept = []
            if prev_idx:
                miss_x = [e for e in es if e.get("extra") and (e["chain"], e["store_id"]) not in files and f'{e["chain"]}:{e["store_id"]}' in prev_idx]
                room = per_chain - len([e for e in kept if not e.get("extra")])
                miss_m = [e for e in es if not e.get("extra") and (e["chain"], e["store_id"]) not in files and f'{e["chain"]}:{e["store_id"]}' in prev_idx][:max(0, room)]
                kept += miss_x + miss_m
                if miss_x or miss_m:
                    log(f"[stale] {city['name']}/{ch}: {len(miss_x) + len(miss_m)} branch(es) not downloaded, shown from the previous run")
            if not kept:
                kept = es[:per_chain]
            entries += kept

        columns = []  # (store dict, {gtin: price}, fresh)
        names: dict[str, Counter] = {}
        chain_names: dict[str, dict] = {}     # barcode -> chain -> names it uses (to spot a code reused for different things)
        weighted_ch: dict[str, set] = {}      # barcode -> chains that sell it by weight
        sizes: dict[str, Counter] = {}
        carriers: dict[str, set] = {}
        weighted: set[str] = set()
        watch_hits: dict[str, list] = {}
        prices_seen: dict[str, list] = {}

        filled: set[str] = set()
        for e in entries:
            sid = f'{e["chain"]}:{e["store_id"]}'
            sd = {"id": sid, "chain": e["chain"], "cn": status[e["chain"]]["name"], "b": e["label"]}
            pf = files.get((e["chain"], e["store_id"]))
            fts = file_ts.get((e["chain"], e["store_id"]))
            if pf is not None and fts is not None:
                sd["ts"] = fts.isoformat(timespec="minutes")   # file time (Israel local time), shown when the data is old
            prices: dict[str, float] = {}
            if pf is not None:
                srcs = [(pf, False)]
                if e["chain"] not in filled:
                    filled.add(e["chain"])
                    srcs += [(x, True) for x in extras.get(e["chain"], [])]
                base_keys = None
                for src, is_extra in srcs:
                    if is_extra:
                        base_keys = set(prices)  # items from larger branches (processed first) win
                    for it in src.items:
                        if it.price is None or it.price <= 0 or not it.name or not it.name.strip():
                            continue
                        if exclude_names and any(x in it.name for x in exclude_names):
                            continue
                        g = it.gtin
                        um = (it.unit_of_measure or "").replace('"', "").replace("'", "").replace("׳", "").replace("״", "")
                        by_weight = bool(it.is_weighted) or um.startswith("קג") or um.startswith("קילו")
                        if g:
                            key = g
                        elif include_loose:
                            # no barcode (produce, fish, bakery...): match across chains by normalized name
                            nn = norm_name(it.name, keep_numbers=not by_weight)
                            if len(nn) < 3:
                                continue
                            key = "n:" + nn
                        else:
                            continue
                        if is_extra and key in base_keys:
                            continue  # the kept branch has it: its price wins
                        if watch_f:
                            nm_f, code = it.name.translate(_FINALS), str(it.item_code)
                            for w, wf in watch_f:
                                if wf in nm_f or w in code:
                                    _md = (getattr(it, "manufacturer_description", None) or "").strip()
                                    shown = it.name.strip() + (f' [{len(it.name.strip())} chars]') + (f' | manufacturer description: {_md}' if _md and _md != it.name.strip() else '')
                                    watch_hits.setdefault(w, []).append((e["chain"], code, shown, key))
                                    prices_seen.setdefault(key, []).append(f'{e["chain"]}={float(it.price):g}')
                        price = float(round(it.price, 2))
                        if not g and key in prices:
                            price = min(price, prices[key])  # several variants of one name: keep the lowest
                        prices[key] = price
                        names.setdefault(key, Counter())[item_name(it)] += 1
                        if by_weight:
                            weighted.add(key)
                        if g:
                            chain_names.setdefault(key, {}).setdefault(e["chain"], Counter())[item_name(it)] += 1
                            if by_weight:
                                weighted_ch.setdefault(key, set()).add(e["chain"])
                            sz = size_label(it)
                            if sz:
                                sizes.setdefault(key, Counter())[sz] += 1
                        carriers.setdefault(key, set()).add(e["chain"])
                columns.append((sd, prices, True))
            elif prev and sid in prev_idx:
                col = prev_idx[sid]
                for p in prev.get("products", []):
                    v = p["p"][col] if col < len(p["p"]) else None
                    if v is not None:
                        prices[p["id"]] = v
                sd["stale"] = True
                # keep the time the data really comes from, also when it has been carried over several runs
                sd["as_of"] = prev["stores"][col].get("as_of") or prev.get("updated_at")
                columns.append((sd, prices, False))

        if not columns:
            log(f"[city] {city['name']}: no data, skipped")
            continue

        present_chains = {c[0]["chain"] for c in columns}      # stale columns count: a chain that failed this time still has its last data
        if len(present_chains) < min_city_chains:
            hidden_cities.append(f"{city['name']} ({len(present_chains)})")
            log(f"[city] {city['name']}: only {len(present_chains)} chain(s) with a branch here, min_city_chains={min_city_chains}: not listed")
            files.clear()
            prev_cache.pop(city['key'], None)
            continue
        fresh_chains = {c[0]["chain"] for c in columns if c[2]}
        need = min(min_chains, max(1, len(fresh_chains)))
        need_loose = min(loose_min_chains, max(1, len(fresh_chains)))

        # A code that chains use for different things (a retailer's internal code, e.g. 7290000000091 is pepper
        # at one chain and a delivery fee at another) is no barcode. When the names used for one weighed code
        # share no word across chains, split it: every group of chains is keyed by its own name, like any
        # product without a barcode, so a delivery fee is no longer shown as pepper.
        def split_conflicts():
            moved = []
            for key in list(chain_names):
                per = chain_names[key]
                if len(per) < 2 or key not in weighted:
                    continue
                rep_name = {ch: cnt.most_common(1)[0][0] for ch, cnt in per.items()}
                toks = {ch: set(norm_name(n).split()) for ch, n in rep_name.items()}
                parent = {ch: ch for ch in per}
                def find(x):
                    while parent[x] != x:
                        parent[x] = parent[parent[x]]
                        x = parent[x]
                    return x
                chs = list(per)
                for i_, a_ in enumerate(chs):
                    for b_ in chs[i_ + 1:]:
                        na, nb = rep_name[a_], rep_name[b_]
                        if (toks[a_] & toks[b_]) or na.startswith(nb) or nb.startswith(na):
                            parent[find(a_)] = find(b_)
                clusters: dict[str, list] = {}
                for ch in chs:
                    clusters.setdefault(find(ch), []).append(ch)
                if len(clusters) < 2:
                    continue
                newkey, name_cluster = {}, {}
                for root, members in clusters.items():
                    best_ch = max(members, key=lambda c_: sum(per[c_].values()))
                    nm_ = rep_name[best_ch]
                    sold_by_weight = any(c_ in weighted_ch.get(key, ()) for c_ in members)
                    nn_ = norm_name(nm_, keep_numbers=not sold_by_weight)
                    for c_ in members:
                        newkey[c_] = ("n:" + nn_) if len(nn_) >= 3 else None
                        for n_ in per[c_]:
                            name_cluster[n_] = newkey[c_]
                for sd_, prices_, _f in columns:
                    nk = newkey.get(sd_["chain"])
                    if key in prices_:
                        v = prices_.pop(key)
                        if nk:
                            prices_[nk] = min(v, prices_[nk]) if nk in prices_ else v
                for ch, nk in newkey.items():
                    if nk:
                        carriers.setdefault(nk, set()).add(ch)
                        if ch in weighted_ch.get(key, ()):
                            weighted.add(nk)
                for n_, cnt_ in names.get(key, {}).items():
                    nk = name_cluster.get(n_)
                    if nk:
                        names.setdefault(nk, Counter())[n_] += cnt_
                names.pop(key, None); carriers.pop(key, None); sizes.pop(key, None); weighted.discard(key)
                moved.append((key, sorted({v for v in rep_name.values()})))
            return moved
        moved_codes = split_conflicts()
        if moved_codes:
            log(f"[codes] {city['name']}: {len(moved_codes)} codes are used for different items in different chains, "
                f"keyed by name instead: " + "; ".join(f"{k.lstrip('0')} = {' / '.join(v)[:70]}" for k, v in moved_codes[:6]))

        def rank(keys):
            return sorted(keys, key=lambda g: (-len(carriers[g]), best_name(names[g])))

        barcoded = [g for g, ch in carriers.items() if not g.startswith("n:") and len(ch) >= need]
        loose = [g for g, ch in carriers.items() if g.startswith("n:") and len(ch) >= need_loose]
        ranked = rank(barcoded)[:max_products] + rank(loose)[:max_loose]
        cut_b, cut_l = max(0, len(barcoded) - max_products), max(0, len(loose) - max_loose)
        if watch_f:
            ranked_set = set(ranked)
            for w, _ in watch_f:
                hits = watch_hits.get(w)
                if not hits:
                    log(f'[watch] {city["name"]}: "{w}" was not found in any of the {len(files)} downloaded price files '
                        f'(the sampled branches may not carry it, try a larger stores_per_chain)')
                    continue
                seen: dict[str, list] = {}
                for chain, code, nm, key in hits:
                    seen.setdefault(key, [nm, code, set()])[2].add(chain)
                for key, (nm, code, chs) in list(seen.items())[:40]:
                    if key in ranked_set:
                        st = "INCLUDED in the site data"
                    elif key.startswith("n:") and len(carriers[key]) < need_loose:
                        st = f"NOT included: found in {len(carriers[key])} chain(s), loose_min_chains={need_loose}"
                    else:
                        st = "NOT included: cut by the max_products / max_loose limit"
                    log(f'[watch] {city["name"]}: "{w}" -> {nm} (code {code}, key {key}, price {"/".join(sorted({str(x) for x in prices_seen.get(key, [])})[:4])}) in {",".join(sorted(chs))}: {st}')
        # products that only stale columns know about are dropped on purpose

        products = []
        for g in ranked:
            row = [c[1].get(g) for c in columns]
            if all(v is None for v in row):
                continue
            for nm_ in names[g]:
                l_ = longest.get(g, "")
                if nm_ != l_ and l_ and (l_.startswith(nm_) or nm_.startswith(l_)):
                    learned[g] = max(l_, nm_, key=len)      # this barcode is spelled both cut and in full somewhere
                if len(nm_) > len(l_):
                    longest[g] = nm_
            prod = {"id": g, "n": best_name(names[g]), "p": row}
            if g in sizes:
                prod["s"] = sizes[g].most_common(1)[0][0]
            if g in weighted:
                prod["w"] = 1
            products.append(prod)

        payload = {
            "key": city["key"],
            "name": city["name"],
            "updated_at": now,
            "stores": [c[0] for c in columns],
            "products": products,
        }
        fname = f"city-{city['key']}.json"
        (out / fname).write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        index_cities.append({
            "key": city["key"], "name": city["name"], "file": fname,
            "stores": len(columns), "products": len(products),
        })
        log(f"[city] {city['name']}: {len(columns)} branches, {len(products)} products"
            + (f" (cut by limits: {cut_b} barcoded, {cut_l} unbarcoded)" if cut_b or cut_l else ""))
        for sd_, pr_, fresh_ in columns:
            log(f"[branch] {city['name']}: {sd_['cn']} -> {sd_['b']} (store {sd_['id'].split(':', 1)[1]}), "
                f"{sum(1 for v in pr_.values() if v is not None)} items" + ("" if fresh_ else ", stale"))
        files.clear()
        prev_cache.pop(city['key'], None)

    pool.shutdown()
    if hidden_cities:
        log(f"[cities] not listed (fewer than {min_city_chains} chains): " + ", ".join(hidden_cities))
    try:
        done_ = complete_names(out, index_cities, longest)
        log(f"[names] {done_} cut-off product names completed from the same barcode in other cities "
            f"({len(learned)} full names remembered for later runs)")
        (out / "names-full.json").write_text(json.dumps(learned, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    except Exception as exc:  # cosmetic step: never fail the run over it
        log(f"[names] skipped: {exc}")
    for slug in chains_cfg:
        c = counters[slug]
        if status[slug]["ok"] and c["failed"] and not c["ok"]:
            status[slug]["ok"] = False
            status[slug]["error"] = c["error"] or "no price files"
        status[slug]["branches_ok"] = c["ok"]
        status[slug]["branches_failed"] = c["failed"]

    log("[summary] chains:")
    for slug in chains_cfg:
        st = status[slug]
        log(f"  {slug}: " + ("ok" if st["ok"] else "FAILED") +
            f" (files ok {st.get('branches_ok', 0)}, failed {st.get('branches_failed', 0)})" +
            (f" - {st['error']}" if st.get("error") else ""))
    # the last runs, to see when a chain's portal is up or down (shown by status.html): [files ok, files failed] per chain
    hist = []
    prev_index = fetch_prev(args.prev_base, "index.json")
    if isinstance(prev_index, dict) and isinstance(prev_index.get("history"), list):
        hist = [h for h in prev_index["history"] if isinstance(h, dict) and h.get("t") != now][-79:]
    hist.append({"t": now, "e": os.environ.get("GITHUB_EVENT_NAME", ""), "c": {slug: [status[slug].get("branches_ok", 0), status[slug].get("branches_failed", 0)] for slug in chains_cfg}})
    index = {"generated_at": now, "cities": index_cities, "chains": status, "history": hist}
    (out / "index.json").write_text(json.dumps(index, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    log(f"done: {len(index_cities)} cities written to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
