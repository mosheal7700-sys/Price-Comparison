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
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
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


def pick_stores(stores, terms, limit):
    """Branches whose city, address or name contains one of the city terms."""
    scored = []
    seen = set()
    for s in stores:
        if s.store_id in seen:
            continue
        in_city = bool(s.city) and has_term(s.city, terms)
        text = " ".join(x for x in (s.address, s.name) if x)
        in_text = has_term(text, terms)
        if in_city or in_text:
            seen.add(s.store_id)
            scored.append((0 if in_city else 1, s))
    scored.sort(key=lambda x: x[0])
    return [s for _, s in scored[:limit]]


def size_label(item) -> str:
    try:
        if item.quantity is not None and item.unit_qty:
            q = format(Decimal(item.quantity).normalize(), "f")
            return f"{q} {item.unit_qty}".strip()
    except Exception:
        pass
    return ""


def branch_label(store) -> str:
    label = (store.name or store.address or store.store_id or "").strip()
    return label[:40]


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
    min_chains = int(cfg.get("min_chains", 1))
    max_products = int(cfg.get("max_products", 0)) or 10**9  # 0 = no limit
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
            return slug, retry(lambda: ilp.get_stores(slug)).stores, None
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

    # 2. which branches serve which city
    prev_cache: dict[str, dict | None] = {}

    def get_prev(key):
        if key not in prev_cache:
            prev_cache[key] = fetch_prev(args.prev_base, f"city-{key}.json")
        return prev_cache[key]

    plans = []  # (city, entries)
    jobs: dict[tuple, None] = {}
    for city in cities_cfg:
        entries = []
        for slug in chains_cfg:
            if slug in store_lists:
                pool = pick_stores(store_lists[slug], city["match"], 10**6)
                pin = (city.get("branches") or {}).get(slug)
                pinned = [x for x in pool if pin and has_term(" ".join(y for y in (x.address, x.name) if y), [pin])]
                if pin and not pinned:
                    log(f'[pin] {city["name"]}/{slug}: no branch in the city matches "{pin}", choosing by size')
                rest = [x for x in pool if x not in pinned]
                for s in pinned + rest[:cand]:
                    entries.append({"chain": slug, "store_id": s.store_id, "label": branch_label(s),
                                    "pin": s in pinned})
                    jobs[(slug, s.store_id)] = None
            else:
                prev = get_prev(city["key"])
                if prev:
                    for st in prev.get("stores", []):
                        if st["chain"] == slug:
                            sid = st["id"].split(":", 1)[1]
                            entries.append({"chain": slug, "store_id": sid, "label": st.get("b", "")})
        plans.append((city, entries))

    # 3. one city at a time: download its price files, write the city file, free the memory.
    # (Holding every parsed price file at once would not fit in the memory of a GitHub runner.)
    def price_task(job):
        chain, sid = job
        try:
            return job, retry(lambda: ilp.get_prices(chain, sid)), None
        except Exception as exc:
            return job, None, str(exc)[:200]

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
        for ch, es in by_chain.items():
            got = [e for e in es if (e["chain"], e["store_id"]) in files]
            if got:
                got.sort(key=lambda e: (not e.get("pin"), -len(files[(e["chain"], e["store_id"])].items)))
                entries += got[:per_chain]
                if fill_gaps:
                    extras[ch] = [files[(e["chain"], e["store_id"])] for e in got[per_chain:]]
            else:
                entries += es[:per_chain]
        prev = get_prev(city["key"]) if any((e["chain"], e["store_id"]) not in files for e in entries) else None
        prev_idx = {}
        if prev:
            prev_idx = {s["id"]: i for i, s in enumerate(prev.get("stores", []))}

        columns = []  # (store dict, {gtin: price}, fresh)
        names: dict[str, Counter] = {}
        sizes: dict[str, Counter] = {}
        carriers: dict[str, set] = {}
        weighted: set[str] = set()
        watch_hits: dict[str, list] = {}

        filled: set[str] = set()
        for e in entries:
            sid = f'{e["chain"]}:{e["store_id"]}'
            sd = {"id": sid, "chain": e["chain"], "cn": status[e["chain"]]["name"], "b": e["label"]}
            pf = files.get((e["chain"], e["store_id"]))
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
                                    watch_hits.setdefault(w, []).append((e["chain"], code, it.name.strip(), key))
                        price = float(round(it.price, 2))
                        if not g and key in prices:
                            price = min(price, prices[key])  # several variants of one name: keep the lowest
                        prices[key] = price
                        names.setdefault(key, Counter())[it.name.strip()] += 1
                        if by_weight:
                            weighted.add(key)
                        if g:
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
                sd["as_of"] = prev.get("updated_at")
                columns.append((sd, prices, False))

        if not columns:
            log(f"[city] {city['name']}: no data, skipped")
            continue

        fresh_chains = {c[0]["chain"] for c in columns if c[2]}
        need = min(min_chains, max(1, len(fresh_chains)))
        need_loose = min(loose_min_chains, max(1, len(fresh_chains)))

        def rank(keys):
            return sorted(keys, key=lambda g: (-len(carriers[g]), names[g].most_common(1)[0][0]))

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
                    log(f'[watch] {city["name"]}: "{w}" -> {nm} (code {code}) in {",".join(sorted(chs))}: {st}')
        # products that only stale columns know about are dropped on purpose

        products = []
        for g in ranked:
            row = [c[1].get(g) for c in columns]
            if all(v is None for v in row):
                continue
            prod = {"id": g, "n": names[g].most_common(1)[0][0], "p": row}
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
    index = {"generated_at": now, "cities": index_cities, "chains": status}
    (out / "index.json").write_text(json.dumps(index, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    log(f"done: {len(index_cities)} cities written to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
