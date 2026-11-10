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


def pick_stores(stores, terms, limit):
    """Branches whose city, address or name contains one of the city terms."""
    scored = []
    seen = set()
    for s in stores:
        if s.store_id in seen:
            continue
        in_city = bool(s.city) and any(t in s.city for t in terms)
        text = " ".join(x for x in (s.address, s.name) if x)
        in_text = any(t in text for t in terms)
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
    min_chains = int(cfg.get("min_chains", 2))
    max_products = int(cfg.get("max_products", 3000))
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
    for city in cfg["cities"]:
        entries = []
        for slug in chains_cfg:
            if slug in store_lists:
                for s in pick_stores(store_lists[slug], city["match"], per_chain):
                    entries.append({"chain": slug, "store_id": s.store_id, "label": branch_label(s)})
                    jobs[(slug, s.store_id)] = None
            else:
                prev = get_prev(city["key"])
                if prev:
                    for st in prev.get("stores", []):
                        if st["chain"] == slug:
                            sid = st["id"].split(":", 1)[1]
                            entries.append({"chain": slug, "store_id": sid, "label": st.get("b", "")})
        plans.append((city, entries))

    # 3. download price files
    def price_task(job):
        chain, sid = job
        try:
            return job, retry(lambda: ilp.get_prices(chain, sid)), None
        except Exception as exc:
            return job, None, str(exc)[:200]

    files: dict[tuple, object] = {}
    errors: dict[tuple, str] = {}
    log(f"[prices] downloading {len(jobs)} branch files")
    with ThreadPoolExecutor(WORKERS) as ex:
        for job, pf, err in ex.map(price_task, list(jobs)):
            if err:
                errors[job] = err
                log(f"[prices] {job[0]}/{job[1]}: FAILED {err}")
            else:
                files[job] = pf

    for slug in chains_cfg:
        mine = [j for j in jobs if j[0] == slug]
        if mine and not any(j in files for j in mine) and status[slug]["ok"]:
            status[slug]["ok"] = False
            status[slug]["error"] = next((errors[j] for j in mine if j in errors), "no price files")
        status[slug]["branches_ok"] = sum(1 for j in mine if j in files)
        status[slug]["branches_failed"] = sum(1 for j in mine if j not in files)

    # 4. build one file per city
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    index_cities = []
    for city, entries in plans:
        prev = get_prev(city["key"]) if any((e["chain"], e["store_id"]) not in files for e in entries) else None
        prev_idx = {}
        if prev:
            prev_idx = {s["id"]: i for i, s in enumerate(prev.get("stores", []))}

        columns = []  # (store dict, {gtin: price}, fresh)
        names: dict[str, Counter] = {}
        sizes: dict[str, Counter] = {}
        carriers: dict[str, set] = {}

        for e in entries:
            sid = f'{e["chain"]}:{e["store_id"]}'
            sd = {"id": sid, "chain": e["chain"], "cn": status[e["chain"]]["name"], "b": e["label"]}
            pf = files.get((e["chain"], e["store_id"]))
            prices: dict[str, float] = {}
            if pf is not None:
                for it in pf.items:
                    g = it.gtin
                    if not g or it.price is None or it.price <= 0:
                        continue
                    prices[g] = float(round(it.price, 2))
                    if it.name:
                        names.setdefault(g, Counter())[it.name.strip()] += 1
                    sz = size_label(it)
                    if sz:
                        sizes.setdefault(g, Counter())[sz] += 1
                    carriers.setdefault(g, set()).add(e["chain"])
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
        ranked = sorted(
            (g for g, ch in carriers.items() if len(ch) >= need),
            key=lambda g: (-len(carriers[g]), names[g].most_common(1)[0][0]),
        )[:max_products]
        # products that only stale columns know about are dropped on purpose

        products = []
        for g in ranked:
            row = [c[1].get(g) for c in columns]
            if all(v is None for v in row):
                continue
            prod = {"id": g, "n": names[g].most_common(1)[0][0], "p": row}
            if g in sizes:
                prod["s"] = sizes[g].most_common(1)[0][0]
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
        log(f"[city] {city['name']}: {len(columns)} branches, {len(products)} products")

    index = {"generated_at": now, "cities": index_cities, "chains": status}
    (out / "index.json").write_text(json.dumps(index, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    log(f"done: {len(index_cities)} cities written to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
