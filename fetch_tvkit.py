"""
update_prices_tvkit_mp.py — Multiprocess OHLCV downloader using tvkit.

Delta-fetch / persistence strategy
──────────────────────────────────
• If the SQLite DB exists, its `fetch_meta` drives the delta (only dates
  after `last_fetched_date` are requested per ticker).
• If the DB is gone but `index.json` exists (our final output), the DB is
  bootstrapped from `index.json` first — so re-running after a cleanup
  still performs only a delta fetch, not a full backfill.
• If neither exists, all symbols are backfilled for LOOKBACK_DAYS.
• After merging shards, everything is dumped to `index.json`, then the
  shard directory and the DB (+ WAL/SHM) are removed.

Symbol sanitization
───────────────────
TradingView rejects certain characters in its WebSocket upgrade URL.
We keep the ORIGINAL spelling in the DB and in `index.json` (e.g.
`NSE:M&M`) and only sanitize the WIRE symbol passed to tvkit
(e.g. `NSE:M_M`). Mapping is defined in `_TV_CHAR_MAP`.

Rate-limit strategy
───────────────────
• default 12 workers × 1 concurrent fetch (configurable)
• staggered startup by STARTUP_STAGGER_SEC per worker
• 429 → RATE_LIMIT_BACKOFF + jitter, then retry
• RATE_LIMIT_TRIP consecutive 429s → RATE_LIMIT_COOLDOWN pause
• optional --cookie / --auth-token for authenticated limits

Usage
─────
    python update_prices_tvkit_mp.py
    python update_prices_tvkit_mp.py --workers 3 --concurrency 1
    python update_prices_tvkit_mp.py --sequential
    python update_prices_tvkit_mp.py --cookie "sessionid=..."
    python update_prices_tvkit_mp.py --full           # ignore delta, refetch all
    python update_prices_tvkit_mp.py --keep-db        # don't delete DB after
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import multiprocessing as mp
import random
import shutil
import sqlite3
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from tvkit.api.chart.ohlcv import OHLCV
from tvkit.api.chart.exceptions import (
    NoHistoricalDataError,
    RangeTooLargeError,
)


# ════════════════════════════════════════════════════════════════════════
#  CONFIG
# ════════════════════════════════════════════════════════════════════════
DB_PATH              = "prices_tvkit.db"
SHARD_DIR            = "shards_tvkit"
SYMBOL_CSV           = "piotroski_india.csv"
LOOKBACK_DAYS        = 800

JSON_OUT             = "index.json"

DEFAULT_WORKERS      = 3
DEFAULT_CONCUR       = 1
STARTUP_STAGGER_SEC  = 5.0

RATE_LIMIT_BACKOFF   = 30.0
RATE_LIMIT_TRIP      = 5
RATE_LIMIT_COOLDOWN  = 60.0
MAX_RETRIES          = 4
RETRY_BACKOFF        = 3.0

INTERVAL             = "1D"
PROGRESS_EVERY       = 500

logging.getLogger("tvkit").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("tvkit-mp")


# ════════════════════════════════════════════════════════════════════════
#  SQLITE
# ════════════════════════════════════════════════════════════════════════
SCHEMA = """
CREATE TABLE IF NOT EXISTS prices (
    ticker  TEXT    NOT NULL,
    date    TEXT    NOT NULL,
    open    REAL,
    high    REAL,
    low     REAL,
    close   REAL,
    volume  INTEGER,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON prices(ticker, date);

CREATE TABLE IF NOT EXISTS fetch_meta (
    ticker             TEXT PRIMARY KEY,
    exchange           TEXT,
    last_fetched_date  TEXT,
    last_fetched_at    TEXT,
    rows_total         INTEGER,
    last_error         TEXT
);
"""


def init_db(path: str) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(SCHEMA)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.commit()
    finally:
        conn.close()


def get_all_meta(path: str) -> dict[str, date | None]:
    """Read (ticker → last_fetched_date) from an existing DB."""
    if not Path(path).exists():
        return {}
    conn = sqlite3.connect(path)
    try:
        rows = conn.execute(
            "SELECT ticker, last_fetched_date FROM fetch_meta"
        ).fetchall()
        return {
            t: (datetime.strptime(d, "%Y-%m-%d").date() if d else None)
            for t, d in rows
        }
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════════════
#  SYMBOL SANITIZATION
# ════════════════════════════════════════════════════════════════════════
# TradingView's ticker endpoint rejects these in the WebSocket upgrade.
# Replace with '_' for the WIRE request only; the original spelling is
# preserved in the DB and in index.json.
_TV_CHAR_MAP = {
    "&":  "_",
    "+":  "_",
    " ":  "_",
    ",":  "_",
    "/":  "_",
    "\\": "_",
    "(":  "_",
    ")":  "_",
    "'":  "_",
    '"':  "_",
}


def sanitize_tv_symbol(symbol: str) -> str:
    """
    Replace TradingView-incompatible characters with '_'.

    Examples:
        'NSE:M&M'      -> 'NSE:M_M'
        'NSE:M&MFIN'   -> 'NSE:M_MFIN'
        'NSE:BAJAJ-AUTO' -> 'NSE:BAJAJ-AUTO'  (hyphen is fine on TV)
    """
    return "".join(_TV_CHAR_MAP.get(c, c) for c in symbol)


# ════════════════════════════════════════════════════════════════════════
#  BOOTSTRAP DB FROM index.json (for delta fetch across runs)
# ════════════════════════════════════════════════════════════════════════
def bootstrap_db_from_json(json_path: str, db_path: str) -> bool:
    """
    Rebuild db_path from index.json if the DB is missing.

    Returns True if a bootstrap was performed, False otherwise.
    """
    if Path(db_path).exists():
        return False
    if not Path(json_path).exists():
        return False

    log.info("DB missing — bootstrapping %s from %s …", db_path, json_path)
    t0 = time.time()

    with open(json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    init_db(db_path)
    conn = sqlite3.connect(db_path, timeout=120)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        # ── fetch_meta ────────────────────────────────────────────
        meta_rows = payload.get("fetch_meta", []) or []
        if meta_rows:
            conn.executemany(
                "INSERT OR REPLACE INTO fetch_meta "
                "(ticker, exchange, last_fetched_date, last_fetched_at, "
                " rows_total, last_error) VALUES (?,?,?,?,?,?)",
                [
                    (
                        m.get("ticker"), m.get("exchange"),
                        m.get("last_fetched_date"), m.get("last_fetched_at"),
                        m.get("rows_total"), m.get("last_error"),
                    )
                    for m in meta_rows
                ],
            )

        # ── prices ────────────────────────────────────────────────
        prices = payload.get("prices", {}) or {}
        total_inserted = 0
        for ticker, rows in prices.items():
            batch = [
                (ticker, r[0], r[1], r[2], r[3], r[4], r[5])
                for r in rows
                if r and len(r) >= 6
            ]
            for i in range(0, len(batch), 10_000):
                chunk = batch[i:i + 10_000]
                conn.executemany(
                    "INSERT OR REPLACE INTO prices "
                    "(ticker, date, open, high, low, close, volume) "
                    "VALUES (?,?,?,?,?,?,?)",
                    chunk,
                )
                total_inserted += len(chunk)

        conn.commit()
        log.info(
            "Bootstrap done in %.1fs — %d tickers, %d rows restored",
            time.time() - t0, len(prices), total_inserted,
        )
        return True
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════════════
#  SYMBOL LOADING
# ════════════════════════════════════════════════════════════════════════
def load_symbols(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    col = next(
        (c for c in ("ticker", "symbol", "Ticker", "Symbol") if c in df.columns),
        None,
    )
    if col is None:
        raise ValueError("CSV must contain a 'ticker' or 'symbol' column.")

    # Optional columns used purely for the HTML's dedup key.
    # The HTML groups by `description || name` (displayName), falling back
    # to the bare symbol if neither is present.
    name_col = next((c for c in ("description", "name") if c in df.columns), None)

    rows: list[dict] = []
    skipped = 0
    sanitized = 0

    for _, csv_row in df.iterrows():
        raw = str(csv_row[col]).strip()
        if not raw or raw.lower() == "nan":
            continue
        if ":" in raw:
            exch, sym = raw.split(":", 1)
        else:
            exch, sym = "NSE", raw
        exch = exch.upper().strip()
        sym  = sym.strip()
        if exch not in ("NSE", "BSE"):
            skipped += 1
            continue

        orig_ticker = f"{exch}:{sym}"
        safe_tv     = sanitize_tv_symbol(orig_ticker)
        if safe_tv != orig_ticker:
            sanitized += 1

        # HTML dedup key: description/name if present, else bare symbol —
        # exactly what displayName(r) || baseSymbol(r) resolves to in the JS.
        display_name = None
        if name_col is not None:
            val = csv_row.get(name_col)
            if val is not None and str(val).strip().lower() != "nan":
                display_name = str(val).strip()
        dedup_key = (display_name or sym).strip().upper()

        rows.append({
            "ticker":    orig_ticker,
            "exchange":  exch,
            "tv_symbol": safe_tv,
            "dedup_key": dedup_key,
        })

    if skipped:
        log.warning("Skipped %d symbols with unsupported exchange", skipped)
    if sanitized:
        log.info(
            "Sanitized %d symbols for TradingView "
            "(e.g. '&' -> '_'); original spelling preserved in DB / JSON",
            sanitized,
        )

    out = pd.DataFrame(rows).drop_duplicates("ticker").reset_index(drop=True)

    # ── Mirror the HTML's NSE-preferred dedup so we never fetch a BSE
    #    listing the frontend will discard in favour of its NSE twin. ──
    before = len(out)
    keep_idx: dict[str, int] = {}
    for idx, r in out.iterrows():
        key = r["dedup_key"]
        if key not in keep_idx:
            keep_idx[key] = idx
            continue
        existing = out.loc[keep_idx[key]]
        if existing["exchange"] != "NSE" and r["exchange"] == "NSE":
            keep_idx[key] = idx
    out = out.loc[sorted(keep_idx.values())].drop(columns=["dedup_key"]).reset_index(drop=True)
    dropped = before - len(out)
    if dropped:
        log.info(
            "Dropped %d BSE-duplicate symbols not used by the HTML "
            "(NSE listing preferred for the same company)",
            dropped,
        )

    # Detect sanitization collisions (two originals mapping to same wire sym)
    dup = out.groupby("tv_symbol")["ticker"].apply(list)
    dup = dup[dup.apply(len) > 1]
    if not dup.empty:
        log.warning(
            "Wire-symbol collisions after sanitization — these will share "
            "TradingView data:\n%s", dup.to_string(),
        )

    log.info("Loaded %d unique symbols from %s (post HTML-dedup)", len(out), csv_path)
    return out


def split_chunks(rows: list[dict], n: int) -> list[list[dict]]:
    chunks: list[list[dict]] = [[] for _ in range(n)]
    for i, row in enumerate(rows):
        chunks[i % n].append(row)
    return [c for c in chunks if c]


# ════════════════════════════════════════════════════════════════════════
#  WORKER-SIDE
# ════════════════════════════════════════════════════════════════════════
def _bars_to_dicts(bars) -> list[dict]:
    out: list[dict] = []
    for b in bars:
        ts = getattr(b, "timestamp", None)
        if ts is None:
            continue
        d = datetime.utcfromtimestamp(float(ts)).date()
        out.append({
            "date":   d,
            "open":   getattr(b, "open",   None),
            "high":   getattr(b, "high",   None),
            "low":    getattr(b, "low",    None),
            "close":  getattr(b, "close",  None),
            "volume": getattr(b, "volume", None),
        })
    out.sort(key=lambda r: r["date"])
    return out


async def _fetch(client: OHLCV, sym: str, start: date, end: date) -> list[dict]:
    bars = await client.get_historical_ohlcv(
        exchange_symbol=sym,
        interval=INTERVAL,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
    )
    return _bars_to_dicts(bars)


def _is_rate_limit(err: str | None) -> bool:
    if not err:
        return False
    e = err.lower()
    return ("429" in e) or ("too many requests" in e) or ("rate limit" in e)


async def _fetch_retry(client: OHLCV, sym: str, start: date, end: date,
                       worker_state: dict) -> tuple[list[dict], str | None]:
    """Fetch with backoff. Dedicated handling for HTTP 429."""
    last_err: str | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            rows = await _fetch(client, sym, start, end)
            worker_state["consecutive_429"] = 0
            return rows, None
        except RangeTooLargeError as e:
            return [], f"RangeTooLarge: {e}"
        except NoHistoricalDataError as e:
            return [], f"NoHistoricalData: {e}"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"

            if _is_rate_limit(last_err):
                worker_state["consecutive_429"] += 1
                if worker_state["consecutive_429"] >= RATE_LIMIT_TRIP:
                    pause = RATE_LIMIT_COOLDOWN + random.uniform(0, 15)
                    worker_state["log"](
                        f"circuit-breaker tripped after "
                        f"{worker_state['consecutive_429']} consecutive 429s; "
                        f"pausing {pause:.0f}s"
                    )
                    await asyncio.sleep(pause)
                    worker_state["consecutive_429"] = 0
                    continue
                pause = RATE_LIMIT_BACKOFF + random.uniform(0, 10)
                worker_state["log"](
                    f"429 on {sym} — backing off {pause:.0f}s "
                    f"(attempt {attempt}/{MAX_RETRIES})"
                )
                await asyncio.sleep(pause)
            else:
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_BACKOFF ** attempt)
    return [], last_err


async def _worker_async(worker_id: int,
                        chunk: list[dict],
                        meta: dict[str, date | None],
                        today: date,
                        force_full: bool,
                        concurrency: int,
                        shard_path: str,
                        progress_q,
                        auth_token: str | None,
                        cookie: str | None) -> dict:
    """One worker's entire async workload."""
    sem = asyncio.Semaphore(concurrency)
    write_lock = asyncio.Lock()
    client_ref: list = [None]

    def wlog(msg: str) -> None:
        try:
            progress_q.put(f"[w{worker_id}] {msg}")
        except Exception:
            pass

    worker_state = {"consecutive_429": 0, "log": wlog}

    async def persist(ticker: str, exchange: str,
                      last_dt: date | None,
                      rows: list[dict],
                      err: str | None) -> int:
        async with write_lock:
            conn = sqlite3.connect(shard_path, timeout=60)
            try:
                if rows:
                    payload = [
                        (ticker, r["date"].strftime("%Y-%m-%d"),
                         r.get("open"), r.get("high"), r.get("low"),
                         r.get("close"),
                         None if r.get("volume") is None else int(r["volume"]))
                        for r in rows
                    ]
                    # INSERT OR REPLACE dedupes on the (ticker,date) PK —
                    # re-fetching an overlapping window is safe.
                    conn.executemany(
                        "INSERT OR REPLACE INTO prices "
                        "(ticker,date,open,high,low,close,volume) "
                        "VALUES (?,?,?,?,?,?,?)",
                        payload,
                    )
                total = conn.execute(
                    "SELECT COUNT(*) FROM prices WHERE ticker = ?", (ticker,)
                ).fetchone()[0]
                conn.execute(
                    """
                    INSERT INTO fetch_meta
                        (ticker, exchange, last_fetched_date, last_fetched_at,
                         rows_total, last_error)
                    VALUES (?,?,?,?,?,?)
                    ON CONFLICT(ticker) DO UPDATE SET
                        exchange          = excluded.exchange,
                        last_fetched_date = COALESCE(excluded.last_fetched_date,
                                                     fetch_meta.last_fetched_date),
                        last_fetched_at   = excluded.last_fetched_at,
                        rows_total        = excluded.rows_total,
                        last_error        = excluded.last_error
                    """,
                    (
                        ticker, exchange,
                        last_dt.strftime("%Y-%m-%d") if last_dt else None,
                        datetime.utcnow().isoformat(timespec="seconds"),
                        total, err,
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        return len(rows)

    async def process(row: dict) -> dict:
        ticker    = row["ticker"]
        exchange  = row["exchange"]
        tv_symbol = row["tv_symbol"]

        # ── Delta decision ─────────────────────────────────────────
        last_dt = None if force_full else meta.get(ticker)

        if last_dt is None:
            start = today - timedelta(days=LOOKBACK_DAYS)
            mode  = "backfill"
        elif last_dt >= today:
            return {"ticker": ticker, "status": "up-to-date", "rows": 0,
                    "last": last_dt, "error": None}
        else:
            start = last_dt + timedelta(days=1)
            mode  = "incremental"

        async with sem:
            rows, err = await _fetch_retry(
                client_ref[0], tv_symbol, start, today, worker_state,
            )

        if err or not rows:
            await persist(ticker, exchange, last_dt, [], err or "empty response")
            return {"ticker": ticker, "status": "error", "rows": 0,
                    "last": last_dt, "error": err or "empty response"}

        new_last = max(r["date"] for r in rows)
        written = await persist(ticker, exchange, new_last, rows, None)
        return {"ticker": ticker, "status": mode, "rows": written,
                "last": new_last, "error": None}

    # ─── Open the client ──────────────────────────────────────────
    client_kwargs: dict = {}
    if auth_token:
        client_kwargs["auth_token"] = auth_token
    if cookie:
        client_kwargs["cookie"] = cookie

    stats = {"fetched": 0, "up_to_date": 0, "errors": 0, "rows": 0}

    try:
        client_ctx = OHLCV(**client_kwargs) if client_kwargs else OHLCV()
    except TypeError:
        client_ctx = OHLCV()

    async with client_ctx as client:
        client_ref[0] = client

        tasks = [asyncio.create_task(process(row)) for row in chunk]
        done = 0
        for coro in asyncio.as_completed(tasks):
            done += 1
            try:
                r = await coro
            except Exception as e:
                r = {"status": "error", "rows": 0, "error": str(e)}

            if r["status"] in ("backfill", "incremental"):
                stats["fetched"] += 1
                stats["rows"]    += r["rows"]
            elif r["status"] == "up-to-date":
                stats["up_to_date"] += 1
            else:
                stats["errors"] += 1

            if done % PROGRESS_EVERY == 0 or done == len(tasks):
                wlog(f"{done}/{len(tasks)} "
                     f"ok={stats['fetched']} skip={stats['up_to_date']} "
                     f"err={stats['errors']} rows={stats['rows']}")

    stats["worker_id"] = worker_id
    stats["total"]     = len(chunk)
    return stats


def _worker_entry(worker_id: int,
                  chunk: list[dict],
                  meta: dict[str, date | None],
                  today_iso: str,
                  force_full: bool,
                  concurrency: int,
                  shard_path: str,
                  progress_q,
                  auth_token: str | None,
                  cookie: str | None,
                  stagger_sec: float) -> dict:
    """Sync entry point for the subprocess."""
    logging.basicConfig(
        level=logging.WARNING,
        format=f"%(asctime)s  [w{worker_id}]  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    if stagger_sec > 0 and worker_id > 0:
        delay = worker_id * stagger_sec
        try:
            progress_q.put(f"[w{worker_id}] startup stagger: sleeping {delay:.0f}s")
        except Exception:
            pass
        time.sleep(delay)

    init_db(shard_path)
    today = datetime.strptime(today_iso, "%Y-%m-%d").date()

    t0 = time.time()
    try:
        result = asyncio.run(_worker_async(
            worker_id, chunk, meta, today, force_full,
            concurrency, shard_path, progress_q,
            auth_token, cookie,
        ))
    except Exception as e:
        import traceback
        tb = traceback.format_exc(limit=4)
        result = {"worker_id": worker_id, "total": len(chunk),
                  "fetched": 0, "up_to_date": 0, "errors": len(chunk),
                  "rows": 0, "fatal": f"{type(e).__name__}: {e}\n{tb}"}
    result["elapsed"] = round(time.time() - t0, 1)
    return result


# ════════════════════════════════════════════════════════════════════════
#  SHARD MERGE
# ════════════════════════════════════════════════════════════════════════
def merge_shards(main_db: str, shard_paths: list[Path]) -> tuple[int, int]:
    main_conn = sqlite3.connect(main_db, timeout=120)
    try:
        main_conn.execute("PRAGMA journal_mode=WAL;")
        main_conn.execute("PRAGMA synchronous=NORMAL;")

        merged_shards = 0
        for sp in shard_paths:
            if not sp.exists():
                continue

            for suffix in ("-wal", "-shm"):
                stale = Path(str(sp) + suffix)
                if stale.exists():
                    try:
                        stale.unlink()
                    except OSError:
                        pass

            uri = f"file:{sp}?mode=ro"
            try:
                shard_conn = sqlite3.connect(uri, uri=True, timeout=60)
            except sqlite3.OperationalError as e:
                log.warning("Skipping shard %s — cannot open: %s", sp.name, e)
                continue

            try:
                cur = shard_conn.execute(
                    "SELECT ticker, date, open, high, low, close, volume "
                    "FROM prices"
                )
                while True:
                    batch = cur.fetchmany(10_000)
                    if not batch:
                        break
                    # OR REPLACE dedupes on the (ticker,date) PK so
                    # re-running or overlapping shards never duplicate.
                    main_conn.executemany(
                        "INSERT OR REPLACE INTO prices "
                        "(ticker, date, open, high, low, close, volume) "
                        "VALUES (?,?,?,?,?,?,?)",
                        batch,
                    )

                cur = shard_conn.execute(
                    "SELECT ticker, exchange, last_fetched_date, "
                    "       last_fetched_at, rows_total, last_error "
                    "FROM fetch_meta"
                )
                while True:
                    batch = cur.fetchmany(10_000)
                    if not batch:
                        break
                    main_conn.executemany(
                        "INSERT OR REPLACE INTO fetch_meta "
                        "(ticker, exchange, last_fetched_date, "
                        " last_fetched_at, rows_total, last_error) "
                        "VALUES (?,?,?,?,?,?)",
                        batch,
                    )

                main_conn.commit()
                merged_shards += 1
            except sqlite3.DatabaseError as e:
                log.warning("Shard %s failed mid-merge: %s", sp.name, e)
                main_conn.rollback()
            finally:
                shard_conn.close()

        if merged_shards == 0:
            log.warning("No shards were merged — all were missing or unreadable.")

        # Explicit deduplication: remove any stale duplicate rows by keeping only
        # the latest version if somehow duplicates slipped through.
        log.info("Deduplicating prices table…")
        main_conn.execute(
            """
            DELETE FROM prices WHERE rowid NOT IN (
                SELECT MAX(rowid) FROM prices GROUP BY ticker, date
            )
            """
        )
        main_conn.commit()

        total_rows = main_conn.execute(
            "SELECT COUNT(*) FROM prices"
        ).fetchone()[0]
        tickers = main_conn.execute(
            "SELECT COUNT(*) FROM fetch_meta"
        ).fetchone()[0]
        return total_rows, tickers
    finally:
        main_conn.close()


# ════════════════════════════════════════════════════════════════════════
#  JSON DUMP
# ════════════════════════════════════════════════════════════════════════
def dump_to_json(db_path: str, out_path: str = JSON_OUT) -> None:
    """
    Dump the merged DB to a single compact index.json.

    Shape:
        {
          "generated_at": ISO8601,
          "columns": ["date","open","high","low","close","volume"],
          "fetch_meta": [ {ticker, exchange, last_fetched_date,
                           last_fetched_at, rows_total, last_error}, ... ],
          "prices": {
             "NSE:M&M": [[date,o,h,l,c,v], ...],
             "NSE:RELIANCE": [[date,o,h,l,c,v], ...],
             ...
          }
        }

    Arrays-of-arrays keeps the file 3–4× smaller than objects and parses
    faster in the browser. Original ticker spelling is used as the JSON
    key (e.g. "NSE:M&M"), NOT the sanitized wire symbol.
    """
    log.info("Dumping %s → %s …", db_path, out_path)
    t0 = time.time()

    conn = sqlite3.connect(db_path, timeout=120)
    conn.row_factory = sqlite3.Row
    try:
        meta_rows = [dict(r) for r in conn.execute(
            "SELECT ticker, exchange, last_fetched_date, last_fetched_at, "
            "       rows_total, last_error "
            "FROM fetch_meta ORDER BY ticker"
        )]

        prices: dict[str, list] = {}
        cur = conn.execute(
            "SELECT ticker, date, open, high, low, close, volume "
            "FROM prices ORDER BY ticker, date"
        )
        current: str | None = None
        buf: list = []
        for row in cur:
            t = row["ticker"]
            if t != current:
                if current is not None:
                    prices[current] = buf
                current = t
                buf = []
            buf.append([
                row["date"], row["open"], row["high"], row["low"],
                row["close"], row["volume"],
            ])
        if current is not None:
            prices[current] = buf
    finally:
        conn.close()

    payload = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds"),
        "columns": ["date", "open", "high", "low", "close", "volume"],
        "fetch_meta": meta_rows,
        "prices": prices,
    }

    # Write to a tmp file first, then atomically rename — so a crash
    # mid-write never leaves a truncated index.json behind.
    tmp_path = out_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), default=str)
    Path(tmp_path).replace(out_path)

    size_mb = Path(out_path).stat().st_size / (1024 * 1024)
    log.info(
        "JSON dump done in %.1fs — %s (%.1f MB, %d tickers, %d meta rows)",
        time.time() - t0, out_path, size_mb, len(prices), len(meta_rows),
    )


# ════════════════════════════════════════════════════════════════════════
#  CLEANUP
# ════════════════════════════════════════════════════════════════════════
def cleanup(shard_dir: Path, db_path: str) -> None:
    """Remove shard directory and main DB (+ WAL/SHM sidecars)."""
    log.info("Cleaning up temporary files …")

    if shard_dir.exists():
        shutil.rmtree(shard_dir, ignore_errors=True)
        log.info("  removed shard dir: %s", shard_dir)

    for suffix in ("", "-wal", "-shm"):
        p = Path(db_path + suffix)
        if p.exists():
            try:
                p.unlink()
                log.info("  removed db file : %s", p)
            except OSError as e:
                log.warning("  could not remove %s: %s", p, e)


# ════════════════════════════════════════════════════════════════════════
#  PROGRESS DRAINER
# ════════════════════════════════════════════════════════════════════════
def _drain_progress(q, stop) -> None:
    while not stop.is_set():
        try:
            msg = q.get(timeout=0.5)
        except Exception:
            continue
        if msg is None:
            break
        log.info(msg)
    while True:
        try:
            msg = q.get_nowait()
        except Exception:
            break
        if msg is None:
            break
        log.info(msg)


# ════════════════════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════════════════════
def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    ap.add_argument("--full", action="store_true",
                    help="Force full LOOKBACK_DAYS refetch (ignores delta).")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                    help=f"Worker processes (default {DEFAULT_WORKERS}).")
    ap.add_argument("--concurrency", type=int, default=DEFAULT_CONCUR,
                    help=f"Async fetches per worker (default {DEFAULT_CONCUR}).")
    ap.add_argument("--sequential", action="store_true",
                    help="Force --workers 1 --concurrency 1 (safest, slowest).")
    ap.add_argument("--csv", default=SYMBOL_CSV, help="Symbol CSV path.")
    ap.add_argument("--cookie", default=None,
                    help="Optional TradingView session cookie "
                         "(e.g. 'sessionid=...; sessionid_sign=...').")
    ap.add_argument("--auth-token", default=None,
                    help="Optional TradingView auth token.")
    ap.add_argument("--keep-db", action="store_true",
                    help="Do NOT delete the SQLite DB / shards after dumping.")
    ap.add_argument("--json-out", default=JSON_OUT,
                    help=f"Path to write JSON output (default {JSON_OUT}).")
    args = ap.parse_args()

    if args.sequential:
        args.workers = 1
        args.concurrency = 1

    if not Path(args.csv).exists():
        log.error("Symbol CSV not found: %s", args.csv)
        return 1

    # Convert DB_PATH to absolute for consistency across multiprocessing
    db_path = str(Path(DB_PATH).resolve())

    # ── Step 1: bootstrap DB from existing index.json (delta-fetch enabler)
    bootstrapped = bootstrap_db_from_json(args.json_out, db_path)
    if bootstrapped:
        log.info("Delta mode enabled: will fetch only dates after "
                 "each ticker's last_fetched_date.")
    elif Path(db_path).exists():
        log.info("Existing DB found — delta mode enabled.")
    else:
        log.info("No DB and no index.json — full backfill for all symbols.")

    # Ensure schema exists even for a fresh run
    init_db(db_path)

    today     = date.today()
    today_iso = today.strftime("%Y-%m-%d")

    symbols = load_symbols(args.csv)
    meta    = {} if args.full else get_all_meta(db_path)

    smart_mode = False
    if args.full:
        log.info("--full set: ignoring delta, refetching %d days for all symbols",
                 LOOKBACK_DAYS)
    

    shard_dir = Path(SHARD_DIR)
    if shard_dir.exists():
        shutil.rmtree(shard_dir)
    shard_dir.mkdir(parents=True, exist_ok=True)

    chunks = split_chunks(symbols.to_dict(orient="records"), args.workers)
    log.info("Split %d symbols into %d chunks "
             "(~%d symbols each, %d async each)",
             len(symbols), len(chunks),
             len(symbols) // max(len(chunks), 1),
             args.concurrency)
    log.info("Rate-limit policy: %d workers × %d concurrent = %d WebSockets, "
             "%.0fs startup stagger, %.0fs backoff on 429",
             len(chunks), args.concurrency,
             len(chunks) * args.concurrency,
             STARTUP_STAGGER_SEC, RATE_LIMIT_BACKOFF)
    if args.cookie or args.auth_token:
        log.info("Using TradingView authentication (higher rate limits).")
    else:
        log.info("Running anonymously — expect some 429s. "
                 "Pass --cookie to authenticate.")

    manager = mp.Manager()
    progress_q = manager.Queue()
    stop_event = manager.Event()

    drainer = threading.Thread(
        target=_drain_progress, args=(progress_q, stop_event), daemon=True,
    )
    drainer.start()

    started = time.time()
    worker_results: list[dict] = []

    if sys.platform == "darwin":
        ctx = mp.get_context("spawn")
        log.info("Start method: spawn (forced on macOS)")
    elif sys.platform.startswith("linux"):
        try:
            ctx = mp.get_context("fork")
            log.info("Start method: fork (Linux)")
        except (ValueError, RuntimeError):
            ctx = mp.get_context()
            log.info("Start method: %s", ctx.get_start_method())
    else:
        ctx = mp.get_context("spawn")
        log.info("Start method: spawn (default on this platform)")

    shard_paths = [shard_dir / f"shard_{i}.db" for i in range(len(chunks))]
    stagger = STARTUP_STAGGER_SEC if (len(chunks) > 1 and not smart_mode) else 0.0

    with ProcessPoolExecutor(max_workers=len(chunks), mp_context=ctx) as pool:
        futures = {
            pool.submit(
                _worker_entry,
                i, chunk, meta, today_iso, args.full,
                args.concurrency, str(shard_paths[i]), progress_q,
                args.auth_token, args.cookie, stagger,
            ): i
            for i, chunk in enumerate(chunks)
        }
        for fut in as_completed(futures):
            wid = futures[fut]
            try:
                r = fut.result()
            except Exception as e:
                r = {"worker_id": wid, "total": 0, "fetched": 0,
                     "up_to_date": 0, "errors": 0, "rows": 0,
                     "fatal": f"{type(e).__name__}: {e}"}
            worker_results.append(r)
            status = f"fatal: {r['fatal']}" if r.get("fatal") else (
                f"ok={r['fetched']} skip={r['up_to_date']} "
                f"err={r['errors']} rows={r['rows']}"
            )
            log.info("[w%d] done in %ss — %s",
                     r["worker_id"], r.get("elapsed", "?"), status)

    stop_event.set()
    try:
        progress_q.put_nowait(None)
    except Exception:
        pass
    drainer.join(timeout=2)

    elapsed = time.time() - started

    log.info("Merging %d shards into %s …",
             len(shard_paths), Path(db_path).resolve())
    t0 = time.time()
    total_rows, total_tickers = merge_shards(db_path, shard_paths)
    log.info("Merge done in %.1fs — %d price rows, %d tickers tracked "
             "(INSERT OR REPLACE deduped any overlap)",
             time.time() - t0, total_rows, total_tickers)

    # ── Dump to JSON ──────────────────────────────────────────────
    json_ok = False
    try:
        dump_to_json(db_path, args.json_out)
        json_ok = True
    except Exception as e:
        log.exception("JSON dump failed: %s", e)

    # ── Cleanup (only delete DB if JSON succeeded) ────────────────
    if args.keep_db:
        log.info("--keep-db set: leaving shards and DB in place.")
    elif json_ok:
        cleanup(shard_dir, db_path)
    else:
        log.warning("JSON dump failed — keeping shards and DB for safety.")

    fetched    = sum(r["fetched"]    for r in worker_results)
    up_to_date = sum(r["up_to_date"] for r in worker_results)
    errors     = sum(r["errors"]     for r in worker_results)
    rows_new   = sum(r["rows"]       for r in worker_results)

    log.info("")
    log.info("═" * 62)
    log.info("  Fetched fresh  : %d symbols", fetched)
    log.info("  Already current: %d symbols", up_to_date)
    log.info("  Errors         : %d symbols", errors)
    log.info("  Rows written   : %d", rows_new)
    log.info("  Elapsed        : %.1fs  (%.1f symbols/s)",
             elapsed, len(symbols) / elapsed if elapsed else 0)
    log.info("  JSON output    : %s", Path(args.json_out).resolve() if json_ok
             else "(FAILED)")
    log.info("═" * 62)

    for r in worker_results:
        if r.get("fatal"):
            log.error("Worker %d fatal:\n%s", r["worker_id"], r["fatal"])

    try:
        manager.shutdown()
    except Exception:
        pass

    return 0 if errors == 0 and json_ok else 2


if __name__ == "__main__":
    sys.exit(main())
