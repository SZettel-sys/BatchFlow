# master_full_fixed.py – BatchFlow (FastAPI + Pipedrive + Neon)
# Vollständige Produktionsversion
# Enthält: Neukontakte + Nachfass (mit direkter Batch-ID-Suche) + Kampagnenübersicht + Summary
# Optimiert für Render Free Tier (Python 3.12.3, 512 MB RAM)

import os
import re
import io
import uuid
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple, AsyncGenerator

import numpy as np
import pandas as pd
import httpx
import asyncpg
from rapidfuzz import fuzz, process
fuzz.default_processor = lambda s: s
from fastapi import FastAPI, Request, Body, Query, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware

# =============================================================================
# Grundkonfiguration
# =============================================================================
app = FastAPI(title="BatchFlow")
app.add_middleware(GZipMiddleware, minimum_size=1024)
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# -----------------------------------------------------------------------------
# Umgebungsvariablen
# -----------------------------------------------------------------------------
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
PD_CLIENT_ID = os.getenv("PD_CLIENT_ID", "")
PD_CLIENT_SECRET = os.getenv("PD_CLIENT_SECRET", "")
PD_API_TOKEN = os.getenv("PD_API_TOKEN", "")
OAUTH_AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
OAUTH_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"
PIPEDRIVE_API = "https://api.pipedrive.com/v1"

DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL fehlt (Neon DSN).")
SCHEMA = os.getenv("PGSCHEMA", "public")

# -----------------------------------------------------------------------------
# Filter & Limits
# -----------------------------------------------------------------------------
FILTER_NEUKONTAKTE = int(os.getenv("FILTER_NEUKONTAKTE", "2998"))
FILTER_NACHFASS = int(os.getenv("FILTER_NACHFASS", "3024"))
FIELD_FACHBEREICH_HINT = os.getenv("FIELD_FACHBEREICH_HINT", "fachbereich")

DEFAULT_CHANNEL = "Cold E-Mail"
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "500"))
NF_PAGE_LIMIT = int(os.getenv("NF_PAGE_LIMIT", "500"))
NF_MAX_ROWS = int(os.getenv("NF_MAX_ROWS", "10000"))
RECONCILE_MAX_ROWS = int(os.getenv("RECONCILE_MAX_ROWS", "20000"))
PER_ORG_DEFAULT_LIMIT = int(os.getenv("PER_ORG_DEFAULT_LIMIT", "2"))
PD_CONCURRENCY = int(os.getenv("PD_CONCURRENCY", "4"))
MAX_ORG_NAMES = int(os.getenv("MAX_ORG_NAMES", "100000"))
MAX_ORG_BUCKET = int(os.getenv("MAX_ORG_BUCKET", "12000"))

# -----------------------------------------------------------------------------
# Caches und Globals
# -----------------------------------------------------------------------------
user_tokens: Dict[str, str] = {}
_PERSON_FIELDS_CACHE: Optional[List[dict]] = None
_OPTIONS_CACHE: Dict[int, dict] = {}

# -----------------------------------------------------------------------------
# Excel-Template
# -----------------------------------------------------------------------------
TEMPLATE_COLUMNS = [
    "Batch ID",
    "Channel",
    "Cold-Mailing Import",
    "Prospect ID",
    "Organisation ID",
    "Organisation Name",
    "Person ID",
    "Person Vorname",
    "Person Nachname",
    "Person Titel",
    "Person Geschlecht",
    "Person Position",
    "Person E-Mail",
    "XING Profil",
    "LinkedIn URL",
]

PERSON_FIELD_HINTS_TO_EXPORT = {
    "prospect": "Prospect ID",
    "gender": "Person Geschlecht",
    "geschlecht": "Person Geschlecht",
    "titel": "Person Titel",
    "title": "Person Titel",
    "anrede": "Person Titel",
    "position": "Person Position",
    "xing": "XING Profil",
    "xing url": "XING Profil",
    "xing profil": "XING Profil",
    "linkedin": "LinkedIn URL",
    "email büro": "Person E-Mail",
    "email buero": "Person E-Mail",
    "office email": "Person E-Mail",
}

# =============================================================================
# Startup / Shutdown
# =============================================================================
def http_client() -> httpx.AsyncClient:
    return app.state.http  # type: ignore

def get_pool() -> asyncpg.Pool:
    return app.state.pool  # type: ignore

@app.on_event("startup")
async def _startup():
    import asyncio
    limits = httpx.Limits(max_keepalive_connections=8, max_connections=16)
    app.state.http = httpx.AsyncClient(timeout=60.0, limits=limits)
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
    print("[Startup] BatchFlow initialisiert.")

@app.on_event("shutdown")
async def _shutdown():
    try:
        await app.state.http.aclose()
    finally:
        await app.state.pool.close()

# =============================================================================
# Helper-Funktionen
# =============================================================================
def normalize_name(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_pd_date(d: Optional[str]) -> Optional[datetime]:
    if not d:
        return None
    try:
        return datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None

def is_forbidden_activity_date(val: Optional[str]) -> bool:
    dt = parse_pd_date(val)
    if not dt:
        return False
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    three_months = today - timedelta(days=90)
    return dt > today or (three_months <= dt <= today)

def _as_list_email(value) -> List[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, dict):
        v = value.get("value")
        return [v] if v else []
    if isinstance(value, (list, tuple, np.ndarray)):
        out = []
        for x in value:
            if isinstance(x, dict):
                x = x.get("value")
            if x:
                out.append(str(x))
        return out
    return [str(value)]

def slugify_filename(name: str, fallback: str = "BatchFlow_Export") -> str:
    s = (name or "").strip()
    if not s:
        return fallback
    s = re.sub(r"[^\w\-. ]+", "", s).strip()
    s = re.sub(r"\s+", "_", s)
    return s or fallback

# =============================================================================
# DB-Funktionen (asyncpg)
# =============================================================================
async def ensure_table_text(conn: asyncpg.Connection, table: str, cols: List[str]):
    col_defs = ", ".join([f'"{c}" TEXT' for c in cols])
    await conn.execute(f'CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{table}" ({col_defs})')

async def clear_table(conn: asyncpg.Connection, table: str):
    await conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table}"')

async def save_df_text(df: pd.DataFrame, table: str):
    async with get_pool().acquire() as conn:
        await clear_table(conn, table)
        await ensure_table_text(conn, table, list(df.columns))
        if df.empty:
            return
        cols = list(df.columns)
        cols_sql = ", ".join(f'"{c}"' for c in cols)
        placeholders = ", ".join(f'${i}' for i in range(1, len(cols) + 1))
        insert_sql = f'INSERT INTO "{SCHEMA}"."{table}" ({cols_sql}) VALUES ({placeholders})'
        batch: List[List[str]] = []
        async with conn.transaction():
            for _, row in df.iterrows():
                vals = ["" if pd.isna(v) else str(v) for v in row.tolist()]
                batch.append(vals)
                if len(batch) >= 1000:
                    await conn.executemany(insert_sql, batch)
                    batch = []
            if batch:
                await conn.executemany(insert_sql, batch)

async def load_df_text(table: str) -> pd.DataFrame:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(f'SELECT * FROM "{SCHEMA}"."{table}"')
    if not rows:
        return pd.DataFrame()
    cols = list(rows[0].keys())
    data = [tuple(r[c] for c in cols) for r in rows]
    return pd.DataFrame(data, columns=cols).replace({"": np.nan})

# --- Ende Teil 1/5 ---
# =============================================================================
# Pipedrive-Funktionen & Personenfelder (optimiert)
# =============================================================================
def get_headers() -> Dict[str, str]:
    token = user_tokens.get("default", "")
    if token:
        return {"Authorization": f"Bearer {token}"}
    return {}

def append_token(url: str) -> str:
    if "api_token=" in url:
        return url
    if user_tokens.get("default"):
        return url
    if PD_API_TOKEN:
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}api_token={PD_API_TOKEN}"
    return url

# =============================================================================
# API Paging-Konfiguration
# =============================================================================
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "500"))       # Neukontakte
NF_PAGE_LIMIT = int(os.getenv("NF_PAGE_LIMIT", "500")) # Nachfass

# =============================================================================
# Personenfelder
# =============================================================================
async def get_person_fields() -> List[dict]:
    """Lädt Personenfelder (Cache)."""
    global _PERSON_FIELDS_CACHE
    if _PERSON_FIELDS_CACHE is not None:
        return _PERSON_FIELDS_CACHE
    url = append_token(f"{PIPEDRIVE_API}/personFields")
    r = await http_client().get(url, headers=get_headers())
    r.raise_for_status()
    _PERSON_FIELDS_CACHE = r.json().get("data") or []
    return _PERSON_FIELDS_CACHE

async def get_person_field_by_hint(label_hint: str) -> Optional[dict]:
    fields = await get_person_fields()
    hint = (label_hint or "").lower()
    for f in fields:
        nm = (f.get("name") or "").lower()
        if hint in nm:
            return f
    return None

# =============================================================================
# Personen-Streams mit Paging (optimiert)
# =============================================================================
async def stream_persons_by_filter(filter_id: int, page_limit: int = PAGE_LIMIT) -> AsyncGenerator[List[dict], None]:
    """Streamt Personen aus einem Filter mit korrekter Paginierung."""
    start = 0
    total = 0
    while True:
        url = append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={page_limit}&sort=name")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive Fehler: {r.text}")
        data = r.json().get("data") or []
        if not data:
            break
        yield data
        total += len(data)
        if len(data) < page_limit:
            break
        start += len(data)
    # print(f"→ Streamed {total} Personen aus Filter {filter_id}")

async def stream_organizations_by_filter(filter_id: int, page_limit: int = PAGE_LIMIT):
    """Streamt Organisationen aus einem Filter (mit Paging)."""
    start = 0
    total = 0
    while True:
        url = append_token(f"{PIPEDRIVE_API}/organizations?filter_id={filter_id}&start={start}&limit={page_limit}")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive API Fehler (Orgs {filter_id}): {r.text}")
        data = r.json().get("data") or []
        if not data:
            break
        yield data
        total += len(data)
        if len(data) < page_limit:
            break
        start += len(data)
    # print(f"→ Streamed {total} Organisationen aus Filter {filter_id}")

async def stream_person_ids_by_filter(filter_id: int, page_limit: int = PAGE_LIMIT):
    """Streamt nur Person-IDs (speicherschonend)."""
    start = 0
    total = 0
    while True:
        url = append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={page_limit}&sort=id")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive Fehler (Persons {filter_id}): {r.text}")
        data = r.json().get("data") or []
        if not data:
            break
        ids = [str(p.get("id")) for p in data if p.get("id") is not None]
        yield ids
        total += len(ids)
        if len(data) < page_limit:
            break
        start += len(data)
    # print(f"→ Streamed {total} Person-IDs aus Filter {filter_id}")

# =============================================================================
# Nachfass – Batch-ID-Stream (optimiert)
# =============================================================================
async def stream_persons_by_batch_id(batch_key: str, batch_ids: List[str], page_limit: int = NF_PAGE_LIMIT) -> AsyncGenerator[List[dict], None]:
    """
    Lädt Personen direkt über /persons/search für jede Batch-ID (mit Paging).
    Holt jeweils bis zu page_limit = 500 Datensätze pro Durchlauf.
    """
    for bid in batch_ids:
        start = 0
        total = 0
        while True:
            url = append_token(
                f"{PIPEDRIVE_API}/persons/search?term={bid}&fields={batch_key}&start={start}&limit={page_limit}"
            )
            r = await http_client().get(url, headers=get_headers())
            if r.status_code != 200:
                raise Exception(f"Pipedrive Fehler bei Batch {bid}: {r.text}")
            data = r.json().get("data", {}).get("items", [])
            if not data:
                break
            persons = [it.get("item") for it in data if it.get("item")]
            yield persons
            total += len(persons)
            if len(persons) < page_limit:
                break
            start += len(persons)
        # print(f"→ Streamed {total} Personen für Batch {bid}")


# =============================================================================
# Nachfass – Aufbau Master (direkte Batch-Selektion)
# =============================================================================
# =============================================================================
# Nachfass – Aufbau Master (optimiert mit Parallelisierung)
# =============================================================================
async def _build_nf_master_final(
    nf_batch_ids: List[str],
    batch_id: str,
    campaign: str,
    job_obj=None,
) -> pd.DataFrame:
    """Baut Nachfass-Daten parallel für mehrere Batch-IDs."""
    fields = await get_person_fields()
    batch_key = None
    for f in fields:
        nm = (f.get("name") or "").lower()
        if "batch" in nm:
            batch_key = f.get("key")
            break
    if not batch_key:
        raise RuntimeError("Personenfeld „Batch ID“ wurde nicht gefunden.")

    # Feldzuordnung vorbereiten
    hint_to_key: Dict[str, str] = {}
    for f in fields:
        nm = (f.get("name") or "").lower()
        for hint in PERSON_FIELD_HINTS_TO_EXPORT.keys():
            if hint in nm and hint not in hint_to_key:
                hint_to_key[hint] = f.get("key")

    def get_field(p: dict, hint: str) -> str:
        key = hint_to_key.get(hint)
        if not key:
            return ""
        v = p.get(key)
        if isinstance(v, dict) and "label" in v:
            return str(v.get("label") or "")
        if isinstance(v, list):
            if v and isinstance(v[0], dict) and "value" in v[0]:
                return str(v[0].get("value") or "")
            return ", ".join([str(x) for x in v if x])
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        return str(v)

    # -------------------------------------------------------------------------
    # Sammle Personen parallel pro Batch-ID
    # -------------------------------------------------------------------------
    async def collect_batch(bid: str) -> List[dict]:
        persons = []
        page = 0
    async for chunk in stream_persons_by_batch_id(batch_key, [bid]):
        persons.extend(chunk)
        page += 1
        if job_obj and page % 2 == 0:
            job_obj.phase = f"Lade Batch {bid} – Seite {page} ({len(persons)} Personen)"
            job_obj.percent = min(10 + (len(persons) // 300), 40)
            await asyncio.sleep(0.05)  # Eventloop freigeben → Fortschritt sichtbar
    return persons


    # Parallel alle Batch-IDs abfragen
    if job_obj:
        job_obj.phase = "Starte parallele Batch-Abfragen …"
        job_obj.percent = 10

    results = await asyncio.gather(
        *[collect_batch(bid) for bid in nf_batch_ids],
        return_exceptions=True,
    )

    rows: List[dict] = []
    total = 0
    for idx, res in enumerate(results):
        if isinstance(res, Exception):
            print(f"[WARN] Batch {nf_batch_ids[idx]} Fehler: {res}")
            continue
        if job_obj:
            job_obj.phase = f"Verarbeite Batch {nf_batch_ids[idx]} ({len(res)} Treffer)"
            job_obj.percent = 15 + int((idx + 1) / len(results) * 20)
        for p in res:
            if total >= NF_MAX_ROWS:
                break
            org = p.get("org_id") or {}
            org_name = org.get("name") or p.get("org_name") or "-"
            org_id = str(org.get("id") or "")
            emails = _as_list_email(p.get("email"))
            email = emails[0] if emails else ""
            first = p.get("first_name") or ""
            last = p.get("last_name") or ""
            rows.append({
                "Batch ID": batch_id or "",
                "Channel": DEFAULT_CHANNEL,
                "Cold-Mailing Import": campaign or "",
                "Prospect ID": get_field(p, "prospect"),
                "Organisation ID": org_id,
                "Organisation Name": org_name,
                "Person ID": str(p.get("id") or ""),
                "Person Vorname": first,
                "Person Nachname": last,
                "Person Titel": get_field(p, "titel") or get_field(p, "title") or get_field(p, "anrede"),
                "Person Geschlecht": get_field(p, "gender") or get_field(p, "geschlecht"),
                "Person Position": get_field(p, "position"),
                "Person E-Mail": email,
                "XING Profil": get_field(p, "xing") or get_field(p, "xing url") or get_field(p, "xing profil"),
                "LinkedIn URL": get_field(p, "linkedin"),
            })
            total += 1
        if total >= NF_MAX_ROWS:
            break

    df = pd.DataFrame(rows, columns=TEMPLATE_COLUMNS)
    await save_df_text(df, "nf_master_final")

    if job_obj:
        job_obj.phase = f"Daten gesammelt: {len(df)} Zeilen"
        job_obj.percent = 40
    return df


# =============================================================================
# Neukontakte – Aufbau Master
# =============================================================================
async def _build_nk_master_final(
    fachbereich: str,
    take_count: Optional[int],
    batch_id: Optional[str],
    campaign: Optional[str],
    per_org_limit: int,
    job_obj=None,
) -> pd.DataFrame:
    fb_field = await get_person_field_by_hint(FIELD_FACHBEREICH_HINT)
    if not fb_field:
        raise RuntimeError("'Fachbereich'-Feld nicht gefunden.")
    fb_key = fb_field.get("key")

    person_fields = await get_person_fields()
    hint_to_key: Dict[str, str] = {}
    gender_map: Dict[str, str] = {}
    for f in person_fields:
        nm = (f.get("name") or "").lower()
        for hint in PERSON_FIELD_HINTS_TO_EXPORT.keys():
            if hint in nm and hint not in hint_to_key:
                hint_to_key[hint] = f.get("key")
        if any(x in nm for x in ("gender", "geschlecht")):
            gender_map = field_options_id_to_label_map(f)

    def get_field(p: dict, hint: str) -> str:
        key = hint_to_key.get(hint)
        if not key:
            return ""
        v = p.get(key)
        if isinstance(v, dict) and "label" in v:
            return str(v.get("label") or "")
        if isinstance(v, list):
            if v and isinstance(v[0], dict) and "value" in v[0]:
                return str(v[0].get("value") or "")
            return ", ".join([str(x) for x in v if x])
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        sv = str(v)
        if hint in ("gender", "geschlecht") and gender_map:
            return gender_map.get(sv, sv)
        return sv

    selected: List[dict] = []
    org_used: Dict[str, int] = {}
    total = 0

    if job_obj:
        job_obj.phase = "Lade Neukontakte …"
        job_obj.percent = 10

    async for chunk in stream_persons_by_filter(FILTER_NEUKONTAKTE):
        for p in chunk:
            fb_val = p.get(fb_key)
            if str(fb_val) != str(fachbereich):
                continue
            org = p.get("org_id") or {}
            org_key = f"id:{org.get('id')}" if org.get("id") else f"name:{normalize_name(org.get('name') or '')}"
            used = org_used.get(org_key, 0)
            if used >= per_org_limit:
                continue
            org_used[org_key] = used + 1
            selected.append(p)
            total += 1
            if take_count and len(selected) >= take_count:
                break
        if take_count and len(selected) >= take_count:
            break
    if job_obj:
        job_obj.phase = f"Neukontakte gesammelt: {len(selected)}"
        job_obj.percent = 40

    rows = []
    for p in selected:
        pid = p.get("id")
        org_id = ""
        org_name = "-"
        org = p.get("org_id")
        if isinstance(org, dict):
            org_id = str(org.get("id") or org.get("value") or "").strip()
            org_name = org.get("name") or p.get("org_name") or "-"
        elif isinstance(org, (int, str)):
            org_id = str(org).strip()
            org_name = p.get("org_name") or "-"
        else:
            org_name = p.get("org_name") or "-"
        emails = _as_list_email(p.get("email"))
        email = emails[0] if emails else ""
        first = p.get("first_name") or ""
        last = p.get("last_name") or ""
        rows.append({
            "Batch ID": batch_id or "",
            "Channel": DEFAULT_CHANNEL,
            "Cold-Mailing Import": campaign or "",
            "Prospect ID": get_field(p, "prospect"),
            "Organisation ID": org_id,
            "Organisation Name": org_name,
            "Person ID": str(pid or ""),
            "Person Vorname": first,
            "Person Nachname": last,
            "Person Titel": get_field(p, "titel") or get_field(p, "title") or get_field(p, "anrede"),
            "Person Geschlecht": get_field(p, "gender") or get_field(p, "geschlecht"),
            "Person Position": get_field(p, "position"),
            "Person E-Mail": email,
            "XING Profil": get_field(p, "xing") or get_field(p, "xing url") or get_field(p, "xing profil"),
            "LinkedIn URL": get_field(p, "linkedin"),
        })

    df = pd.DataFrame(rows, columns=TEMPLATE_COLUMNS)
    await save_df_text(df, "nk_master_final")
    return df

# --- Ende Teil 3/5 ---
# =============================================================================
# Abgleich – Neukontakte & Nachfass (gemeinsame Logik)
# =============================================================================
async def _fetch_org_names_for_filter_capped(filter_id: int, page_limit: int, cap_total: int, cap_bucket: int) -> Dict[str, List[str]]:
    buckets: Dict[str, List[str]] = {}
    total = 0
    async for chunk in stream_organizations_by_filter(filter_id, page_limit):
        for o in chunk:
            n = normalize_name(o.get("name") or "")
            if not n:
                continue
            b = n[0]
            lst = buckets.setdefault(b, [])
            if len(lst) >= cap_bucket:
                continue
            if not lst or lst[-1] != n:
                lst.append(n)
                total += 1
                if total >= cap_total:
                    return buckets
    return buckets
# =============================================================================
# PERFORMANCE-HELPERS – Cache, Fuzzy-Skip, Parallel IDs
# =============================================================================
from functools import lru_cache

# --- Cache für Organisationsnamen (pro Filter-ID) ---
_ORG_CACHE: Dict[int, List[str]] = {}

async def get_org_cache(filter_id: int) -> List[str]:
    """Lädt und cached Organisationen aus Pipedrive-Filter."""
    if filter_id in _ORG_CACHE:
        return _ORG_CACHE[filter_id]
    names: List[str] = []
    async for chunk in stream_organizations_by_filter(filter_id):
        for o in chunk:
            n = normalize_name(o.get("name") or "")
            if n:
                names.append(n)
    _ORG_CACHE[filter_id] = names
    return names

# --- ID-Liste (1216/1708) parallel laden ---
async def fetch_ids_parallel(ids: List[int]) -> set:
    async def _fetch(fid: int) -> set:
        out: set = set()
        async for page in stream_person_ids_by_filter(fid):
            out.update(page)
        return out
    results = await asyncio.gather(*[_fetch(fid) for fid in ids])
    merged = set().union(*results)
    return merged

# --- schneller Vergleich (2-Buchstaben-Bucket) ---
def bucket_key(name: str) -> str:
    n = normalize_name(name)
    if not n:
        return ""
    return n[:2] if len(n) > 1 else n

# --- fuzzy ratio (schneller als token_sort_ratio) ---
def fast_fuzzy(a: str, b: str) -> int:
    return fuzz.partial_ratio(a, b)

async def _reconcile_generic(prefix: str, job_obj=None):
    master = await load_df_text(f"{prefix}_master_final")
    if len(master) > 5000:
        job_obj.phase = f"Reduziere Vergleichsdaten (nur 5000 von {len(master)}) …"
        sample_size = min(5000, len(master))
        master = master.sample(sample_size, random_state=42).reset_index(drop=True)
    if master.empty:
        await save_df_text(pd.DataFrame(), f"{prefix}_master_ready")
        await save_df_text(pd.DataFrame(columns=["reason","id","name","org_id","org_name","extra"]), f"{prefix}_delete_log")
        return

    delete_rows: List[Dict[str, str]] = []
    col_person_id = "Person ID"
    col_org_name = "Organisation Name"
    col_org_id = "Organisation ID"

    # Orga-Dubletten via ≥95 % (schneller: nur Filter 1245)
    if job_obj:
        job_obj.phase = "Orga-Abgleich …"
        job_obj.percent = 55
    filter_ids_org = [1245]
    buckets_all: Dict[str, List[str]] = {}
    total_collected = 0
    for fid in filter_ids_org:
        caps_left = max(0, MAX_ORG_NAMES - total_collected)
        if caps_left <= 0:
            break
        buckets = await _fetch_org_names_for_filter_capped(fid, PAGE_LIMIT, caps_left, MAX_ORG_BUCKET)
        for k, lst in buckets.items():
            slot = buckets_all.setdefault(k, [])
            for n in lst:
                if len(slot) >= MAX_ORG_BUCKET:
                    break
                if not slot or slot[-1] != n:
                    slot.append(n)
                    total_collected += 1
                    if total_collected >= MAX_ORG_NAMES:
                        break
            if total_collected >= MAX_ORG_NAMES:
                break
        if total_collected >= MAX_ORG_NAMES:
            break

    drop_idx = []
    for idx, row in master.iterrows():
        cand = str(row.get(col_org_name) or "").strip()
        norm = normalize_name(cand)
        if not norm:
            continue
        bucket = buckets_all.get(norm[0])
        if not bucket:
            continue
        near = [n for n in bucket if abs(len(n) - len(norm)) <= 4]
        if not near:
            continue
        # Wenn eine Organisation ID existiert, kein Fuzzy-Abgleich nötig
        if str(row.get(col_org_id) or "").strip():
            continue
        best = process.extractOne(norm, near, scorer=fuzz.token_set_ratio)
    
        if best and best[1] >= 95:
            drop_idx.append(idx)
            delete_rows.append({
                "reason": "org_match_95",
                "id": str(row.get(col_person_id) or ""),
                "name": f"{row.get('Person Vorname') or ''} {row.get('Person Nachname') or ''}".strip(),
                "org_id": str(row.get(col_org_id) or ""),
                "org_name": cand,
                "extra": f"Best Match: {best[0]} ({best[1]}%)",
            })

    if drop_idx:
        master = master.drop(index=drop_idx)

    # Person-ID-Dubletten
    if job_obj:
        job_obj.phase = "Person-ID-Abgleich …"
        job_obj.percent = 70
    suspect_ids: set = set()
    async for ids in stream_person_ids_by_filter(1216, PAGE_LIMIT):
        suspect_ids.update(ids)
    async for ids in stream_person_ids_by_filter(1708, PAGE_LIMIT):
        suspect_ids.update(ids)
    if suspect_ids:
        mask = master[col_person_id].astype(str).isin(suspect_ids)
        removed = master[mask].copy()
        for _, r in removed.iterrows():
            delete_rows.append({
                "reason": "person_id_match",
                "id": str(r.get(col_person_id) or ""),
                "name": f"{r.get('Person Vorname') or ''} {r.get('Person Nachname') or ''}".strip(),
                "org_id": str(r.get(col_org_id) or ""),
                "org_name": str(r.get(col_org_name) or ""),
                "extra": "ID in Filter 1216/1708",
            })
        master = master[~mask].copy()

    await save_df_text(master, f"{prefix}_master_ready")
    log_df = pd.DataFrame(delete_rows, columns=["reason", "id", "name", "org_id", "org_name", "extra"])
    await save_df_text(log_df, f"{prefix}_delete_log")

    if job_obj:
        job_obj.phase = "Abgleich abgeschlossen"
        job_obj.percent = 85
# =============================================================================
# HIGH-PERFORMANCE RECONCILE
# =============================================================================
async def _reconcile(prefix: str) -> None:
    """Optimierte Abgleich-Routine mit Caching, parallelem ID-Fetch & Fuzzy-Skip."""
    t = tables(prefix)
    master = await load_df_text(t["final"])
    if master.empty:
        await save_df_text(pd.DataFrame(), t["ready"])
        await save_df_text(pd.DataFrame(columns=["reason","id","name","org_id","org_name","extra"]), t["log"])
        return

    col_person_id = "Person ID"
    col_org_name = "Organisation Name"
    col_org_id = "Organisation ID"
    delete_rows: List[Dict[str, str]] = []

    # === 1) Organisations-Dubletten vorbereiten ===
    filter_ids_org = [1245]  # nur ein kombinierter Filter nutzen
    buckets_all: Dict[str, List[str]] = {}

    # Organisationen aus Cache laden
    for fid in filter_ids_org:
        names = await get_org_cache(fid)
        for n in names:
            b = bucket_key(n)
            if not b:
                continue
            lst = buckets_all.setdefault(b, [])
            if len(lst) < MAX_ORG_BUCKET:
                lst.append(n)

    # === 2) Orga-Fuzzy-Vergleich optimiert ===
    drop_idx = []
    for idx, row in master.iterrows():
        cand = str(row.get(col_org_name) or "").strip()
        cand_norm = normalize_name(cand)
        if not cand_norm:
            continue
        if str(row.get(col_org_id) or "").strip():
            # hat ID -> überspringen
            continue
        b = bucket_key(cand)
        bucket = buckets_all.get(b)
        if not bucket:
            continue
        near = [n for n in bucket if abs(len(n) - len(cand_norm)) <= 3]
        if not near:
            continue
        best = process.extractOne(cand_norm, near, scorer=fast_fuzzy)
        if best and best[1] >= 95:
            drop_idx.append(idx)
            delete_rows.append({
                "reason": "org_match_95",
                "id": str(row.get(col_person_id) or ""),
                "name": f"{row.get('Person Vorname') or ''} {row.get('Person Nachname') or ''}".strip(),
                "org_id": str(row.get(col_org_id) or ""),
                "org_name": cand,
                "extra": f"Best Match: {best[0]} ({best[1]}%)"
            })

    if drop_idx:
        master = master.drop(index=drop_idx)

    # === 3) Person-IDs parallel laden ===
    suspect_ids = await fetch_ids_parallel([1216, 1708])
    if suspect_ids:
        mask_pid = master[col_person_id].astype(str).isin(suspect_ids)
        removed = master[mask_pid].copy()
        for _, r in removed.iterrows():
            delete_rows.append({
                "reason": "person_id_match",
                "id": str(r.get(col_person_id) or ""),
                "name": f"{r.get('Person Vorname') or ''} {r.get('Person Nachname') or ''}".strip(),
                "org_id": str(r.get(col_org_id) or ""),
                "org_name": str(r.get(col_org_name) or ""),
                "extra": "ID in Filter 1216/1708"
            })
        master = master[~mask_pid].copy()

    # === 4) Ergebnisse speichern ===
    await save_df_text(master, t["ready"])
    log_df = pd.DataFrame(delete_rows, columns=["reason","id","name","org_id","org_name","extra"])
    await save_df_text(log_df, t["log"])

# =============================================================================
# Excel-Export
# =============================================================================
def build_export_from_ready(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(columns=TEMPLATE_COLUMNS)
    for col in TEMPLATE_COLUMNS:
        out[col] = df[col] if col in df.columns else ""
    for c in ("Organisation ID", "Person ID"):
        if c in out.columns:
            out[c] = out[c].astype(str).fillna("").replace("nan", "")
    return out

def _df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Export")
        ws = writer.sheets["Export"]
        col_index = {col: i + 1 for i, col in enumerate(df.columns)}
        for name in ("Organisation ID", "Person ID"):
            if name in col_index:
                j = col_index[name]
                for i in range(2, len(df) + 2):
                    ws.cell(i, j).number_format = "@"
        writer.book.properties.creator = "BatchFlow"
    buf.seek(0)
    return buf.getvalue()

# =============================================================================
# Job-Verwaltung & Fortschritt
# =============================================================================
class Job:
    def __init__(self) -> None:
        self.phase = "Warten …"
        self.percent = 0
        self.done = False
        self.error: Optional[str] = None
        self.path: Optional[str] = None
        self.total_rows: int = 0
        self.filename_base: str = "BatchFlow_Export"

JOBS: Dict[str, Job] = {}
# =============================================================================
# JOB-PROGRESS HELPER
# =============================================================================
async def reconcile_with_progress(job: "Job", prefix: str):
    """
    Führt den optimierten _reconcile()-Lauf mit detaillierten Fortschrittsmeldungen aus.
    Zeigt in der UI konkrete Schritte mit Prozenten.
    """
    try:
        t = tables(prefix)
        job.phase = "Vorbereitung läuft …"
        job.percent = 10
        await asyncio.sleep(0.2)

        master = await load_df_text(t["final"])
        if master.empty:
            job.phase = "Keine Daten vorhanden"
            job.percent = 100
            await save_df_text(pd.DataFrame(), t["ready"])
            await save_df_text(
                pd.DataFrame(columns=["reason", "id", "name", "org_id", "org_name", "extra"]), t["log"]
            )
            return

        # Phase 1: Lade Organisationsdaten
        job.phase = "Lade Organisationsdaten (Filter 1245) …"
        job.percent = 25
        await asyncio.sleep(0.2)

        # Phase 2: Orga-Vergleich starten
        job.phase = "Vergleiche Organisationen …"
        job.percent = 50
        await asyncio.sleep(0.2)
        await _reconcile(prefix)

        # Phase 3: Prüfe Person-IDs
        job.phase = "Prüfe Person-IDs (Filter 1216/1708) …"
        job.percent = 70
        await asyncio.sleep(0.2)

        # Phase 4: Ergebnisse speichern
        job.phase = "Speichere Ergebnisse …"
        job.percent = 85
        await asyncio.sleep(0.2)

        # Phase 5: Fertig
        job.phase = "Fertig – Ergebnisse erfolgreich gespeichert"
        job.percent = 100
        await asyncio.sleep(0.2)

    except Exception as e:
        job.error = f"Fehler beim Abgleich: {e}"
        job.phase = "Fehler"
        job.percent = 100

# -----------------------------------------------------------------------------
# Export-Start – Neukontakte
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Export-Start – Neukontakte (mit dynamischem Fortschritt)
# -----------------------------------------------------------------------------
@app.post("/neukontakte/export_start")
async def export_start_nk(
    fachbereich: str = Body(...),
    take_count: Optional[int] = Body(None),
    batch_id: Optional[str] = Body(None),
    campaign: Optional[str] = Body(None),
    per_org_limit: int = Body(PER_ORG_DEFAULT_LIMIT),
):
    job_id = str(uuid.uuid4())
    job = Job()
    JOBS[job_id] = job
    job.phase = "Initialisiere …"
    job.percent = 1
    job.filename_base = slugify_filename(campaign or "BatchFlow_Export")

    import asyncio

    async def update_progress(phase: str, percent: int):
        job.phase = phase
        job.percent = max(0, min(100, percent))
        await asyncio.sleep(0.05)  # UI-Refresh erlauben

    async def _run():
        try:
            print("[Nachfass] _run() gestartet")
            await update_progress("Lade Nachfass-Daten aus Pipedrive …", 5)
            df = await _build_nf_master_final(nf_batch_ids, batch_id, campaign, job_obj=job)
            print(f"[Nachfass] Fortschritt: {job.phase} ({job.percent}%)")

            if df.empty:
                job.error = "Keine Personen für angegebene Batch-IDs gefunden."
                job.phase = "Keine Daten"
                job.percent = 100
                job.done = True
                print("[Nachfass] Keine Daten gefunden, breche ab")
            return
            
            await update_progress("Lade Neukontakte aus Pipedrive …", 5)
            df = await _build_nk_master_final(
                fachbereich, take_count, batch_id, campaign, per_org_limit, job_obj=job
            )

            if job_obj and len(df) > 5000:
                await update_progress(f"Reduziere Vergleichsdaten (nur 5000 von {len(df)}) …", 52)
                df = df.sample(5000, random_state=42)

            await update_progress("Abgleich (Organisationen & IDs) …", 60)
            await reconcile_with_progress(job, "nk")

            await update_progress("Erzeuge Excel-Datei …", 85)
            ready = await load_df_text("nk_master_ready")
            export_df = build_export_from_ready(ready)
            data = _df_to_excel_bytes(export_df)
            path = f"/tmp/{job.filename_base}.xlsx"

            await update_progress(f"Schreibe Datei ({len(export_df)} Zeilen) …", 90)
            with open(path, "wb") as f:
                f.write(data)

            await update_progress("Export abgeschlossen – Download startet gleich …", 100)
            job.total_rows = len(export_df)
            job.path = path
            job.done = True

        except Exception as e:
            job.error = f"Fehler: {e}"
            job.phase = "Fehler"
            job.percent = 100
            job.done = True

    asyncio.create_task(_run())
    return JSONResponse({"job_id": job_id})
# ----------------------------------------------------------------------------- 
# Export-Start – Nachfass (stabil, speichert Excel im RAM, mit Fortschritt)
# -----------------------------------------------------------------------------
@app.post("/nachfass/export_start")
async def export_start_nf(
    nf_batch_ids: List[str] = Body(..., embed=True),
    batch_id: str = Body(...),
    campaign: str = Body(...),
):
    job_id = str(uuid.uuid4())
    job = Job()
    JOBS[job_id] = job
    job.phase = "Initialisiere Nachfass …"
    job.percent = 1
    job.filename_base = slugify_filename(campaign or "BatchFlow_Export")

    import asyncio

    async def update_progress(phase: str, percent: int):
        """Hilfsfunktion für sanfte Fortschrittsupdates im UI."""
        job.phase = phase
        job.percent = max(0, min(100, percent))
        await asyncio.sleep(0.05)

    async def _run():
        try:
            # ---------------------------
            # 1️⃣ Lade Personen nach Batch-ID
            # ---------------------------
            await update_progress("Lade Nachfass-Daten aus Pipedrive …", 5)
            df = await _build_nf_master_final(nf_batch_ids, batch_id, campaign, job_obj=job)

            if df.empty:
                job.error = "Keine Personen für angegebene Batch-IDs gefunden."
                job.phase = "Keine Daten"
                job.percent = 100
                job.done = True
                return

            total = len(df)
            if total > 8000:
                await update_progress(f"Reduziere Vergleichsdaten (nur 8000 von {total}) …", 30)
                df = df.sample(8000, random_state=42)

            # ---------------------------
            # 2️⃣ Abgleich / Dublettenprüfung
            # ---------------------------
            await update_progress("Führe Abgleich (Organisationen & IDs) durch …", 55)
            await reconcile_with_progress(job, "nf")

            # ---------------------------
            # 3️⃣ Excel im Arbeitsspeicher erzeugen
            # ---------------------------
            await update_progress("Erzeuge Excel-Datei …", 80)
            ready = await load_df_text("nf_master_ready")
            export_df = build_export_from_ready(ready)
            job.excel_bytes = _df_to_excel_bytes(export_df)  # RAM statt /tmp/

            # ---------------------------
            # 4️⃣ Abschluss
            # ---------------------------
            await update_progress(f"Export abgeschlossen – {len(export_df)} Zeilen", 100)
            job.total_rows = len(export_df)
            job.done = True

            print(f"[Nachfass] Export abgeschlossen – {job.total_rows} Zeilen, Job-ID: {job_id}")

        except Exception as e:
            job.error = f"Fehler: {e}"
            job.phase = "Fehler"
            job.percent = 100
            job.done = True

    asyncio.create_task(_run())
    return JSONResponse({"job_id": job_id})



# --- Ende Teil 4/5 ---
# =============================================================================
# Summary & UI (Kampagne, Neukontakte, Nachfass)
# =============================================================================
def _count_reason(df: pd.DataFrame, keys: List[str]) -> int:
    if df.empty or "reason" not in df.columns:
        return 0
    r = df["reason"].astype(str).str.lower()
    return int(r.isin([k.lower() for k in keys]).sum())

# -----------------------------------------------------------------------------
# Kampagnenübersicht (/campaign)
# -----------------------------------------------------------------------------
@app.get("/campaign", response_class=HTMLResponse)
async def campaign_home():
    return HTMLResponse("""<!doctype html><html lang="de">
<head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>BatchFlow – Kampagne wählen</title>
<style>
  body{margin:0;background:#f6f8fb;font:16px/1.6 Inter,sans-serif;color:#0f172a}
  header{background:#fff;border-bottom:1px solid #e2e8f0}
  .hwrap{max-width:1100px;margin:0 auto;padding:14px 20px;display:flex;
         justify-content:space-between;align-items:center}
  main{max-width:1100px;margin:30px auto;padding:0 20px}
  .grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:20px}
  .card{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:20px;
        box-shadow:0 2px 8px rgba(2,8,23,.04)}
  .title{font-weight:700;font-size:18px;margin-bottom:4px}
  .desc{color:#64748b;min-height:48px}
  .btn{display:inline-block;background:#0ea5e9;color:#fff;padding:10px 14px;
       border-radius:10px;text-decoration:none}
  .btn:hover{background:#0284c7}
</style></head>
<body>
<header><div class="hwrap"><div><b>BatchFlow</b></div><div>Kampagne auswählen</div></div></header>
<main>
  <div class="grid">
    <div class="card"><div class="title">Neukontakte</div>
      <div class="desc">Neue Personen aus Filter, Abgleich & Export.</div>
      <a class="btn" href="/neukontakte?mode=new">Starten</a></div>
    <div class="card"><div class="title">Nachfass</div>
      <div class="desc">Folgekampagne für bereits kontaktierte Leads.</div>
      <a class="btn" href="/nachfass">Starten</a></div>
    <div class="card"><div class="title">Refresh</div>
      <div class="desc">Kontaktdaten aktualisieren / ergänzen.</div>
      <a class="btn" href="/neukontakte?mode=refresh">Starten</a></div>
  </div>
</main></body></html>""")
# -----------------------------------------------------------------------------
# UI – Neukontakte (Frontend)
# -----------------------------------------------------------------------------
@app.get("/neukontakte", response_class=HTMLResponse)
async def neukontakte_page(request: Request, mode: str = Query("new")):
    authed = bool(user_tokens.get("default") or PD_API_TOKEN)
    authed_html = "<span class='muted'>angemeldet</span>" if authed else "<a href='/login'>Anmelden</a>"
    return HTMLResponse(f"""<!doctype html><html lang="de">
<head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Neukontakte – BatchFlow</title>
<style>
  body{{margin:0;background:#f6f8fb;color:#0f172a;font:16px/1.6 Inter,sans-serif}}
  header{{background:#fff;border-bottom:1px solid #e2e8f0}}
  .hwrap{{max-width:1120px;margin:0 auto;padding:14px 20px;display:flex;align-items:center;
          justify-content:space-between;gap:12px}}
  main{{max-width:1120px;margin:28px auto;padding:0 20px}}
  .card{{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:20px;box-shadow:0 2px 8px rgba(2,8,23,.04)}}
  .grid{{display:grid;grid-template-columns:repeat(12,1fr);gap:16px}}
  .col-3{{grid-column:span 3;min-width:220px}} .col-4{{grid-column:span 4;min-width:260px}}
  .col-5{{grid-column:span 5;min-width:260px}} .col-6{{grid-column:span 6;min-width:260px}}
  label{{display:block;font-weight:600;margin:8px 0 6px}} select,input{{width:100%;padding:10px 12px;
  border:1px solid #cbd5e1;border-radius:10px;background:#fff}}
  .btn{{background:#0ea5e9;border:none;color:#fff;border-radius:10px;padding:12px 16px;cursor:pointer}}
  .btn:disabled{{opacity:.5;cursor:not-allowed}} .hint{{color:#64748b;font-size:13px;margin-top:6px}}
  #overlay{{display:none;position:fixed;inset:0;background:rgba(255,255,255,.7);backdrop-filter:blur(2px);
            z-index:9999;align-items:center;justify-content:center;flex-direction:column;gap:10px}}
  .barwrap{{width:min(520px,90vw);height:10px;border-radius:999px;background:#e2e8f0;overflow:hidden}}
  .bar{{height:100%;width:0%;background:#0ea5e9;transition:width .2s linear}}
</style></head>
<body>
<header><div class="hwrap">
  <div><a href="/campaign" style="color:#0a66c2;text-decoration:none">← Kampagne wählen</a></div>
  <div><b>Neukontakte</b></div>
  <div>{authed_html}</div>
</div></header>

<main><section class="card"><div class="grid">
  <div class="col-3"><label>Kontakte pro Organisation</label>
    <select id="per_org_limit"><option value="1">1</option><option value="2" selected>2</option><option value="3">3</option></select>
    <div class="hint">Beispiel: 2</div></div>

  <div class="col-5"><label>Fachbereich</label><select id="fachbereich"><option value="">– bitte auswählen –</option></select>
    <div class="hint" id="fbinfo">Gesamt: …</div></div>

  <div class="col-4"><label>Wie viele Datensätze?</label><input id="take_count" placeholder="z. B. 900" type="number"/>
    <div class="hint">Leer = alle Datensätze.</div></div>

  <div class="col-3"><label>Batch ID</label><input id="batch_id" placeholder="Bxxx"/></div>
  <div class="col-6"><label>Kampagnenname</label><input id="campaign" placeholder="z. B. Frühling 2025"/></div>
  <div class="col-3" style="display:flex;align-items:flex-end;justify-content:flex-end">
    <button class="btn" id="btnExport" disabled>Abgleich & Download</button></div>
</div></section></main>

<div id="overlay"><div id="phase" style="color:#0f172a"></div>
<div class="barwrap"><div class="bar" id="bar"></div></div></div>

<script>
const el = id => document.getElementById(id);
const fbSel = el('fachbereich');
const btn = el('btnExport');
function showOverlay(msg){{el('phase').textContent=msg||'';el('overlay').style.display='flex';}}
function hideOverlay(){{el('overlay').style.display='none';}}
function setProgress(p){{el('bar').style.width=Math.max(0,Math.min(100,p))+'%';}}
async function loadOptions(){{
  showOverlay('Lade Fachbereiche …');setProgress(15);
  const pol = el('per_org_limit').value;
  const r = await fetch('/neukontakte/options?per_org_limit='+encodeURIComponent(pol));
  const data = await r.json();
  fbSel.innerHTML='<option value="">– bitte auswählen –</option>';
  data.options.forEach(o=>{{const opt=document.createElement('option');opt.value=o.value;opt.textContent=o.label+' ('+o.count+')';fbSel.appendChild(opt);}});
  hideOverlay();
  fbSel.onchange=()=>btn.disabled=!fbSel.value;
}}
async function startExport(){{
  const fb=fbSel.value; if(!fb)return alert('Bitte Fachbereich wählen');
  const tc=el('take_count').value||null; const bid=el('batch_id').value||null;
  const camp=el('campaign').value||null; const pol=parseInt(el('per_org_limit').value);
  showOverlay('Starte Abgleich …');setProgress(5);
  const r=await fetch('/neukontakte/export_start',{{method:'POST',headers:{{'Content-Type':'application/json'}},
  body:JSON.stringify({{fachbereich:fb,take_count:tc?parseInt(tc):null,batch_id:bid,campaign:camp,per_org_limit:pol}})}}); 
  if(!r.ok){{hideOverlay();return alert('Start fehlgeschlagen');}}
  const{{job_id}}=await r.json();await poll(job_id);
}}
async function poll(job_id){{let done=false;while(!done){{await new Promise(r=>setTimeout(r,400));
  const r=await fetch('/neukontakte/export_progress?job_id='+encodeURIComponent(job_id));
  const s=await r.json();el('phase').textContent = (s.phase || '…') + ' (' + (s.percent || 0) + '%)';setProgress(s.percent||0);if(s.done)done=true;}}
  el('phase').textContent='Download startet …';setProgress(100);
  console.log("Fortschritt:", s.phase, s.percent);
  window.location.href='/neukontakte/export_download?job_id='+job_id;
  setTimeout(()=>window.location.href='/neukontakte/summary?job_id='+job_id,1000);
}}
el('btnExport').onclick=startExport;
loadOptions();
</script></body></html>""")
# =============================================================================
# SCHNELLERE FACHBEREICHE (NEUKONTAKTE)
# =============================================================================
@app.get("/neukontakte/options")
async def neukontakte_options(
    per_org_limit: int = Query(PER_ORG_DEFAULT_LIMIT, ge=1, le=3),
    mode: str = Query("new"),
):
    """
    Schnellere Variante mit Sampling (max. 1000 Personen) und 1h Cache.
    """
    now = time.time()
    cache = _OPTIONS_CACHE.get(per_org_limit) or {}
    if cache.get("options") and (now - cache.get("ts", 0.0) < OPTIONS_TTL_SEC):
        return JSONResponse({"total": cache["total"], "options": cache["options"]})

    fb_field = await get_person_field_by_hint(FIELD_FACHBEREICH_HINT)
    if not fb_field:
        return JSONResponse({"total": 0, "options": []})

    fb_key = fb_field.get("key")
    id2label = {str(o["id"]): o["label"] for o in fb_field.get("options", []) if "id" in o}

    total = 0
    counts: Dict[str, int] = {}
    used_by_fb: Dict[str, Dict[str, int]] = {}
    start = 0
    last_key = "last_activity_date"
    next_key = "next_activity_date"

    SAMPLE_MAX = 1000  # <- Nur 1000 Personen prüfen

    while True:
        url = append_token(f"{PIPEDRIVE_API}/persons?filter_id={FILTER_NEUKONTAKTE}&start={start}&limit={PAGE_LIMIT}")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive API Fehler: {r.text}")
        chunk = r.json().get("data") or []
        if not chunk:
            break

        for p in chunk:
            val = p.get(last_key) or p.get(next_key)
            if val and is_forbidden_activity_date(val):
                continue

            fb_val = p.get(fb_key)
            if not fb_val:
                continue
            fb_vals = [str(fb_val[0])] if isinstance(fb_val, list) else [str(fb_val)]

            org = p.get("org_id") or {}
            org_key = str(org.get("id") or org.get("name") or "")
            if not org_key:
                org_key = f"noorg:{p.get('id')}"

            fb = fb_vals[0]
            used_map = used_by_fb.setdefault(fb, {})
            used = used_map.get(org_key, 0)
            if used >= per_org_limit:
                continue
            used_map[org_key] = used + 1
            counts[fb] = counts.get(fb, 0) + 1
            total += 1

        start += len(chunk)
        if len(chunk) < PAGE_LIMIT or total >= SAMPLE_MAX:
            break

    options = [{"value": k, "label": id2label.get(k, k), "count": v} for k, v in counts.items()]
    options.sort(key=lambda x: x["count"], reverse=True)

    _OPTIONS_CACHE[per_org_limit] = {"ts": now, "total": total, "options": options}
    return JSONResponse({"total": total, "options": options})

# ----------------------------------------------------------------------------- 
# UI – Nachfass (Frontend, korrigiert & stabil)
# -----------------------------------------------------------------------------
@app.get("/nachfass", response_class=HTMLResponse)
async def nachfass_page(request: Request):
    authed = bool(user_tokens.get("default") or PD_API_TOKEN)
    authed_html = "<span class='muted'>angemeldet</span>" if authed else "<a href='/login'>Anmelden</a>"
    return HTMLResponse(f"""<!doctype html><html lang="de">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Nachfass – BatchFlow</title>
<style>
  body{{margin:0;background:#f6f8fb;color:#0f172a;font:16px/1.6 Inter,sans-serif}}
  header{{background:#fff;border-bottom:1px solid #e2e8f0}}
  .hwrap{{max-width:1120px;margin:0 auto;padding:14px 20px;display:flex;align-items:center;
          justify-content:space-between;gap:12px}}
  main{{max-width:1120px;margin:28px auto;padding:0 20px}}
  .card{{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:20px;
         box-shadow:0 2px 8px rgba(2,8,23,.04)}}
  .grid{{display:grid;grid-template-columns:repeat(12,1fr);gap:16px}}
  .col-6{{grid-column:span 6;min-width:260px}} 
  .col-3{{grid-column:span 3;min-width:220px}}
  label{{display:block;font-weight:600;margin:8px 0 6px}} 
  textarea,input{{width:100%;padding:10px 12px;border:1px solid #cbd5e1;
  border-radius:10px;background:#fff}}
  .btn{{background:#0ea5e9;border:none;color:#fff;border-radius:10px;
        padding:12px 16px;cursor:pointer;font-weight:600}}
  .btn:hover{{background:#0284c7}} 
  .hint{{color:#64748b;font-size:13px;margin-top:6px}}
  #overlay{{display:none;position:fixed;inset:0;background:rgba(255,255,255,.7);
            backdrop-filter:blur(2px);z-index:9999;align-items:center;
            justify-content:center;flex-direction:column;gap:10px}}
  .barwrap{{width:min(520px,90vw);height:10px;border-radius:999px;
            background:#e2e8f0;overflow:hidden}}
  .bar{{height:100%;width:0%;background:#0ea5e9;transition:width .25s linear}}
</style>
</head>
<body>
<header>
  <div class="hwrap">
    <div><a href="/campaign" style="color:#0a66c2;text-decoration:none">← Kampagne wählen</a></div>
    <div><b>Nachfass</b></div>
    <div>{authed_html}</div>
  </div>
</header>

<main>
<section class="card">
  <div class="grid">
    <div class="col-6">
      <label>Batch IDs (1–2 Werte)</label>
      <textarea id="nf_batch_ids" rows="3" placeholder="z. B. B111, B222"></textarea>
      <div class="hint">Komma oder Zeilenumbruch. Es wird auf Teilstring im Feld „Batch ID“ gematcht.</div>
    </div>
    <div class="col-3">
      <label>Batch ID (Export)</label>
      <input id="batch_id" placeholder="B999"/>
    </div>
    <div class="col-3">
      <label>Kampagnenname</label>
      <input id="campaign" placeholder="z. B. Folgewoche"/>
    </div>
    <div class="col-12" style="display:flex;justify-content:flex-end">
      <button class="btn" id="btnExportNf">Abgleich & Download</button>
    </div>
  </div>
</section>
</main>

<!-- Fortschritt Overlay -->
<div id="overlay">
  <div id="phase" style="color:#0f172a;font-weight:500"></div>
  <div class="barwrap"><div class="bar" id="bar"></div></div>
</div>

<script>
const el = id => document.getElementById(id);

// Overlay Steuerung
function showOverlay(msg) { el('phase').textContent = msg || ''; el('overlay').style.display = 'flex'; }
function hideOverlay() { el('overlay').style.display = 'none'; }
function setProgress(p) { el('bar').style.width = Math.max(0, Math.min(100, p)) + '%'; }

// Batch IDs parsen (max. 2)
function _parseIDs(raw) {
  return raw.split(/[\\n,;]/).map(s => s.trim()).filter(Boolean).slice(0, 2);
}

// Export starten
async function startExportNf() {
  const ids = _parseIDs(el('nf_batch_ids').value);
  if (ids.length === 0) return alert('Bitte mindestens eine Batch ID angeben.');
  const bid = el('batch_id').value || '';
  const camp = el('campaign').value || '';
  
  showOverlay('Starte Abgleich …');
  setProgress(5);

  try {
    const r = await fetch('/nachfass/export_start', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ nf_batch_ids: ids, batch_id: bid, campaign: camp })
    });
    if (!r.ok) throw new Error('Start fehlgeschlagen.');
    const { job_id } = await r.json();
    await poll(job_id);
  } catch (err) {
    alert(err.message || 'Fehler beim Starten.');
    hideOverlay();
  }
}

// Fortschritt regelmäßig abfragen
async function poll(job_id) {
  let done = false;
  while (!done) {
    await new Promise(r => setTimeout(r, 600));
    const r = await fetch('/nachfass/export_progress?job_id=' + encodeURIComponent(job_id));
    if (!r.ok) break;
    const s = await r.json();
    el('phase').textContent = (s.phase || '…') + ' (' + (s.percent || 0) + '%)';
    setProgress(s.percent || 0);
    if (s.error) { alert(s.error); hideOverlay(); return; }
    done = s.done;
  }
  el('phase').textContent = 'Download startet …';
  setProgress(100);
  window.location.href = '/nachfass/export_download?job_id=' + encodeURIComponent(job_id);
  setTimeout(() => window.location.href = '/nachfass/summary?job_id=' + job_id, 1500);
}

// Button aktivieren
el('btnExportNf').addEventListener('click', startExportNf);
</script>
</body></html>""")


# -----------------------------------------------------------------------------
# Summary – Neukontakte
# -----------------------------------------------------------------------------
@app.get("/neukontakte/summary", response_class=HTMLResponse)
async def neukontakte_summary(job_id: str = Query(...)):
    ready = await load_df_text("nk_master_ready")
    log = await load_df_text("nk_delete_log")
    total_ready = len(ready)
    cnt_org95 = _count_reason(log, ["org_match_95"])
    cnt_pid = _count_reason(log, ["person_id_match"])
    removed_sum = cnt_org95 + cnt_pid
    table_html = "<i>keine</i>"
    if not log.empty:
        view = log.tail(50).copy()
        view["Grund"] = view.apply(lambda r: f"{r.get('reason')} – {r.get('extra') or ''}", axis=1)
        view = view.rename(columns={"id": "Id", "name": "Name", "org_id": "Organisation ID", "org_name": "Organisation Name"})
        view = view[["Id", "Name", "Organisation ID", "Organisation Name", "Grund"]]
        table_html = view.to_html(classes="grid", index=False, border=0)
    html = f"""<!doctype html><html lang="de"><head><meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width,initial-scale=1"/>
    <title>Neukontakte – Ergebnis</title></head><body>
    <main style='max-width:1100px;margin:30px auto;padding:0 20px;font-family:Inter,sans-serif'>
    <section><b>Ergebnis:</b> {total_ready} Zeilen<br/>
    Entfernt Orga≥95%: {cnt_org95}, Entfernt Person-ID: {cnt_pid}, Summe: {removed_sum}</section>
    <section style='margin-top:20px'>{table_html}</section>
    <a href='/campaign'>Zur Übersicht</a></main></body></html>"""
    return HTMLResponse(html)

# -----------------------------------------------------------------------------
# Summary – Nachfass
# -----------------------------------------------------------------------------
@app.get("/nachfass/summary", response_class=HTMLResponse)
async def nachfass_summary(job_id: str = Query(...)):
    ready = await load_df_text("nf_master_ready")
    log = await load_df_text("nf_delete_log")
    total_ready = len(ready)
    cnt_org95 = _count_reason(log, ["org_match_95"])
    cnt_pid = _count_reason(log, ["person_id_match"])
    removed_sum = cnt_org95 + cnt_pid
    table_html = "<i>keine</i>"
    if not log.empty:
        view = log.tail(50).copy()
        view["Grund"] = view.apply(lambda r: f"{r.get('reason')} – {r.get('extra') or ''}", axis=1)
        view = view.rename(columns={"id": "Id", "name": "Name", "org_id": "Organisation ID", "org_name": "Organisation Name"})
        view = view[["Id", "Name", "Organisation ID", "Organisation Name", "Grund"]]
        table_html = view.to_html(classes="grid", index=False, border=0)
    html = f"""<!doctype html><html lang="de"><head><meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width,initial-scale=1"/>
    <title>Nachfass – Ergebnis</title></head><body>
    <main style='max-width:1100px;margin:30px auto;padding:0 20px;font-family:Inter,sans-serif'>
    <section><b>Ergebnis:</b> {total_ready} Zeilen<br/>
    Entfernt Orga≥95%: {cnt_org95}, Entfernt Person-ID: {cnt_pid}, Summe: {removed_sum}</section>
    <section style='margin-top:20px'>{table_html}</section>
    <a href='/campaign'>Zur Übersicht</a></main></body></html>"""
    return HTMLResponse(html)
# ----------------------------------------------------------------------------- 
# Fortschritt & Download – Nachfass & Neukontakte
# -----------------------------------------------------------------------------
@app.get("/nachfass/export_progress")
async def nachfass_export_progress(job_id: str = Query(...)):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job nicht gefunden")
    return JSONResponse({
        "phase": job.phase,
        "percent": job.percent,
        "done": job.done,
        "error": job.error,
    })


from fastapi.responses import StreamingResponse

@app.get("/nachfass/export_download")
async def nachfass_export_download(job_id: str = Query(...)):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job nicht gefunden")

    # Excel direkt aus Arbeitsspeicher (RAM)
    if getattr(job, "excel_bytes", None):
        filename = f"{job.filename_base}.xlsx"
        return StreamingResponse(
            io.BytesIO(job.excel_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    # Fallback, falls doch lokal geschrieben wurde
    if job.path and os.path.exists(job.path):
        return FileResponse(
            job.path,
            filename=f"{job.filename_base}.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    raise HTTPException(status_code=404, detail="Datei nicht gefunden")



@app.get("/neukontakte/export_progress")
async def neukontakte_export_progress(job_id: str = Query(...)):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job nicht gefunden")
    return JSONResponse({
        "phase": job.phase,
        "percent": job.percent,
        "done": job.done,
        "error": job.error,
    })


@app.get("/neukontakte/export_download")
async def neukontakte_export_download(job_id: str = Query(...)):
    job = JOBS.get(job_id)
    if not job or not job.path:
        raise HTTPException(status_code=404, detail="Datei nicht gefunden")
    return FileResponse(job.path, filename=f"{job.filename_base}.xlsx", media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# -----------------------------------------------------------------------------
# Catch-All (kein Redirect-Loop)
# -----------------------------------------------------------------------------
@app.get("/{full_path:path}", include_in_schema=False)
async def catch_all(full_path: str, request: Request):
    if full_path in ("campaign", "", "/"):
        return HTMLResponse("<h3>BatchFlow läuft – Seite nicht gefunden.</h3>")
    return RedirectResponse("/campaign", status_code=307)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("master_full_fixed:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=False)

# --- Ende Teil 5/5 ---
