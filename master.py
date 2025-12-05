
import logging


import os, re, io, uuid, time, asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, AsyncGenerator, Any
import numpy as np, pandas as pd, json, httpx, asyncpg
from rapidfuzz import fuzz, process
from fastapi import FastAPI, Request, Body, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware
from collections import defaultdict

fuzz.default_processor = lambda s: s  # kein Vor-Preprocessing

# ------------------------------------------------------------------------------
# 
# -----------------------------------------------------------------------------# 
# PIPEDRIVE API - V2 
# -----------------------------------------------------------------------------# 
# 
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# App-Grundkonfiguration allgemein (start) 
# -----------------------------------------------------------------------------
app = FastAPI(title="BatchFlow")
app.add_middleware(GZipMiddleware, minimum_size=1024)
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")
    
@app.get("/healthz")
async def healthz():
    """Einfacher Healthcheck für Render: immer 200."""
    return {"status": "ok"}
# -----------------------------------------------------------------------------
# Umgebungsvariablen & Konstanten setzen
# -----------------------------------------------------------------------------
PD_API_TOKEN = os.getenv("PD_API_TOKEN", "")
PIPEDRIVE_API = "https://api.pipedrive.com/api/v2"
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL fehlt (Neon-DSN).")

SCHEMA = os.getenv("PGSCHEMA", "public")
FILTER_NEUKONTAKTE = int(os.getenv("FILTER_NEUKONTAKTE", "2998"))
FILTER_NACHFASS   = int(os.getenv("FILTER_NACHFASS", "3024"))
FIELD_FACHBEREICH_HINT = os.getenv("FIELD_FACHBEREICH_HINT", "fachbereich")

DEFAULT_CHANNEL = "Cold E-Mail"
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "200"))
NF_PAGE_LIMIT = int(os.getenv("NF_PAGE_LIMIT", "500"))
NF_MAX_ROWS = int(os.getenv("NF_MAX_ROWS", "10000"))
RECONCILE_MAX_ROWS = int(os.getenv("RECONCILE_MAX_ROWS", "20000"))
PER_ORG_DEFAULT_LIMIT = int(os.getenv("PER_ORG_DEFAULT_LIMIT", "2"))
MAX_ORG_NAMES = int(os.getenv("MAX_ORG_NAMES", "1000"))
MAX_ORG_BUCKET = int(os.getenv("MAX_ORG_BUCKET", "200"))

# -----------------------------------------------------------------------------
# Cache-Strukturen
# -----------------------------------------------------------------------------
user_tokens: Dict[str, str] = {}
_PERSON_FIELDS_CACHE: Optional[List[dict]] = None
_OPTIONS_CACHE: Dict[int, dict] = {}
_ORG_CACHE: Dict[int, List[str]] = {}

# -----------------------------------------------------------------------------
# Template-Spalten
# -----------------------------------------------------------------------------
TEMPLATE_COLUMNS = [
    "Batch ID","Channel","Cold-Mailing Import","Prospect ID","Organisation ID","Organisation Name",
    "Person ID","Person Vorname","Person Nachname","Person Titel","Person Geschlecht","Person Position",
    "Person E-Mail","XING Profil","LinkedIn URL"
]

# -----------------------------------------------------------------------------
# Geschlecht
# -----------------------------------------------------------------------------
GENDER_OPTION_MAP = {
    "19": "männlich",
    "20": "weiblich",
    "21": "divers",
    # "22": "keine Angabe",
}

# -----------------------------------------------------------------------------  
# Feldzuordnung (Personenfelder → Excel-Spalten)  
# -----------------------------------------------------------------------------  
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

# -----------------------------------------------------------------------------
# Startup / Shutdown
# -----------------------------------------------------------------------------
def http_client() -> httpx.AsyncClient: return app.state.http
def get_pool() -> asyncpg.Pool: return app.state.pool

@app.on_event("startup")
async def _startup():
    limits = httpx.Limits(max_keepalive_connections=8, max_connections=16)
    app.state.http = httpx.AsyncClient(timeout=60.0, limits=limits)
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
    print("[Startup] BatchFlow initialisiert.")

@app.on_event("shutdown")
async def _shutdown():
    await app.state.http.aclose()
    await app.state.pool.close()

import random
import asyncio

async def _get_with_retries(client, url: str, sem: asyncio.Semaphore, label: str,
                            retries: int = 6, base_delay: float = 1.0, max_delay: float = 30.0):
    """
    Robuster GET mit Retries bei 429/5xx und leichtem Jitter.
    Gibt (response, None) zurück wenn ok, sonst (None, last_error_str).
    """
    delay = base_delay
    last_err = None

    for attempt in range(1, retries + 1):
        try:
            async with sem:
                r = await client.get(url, headers=get_headers())

            status = getattr(r, "status_code", None)

            # OK
            if status == 200:
                return r, None

            # Not found: nicht weiter retryen
            if status == 404:
                return r, "404"

            # Rate limit / server errors -> retry
            if status == 429 or (status is not None and status >= 500):
                last_err = f"HTTP {status}: {getattr(r, 'text', '')[:200]}"
                # exponential backoff + jitter
                jitter = random.uniform(0, 0.3 * delay)
                await asyncio.sleep(min(delay + jitter, max_delay))
                delay = min(delay * 2, max_delay)
                continue

            # andere HTTP Fehler: 4xx außer 404 -> retry ist manchmal sinnvoll, aber begrenzt
            last_err = f"HTTP {status}: {getattr(r, 'text', '')[:500]}"
            jitter = random.uniform(0, 0.2 * delay)
            await asyncio.sleep(min(delay + jitter, max_delay))
            delay = min(delay * 2, max_delay)

        except Exception as e:
            last_err = f"EXC: {e}"
            jitter = random.uniform(0, 0.2 * delay)
            await asyncio.sleep(min(delay + jitter, max_delay))
            delay = min(delay * 2, max_delay)

    return None, last_err
import random
import asyncio
from typing import Optional, Tuple

# Global: eine Bremse für alle Requests (Pipedrive mag keine parallelen Bursts)
PD_SEM = asyncio.Semaphore(1)

def _retry_after_seconds(resp) -> float:
    ra = (resp.headers or {}).get("Retry-After")
    if not ra:
        return 0.0
    try:
        return float(ra)
    except Exception:
        return 0.0

async def pd_get_with_retry(
    client,
    url: str,
    headers: dict,
    label: str = "",
    retries: int = 8,
    base_delay: float = 0.8,
    sem: asyncio.Semaphore = PD_SEM,
):
    """
    Einheitlicher GET mit Retries (429/5xx/Netzwerk).
    Wichtig: respektiert Retry-After und macht Jitter -> weniger 429.
    """
    delay = base_delay
    last_err = None

    for attempt in range(1, retries + 1):
        try:
            async with sem:
                r = await client.get(url, headers=headers)

            # OK
            if r.status_code == 200:
                return r

            # Nicht retrybar
            if r.status_code in (400, 401, 403, 404):
                return r

            # 429 / 5xx -> retry
            if r.status_code == 429 or (500 <= r.status_code <= 599):
                ra = _retry_after_seconds(r)
                sleep_s = max(delay, ra)
                sleep_s = min(sleep_s, 30.0)
                sleep_s = sleep_s + random.uniform(0, 0.35)  # jitter
                print(f"[pd_get_with_retry] {label} HTTP {r.status_code} attempt={attempt}/{retries}, sleep={sleep_s:.2f}s")
                await asyncio.sleep(sleep_s)
                delay = min(delay * 1.8, 30.0)
                continue

            # Sonstige Fehler -> retry konservativ
            print(f"[pd_get_with_retry] {label} HTTP {r.status_code}, retry attempt={attempt}/{retries}")
            await asyncio.sleep(delay)
            delay = min(delay * 1.8, 30.0)

        except Exception as e:
            last_err = e
            sleep_s = delay + random.uniform(0, 0.35)
            print(f"[pd_get_with_retry] {label} EXC {e}, retry attempt={attempt}/{retries}, sleep={sleep_s:.2f}s")
            await asyncio.sleep(sleep_s)
            delay = min(delay * 1.8, 30.0)

    raise RuntimeError(f"[pd_get_with_retry] FAILED {label} after {retries} retries. last_err={last_err}")

# -----------------------------------------------------------------------------
# Hilfsfunktionen
# -----------------------------------------------------------------------------
def normalize_name(s: str) -> str:
    if not s: return ""
    s = re.sub(r"[^a-z0-9 ]", "", s.lower())
    return re.sub(r"\s+", " ", s).strip()
    
def _contains_any_text(val, wanted: List[str]) -> bool:
    """Robust: prüft, ob ein Wert oder ein Value-Feld eines Dicts einen Suchtext enthält."""
    if not wanted:
        return True
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return False
    if isinstance(val, dict):
        val = val.get("value") or val.get("label") or ""
    if isinstance(val, (list, tuple, np.ndarray)):
        flat = []
        for x in val:
            if isinstance(x, dict):
                x = x.get("value") or x.get("label")
            if x:
                flat.append(str(x))
        val = " | ".join(flat)
    s = str(val).lower().strip()
    return any(k.lower() in s for k in wanted if k)


def parse_pd_date(d: Optional[str]) -> Optional[datetime]:
    try: return datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception: return None

def is_forbidden_activity_date(val: Optional[str]) -> bool:
    dt = parse_pd_date(val)
    if not dt: return False
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    three_months = today - timedelta(days=90)
    return dt > today or (three_months <= dt <= today)

def _as_list_email(value) -> List[str]:
    if not value: return []
    if isinstance(value, dict):
        v = value.get("value"); return [v] if v else []
    if isinstance(value, (list, tuple, np.ndarray)):
        out = []
        for x in value:
            if isinstance(x, dict): x = x.get("value")
            if x: out.append(str(x))
        return out
    return [str(value)]

def slugify_filename(name: str, fallback="BatchFlow_Export") -> str:
    s = re.sub(r"[^\w\-. ]+", "", (name or "").strip())
    return re.sub(r"\s+", "_", s) or fallback


def _df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    """
    Wandelt ein DataFrame in eine Excel-Datei (Bytes) um.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Export")
    return output.getvalue()


def _build_export_from_ready(filename: str):
    """
    Baut die FileResponse für Downloads aus /tmp/.
    """
    path = f"/tmp/{filename}"

    # Falls Datei nicht existiert → Fehler zurückgeben
    if not os.path.exists(path):
        return Response(
            content=f"File not found: {filename}",
            status_code=404
        )

    # Excel zurückgeben
    return FileResponse(
        path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# -----------------------------------------------------------------------------
# DB-Helper Neon-DB
# -----------------------------------------------------------------------------
async def ensure_table_text(conn: asyncpg.Connection, table: str, cols: List[str]):
    defs = ", ".join([f'"{c}" TEXT' for c in cols])
    await conn.execute(f'CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{table}" ({defs})')

async def clear_table(conn: asyncpg.Connection, table: str):
    await conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table}"')

async def save_df_text(df: pd.DataFrame, table: str):
    """
    Speichert DataFrame absolut safe in die TEXT-Tabelle.
    * Entfernt Listen, Dicts, Arrays
    * Entfernt NaN / None
    * Rest -> sauberer String
    """
    # --- Sicherer Sanitizer ---
    def sanitize_value(v):
        if v is None:
            return ""

        # floats mit NaN
        if isinstance(v, float) and pd.isna(v):
            return ""

        # Strings normalisieren
        if isinstance(v, str):
            s = v.strip()
            # JSON-Strings von Pipedrive entschärfen
            if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
                try:
                    return sanitize_value(json.loads(s))
                except:
                    return s
            return s

        # Dicts => wichtigsten Wert extrahieren
        if isinstance(v, dict):
            for k in ("value", "label", "name", "id"):
                if k in v:
                    return sanitize_value(v[k])
            # fallback
            return ""

        # Listen => erstes Element nehmen
        if isinstance(v, list):
            if not v:
                return ""
            return sanitize_value(v[0])

        # Rest als String
        return str(v)

    # ------------------------------
    # EMPTY
    # ------------------------------
    if df.empty:
        return

    async with get_pool().acquire() as conn:
        await clear_table(conn, table)
        await ensure_table_text(conn, table, list(df.columns))

        cols = list(df.columns)
        cols_sql = ", ".join(f'"{c}"' for c in cols)
        ph = ", ".join([f"${i}" for i in range(1, len(cols) + 1)])

        sql = f'INSERT INTO "{SCHEMA}"."{table}" ({cols_sql}) VALUES ({ph})'

        batch = []

        async with conn.transaction():
            for _, row in df.iterrows():
                vals = [sanitize_value(v) for v in row.tolist()]
                batch.append(vals)

                # Flush
                if len(batch) >= 1000:
                    await conn.executemany(sql, batch)
                    batch = []

            if batch:
                await conn.executemany(sql, batch)


# =============================================================================
# Tabellen-Namenszuordnung (einheitlich für Nachfass / Neukontakte)
# =============================================================================
def tables(prefix: str) -> dict:
    """
    Liefert standardisierte Tabellennamen für master_final / ready / log.
    Beispiel: tables("nf") → {"final": "nf_master_final", "ready": "nf_master_ready", "log": "nf_delete_log"}
    """
    prefix = prefix.lower().strip()
    return {
        "final": f"{prefix}_master_final",
        "ready": f"{prefix}_master_ready",
        "log":   f"{prefix}_delete_log",
    }


async def load_df_text(table: str) -> pd.DataFrame:
    """
    Lädt eine TEXT-Tabelle und wandelt ALLE Werte in saubere Strings um:
    - Listen → erster Eintrag
    - Dicts → value/label/name/id
    - JSON-Strings → decodieren
    - None/NaN → ""
    - verschachtelte Strukturen → vollständig flatten
    API-v2-sicher & frontend-sicher.
    """

    def flatten(v):
        if v is None:
            return ""
        if isinstance(v, float) and pd.isna(v):
            return ""
        if isinstance(v, str):
            s = v.strip()
            # JSON-Strings deserialisieren
            if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
                try:
                    return flatten(json.loads(s))
                except:
                    return s
            return s
        if isinstance(v, list):
            return flatten(v[0]) if v else ""
        if isinstance(v, dict):
            return flatten(
                v.get("value")
                or v.get("label")
                or v.get("name")
                or v.get("id")
                or ""
            )
        # Fallback
        return str(v)

    async with get_pool().acquire() as conn:
        rows = await conn.fetch(f'SELECT * FROM "{SCHEMA}"."{table}"')
        if not rows:
            return pd.DataFrame()

        cols = list(rows[0].keys())
        clean_rows = []

        for r in rows:
            clean_rows.append({
                c: flatten(r[c])  # zentral flatten
                for c in cols
            })

        return pd.DataFrame(clean_rows)


    async with get_pool().acquire() as conn:
        rows = await conn.fetch(f'SELECT * FROM "{SCHEMA}"."{table}"')
        if not rows:
            return pd.DataFrame()

        cols = list(rows[0].keys())
        clean_rows = []

        for r in rows:
            clean_rows.append({c: sanitize_value(r[c]) for c in cols})

        return pd.DataFrame(clean_rows).replace({"": np.nan})

# =============================================================================
# PIPEDRIVE API-HELPERS
# =============================================================================
def get_headers() -> Dict[str, str]:
    token = user_tokens.get("default", "")
    return {"Authorization": f"Bearer {token}"} if token else {}

def append_token(url: str) -> str:
    """Hängt api_token automatisch an (wenn kein OAuth-Token genutzt wird)."""
    if "api_token=" in url:
        return url
    if not user_tokens.get("default") and PD_API_TOKEN:
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}api_token={PD_API_TOKEN}"
    return url
    
# =============================================================================
# GET Retry/Backoff
# =============================================================================


# =============================================================================
# PERSONENFELDER (Cache) Leer aufgrund von Umstellung auf API v2
# =============================================================================
async def get_person_fields() -> List[dict]:
   
    return []

def field_options_id_to_label_map(field: dict) -> Dict[str, str]:
    """Erstellt ein Mapping von ID → Label für Dropdown-Optionen eines Pipedrive-Feldes."""
    opts = field.get("options") or []
    mp: Dict[str, str] = {}
    for o in opts:
        oid = str(o.get("id"))
        lab = str(o.get("label") or o.get("name") or oid)
        mp[oid] = lab
    return mp


async def get_person_field_by_hint(label_hint: str) -> Optional[dict]:
    """Findet ein Personenfeld anhand eines Text-Hints (z. B. 'fachbereich')."""
    fields = await get_person_fields()
    hint = (label_hint or "").lower()
    for f in fields:
        nm = (f.get("name") or "").lower()
        if hint in nm:
            return f
    return None


# =============================================================================
# stream_organizations_by_filter  (FINAL)
# =============================================================================

from typing import AsyncGenerator, List, Optional

async def stream_organizations_by_filter(
    filter_id: int,
    page_limit: int = PAGE_LIMIT,
) -> AsyncGenerator[List[str], None]:
    """
    Streamt Organisationen eines Pipedrive-Filters seitenweise.
    - Pagination: limit + cursor
    - 429: NICHT abbrechen, sondern warten+weiter (mit Retry)
    """
    client = http_client()
    cursor: Optional[str] = None

    while True:
        base = (
            f"{PIPEDRIVE_API}/organizations"
            f"?filter_id={filter_id}&limit={page_limit}"
            f"&sort_by=id&sort_direction=asc"
        )
        url = append_token(f"{base}&cursor={cursor}") if cursor else append_token(base)

        payload, status, err = await pd_get_json_with_retry(
            client, url, get_headers(), label=f"orgs_filter[{filter_id}]",
            retries=10, base_delay=0.8
        )

        if status != 200 or not payload:
            print(f"[stream_organizations_by_filter] WARN status={status} filter={filter_id} err={err}")
            break

        data = payload.get("data") or []
        if not data:
            break

        names: List[str] = []
        for org in data:
            name = (org.get("name") or "").strip()
            if name:
                names.append(name)

        if names:
            yield names

        additional = payload.get("additional_data") or {}
        cursor = additional.get("next_cursor")
        if not cursor:
            break

async def stream_person_ids_by_filter(
    filter_id: int,
    page_limit: int = PAGE_LIMIT,
) -> AsyncGenerator[List[str], None]:
    """
    Streamt Person-IDs eines Pipedrive-Filters seitenweise (v2).
    - Endpoint: GET /persons?filter_id=...&limit=...&cursor=...
    - Pagination: additional_data.next_cursor
    - Robust gegen RateLimit (429): wir retryen via pd_get_with_retry
    """
    client = http_client()
    cursor: Optional[str] = None

    while True:
        base = (
            f"{PIPEDRIVE_API}/persons"
            f"?filter_id={filter_id}&limit={page_limit}"
            f"&sort_by=id&sort_direction=asc"
        )
        url = append_token(f"{base}&cursor={cursor}") if cursor else append_token(base)

        r = await pd_get_with_retry(client, url, get_headers(), label=f"person_ids filter={filter_id}")
        if r.status_code != 200:
            print(f"[stream_person_ids_by_filter] HTTP {r.status_code} für Filter {filter_id}: {r.text}")
            break

        payload = r.json() or {}
        data = payload.get("data") or []
        if not data:
            break

        ids: List[str] = []
        for p in data:
            pid = sanitize((p or {}).get("id"))
            if pid:
                ids.append(pid)

        if ids:
            yield ids

        additional = payload.get("additional_data") or {}
        cursor = additional.get("next_cursor") or additional.get("nextCursor")
        if not cursor:
            break




# =============================================================================
# Organisationen – Bucketing + Kappung (Performanceoptimiert)
# =============================================================================
async def _fetch_org_names_for_filter_capped(
    page_limit: int,
    cap_limit: int,
    cap_bucket: int
) -> dict:
    """
    Holt Organisationen für Filter 1245/851/1521 (ohne "term"-Pflicht!)
    und begrenzt:
       - cap_limit = maximale Gesamtzahl Namen
       - cap_bucket = maximale Namen pro Bucket
    """

    buckets_all = {}
    total = 0

    # feste Filter-IDs (du hattest das hart verdrahtet)
    filter_ids_org = [1245, 851, 1521]

    for filter_id in filter_ids_org:

        async for chunk in stream_organizations_by_filter(filter_id, page_limit):
            for name in chunk:

                norm = normalize_name(name)
                if not norm:
                    continue

                b = bucket_key(norm)
                bucket = buckets_all.setdefault(b, [])

                # limit pro bucket
                if len(bucket) >= cap_bucket:
                    continue

                # Namen hinzufügen
                if name not in bucket:
                    bucket.append(name)
                    total += 1

                    # globales Limit erreicht?
                    if total >= cap_limit:
                        return buckets_all

    return buckets_all


def _pretty_reason(reason: str, extra: str = "") -> str:
    """Liefert verständlichen Grundtext für entfernte Zeilen."""
    reason = (reason or "").lower()
    base = {
        "org_match_95": "Organisations-Duplikat (≥95 % Ähnlichkeit)",
        "person_id_match": "Person bereits kontaktiert (Filter 1216 / 1708)"
    }.get(reason, "Entfernt")
    return f"{base}{(' – ' + extra) if extra else ''}"
# =============================================================================
# Nachfass – Personen laden nach Batch-ID (mit Paging & Fortschritt)
# =============================================================================
async def stream_persons_by_batch_id(
    batch_key: str,
    batch_ids: List[str],
    page_limit: int = 100,
    job_obj=None
) -> List[dict]:
    results: List[dict] = []
    sem = asyncio.Semaphore(2)  # gedrosselt, um 429 zu vermeiden

    async def fetch_one(bid: str):
        cursor: Optional[str] = None
        total = 0
        local: List[dict] = []
    
        async with sem:
            while True:
                base_url = (
                    f"{PIPEDRIVE_API}/persons/search?"
                    f"term={bid}&fields=custom_fields&limit={page_limit}"
                )
                url = append_token(base_url)
                if cursor:
                    url += f"&cursor={cursor}"

            
                r = await pd_get_with_retry(http_client(), url, get_headers(), label=f"persons_search bid={bid}", sem=PD_SEM)


                
                if r.status_code != 200:
                    print(f"[WARN] Batch {bid} Fehler: {r.text}")
                    break

                payload = r.json() or {}
                raw_items = (payload.get("data") or {}).get("items") or []
                if not raw_items:
                    break

                # v2: data.items[*].item → Person
                persons: List[dict] = []

                def add_item(obj):
                    if obj is None:
                        return
                    if isinstance(obj, dict):
                        item = obj.get("item")
                        if isinstance(item, dict):
                            persons.append(item)
                    elif isinstance(obj, list):
                        for sub in obj:
                            add_item(sub)

                for it in raw_items:
                    add_item(it)

                if not persons:
                    break

                local.extend(persons)
                total += len(persons)

                cursor = (payload.get("additional_data") or {}).get("next_cursor")
                if not cursor:
                    break

        if local:
            print(f"[Batch {bid}] {total} Personen gefunden.")
            # KEIN custom_fields-Mutieren mehr – v2 liefert hier eine Liste!
            results.extend(local)

        print(f"[DEBUG] Batch {bid}: {total} Personen geladen")
        # wichtig: hier **nicht** nochmal extend, sonst doppelte Einträge
        # results.extend(local)  # <- diese Zeile entfernen

    await asyncio.gather(*(fetch_one(bid) for bid in batch_ids))
    print(f"[INFO] Alle Batch-IDs geladen: {len(results)} Personen gesamt")
    return results



# =============================================================================
# Nachfass – Aufbau Master (robust, progressiv & vollständig)
# =============================================================================


# -----------------------------------------------------------------------------
# INTERNER CACHE
# -----------------------------------------------------------------------------
_NEXT_ACTIVITY_KEY: Optional[str] = None
_LAST_ACTIVITY_KEY: Optional[str] = None
_BATCH_FIELD_KEY: Optional[str] = None

# -----------------------------------------------------------------------------
# PIPEDRIVE HILFSFUNKTIONEN
# -----------------------------------------------------------------------------
# =============================================================================
# Nachfass – Personendetails laden (v2-sicher)
# =============================================================================
# =============================================================================
# Nachfass – Personendetails laden (v2-sicher)
# =============================================================================
async def fetch_person_details(person_ids: List[str]) -> List[dict]:
    """
    Lädt vollständige Personendetails für IDs – v2-sicher.
    Nutzt /persons/{id} (weil /persons/collection bei dir 404 macht).
    """
    if not person_ids:
        return []

    # nutzt deine bestehende Funktion, die /persons/{id} macht
    lookup = await fetch_person_details_many(person_ids)

    missing = [str(pid) for pid in person_ids if str(pid) not in lookup]
    if missing:
        print(
            f"[fetch_person_details] WARN: fehlende Personendetails: {len(missing)} "
            f"(Beispiele: {missing[:10]})"
        )

    ordered: List[dict] = []
    for pid in person_ids:
        p = lookup.get(str(pid))
        if p:
            ordered.append(p)

    return ordered


# -----------------------------------------------------------------------------
# PIPEDRIVE HILFSFUNKTIONEN
# -----------------------------------------------------------------------------
def sanitize(v: Any) -> str:
    """Konvertiert beliebige Werte sicher in einen String."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    if isinstance(v, str):
        s = v.strip()
        # JSON-Strings ggf. dekodieren
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                return sanitize(json.loads(s))
            except Exception:
                return s
        return s
    if isinstance(v, dict):
        return (
            sanitize(v.get("value"))
            or sanitize(v.get("label"))
            or sanitize(v.get("name"))
            or sanitize(v.get("id"))
            or ""
        )
    if isinstance(v, list):
        return sanitize(v[0]) if v else ""
    return str(v)

async def get_next_activity_key() -> Optional[str]:
    """Ermittelt das Feld für 'Nächste Aktivität'."""
    global _NEXT_ACTIVITY_KEY
    if _NEXT_ACTIVITY_KEY is not None:
        return _NEXT_ACTIVITY_KEY
    _NEXT_ACTIVITY_KEY = "next_activity_date"
    try:
        fields = await get_person_fields()
        for f in fields:
            nm = (f.get("name") or "").lower()
            if "next activity" in nm or "nächste" in nm:
                _NEXT_ACTIVITY_KEY = f.get("key")
                break
    except Exception:
        pass
    return _NEXT_ACTIVITY_KEY


async def get_last_activity_key() -> Optional[str]:
    """Ermittelt das Feld für 'Letzte Aktivität'."""
    global _LAST_ACTIVITY_KEY
    if _LAST_ACTIVITY_KEY is not None:
        return _LAST_ACTIVITY_KEY
    _LAST_ACTIVITY_KEY = "last_activity_date"
    try:
        fields = await get_person_fields()
        for f in fields:
            nm = (f.get("name") or "").lower()
            if "last activity" in nm or "letzte" in nm:
                _LAST_ACTIVITY_KEY = f.get("key")
                break
    except Exception:
        pass
    return _LAST_ACTIVITY_KEY


async def get_batch_field_key() -> Optional[str]:
    """Sucht das Personenfeld in Pipedrive, das die Batch-ID enthält."""
    global _BATCH_FIELD_KEY
    if _BATCH_FIELD_KEY is not None:
        return _BATCH_FIELD_KEY

    fields = await get_person_fields()
    for f in fields:
        nm = (f.get("name") or "").lower()
        if any(x in nm for x in ("batch id", "batch-id", "batch_id", "batch")):
            _BATCH_FIELD_KEY = f.get("key")
            break
    return _BATCH_FIELD_KEY


def extract_field_date(p: dict, key: Optional[str]) -> Optional[str]:
    """Extrahiert ein Datumsfeld aus einer Person."""
    if not key:
        return None
    v = p.get(key)
    if isinstance(v, dict):
        v = v.get("value")
    elif isinstance(v, list):
        v = v[0] if v else None
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    return str(v)


def split_name(first: Optional[str], last: Optional[str], full: Optional[str]) -> tuple[str, str]:
    """Zerlegt Namen in Vor- und Nachname."""
    if first or last:
        return first or "", last or ""
    if not full:
        return "", ""
    parts = full.strip().split()
    if len(parts) == 1:
        return parts[0], ""
    return " ".join(parts[:-1]), parts[-1]


from typing import AsyncGenerator  # oben bereits importiert; sonst hinzufügen

# -----------------------------------------------------------------------------
# STREAMING-FUNKTION
# -----------------------------------------------------------------------------
async def stream_persons_by_filter(
    filter_id: int,
    page_limit: int = NF_PAGE_LIMIT
) -> AsyncGenerator[List[dict], None]:
    """
    Liefert Personen seitenweise (Paging) aus einem Pipedrive-Filter
    über die v2-API mit Cursor-Pagination.
    """
    cursor: Optional[str] = None

    while True:
        base_url = (
            f"{PIPEDRIVE_API}/persons"
            f"?filter_id={filter_id}"
            f"&limit={page_limit}"
            f"&sort_by=id&sort_direction=asc"
        )
        url = append_token(base_url)
        if cursor:
            url += f"&cursor={cursor}"

        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive Fehler: {r.text}")

        payload = r.json() or {}
        data = payload.get("data") or []
        if not data:
            break

        yield data

        cursor = (payload.get("additional_data") or {}).get("next_cursor")
        if not cursor:
            break

# -----------------------------------------------------------------------------
# Organisationsdaten v2 - only
# -----------------------------------------------------------------------------
def extract_org_id_from_person(p: dict) -> str:
    """
    Robust über mehrere Pipedrive Shapes:
    - organization: {id, name, ...} oder {value: ...}
    - org_id: int ODER dict {"value": id, ...} (sehr häufig in v1/v2 Mischformen)
    - organization_id / orgId / orgid
    """
    # 0) org_id kann ein dict sein: {"value": 123, ...}
    org_id_field = p.get("org_id")
    if isinstance(org_id_field, dict):
        return sanitize(org_id_field.get("value") or org_id_field.get("id"))

    # 1) "organization" Objekt
    org_obj = p.get("organization")
    if isinstance(org_obj, dict):
        return sanitize(
            org_obj.get("id")
            or org_obj.get("value")
            or org_obj.get("org_id")
            or org_obj.get("organization_id")
        )

    # 2) organization Liste (manche Search-Shapes)
    if isinstance(org_obj, list) and org_obj:
        first = org_obj[0]
        if isinstance(first, dict):
            return sanitize(first.get("id") or first.get("value"))

    # 3) direkte Felder (int/string)
    return sanitize(
        org_id_field
        or p.get("organization_id")
        or p.get("orgId")
        or p.get("orgid")
    )


def extract_org_name(org: dict) -> str:
    """
    Robust aus Orga-Objekt den Namen holen – verschiedene Shapes abfangen.
    """
    if not isinstance(org, dict):
        return ""
    return sanitize(
        org.get("name")
        or org.get("label")
        or org.get("org_name")
        or (org.get("item") or {}).get("name")  # falls irgendwo item-shape reinsickert
        or ""
    )
# -----------------------------------------------------------------------------
# Personen per Bulk laden
# -----------------------------------------------------------------------------
import asyncio
from typing import Dict, List, Optional, Tuple

# ---------- global throttle (sehr wichtig gegen 429) ----------
_GLOBAL_PD_LOCK = asyncio.Lock()
_GLOBAL_PD_NEXT_TS = 0.0

async def pd_throttle(min_interval: float = 0.25):
    """
    Globale Drosselung: max ~4 Requests/Sek über den ganzen Job.
    (min_interval=0.25 => 4/s)
    """
    import time
    global _GLOBAL_PD_NEXT_TS
    async with _GLOBAL_PD_LOCK:
        now = time.monotonic()
        wait = _GLOBAL_PD_NEXT_TS - now
        if wait > 0:
            await asyncio.sleep(wait)
        _GLOBAL_PD_NEXT_TS = time.monotonic() + min_interval


async def pd_get_json_with_retry(
    client,
    url: str,
    headers: dict,
    label: str,
    retries: int = 8,
    base_delay: float = 0.8,
) -> Tuple[Optional[dict], Optional[int], Optional[str]]:
    """
    GET JSON + Retry auf 429/5xx.
    Rückgabe: (payload_json, status_code, error_text)
    """
    import random

    delay = base_delay
    for attempt in range(1, retries + 1):
        await pd_throttle(0.25)  # <= wichtig

        try:
            r = await client.get(url, headers=headers)
            status = r.status_code

            if status == 200:
                return (r.json() or {}), status, None

            if status == 404:
                return None, status, r.text

            if status == 429 or (500 <= status <= 599):
                # Retry-After wenn vorhanden
                ra = 0
                try:
                    ra = int(r.headers.get("Retry-After") or 0)
                except Exception:
                    ra = 0

                sleep_for = ra if ra > 0 else delay + random.uniform(0, 0.25)
                print(f"[{label}] HTTP {status} attempt {attempt}/{retries} -> sleep {sleep_for:.2f}s")
                await asyncio.sleep(sleep_for)
                delay = min(delay * 1.6, 8.0)
                continue

            # andere Fehler -> begrenzt retry
            print(f"[{label}] HTTP {status}: {r.text}")
            await asyncio.sleep(delay)
            delay = min(delay * 1.6, 8.0)

        except Exception as e:
            print(f"[{label}] Exception attempt {attempt}/{retries}: {e}")
            await asyncio.sleep(delay)
            delay = min(delay * 1.6, 8.0)

    return None, None, f"retries_exhausted ({label})"

async def fetch_person_details_many(person_ids: List[str]) -> Dict[str, dict]:
    """
    Stabiler Load von Persondetails über /persons/{id}
    - dedupe
    - throttling + retries
    - Rückgabe als lookup: {person_id(str): person_dict}
    """
    if not person_ids:
        return {}

    client = http_client()

    # dedupe stable
    seen = set()
    deduped = []
    for pid in person_ids:
        spid = str(pid).strip()
        if spid and spid not in seen:
            seen.add(spid)
            deduped.append(spid)

    out: Dict[str, dict] = {}

    # bewusst kleine Parallelität (sonst 429)
    sem = asyncio.Semaphore(3)

    async def one(pid: str):
        async with sem:
            url = append_token(f"{PIPEDRIVE_API}/persons/{pid}")
            payload, status, err = await pd_get_json_with_retry(
                client, url, get_headers(), label=f"person[{pid}]"
            )
            if status == 200 and payload:
                data = (payload.get("data") or {})
                rid = str(data.get("id") or pid)
                out[rid] = data
            elif status == 404:
                # ok: Person gelöscht/ungültig
                return
            else:
                # bei Problemen: nicht crashen, aber loggen
                print(f"[fetch_person_details_many] WARN pid={pid} status={status} err={err}")

    await asyncio.gather(*(one(pid) for pid in deduped))
    return out

# -----------------------------------------------------------------------------
# fehlende Organisation-IDs einzeln nachladen
# -----------------------------------------------------------------------------
async def fetch_org_details(org_ids: list[str]) -> dict[str, dict]:
    if not org_ids:
        return {}

    client = http_client()
    results: dict[str, dict] = {}
    sem = asyncio.Semaphore(1)

    async def fetch_one(oid: str):
        retries = 6
        delay = 1.0
        while retries > 0:
            try:
                async with sem:
                    url = append_token(f"{PIPEDRIVE_API}/organizations/{oid}")
                    r = await client.get(url, headers=get_headers())

                if r.status_code == 200:
                    data = (r.json() or {}).get("data") or {}
                    rid = str(data.get("id") or oid)
                    results[rid] = data
                    return

                if r.status_code == 404:
                    return

                if r.status_code == 429:
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30)
                    retries -= 1
                    continue

                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
                retries -= 1

            except Exception:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
                retries -= 1

    await asyncio.gather(*[asyncio.create_task(fetch_one(str(x))) for x in org_ids])
    return results


# -----------------------------------------------------------------------------
# build_nf_master
# -----------------------------------------------------------------------------
async def _build_nf_master_final(
    batch_id: str,
    campaign: str,
    nf_batch_ids: List[str],
    job_obj=None,
) -> pd.DataFrame:
    """
    Baut den Nachfass-Master:
    - Persons: über Batch-IDs (stream_persons_by_batch_id) -> IDs -> Details per /persons/{id}
    - Orgs: org_ids aus Persons -> fetch_orgs_bulk -> org_lookup
    - Export-Zeilen + Filter
    """

    # -------------------------------------------------------------
    # Mini-Helper: Personen-Details schnell & robust laden (statt /persons/collection)
    # -------------------------------------------------------------
    async def fetch_person_details_fast(person_ids: List[str]) -> Dict[str, dict]:
        if not person_ids:
            return {}

        client = http_client()
        sem = asyncio.Semaphore(2)  # bewusst klein (RateLimit)

        results: Dict[str, dict] = {}

        async def fetch_one(pid: str):
            url = append_token(f"{PIPEDRIVE_API}/persons/{pid}")
            r = await pd_get_with_retry(client, url, get_headers(), label=f"person={pid}")
            if r.status_code == 200:
                data = (r.json() or {}).get("data") or {}
                rid = sanitize(data.get("id") or pid)
                if rid:
                    results[rid] = data
            elif r.status_code == 404:
                print(f"[fetch_person_details_fast] 404 für Person {pid}")
            else:
                print(f"[fetch_person_details_fast] HTTP {r.status_code} für Person {pid}: {r.text}")

        # dedupe + stable order
        seen = set()
        deduped = []
        for pid in person_ids:
            spid = str(pid).strip()
            if spid and spid not in seen:
                seen.add(spid)
                deduped.append(spid)

        async def worker(pid: str):
            async with sem:
                await fetch_one(pid)

        tasks = [asyncio.create_task(worker(pid)) for pid in deduped]
        await asyncio.gather(*tasks)

        return results

    # -------------------------------------------------------------
    # 1) Personen aus Batch-IDs sammeln (IDs, später Details)
    # -------------------------------------------------------------
    if job_obj:
        job_obj.phase = "Personen aus Batch-IDs sammeln"
        job_obj.percent = 5

    person_ids_all: List[str] = []
    seen_pids = set()

    for nf_batch_id in nf_batch_ids:
        async for chunk in stream_persons_by_batch_id(nf_batch_id):
            for p in chunk:
                pid = sanitize((p or {}).get("id"))
                if pid and pid not in seen_pids:
                    seen_pids.add(pid)
                    person_ids_all.append(pid)

    print(f"[NF] Personen aus Suche: {len(person_ids_all)} IDs")

    # -------------------------------------------------------------
    # 2) Personendetails laden (v2 /persons/{id})
    # -------------------------------------------------------------
    if job_obj:
        job_obj.phase = "Lade Personendetails"
        job_obj.percent = 10

    person_lookup = await fetch_person_details_fast(person_ids_all)

    missing_p = [pid for pid in person_ids_all if pid not in person_lookup]
    if missing_p:
        print(f"[NF] WARN: fehlende Personendetails: {len(missing_p)} (Beispiele: {missing_p[:10]})")

    selected: List[dict] = [person_lookup[pid] for pid in person_ids_all if pid in person_lookup]
    print(f"[NF] Vollständige Personendaten: {len(selected)} Datensätze")

    # -------------------------------------------------------------
    # 3) Organisations-Details (Bulk) + org_lookup
    # -------------------------------------------------------------
    if job_obj:
        job_obj.phase = "Lade Organisationsdetails"
        job_obj.percent = 25

    unique_org_ids: List[str] = []
    seen_oids = set()
    for p in selected:
        oid = extract_org_id_from_person(p)
        if oid and oid not in seen_oids:
            seen_oids.add(oid)
            unique_org_ids.append(oid)

    # WICHTIG: chunk_size kleiner -> weniger 429
    org_lookup = await fetch_orgs_bulk(unique_org_ids, chunk_size=25)
    print(f"[NF] org_lookup: {len(org_lookup)} orgs für {len(unique_org_ids)} org_ids")

    if unique_org_ids:
        sample_missing = [oid for oid in unique_org_ids[:30] if oid not in org_lookup]
        if sample_missing:
            print(f"[NF] WARN: org_ids nicht im lookup (Beispiel): {sample_missing[:10]}")

    # -------------------------------------------------------------
    # 4) Export-Zeilen aufbauen
    # -------------------------------------------------------------
    if job_obj:
        job_obj.phase = "Baue Export-Zeilen"
        job_obj.percent = 55

    rows: List[dict] = []

    for p in selected:
        # ---------------- Basis-Personendaten ----------------
        pid = sanitize(p.get("id"))
        first = sanitize(p.get("first_name"))
        last = sanitize(p.get("last_name"))
        fullname = sanitize(p.get("name"))

        if not first and not last and fullname:
            parts = fullname.split()
            if len(parts) == 1:
                first, last = parts[0], ""
            else:
                first = " ".join(parts[:-1])
                last = parts[-1]

        # ---------------- Organisation ----------------
        org_id = extract_org_id_from_person(p)

        # (1) Primär: aus org_lookup
        org_name = ""
        if org_id:
            org = org_lookup.get(str(org_id)) or {}
            org_name = sanitize(
                org.get("name")
                or org.get("label")
                or org.get("org_name")
                or org.get("title")
            )

        # (2) Fallback: häufig steckt der Name schon in der Person (v2: organization:{id,name})
        if not org_name:
            org_obj = p.get("organization") or {}
            if isinstance(org_obj, dict):
                org_name = sanitize(org_obj.get("name") or "")
            else:
                org_name = sanitize(p.get("org_name") or p.get("organization_name") or "")

        # ---------------- E-Mail ----------------
        email_field = p.get("email") or p.get("emails") or []
        emails_flat: List[dict] = []

        def add_email(obj):
            if obj is None:
                return
            if isinstance(obj, dict):
                emails_flat.append(obj)
            elif isinstance(obj, list):
                for sub in obj:
                    add_email(sub)
            else:
                emails_flat.append({"value": obj})

        add_email(email_field)

        email = ""
        for e in emails_flat:
            if e.get("primary"):
                email = sanitize(e.get("value"))
                break
        if not email and emails_flat:
            email = sanitize(emails_flat[0].get("value"))

        # ---------------- Prospect-ID etc. ----------------
        prospect_id = cf(p, PERSON_CF_KEYS["Prospect ID"])
        gender_id = str(cf(p, PERSON_CF_KEYS["Person Geschlecht"]) or "").strip()
        gender_label = GENDER_OPTION_MAP.get(gender_id, gender_id)

        # ---------------- Filter ----------------
        if org_name.strip().lower() == "freelancer":
            continue
        if not prospect_id:
            continue
        if not email:
            continue

        row = {
            "Batch ID": sanitize(batch_id),
            "Channel": DEFAULT_CHANNEL,
            "Cold-Mailing Import": sanitize(campaign),
            "Prospect ID": prospect_id,
            "Organisation ID": sanitize(org_id),
            "Organisation Name": sanitize(org_name),
            "Person ID": pid,
            "Person Vorname": first,
            "Person Nachname": last,
            "Person Titel": cf(p, PERSON_CF_KEYS["Person Titel"]),
            "Person Geschlecht": gender_label,
            "Person Position": cf(p, PERSON_CF_KEYS["Person Position"]),
            "Person E-Mail": email,
            "XING Profil": cf(p, PERSON_CF_KEYS["XING Profil"]),
            "LinkedIn URL": cf(p, PERSON_CF_KEYS["LinkedIn URL"]),
        }

        for k, v in row.items():
            row[k] = sanitize(v)

        rows.append(row)

    df = pd.DataFrame(rows).replace({None: ""})

    # -------------------------------------------------------------
    # 5) Speichern
    # -------------------------------------------------------------
    if job_obj:
        job_obj.phase = "Speichere Dateien"
        job_obj.percent = 80

    excluded_rows = [
        {"Grund": "Max 2 Kontakte pro Organisation", "Anzahl": int(0)},
        {"Grund": "Datum nächste Aktivität steht an bzw. liegt in naher Vergangenheit", "Anzahl": int(0)},
    ]
    await save_df_text(pd.DataFrame(excluded_rows), "nf_excluded")
    await save_df_text(df, "nf_master_final")

    if job_obj:
        job_obj.phase = "Nachfass-Master erstellt"
        job_obj.percent = 95

    return df


# =============================================================================
# BASIS-ABGLEICH (Organisationen & IDs) – MODUL 4 FINAL
# =============================================================================

def bucket_key(name: str) -> str:
    """2-Buchstaben-Bucket für schnellen Fuzzy-Match."""
    n = normalize_name(name)
    return n[:2] if len(n) > 1 else n

def fast_fuzzy(a: str, b: str) -> int:
    """Schnellerer Fuzzy-Matcher."""
    return fuzz.partial_ratio(a, b)


# =============================================================================
# _reconcile_nf
# =============================================================================
async def _reconcile(prefix: str) -> None:
    """
    Gemeinsamer Abgleich für Neukontakte / Nachfass:
      - fuzzy Orga-Matching (≥95 % Ähnlichkeit) gegen Referenzfilter
      - Entfernen von Personen, die bereits in bestimmten Filtern sind (1216/1708)
      - Schreiben von <prefix>_master_ready und <prefix>_delete_log
    Das Log-Schema ist so gebaut, dass sowohl /summary als auch /nachfass/excluded
    keine KeyErrors mehr bekommen (id/name/org_name/extra etc. sind vorhanden).
    """
    t = tables(prefix)
    df = await load_df_text(t["final"])

    if df.empty:
        await save_df_text(pd.DataFrame(), t["ready"])
        await save_df_text(pd.DataFrame(), t["log"])
        return

    # Spalten im Master
    col_pid = "Person ID"
    col_orgname = "Organisation Name"
    col_orgid = "Organisation ID"

    delete_rows: List[dict] = []
    drop_idx: List[int] = []

    # -----------------------------------------------------------
    # UNIVERSAL SANITIZER
    # -----------------------------------------------------------
    def sanitize_cell(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""

        if isinstance(v, str):
            s = v.strip()
            # JSON decode
            if (s.startswith("[") and s.endswith("]")) or (
                s.startswith("{") and s.endswith("}")
            ):
                try:
                    parsed = json.loads(s)
                    return sanitize_cell(parsed)
                except Exception:
                    return s
            return s

        if isinstance(v, dict):
            return (
                sanitize_cell(v.get("value"))
                or sanitize_cell(v.get("label"))
                or sanitize_cell(v.get("name"))
                or sanitize_cell(v.get("id"))
                or ""
            )

        if isinstance(v, list):
            return sanitize_cell(v[0]) if v else ""

        return str(v)

    # -----------------------------------------------------------
    # ALLE ORGANISATIONSNAMEN LADEN (mit CAPs)
    # -----------------------------------------------------------
    MAX_NAMES = 50000
    MAX_BUCKET = 200

    buckets_all = await _fetch_org_names_for_filter_capped(
        PAGE_LIMIT, MAX_NAMES, MAX_BUCKET
    )

    # -----------------------------------------------------------
    # 1) FUZZY ORGANISATIONS-MATCHING
    # -----------------------------------------------------------
    for idx, row in df.iterrows():
        name_raw = row.get(col_orgname, "")
        name_clean = sanitize_cell(name_raw)
        norm = normalize_name(name_clean)

        if not norm:
            continue

        key = bucket_key(norm)
        bucket = buckets_all.get(key)
        if not bucket:
            continue

        near = [n for n in bucket if abs(len(n) - len(norm)) <= 4]
        if not near:
            continue

        best = process.extractOne(norm, near, scorer=fuzz.token_sort_ratio)
        if not best:
            continue

        best_name, score, *_ = best
        if score < 95:
            continue

        # Daten für Log
        kontakt_id = sanitize_cell(row.get(col_pid))
        full_name = sanitize_cell(
            f"{row.get('Person Vorname', '')} {row.get('Person Nachname', '')}"
        )
        org_id_val = sanitize_cell(row.get(col_orgid))
        org_name_val = name_clean
        extra_text = f"Ähnlichkeit {score}% mit '{best_name}'"

        delete_rows.append(
            {
                "reason": "org_match_95",
                "Kontakt ID": kontakt_id,
                "id": kontakt_id,
                "Name": full_name,
                "name": full_name,
                "Organisation ID": org_id_val,
                "Organisationsname": org_name_val,
                "org_name": org_name_val,
                "Grund": extra_text,
                "extra": extra_text,
            }
        )

        drop_idx.append(idx)

    if drop_idx:
        df = df.drop(drop_idx)

    # -----------------------------------------------------------
    # 2) PERSONEN BEREITS KONTAKTIERT (Filter 1216/1708)
    # -----------------------------------------------------------
    suspect_ids: set[str] = set()
    for fid in (1216, 1708):
        async for ids in stream_person_ids_by_filter(fid):
            suspect_ids.update(ids)

    if col_pid not in df.columns:
        # Wenn es keine Person-ID-Spalte gibt, können wir nichts filtern
        ready_df = df
    else:
        mask = df[col_pid].astype(str).isin(suspect_ids)
        removed = df[mask]

        for _, r in removed.iterrows():
            kontakt_id = sanitize_cell(r.get(col_pid))
            full_name = sanitize_cell(
                f"{r.get('Person Vorname', '')} {r.get('Person Nachname', '')}"
            )
            org_id_val = sanitize_cell(r.get(col_orgid))
            org_name_val = sanitize_cell(r.get(col_orgname))
            extra_text = "Bereits in Filter 1216/1708"

            delete_rows.append(
                {
                    "reason": "person_id_match",
                    "Kontakt ID": kontakt_id,
                    "id": kontakt_id,
                    "Name": full_name,
                    "name": full_name,
                    "Organisation ID": org_id_val,
                    "Organisationsname": org_name_val,
                    "org_name": org_name_val,
                    "Grund": extra_text,
                    "extra": extra_text,
                }
            )

        ready_df = df[~mask]

    # -----------------------------------------------------------
    # SPEICHERN
    # -----------------------------------------------------------
    if not ready_df.empty:
        ready_df = ready_df.applymap(sanitize_cell)
    if delete_rows:
        log_df = pd.DataFrame(delete_rows).applymap(sanitize_cell)
    else:
        log_df = pd.DataFrame()

    await save_df_text(ready_df, t["ready"])
    await save_df_text(log_df, t["log"])

import traceback  # falls oben noch nicht importiert

async def run_nachfass_job(
    job_id: str,
    batch_id: str,
    campaign: str,
    filters: List[int],
    nf_batch_ids: List[str],
):
    """
    Orchestrierung:
    - Fortschritt updaten
    - KEIN /persons/collection verwenden (404)
    - RateLimit wird in pd_get_with_retry gehandhabt
    """
    job = JOBS.get(job_id)
    if not job:
        return

    try:
        job.phase = "Starte Nachfass-Export"
        job.percent = 1
        job.done = False
        job.error = None

        job.phase = "Initialisiere"
        job.percent = 3

        job.phase = "Erstelle Nachfass-Master"
        job.percent = 5
        await _build_nf_master_final(
            batch_id=batch_id,
            campaign=campaign,
            nf_batch_ids=nf_batch_ids,
            job_obj=job,
        )

        job.phase = "Fertig"
        job.percent = 100
        job.done = True

    except Exception as e:
        job.error = str(e)
        job.done = True
        job.phase = "Fehler"
        print(f"[run_nachfass_job] ERROR: {e}")

# =============================================================================
# Excel-Export-Helfer – FINAL MODUL 3
# =============================================================================

# 1) Reihenfolge der Exportspalten (Final)
NF_EXPORT_COLUMNS = [
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


def build_nf_export(df: pd.DataFrame) -> pd.DataFrame:
    """
    Baut den finalen Excel-Export in exakt definierter Spaltenreihenfolge.
    Fehlende Spalten werden automatisch erzeugt.
    """
    out = pd.DataFrame(columns=NF_EXPORT_COLUMNS)

    for col in NF_EXPORT_COLUMNS:
        if col in df.columns:
            out[col] = df[col]
        else:
            out[col] = ""

    # String-Säuberung
    for c in ("Person ID", "Organisation ID"):
        if c in out.columns:
            out[c] = out[c].astype(str).replace("nan", "").fillna("")

    return out

# ------------------------------------------------------------
# HINWEIS: Nicht berücksichtigte Datensätze (nur Zähler)
# ------------------------------------------------------------
nf_info = {
    "excluded_date": 0,
    "excluded_org": 0
}

def _df_to_excel_bytes_nf(df: pd.DataFrame) -> bytes:
    """Konvertiert DataFrame → Excel Bytes."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Nachfass")

        ws = writer.sheets["Nachfass"]

        # IDs in Excel als TEXT formatieren
        id_cols = ["Organisation ID", "Person ID"]
        col_index = {col: i + 1 for i, col in enumerate(df.columns)}

        for name in id_cols:
            if name in col_index:
                j = col_index[name]
                for i in range(2, len(df) + 2):
                    ws.cell(i, j).number_format = "@"

        writer.book.properties.creator = "BatchFlow"

    buf.seek(0)
    return buf.getvalue()



# =============================================================================
# JOB-VERWALTUNG & FORTSCHRITT
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
        self.excel_bytes: Optional[bytes] = None

JOBS: Dict[str, Job] = {}

# =============================================================================
# RECONCILE MIT FORTSCHRITT
# =============================================================================
async def reconcile_with_progress(job: "Job", prefix: str):
    """Führt _reconcile_generic() mit UI-Fortschritt durch."""
    try:
        job.phase = "Vorbereitung läuft …"; job.percent = 10
        await asyncio.sleep(2)

        job.phase = "Lade Vergleichsdaten …"; job.percent = 25
        await asyncio.sleep(2)

        await _reconcile(prefix)

        job.phase = "Abgleich abgeschlossen"; job.percent = 100
        job.done = True
    except Exception as e:
        job.error = f"Fehler beim Abgleich: {e}"
        job.phase = "Fehler"; job.percent = 100
        job.done = True

# =============================================================================
# EXPORT-START – NEUKONTAKTE
# =============================================================================
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

    async def update_progress(phase: str, percent: int):
        job.phase = phase
        job.percent = min(100, max(0, percent))
        await asyncio.sleep(2)

    async def _run():
        try:
            # 1️⃣ Daten laden
            await update_progress("Lade Neukontakte aus Pipedrive …", 5)
            df = await _build_nk_master_final(fachbereich, take_count, batch_id, campaign, per_org_limit, job_obj=job)

            # 2️⃣ Abgleich durchführen
            await update_progress("Führe Abgleich (Orga & IDs) durch …", 55)
            await reconcile_with_progress(job, "nk")

            # 3️⃣ Excel-Datei generieren
            await update_progress("Erzeuge Excel-Datei …", 80)
            ready = await load_df_text("nk_master_ready")
            export_df = build_nf_export(ready)
            data = _df_to_excel_bytes(export_df)

            # 4️⃣ Datei speichern
            path = f"/tmp/{job.filename_base}.xlsx"
            with open(path, "wb") as f:
                f.write(data)
            job.path = path
            job.total_rows = len(export_df)
            job.phase = f"Fertig – {job.total_rows} Zeilen"
            job.percent = 100
            job.done = True
        except Exception as e:
            job.error = f"Fehler: {e}"
            job.phase = "Fehler"
            job.percent = 100
            job.done = True

    asyncio.create_task(_run())
    return JSONResponse({"job_id": job_id})



# =============================================================================
# EXPORT-FORTSCHRITT & DOWNLOAD-ENDPUNKTE
# =============================================================================
@app.get("/neukontakte/export_progress")
async def neukontakte_export_progress(job_id: str = Query(...)):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job nicht gefunden")
    return JSONResponse({
        "phase": job.phase, "percent": job.percent,
        "done": job.done, "error": job.error,
        "note_org_limit": job.get("note_org_limit", 0),
        "note_date_invalid": job.get("note_date_invalid", 0)
    })



# -------------------------------------------------------------------------
# Download des erzeugten Nachfass-Exports
# -------------------------------------------------------------------------
@app.get("/campaign", response_class=HTMLResponse)
async def campaign_home():
    return HTMLResponse("""<!doctype html><html lang="de">
<head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>BatchFlow – Kampagnen</title>
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
      <div class="desc">Neue Leads aus Filter, Abgleich & Export.</div>
      <a class="btn" href="/neukontakte">Öffnen</a></div>
    <div class="card"><div class="title">Nachfass</div>
      <div class="desc">Nachfassen anhand einer oder zwei Batch IDs (Filter 3024).</div>
      <a class="btn" href="/nachfass">Öffnen</a></div>
    <div class="card"><div class="title">Refresh</div>
      <div class="desc">Kontaktdaten aktualisieren / ergänzen.</div>
      <a class="btn" href="/neukontakte?mode=refresh">Öffnen</a></div>
  </div>
</main></body></html>""")

# =============================================================================
# Frontend – Neukontakte
# =============================================================================
@app.get("/neukontakte", response_class=HTMLResponse)
async def neukontakte_page(request: Request, mode: str = Query("new")):
    authed = bool(user_tokens.get("default") or PD_API_TOKEN)
    authed_html = "<span class='muted'>angemeldet</span>" if authed else "<a href='/login'>Anmelden</a>"

    return HTMLResponse(
        r"""<!doctype html><html lang="de">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Neukontakte – BatchFlow</title>

<style>
  body{margin:0;background:#f6f8fb;color:#0f172a;font:16px/1.6 Inter,sans-serif}
  header{background:#fff;border-bottom:1px solid #e2e8f0}
  .hwrap{max-width:1120px;margin:0 auto;padding:14px 20px;display:flex;align-items:center;
          justify-content:space-between;gap:12px}
  main{max-width:1120px;margin:28px auto;padding:0 20px}
  .card{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:20px;
         box-shadow:0 2px 8px rgba(2,8,23,.04)}
  .grid{display:grid;grid-template-columns:repeat(12,1fr);gap:16px}
  label{display:block;font-weight:600;margin:8px 0 6px}
  select,input{width:100%;padding:10px 12px;border:1px solid #cbd5e1;border-radius:10px;background:#fff}
  .btn{background:#0ea5e9;border:none;color:#fff;border-radius:10px;padding:12px 16px;cursor:pointer}
  .btn:disabled{opacity:.5;cursor:not-allowed}
  #overlay{display:none;position:fixed;inset:0;background:rgba(255,255,255,.7);backdrop-filter:blur(2px);
            z-index:9999;align-items:center;justify-content:center;flex-direction:column;gap:10px}
  .barwrap{width:min(520px,90vw);height:10px;border-radius:999px;background:#e2e8f0;overflow:hidden}
  .bar{height:100%;width:0%;background:#0ea5e9;transition:width .2s linear}
</style>
</head>

<body>
<header>
  <div class="hwrap">
    <div><a href="/campaign" style="color:#0a66c2;text-decoration:none">← Kampagne wählen</a></div>
    <div><b>Neukontakte</b></div>
    <div>""" + authed_html + r"""</div>
  </div>
</header>

<main>
<section class="card">
  <div class="grid">
    <div class="col-4">
      <label>Fachbereich</label>
      <select id="fachbereich"><option value="">– bitte auswählen –</option></select>
    </div>

    <div class="col-3">
      <label>Batch ID</label>
      <input id="batch_id" placeholder="Bxxx"/>
    </div>

    <div class="col-3">
      <label>Kampagne</label>
      <input id="campaign" placeholder="z. B. Frühling 2025"/>
    </div>

    <div class="col-2" style="display:flex;align-items:flex-end;justify-content:flex-end">
      <button class="btn" id="btnExport" disabled>Abgleich & Download</button>
    </div>
  </div>
</section>
</main>

<div id="overlay">
  <div id="phase"></div>
  <div class="barwrap"><div class="bar" id="bar"></div></div>
</div>

<script>
// ---------------------------------------------------------------------
// Sicherer Element-Getter
// ---------------------------------------------------------------------
const el = id => {
    const n = document.getElementById(id);
    if (!n) {
        console.error("Element with ID", id, "not found");
        return { textContent: "" };
    }
    return n;
};

// ---------------------------------------------------------------------
// Overlay-Control
// ---------------------------------------------------------------------
function showOverlay(msg){
    el('phase').textContent = msg;
    el('overlay').style.display = 'flex';
}

function setProgress(p){
    el('bar').style.width = Math.min(100, Math.max(0, p)) + '%';
}

// ---------------------------------------------------------------------
// Optionen laden
// ---------------------------------------------------------------------
async function loadOptions(){
    showOverlay('Lade Fachbereiche …');
    setProgress(15);

    const r = await fetch('/neukontakte/options');
    const data = await r.json();

    const sel = el('fachbereich');
    sel.innerHTML = '<option value="">– bitte auswählen –</option>';

    data.options.forEach(o => {
        const opt = document.createElement('option');
        opt.value = o.value;
        opt.textContent = o.label + ' (' + o.count + ')';
        sel.appendChild(opt);
    });

    el('overlay').style.display = 'none';
    sel.onchange = () => el('btnExport').disabled = !sel.value;
}

// ---------------------------------------------------------------------
// Export starten
// ---------------------------------------------------------------------
async function startExport(){
    const fb   = el('fachbereich').value;
    if (!fb) return alert('Bitte Fachbereich wählen');

    const bid  = el('batch_id').value;
    const camp = el('campaign').value;

    showOverlay('Starte Export …');
    setProgress(10);

    const r = await fetch('/neukontakte/export_start', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({
            fachbereich: fb,
            batch_id: bid,
            campaign: camp
        })
    });

    if (!r.ok) return alert("Fehler beim Start");

    const res = await r.json();
    const job_id = res.job_id;

    let done = false;

    while (!done){
        await new Promise(r => setTimeout(r, 500));
        const pr = await fetch('/neukontakte/export_progress?job_id=' + job_id);
        const s  = await pr.json();

        el('phase').textContent = s.phase + ' (' + s.percent + '%)';
        setProgress(s.percent);

        done = s.done;
    }

    window.location.href = '/neukontakte/export_download?job_id=' + job_id;
}

// ---------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------
el('btnExport').onclick = startExport;
loadOptions();
</script>

</body>
</html>
"""
    )


# =============================================================================
# Frontend – Nachfass (stabil, modern, sauber)
# =============================================================================
@app.get("/nachfass", response_class=HTMLResponse)
async def nachfass_page(request: Request):
    authed = bool(user_tokens.get("default") or PD_API_TOKEN)
    auth_info = "<span class='muted'>angemeldet</span>" if authed else "<a href='/login'>Anmelden</a>"

    return HTMLResponse(
        r"""<!doctype html><html lang="de">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Nachfass – BatchFlow</title>

<style>
  body{margin:0;background:#f6f8fb;color:#0f172a;font:16px/1.6 Inter,sans-serif}
  header{background:#fff;border-bottom:1px solid #e2e8f0}
  .hwrap{max-width:1120px;margin:0 auto;padding:14px 20px;display:flex;
         align-items:center;justify-content:space-between}
  main{max-width:1120px;margin:28px auto;padding:0 20px}
  .card{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:20px;
         box-shadow:0 2px 8px rgba(2,8,23,.04)}
  label{display:block;font-weight:600;margin:8px 0 6px}
  textarea,input{width:100%;padding:10px;border:1px solid #cbd5e1;border-radius:10px}
  .btn{background:#0ea5e9;border:none;color:#fff;border-radius:10px;
       padding:12px 16px;cursor:pointer;font-weight:600}
  .btn:hover{background:#0284c7}

  #overlay{
    display:none;position:fixed;inset:0;background:rgba(255,255,255,.75);
    backdrop-filter:blur(2px);z-index:9999;align-items:center;
    justify-content:center;flex-direction:column;gap:12px;
  }

  .barwrap{width:min(520px,90vw);height:10px;border-radius:999px;background:#e2e8f0;overflow:hidden}
  .bar{height:100%;width:0%;background:#0ea5e9;transition:width .2s linear}

  table{width:100%;border-collapse:collapse;margin-top:20px;border:1px solid #e2e8f0;
        border-radius:10px;background:#fff;box-shadow:0 2px 8px rgba(2,8,23,.04)}
  th,td{padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:left}
  th{background:#f8fafc;font-weight:600}
</style>

</head>
<body>

<header>
  <div class="hwrap">
    <div><a href="/campaign" style="color:#0a66c2;text-decoration:none">← Kampagne wählen</a></div>
    <div><b>Nachfass</b></div>
    <div>""" + auth_info + r"""</div>
  </div>
</header>

<main>

  <section class="card">
    <label>Batch IDs (1–2 Werte)</label>
    <textarea id="nf_batch_ids" rows="3" placeholder="B111, B222"></textarea>
    <small style="color:#64748b">Komma oder Zeilenumbruch. Max. 2 IDs.</small>

    <label style="margin-top:12px">Export-Batch-ID</label>
    <input id="batch_id" placeholder="B999"/>

    <label style="margin-top:12px">Kampagnenname</label>
    <input id="campaign" placeholder="z. B. Nachfass KW45"/>

    <div style="margin-top:20px;text-align:right">
      <button class="btn" id="btnExportNf">Abgleich & Download</button>
    </div>
  </section>

  <section style="margin-top:30px;">
    <h3>Entfernte Datensätze</h3>
    <div id="excluded-summary-box"></div>

    <table>
      <thead>
        <tr>
          <th>Kontakt ID</th>
          <th>Name</th>
          <th>Organisation ID</th>
          <th>Organisationsname</th>
          <th>Grund</th>
        </tr>
      </thead>
      <tbody id="excluded-table-body">
        <tr><td colspan="5" style="text-align:center;color:#999">Noch keine Daten geladen</td></tr>
      </tbody>
    </table>
  </section>

</main>

<div id="overlay">
  <div id="overlay-phase" style="font-weight:600"></div>
  <div class="barwrap"><div class="bar" id="overlay-bar"></div></div>
</div>

<script>
// ---------------------------------------------------------------------------
// Safe Element Getter
// ---------------------------------------------------------------------------
const el = id => {
    const n = document.getElementById(id);
    if (!n) return { textContent:"", style:{} };
    return n;
};

// ---------------------------------------------------------------------------
// Overlay
// ---------------------------------------------------------------------------
function showOverlay(msg){
    el('overlay-phase').textContent = msg;
    el('overlay').style.display = 'flex';
}
function hideOverlay(){ el('overlay').style.display = 'none'; }
function setProgress(p){ el('overlay-bar').style.width = p + '%'; }

// ---------------------------------------------------------------------------
// ID Parsing
// ---------------------------------------------------------------------------
function parseIDs(raw){
    return raw.split(/[,\n;]/).map(s => s.trim()).filter(Boolean).slice(0,2);
}

// ---------------------------------------------------------------------------
// Excluded Table Loading
// ---------------------------------------------------------------------------
async function loadExcludedTable(){
    const r = await fetch('/nachfass/excluded/json');
    const data = await r.json();

    const body = el('excluded-table-body');
    body.innerHTML = '';

    const summaryBox = el('excluded-summary-box');
    summaryBox.innerHTML = '';

    if (data.summary && data.summary.length){
        let html = "<ul>";
        data.summary.forEach(s => {
            html += `<li>${s.Grund}: <b>${s.Anzahl}</b></li>`;
        });
        html += "</ul>";
        summaryBox.innerHTML = html;
    }

    if (!data.rows || data.rows.length === 0){
        body.innerHTML = '<tr><td colspan="5" style="text-align:center;color:#999">Keine Treffer</td></tr>';
        return;
    }

    data.rows.forEach(row => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${row["Kontakt ID"] || ""}</td>
          <td>${row["Name"] || ""}</td>
          <td>${row["Organisation ID"] || ""}</td>
          <td>${row["Organisationsname"] || ""}</td>
          <td>${row["Grund"] || ""}</td>
        `;
        body.appendChild(tr);
    });
}

// ---------------------------------------------------------------------------
// Export Polling
// ---------------------------------------------------------------------------
async function poll(job_id){
    let done = false;

    while (!done){
        await new Promise(r => setTimeout(r, 500));

        const res = await fetch('/nachfass/export_progress?job_id=' + job_id);
        const s = await res.json();

        el('overlay-phase').textContent = `${s.phase} (${s.percent}%)`;
        setProgress(s.percent);

        if (s.error){
            alert(s.error);
            hideOverlay();
            return;
        }

        done = s.done;
    }

    showOverlay("Download startet …");
    setProgress(100);

    window.location.href = '/nachfass/export_download?job_id=' + job_id;

    await loadExcludedTable();
    hideOverlay();
}

// ---------------------------------------------------------------------------
// Export Start
// ---------------------------------------------------------------------------
async function startExport(){
    const ids = parseIDs(el('nf_batch_ids').value);
    if (ids.length === 0){
        alert("Bitte mindestens eine Batch ID eingeben");
        return;
    }

    showOverlay("Starte Abgleich …");
    setProgress(10);

    const r = await fetch('/nachfass/export_start', {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({
            nf_batch_ids: ids,
            batch_id: el('batch_id').value,
            campaign: el('campaign').value
        })
    });

    if (!r.ok){
        alert("Fehler beim Start");
        hideOverlay();
        return;
    }

    const {job_id} = await r.json();
    await poll(job_id);
}

el('btnExportNf').onclick = startExport;

</script>
</body>
</html>
"""
    )




# =============================================================================
# Summary-Seiten
# =============================================================================
def _count_reason(df: pd.DataFrame, keys: List[str]) -> int:
    if df.empty or "reason" not in df.columns:
        return 0
    return int(df["reason"].astype(str).str.lower().isin([k.lower() for k in keys]).sum())

@app.get("/neukontakte/summary", response_class=HTMLResponse)
async def neukontakte_summary(job_id: str = Query(...)):
    ready = await load_df_text("nk_master_ready")
    log = await load_df_text("nk_delete_log")
    total = len(ready)
    cnt_org = _count_reason(log, ["org_match_95"])
    cnt_pid = _count_reason(log, ["person_id_match"])
    removed = cnt_org + cnt_pid
    table_html = "<i>Keine entfernt</i>"
    if not log.empty:
        view = log.tail(50).copy()
        view["Grund"] = view.apply(lambda r: f"{r['reason']} – {r['extra']}", axis=1)
        table_html = view[["id","name","org_name","Grund"]].to_html(index=False, border=0)
    html = f"""<!doctype html><html lang="de"><head><meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width,initial-scale=1"/><title>Neukontakte – Ergebnis</title></head>
    <body><main style='max-width:1100px;margin:30px auto;padding:0 20px;font-family:Inter,sans-serif'>
    <h2>Ergebnis: {total} Zeilen</h2>
    <ul><li>Orga ≥95% entfernt: {cnt_org}</li><li>Person-ID Dubletten: {cnt_pid}</li><li><b>Gesamt entfernt: {removed}</b></li></ul>
    <section>{table_html}</section>
    <a href='/campaign'>Zur Übersicht</a></main></body></html>"""
    return HTMLResponse(html)

@app.get("/nachfass/summary", response_class=HTMLResponse)
async def nachfass_summary(job_id: str = Query(...)):
    ready = await load_df_text("nf_master_ready")
    # → sauber machen ALLER Felder in ALLEN Zellen:
    for col in ready.columns:
        ready[col] = ready[col].apply(normalize_cell)
    print("DEBUG READY TYPES:\n", ready.applymap(type).head(20))
    log = await load_df_text("nf_delete_log")
    print("DEBUG LOG TYPES:\n", log.applymap(type).head(20))
    total = len(ready)
    cnt_org = _count_reason(log, ["org_match_95"])
    cnt_pid = _count_reason(log, ["person_id_match"])
    removed = cnt_org + cnt_pid
    table_html = "<i>Keine entfernt</i>"
    if not log.empty:
        view = log.tail(50).copy()
        view["Grund"] = view.apply(lambda r: f"{r['reason']} – {r['extra']}", axis=1)
        table_html = view[["id","name","org_name","Grund"]].to_html(index=False, border=0)
    html = f"""<!doctype html><html lang="de"><head><meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width,initial-scale=1"/><title>Nachfass – Ergebnis</title></head>
    <body><main style='max-width:1100px;margin:30px auto;padding:0 20px;font-family:Inter,sans-serif'>
    <h2>Ergebnis: {total} Zeilen</h2>
    <ul><li>Orga ≥95% entfernt: {cnt_org}</li><li>Person-ID Dubletten: {cnt_pid}</li><li><b>Gesamt entfernt: {removed}</b></li></ul>
    <section>{table_html}</section>
    <a href='/campaign'>Zur Übersicht</a></main></body></html>"""
    return HTMLResponse(html)
    
# =============================================================================
# EXCLUDED + SUMMARY + DEBUG (FINAL & KOMPATIBEL)
# =============================================================================
@app.get("/nachfass/excluded/json")
async def nachfass_excluded_json():

    # Universeller Sanitizer – macht JEDE Struktur zu einem sauberen String
    def flatten(v):
        if v is None:
            return ""
        if isinstance(v, float) and pd.isna(v):
            return ""
        if isinstance(v, list):
            return flatten(v[0] if v else "")
        if isinstance(v, dict):
            return flatten(
                v.get("value")
                or v.get("label")
                or v.get("name")
                or v.get("id")
                or ""
            )
        return str(v)

    excluded_df = await load_df_text("nf_excluded")
    deleted_df  = await load_df_text("nf_delete_log")

    # ---------------------------
    # 1) Summary (Batch-/Filter-Ausschlüsse)
    # ---------------------------
    excluded_summary = []
    if not excluded_df.empty:
        for _, r in excluded_df.iterrows():
            excluded_summary.append({
                "Grund": flatten(r.get("Grund")),
                "Anzahl": int(r.get("Anzahl") or 0)
            })

    # ---------------------------
    # 2) Abgleich-Ausschlüsse (Fuzzy / ID-Dubletten)
    # ---------------------------
    rows = []
    if not deleted_df.empty:
        deleted_df = deleted_df.replace({None: "", np.nan: ""})

        for _, r in deleted_df.iterrows():
            rows.append({
                "Kontakt ID":     flatten(r.get("Kontakt ID") or r.get("id")),
                "Name":           flatten(r.get("Name") or r.get("name")),
                "Organisation ID": flatten(r.get("Organisation ID")),
                "Organisationsname": flatten(
                    r.get("Organisationsname")
                    or r.get("org_name")
                ),
                "Grund": flatten(r.get("Grund") or r.get("reason") or r.get("extra")),
            })

    return JSONResponse({
        "summary": excluded_summary,
        "total": len(rows),
        "rows": rows
    })



# =============================================================================
# HTML-Seite (Excluded Viewer)
# =============================================================================
@app.get("/nachfass/excluded", response_class=HTMLResponse)
async def nachfass_excluded():
    """
    HTML-Tabelle für alle nicht berücksichtigten Datensätze.
    Lädt via JS die JSON-Daten aus /nachfass/excluded/json.
    """
    html = r"""
    <!DOCTYPE html>
    <html lang="de">
    <head>
      <meta charset="UTF-8">
      <title>Nachfass – Nicht berücksichtigte Datensätze</title>
      <style>
        body {
          font-family: Inter, sans-serif;
          margin: 30px auto;
          max-width: 1100px;
          padding: 0 20px;
          background: #f6f8fb;
        }
        table { width: 100%; border-collapse: collapse; }
        th, td {
          border-bottom: 1px solid #e5e7eb;
          padding: 8px 10px;
          text-align: left;
        }
        th {
          background: #f1f5f9;
          font-weight: 600;
        }
        tr:hover td {
          background: #f9fafb;
        }
        .center {
          text-align: center;
          color: #6b7280;
        }
      </style>
    </head>
    <body>

      <h2>Nicht berücksichtigte Datensätze</h2>

      <table>
        <thead>
          <tr>
            <th>Kontakt ID</th>
            <th>Name</th>
            <th>Organisation ID</th>
            <th>Organisationsname</th>
            <th>Grund</th>
            <th>Quelle</th>
          </tr>
        </thead>
        <tbody id="excluded-table-body">
          <tr><td colspan="6" class="center">Lade Daten…</td></tr>
        </tbody>
      </table>

      <script>
        async function loadExcludedTable() {
          try {
            const r = await fetch('/nachfass/excluded/json');
            const data = await r.json();
            const body = document.getElementById('excluded-table-body');
            body.innerHTML = '';

            if (!data.rows || data.rows.length === 0) {
              body.innerHTML = '<tr><td colspan="6" class="center">Keine Datensätze ausgeschlossen</td></tr>';
              return;
            }

            for (const row of data.rows) {
              const tr = document.createElement('tr');
              tr.innerHTML = `
                <td>${row["Kontakt ID"] || row["id"] || ""}</td>
                <td>${row["Name"] || row["name"] || ""}</td>
                <td>${row["Organisation ID"] || ""}</td>
                <td>${row["Organisationsname"] || row["org_name"] || ""}</td>
                <td>${row["Grund"] || row["reason"] || ""}</td>
                <td>${row["Quelle"] || ""}</td>
              `;
              body.appendChild(tr);
            }
          } catch (err) {
            console.error("Fehler bei excluded:", err);
            document.getElementById('excluded-table-body').innerHTML =
              '<tr><td colspan="6" class="center" style="color:red">Fehler beim Laden</td></tr>';
          }
        }

        loadExcludedTable();
      </script>

    </body>
    </html>
    """
    return HTMLResponse(html)


# =============================================================================
# SUMMARY-SEITE – Überblick nach Export
# =============================================================================
@app.get("/nachfass/summary", response_class=HTMLResponse)
async def nachfass_summary(job_id: str = Query(...)):
    """
    Übersicht nach Nachfass-Export:
    - Gesamtzeilen
    - Orga ≥95% entfernt
    - Person-ID-Dubletten entfernt
    - letzte 50 geloggte Ausschlüsse
    """
    ready = await load_df_text("nf_master_ready")
    log   = await load_df_text("nf_delete_log")

    def count(df: pd.DataFrame, reason_keys: list) -> int:
        if df.empty:
            return 0
        if "reason" not in df.columns:
            return 0
        keys = [k.lower() for k in reason_keys]
        return int(df["reason"].astype(str).str.lower().isin(keys).sum())

    total    = len(ready)
    cnt_org  = count(log, ["org_match_95"])
    cnt_pid  = count(log, ["person_id_match"])
    removed  = cnt_org + cnt_pid

    # Tabelle mit letzten 50 Ausschlüssen
    if not log.empty:
        view = log.tail(50).copy()
        view["Grund"] = view.apply(
            lambda r: f"{r['reason']} – {r['extra']}", axis=1
        )
        table_html = view[["id", "name", "org_name", "Grund"]].to_html(
            index=False, border=0
        )
    else:
        table_html = "<i>Keine entfernt</i>"

    html = f"""
    <!doctype html>
    <html lang="de">
    <head><meta charset="utf-8"/>
    <title>Nachfass – Ergebnis</title>
    </head>
    <body style="font-family:Inter,sans-serif;max-width:1100px;margin:30px auto;padding:0 20px">
      <h2>Nachfass – Ergebnis</h2>

      <ul>
        <li>Gesamt exportierte Zeilen: <b>{total}</b></li>
        <li>Organisationen ≥95% Ähnlichkeit entfernt: <b>{cnt_org}</b></li>
        <li>Bereits kontaktierte Personen entfernt: <b>{cnt_pid}</b></li>
        <li><b>Gesamt entfernt: {removed}</b></li>
      </ul>

      <h3>Letzte Ausschlüsse</h3>
      {table_html}

      <p><a href="/campaign">Zur Übersicht</a></p>
    </body>
    </html>
    """

    return HTMLResponse(html)

# =============================================================================
# MODUL 6 – FINALER JOB-/WORKFLOW FÜR NACHFASS (EXPORT/PROGRESS/DOWNLOAD)
# =============================================================================
from uuid import uuid4

@app.post("/nachfass/export_start")
async def nachfass_export_start(req: Request):
    body = await req.json()

    nf_batch_ids = body.get("nf_batch_ids") or []
    batch_id     = body.get("batch_id") or ""
    campaign     = body.get("campaign") or ""

    job_id = str(uuid4())
    job = Job()
    JOBS[job_id] = job

    # Job-Inputs speichern
    job.nf_batch_ids = nf_batch_ids
    job.batch_id     = batch_id
    job.campaign     = campaign

    # Reset
    job.error = None
    job.done = False
    job.percent = 0
    job.phase = "Starte …"

    # Hintergrundprozess starten
    asyncio.create_task(run_nachfass_job(job, job_id))

    return {"job_id": job_id}


# =============================================================================
# Fortschritt abfragen
# =============================================================================
@app.get("/nachfass/export_progress")
async def nachfass_export_progress(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        return JSONResponse({"error": "Job nicht gefunden"}, status_code=404)

    return JSONResponse({
        "phase": str(job.phase),
        "percent": int(job.percent),
        "done": bool(job.done),
        "error": str(job.error) if job.error else None
    })


# =============================================================================
# Debug-Endpoint für eine Person
# =============================================================================
from fastapi.responses import JSONResponse  # falls noch nicht importiert

@app.get("/debug/pd_person/{pid}")
async def debug_pd_person(pid: int):
    client = http_client()
    url = append_token(f"{PIPEDRIVE_API}/persons/{pid}")

    r = await client.get(url, headers=get_headers())
    status = r.status_code

    try:
        data = r.json()
    except Exception:
        data = None

    return JSONResponse(
        {
            "status": status,
            "json": data,
            "text": r.text[:500],
        }
    )



# =============================================================================
# Download
# =============================================================================
@app.get("/nachfass/export_download")
async def nachfass_export_download(job_id: str):
    job = JOBS.get(job_id)
    if not job or not job.path:
        return JSONResponse({"error": "Keine Datei gefunden"}, status_code=404)

    return FileResponse(
        job.path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"{slugify_filename(job.filename_base or 'Nachfass_Export')}.xlsx"
    )

# =============================================================================
# Redirects & Fallbacks (fix für /overview & ungültige Pfade)
# =============================================================================

@app.get("/overview", response_class=HTMLResponse)
async def overview_redirect():
    """
    Fängt alte oder externe Aufrufe von Pipedrive ab,
    z. B. /overview?resource=person&view=list…
    Leitet automatisch zur Kampagnenauswahl weiter.
    """
    return RedirectResponse("/campaign", status_code=302)


@app.get("/", response_class=HTMLResponse)
async def root():
    # direkt die Campaign-Seite rendern, ohne Redirect
    return await campaign_home()

@app.get("/{full_path:path}", include_in_schema=False)
async def catch_all(full_path: str, request: Request):
    """
    Sauberer Fallback für alle unbekannten URLs:
    - /overview    → wird separat abgefangen
    - /irgendwas   → leitet automatisch auf /campaign
    """
    if full_path in ("campaign", "", "/"):
        return RedirectResponse("/campaign", status_code=302)
    return RedirectResponse("/campaign", status_code=302)
