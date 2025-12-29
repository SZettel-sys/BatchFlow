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
# -----------------------------------------------------------------------------
# PIPEDRIVE API - V2 
# ----------------------------------------------------------------------------- 
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
FILTER_REFRESH = int(os.getenv("FILTER_REFRESH", "4444"))
FIELD_FACHBEREICH_HINT = os.getenv("FIELD_FACHBEREICH_HINT", "fachbereich")

DEFAULT_CHANNEL = "Cold E-Mail"
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "200"))
NF_PAGE_LIMIT = int(os.getenv("NF_PAGE_LIMIT", "500"))
NF_MAX_ROWS = int(os.getenv("NF_MAX_ROWS", "10000"))
RECONCILE_MAX_ROWS = int(os.getenv("RECONCILE_MAX_ROWS", "20000"))
PER_ORG_DEFAULT_LIMIT = int(os.getenv("PER_ORG_DEFAULT_LIMIT", "2"))
MAX_ORG_NAMES = int(os.getenv("MAX_ORG_NAMES", "1000"))
MAX_ORG_BUCKET = int(os.getenv("MAX_ORG_BUCKET", "200"))

# --- Pipedrive Person Custom Field Keys (fix) ---
PD_PERSON_FIELDS = {
    "Batch ID": "7b5bda2891fd1ce14c53488304afc9b4c639fb4a", 
    "Prospect ID": "f9138f9040c44622808a4b8afda2b1b75ee5acd0",
    "Person Geschlecht": "c4f5f434cdb0cfce3f6d62ec7291188fe968ac72",
    "Person Titel": "0343bc43a91159aaf33a463ca603dc5662422ea5",
    "Person Position": "4585e5de11068a3bccf02d8b93c126bcf5c257ff",
    "XING Profil": "44ebb6feae2a670059bc5261001443a2878a2b43",
    "LinkedIn URL": "25563b12f847a280346bba40deaf527af82038cc",
    "Fachbereich - Kampagne": "f000c9eee4bfa74714a30972383d74dd965d34bf"
   
}

# --- Organisation Custom Field Keys (fix) ---
PD_ORG_FIELDS = {
    "Organisationsart": "0ab03885d6792086a0bb007d6302d14b13b0c7d1",
    "Organisation Vertriebsstop": "61d238b86784db69f7300fe8f12f54c601caeff8",
}

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

# =============================================================================
# API REQUEST COUNTER
# =============================================================================
REQUEST_COUNTER = {
    "total": 0,
    "persons": 0,
    "organizations": 0,
    "search": 0,
    "generic": 0
}

def inc(kind):
    REQUEST_COUNTER["total"] += 1
    REQUEST_COUNTER[kind] = REQUEST_COUNTER.get(kind, 0) + 1

# -----------------------------------------------------------------------------
# Startup / Shutdown
# -----------------------------------------------------------------------------
def http_client() -> httpx.AsyncClient:
    return app.state.http

def get_pool() -> asyncpg.Pool:
    return app.state.pool


@app.on_event("startup")
async def _startup():
    limits = httpx.Limits(
        max_keepalive_connections=20,
        max_connections=50,
        keepalive_expiry=30.0,
    )
    timeout = httpx.Timeout(
        connect=10.0,
        read=30.0,
        write=30.0,
        pool=10.0,
    )

    app.state.http = httpx.AsyncClient(timeout=timeout, limits=limits)
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
    print("[Startup] BatchFlow initialisiert.")


@app.on_event("shutdown")
async def _shutdown():
    await app.state.http.aclose()
    await app.state.pool.close()

# ------------------------------------------------------------------------------
# REQUEST SAUBER HALTEN --> FEHLER 425 VERMEIDEN
# -----------------------------------------------------------------------------

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

# ------------------------------------------------------------------------------
# LISTEN, ARRAYS, DICS SAUBER FÜR API V2 GLÄTTEN
# -----------------------------------------------------------------------------
def _val(obj, key: str) -> str:
    """
    Robust: liest obj[key] und macht daraus einen String.
    Pipedrive Custom Fields können str/int, dict, list sein.
    """
    if not obj or not key:
        return ""

    v = obj.get(key)

    if v is None:
        return ""

    if isinstance(v, dict):
        # häufig {"value": "..."} oder option dict
        if "value" in v:
            return sanitize(v.get("value"))
        if "label" in v:
            return sanitize(v.get("label"))
        if "name" in v:
            return sanitize(v.get("name"))
        return sanitize(str(v))

    if isinstance(v, list):
        # multi-select o.Ä.
        if not v:
            return ""
        first = v[0]
        if isinstance(first, dict):
            return sanitize(first.get("value") or first.get("label") or first.get("name") or str(first))
        return sanitize(str(first))

    return sanitize(v)

# ------------------------------------------------------------------------------
# E-MAIL SAUBER LADEN
# ------------------------------------------------------------------------------
def _primary_email(p: dict) -> str:
    v = p.get("email")
    # häufig: [{"label": "...", "value": "..."}]
    if isinstance(v, list) and v:
        e0 = v[0]
        if isinstance(e0, dict):
            return sanitize(e0.get("value") or e0.get("email") or "")
        return sanitize(e0)
    if isinstance(v, str):
        return sanitize(v)
    return ""

# ------------------------------------------------------------------------------
# PAUSEN FÜR REQUEST EINBAUEN - API ENTLASTUNG
# ------------------------------------------------------------------------------

import random
import asyncio
from typing import Optional, Tuple


PD_SEM = asyncio.Semaphore(1)

def _retry_after_seconds(resp) -> float:
    try:
        ra = (resp.headers or {}).get("Retry-After")
    except Exception:
        ra = None
    if not ra:
        return 0.0
    try:
        return float(ra)
    except Exception:
        return 0.0


import asyncio
import random
import time

async def pd_get_with_retry(
    client: httpx.AsyncClient,
    url: str,
    headers: Optional[dict] = None,
    *,
    label: str = "",
    retries: int = 8,
    base_delay: float = 0.8,
    sem: asyncio.Semaphore = PD_SEM,
    request_timeout: float = 30.0,
    max_total_time: float = 120.0,
) -> httpx.Response:
    """
    Backward compatible:
      - alt: pd_get_with_retry(client, url, headers, label="x")
      - neu: (client, url, label="x")

    Wichtig: label ist KEYWORD-ONLY (wegen '*'), damit "multiple values for label" nicht mehr passieren kann.
    """
    delay = base_delay
    last_err = None
    t0 = time.monotonic()

    def _short(u: str, n: int = 140) -> str:
        return u if len(u) <= n else (u[:n] + "...")

    for attempt in range(1, retries + 1):
        if (time.monotonic() - t0) > max_total_time:
            raise RuntimeError(
                f"[] TIMEOUT(total) {label} after {attempt-1} attempts "
                f"({max_total_time}s). url={_short(url)} last_err={last_err}"
            )

        # --- API REQUEST COUNTING ---
        try:
            if "/persons/" in real_url:
                inc("persons")
            elif "/organizations/" in real_url:
                inc("organizations")
            elif "/search" in real_url:
                inc("search")
            else:
                inc("generic")
        except:
            inc("generic")

        try:
            if headers and headers.get("Authorization"):
                real_url = url
                real_headers = headers
            else:
                real_url, real_headers = pd_auth(url)

            async with sem:
                r = await asyncio.wait_for(
                    client.get(real_url, headers=real_headers, timeout=request_timeout),
                    timeout=request_timeout + 2.0,
                )

            if r.status_code == 200:
                return r

            if r.status_code in (400, 401, 403, 404):
                return r

            if r.status_code == 429 or (500 <= r.status_code <= 599):
                ra = _retry_after_seconds(r)
                sleep_s = min(max(delay, ra) + random.uniform(0, 0.35), 30.0)
                print(
                    f"[] {label} HTTP {r.status_code} "
                    f"attempt={attempt}/{retries}, sleep={sleep_s:.2f}s"
                )
                await asyncio.sleep(sleep_s)
                delay = min(delay * 1.8, 30.0)
                continue

            print(f"[] {label} HTTP {r.status_code}, retry attempt={attempt}/{retries}")
            await asyncio.sleep(delay + random.uniform(0, 0.2))
            delay = min(delay * 1.8, 30.0)

        except asyncio.TimeoutError as e:
            last_err = e
            sleep_s = delay + random.uniform(0, 0.35)
            print(
                f"[] {label} REQ_TIMEOUT {request_timeout}s, "
                f"retry attempt={attempt}/{retries}, sleep={sleep_s:.2f}s"
            )
            await asyncio.sleep(sleep_s)
            delay = min(delay * 1.8, 30.0)

        except Exception as e:
            last_err = e
            sleep_s = delay + random.uniform(0, 0.35)
            print(f"[] {label} EXC {e}, retry attempt={attempt}/{retries}, sleep={sleep_s:.2f}s")
            await asyncio.sleep(sleep_s)
            delay = min(delay * 1.8, 30.0)

    raise RuntimeError(
        f"[] FAILED {label} after {retries} retries "
        f"(total<= {max_total_time}s). url={_short(url)} last_err={last_err}"
    )


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

def is_forbidden_activity_date(dt):
    """
    Gibt True zurück, wenn das Datum eine zukünftige Aktivität darstellt.
    Verarbeitet alle fehlerhaften Pipedrive-Werte sicher.
    """

    # Kein Datum → kein Ausschluss
    if not dt:
        return False

    # Sonderfall aus älteren Pipedrive-Systemen
    if dt in ("0000-00-00", "0000-00-00 00:00:00"):
        return False

    # In echtes datetime wandeln
    try:
        if isinstance(dt, str):
            # ISO-Formate reparieren
            cleaned = dt.replace("Z", "").replace("+00:00", "")
            dt = datetime.fromisoformat(cleaned)
    except:
        # Unbekanntes Format → nicht blockieren
        return False

    now = datetime.utcnow()

    # Vergleich immer try/except schützen
    try:
        return dt > now
    except Exception:
        return False

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



def get_person_custom_field(p: dict, field_key: str):
    """
    Pipedrive custom fields liegen meist direkt als key im Person-Objekt.
    field_key ist z.B. "5ac34dad3ea9...." (dein Custom-Field-Key).
    """
    if not isinstance(p, dict):
        return None
    return p.get(field_key)

def safe_int(s, default=0):
    try:
        return int(s)
    except Exception:
        return default

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
        
        # Leere DF sollen trotzdem vorhandene Daten entfernen.
        # Wenn Spalten bekannt sind, Tabelle leer neu anlegen, sonst nur löschen.
        async with get_pool().acquire() as conn:
            await clear_table(conn, table)
            cols = list(df.columns)
            if cols:
                await ensure_table_text(conn, table, cols)
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
        "final":     f"{prefix}_master_final",
        "ready":     f"{prefix}_master_ready",
        "log":       f"{prefix}_delete_log",
        "excluded":  f"{prefix}_excluded",
    }


async def load_df_text(table: str) -> pd.DataFrame:
    """
    Lädt eine TEXT-Tabelle und wandelt ALLE Werte in saubere Strings um:
    - Listen → erster Eintrag
    - Dicts → value/label/name/id
    - JSON-Strings → decodieren
    - None/NaN → ""
    - verschachtelte Strukturen → vollständig flatten
    API-v2-sicher & -sicher.
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
        try:
            rows = await conn.fetch(f'SELECT * FROM "{SCHEMA}"."{table}"')
        except Exception:
            # Tabelle existiert evtl. noch nicht (z.B. nach Reset) → leeres DF
            return pd.DataFrame()
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
    token = (user_tokens.get("default") or "").strip()
    return {"Authorization": f"Bearer {token}"} if token else {}

def append_token(url: str) -> str:
    """Hängt api_token automatisch an (wenn kein OAuth-Token genutzt wird)."""
    # Wenn OAuth-Token vorhanden ist, NICHT mit api_token mischen
    token = (user_tokens.get("default") or "").strip()
    if token:
        return url

    if "api_token=" in url:
        return url

    if PD_API_TOKEN:
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}api_token={PD_API_TOKEN}"

    return url

def pd_auth(url: str) -> tuple[str, Dict[str, str]]:
    """
    Liefert (url, headers) mit korrekter Auth.
    OAuth -> Bearer Header
    sonst -> api_token Query Param
    """
    headers = get_headers()
    if headers.get("Authorization"):
        return url, headers
    return append_token(url), {}
    
# =============================================================================
# PERSONEN ÜBER FILTER LADEN
# =============================================================================

from typing import Optional, List

async def fetch_persons_by_filter_id_v2(
    filter_id: int,
    limit: int = 200,
    job_obj=None,
    max_pages: int =None,
    max_empty_growth: int = 2,
) -> List[dict]:
    """
    v2: Holt Persons über /persons?filter_id=...
    Cursor-basiert (kein start).
    Enthält Notbremsen gegen Cursor-Loops / keine neuen IDs.
    """
    out: List[dict] = []
    seen_ids: set[str] = set()

    cursor: Optional[str] = None
    seen_cursors: set[str] = set()

    pages = 0
    no_growth_streak = 0

    while True:
        pages += 1
        if max_pages is not None and pages > max_pages:
            print(f"[WARN] fetch_persons_by_filter_id_v2 abort: max_pages={max_pages} reached")
            break

        base = f"{PIPEDRIVE_API}/persons?filter_id={int(filter_id)}&limit={int(limit)}"
        url = append_token(base)
        if cursor:
            url += f"&cursor={cursor}"

        
        # Heartbeat/Detail für UI (auch wenn % lange gleich bleibt)
        if job_obj:
            job_obj.phase = f"Suche Personen (Filter {filter_id})"
            job_obj.detail = f"Seite {pages} · Anfrage an Pipedrive …"
        r = await pd_get_with_retry(
            http_client(),
            url,
            None,
            label=f"persons filter={filter_id} page={pages}",
            retries=10,
            request_timeout=30.0,
            max_total_time=180.0,
        )
        
        if r.status_code != 200:
            print(f"[WARN] /persons?filter_id={filter_id} HTTP {r.status_code} {r.text[:200]}")
            break

        payload = r.json() or {}
        data = payload.get("data") or []

        before = len(seen_ids)

        for p in data:
            if not isinstance(p, dict):
                continue
            pid = p.get("id")
            if pid is None:
                continue
            spid = str(pid)
            if spid in seen_ids:
                continue
            seen_ids.add(spid)
            out.append(p)

        added = len(seen_ids) - before

        # DEBUG
        print(
            f"[persons/filter] filter={filter_id} page={pages} got={len(data)} "
            f"added_ids={added} total={len(out)} cursor={'yes' if cursor else 'no'}"
        )

        # Fortschritt (optional)
        if job_obj:
            job_obj.phase = f"Suche Personen (Filter {filter_id})"
            job_obj.detail = f"Seite {pages} · geladen {len(out)}"
            job_obj.percent = min(22, max(int(getattr(job_obj, "percent", 0) or 0), 12 + min(10, pages)))

        if added == 0:
            no_growth_streak += 1
        else:
            no_growth_streak = 0

        # Notbremse: keine neuen IDs mehr
        if no_growth_streak >= max_empty_growth:
            print(f"[WARN] abort: no new IDs for {no_growth_streak} pages (possible loop)")
            break

        ad = payload.get("additional_data") or {}
        next_cursor = ad.get("next_cursor")
        if not next_cursor:
            pagination = ad.get("pagination") or {}
            next_cursor = pagination.get("next_cursor")

        if not next_cursor:
            break

        next_cursor = str(next_cursor)

        # Cursor-Loop-Abbruch
        if cursor and next_cursor == cursor:
            print(f"[WARN] abort: next_cursor == cursor (no progress) cursor={cursor[:16]}...")
            break

        if next_cursor in seen_cursors:
            print(f"[WARN] abort: cursor repeated ({next_cursor[:16]}...)")
            break

        seen_cursors.add(next_cursor)
        cursor = next_cursor

    print(f"[persons/filter] /persons?filter_id={filter_id}: pages={pages}, personen={len(out)}")
    return out

from typing import List, Optional, Set

async def collect_person_ids_by_filter_fast(
    filter_id: int,
    *,
    limit: int = 500,          # höheres Limit für weniger Seiten
    job_obj=None,
) -> List[str]:
    """
    Holt NUR die Personen-IDs für einen Pipedrive-Filter (API v2).
    - nutzt /persons?filter_id=...
    - cursor-basiert (kein start)
    - deutlich leichter als fetch_persons_by_filter_id_v2 (keine kompletten Datensätze)
    """
    client = http_client()
    cursor: Optional[str] = None
    pages = 0
    ids: List[str] = []

    while True:
        pages += 1
        base = f"{PIPEDRIVE_API}/persons?filter_id={int(filter_id)}&limit={int(limit)}"
        url = append_token(base)
        if cursor:
            url += f"&cursor={cursor}"

        r = await pd_get_with_retry(
            client,
            url,
            None,
            label=f"persons/filter_id={filter_id} page={pages}",
            retries=8,
            request_timeout=30.0,
            max_total_time=180.0,
        )

        if r.status_code != 200:
            print(f"[WARN] collect_person_ids_by_filter_fast /persons?filter_id={filter_id} HTTP {r.status_code} {r.text[:200]}")
            break

        payload = r.json() or {}
        data = payload.get("data") or []
        if not data:
            break

        for p in data:
            if not isinstance(p, dict):
                continue
            pid = p.get("id")
            if pid is None:
                continue
            ids.append(str(pid))

        if job_obj:
            job_obj.phase = f"Suche Personen (Filter {filter_id})"
            # grobe Fortschritts-Schätzung
            job_obj.percent = max(int(getattr(job_obj, "percent", 0) or 0), 10)

        ad = payload.get("additional_data") or {}
        cursor = ad.get("next_cursor") or (ad.get("pagination") or {}).get("next_cursor")

        if not cursor:
            break

    print(f"[INFO] collect_person_ids_by_filter_fast filter={filter_id}: pages={pages}, ids={len(ids)}")
    return ids


from typing import List, Optional, Set

async def fetch_person_ids_for_batch_from_filter_v2(
    filter_id: int,
    batch_ids: List[str],
    *,
    page_limit: int = 200,
    max_pages: int = 200,
) -> List[str]:
    """
    Lädt ALLE Personen aus einem Pipedrive-Filter (v2 /persons?filter_id=...)
    und gibt NUR die Person-IDs zurück, deren Custom Field "Batch ID" exakt
    in batch_ids liegt.
    """
    want: Set[str] = {str(b).strip() for b in (batch_ids or []) if str(b).strip()}
    if not want:
        return []

    FIELD_BATCH_ID = PD_PERSON_FIELDS["Batch ID"]

    persons = await fetch_persons_by_filter_id_v2(
        filter_id=int(filter_id),
        limit=int(page_limit),
        max_pages=int(max_pages),
        job_obj=None,
    )

    ids: List[str] = []
    seen: Set[str] = set()

    for p in persons:
        if not isinstance(p, dict):
            continue
        pid = p.get("id")
        if pid is None:
            continue
        spid = str(pid)
        if spid in seen:
            continue

        bval = str(cf_value(p, FIELD_BATCH_ID) or "").strip()
        if bval in want:
            seen.add(spid)
            ids.append(spid)

    return ids


def get_person_custom_field(p: dict, field_key: str) -> str:
    """
    Pipedrive liefert Custom Fields je nach Endpoint unterschiedlich:
    - manchmal in p["custom_fields"]
    - manchmal direkt als p[field_key]
    - manchmal als dict/list

    Rückgabe: immer ein sauberer string (kann leer sein).
    """
    if not isinstance(p, dict):
        return ""

    # 1) Versuch über deine vorhandene cf_value(...)
    try:
        v = cf_value(p, field_key)
        s = sanitize(v).strip()
        if s:
            return s
    except Exception:
        pass

    # 2) Fallback: direkt im root (häufig bei /persons)
    try:
        v2 = p.get(field_key)
        return sanitize(v2).strip()
    except Exception:
        return ""

def filter_persons_by_batch_values(
    persons: List[dict],
    batch_values: List[str],
    batch_field_key: str,
    debug: bool = True,
) -> List[dict]:
    """
    Filtert Personenliste auf exakt passende Batch-ID(s).
    Robust: liest Batch-Wert über get_person_custom_field().
    Enthält Debug, damit du siehst, warum ggf. 0 matchen.
    """
    want = [str(b).strip() for b in (batch_values or []) if str(b).strip()]
    want = want[:2]
    want_set = set(want)

    if not want_set:
        return []

    out: List[dict] = []
    seen: set[str] = set()

    # Debug stats
    with_batch = 0
    sample_values: Dict[str, int] = {}

    for p in persons or []:
        if not isinstance(p, dict):
            continue

        pid = p.get("id")
        if pid is None:
            continue
        pid = str(pid)

        v = get_person_custom_field(p, batch_field_key)

        if v:
            with_batch += 1
            sample_values[v] = sample_values.get(v, 0) + 1

        if v not in want_set:
            continue

        if pid in seen:
            continue
        seen.add(pid)
        out.append(p)

    if debug:
        top = sorted(sample_values.items(), key=lambda x: x[1], reverse=True)[:10]
        print(
            f"[NF][BatchFilter] want={want} total_in={len(persons or [])} "
            f"with_batch_value={with_batch} matched={len(out)}"
        )
        print(f"[NF][BatchFilter] top_batch_values={top}")

        # optional: wenn 0 gematched, zeig beispiel keys
        if len(out) == 0 and persons:
            ks = list((persons[0] or {}).keys())
            print(f"[NF][BatchFilter] sample keys[0]={ks[:40]}")

    return out



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
# Fachbereich - Kampagne
# =============================================================================
from typing import Dict  # falls oben schon importiert, diesen Import ignorieren

async def get_fachbereich_label_map() -> Dict[str, str]:
    """
    Liefert ein Mapping ID -> Label für das Personenfeld
    'Fachbereich - Kampagne'.
    Ergebnis wird in _OPTIONS_CACHE gecacht.
    """
    field_key = PD_PERSON_FIELDS.get("Fachbereich - Kampagne")
    if not field_key:
        return {}

    cached = _OPTIONS_CACHE.get(field_key)  # Cache nach Field-Key
    if isinstance(cached, dict) and cached:
        return cached  # type: ignore[return-value]

    client = http_client()
    url = append_token(f"{PIPEDRIVE_API}/personFields/{field_key}")
    payload, status, err = await pd_get_json_with_retry(
        client,
        url,
        get_headers(),
        label="personField Fachbereich - Kampagne",
        retries=8,
        base_delay=0.8,
    )

    mapping: Dict[str, str] = {}
    if status == 200 and isinstance(payload, dict):
        data = payload.get("data") or {}
        if isinstance(data, dict):
            mapping = field_options_id_to_label_map(data)

    _OPTIONS_CACHE[field_key] = mapping
    return mapping


# =============================================================================
# org_BULK
# =============================================================================
async def fetch_orgs_bulk(
    org_ids: List[str],
    *,
    concurrency: int = 2,          # << wichtig runter
    request_timeout: float = 25.0,
) -> Dict[str, dict]:
    if not org_ids:
        return {}

    client = http_client()
    local_sem = asyncio.Semaphore(concurrency)

    results: Dict[str, dict] = {}
    total = len(org_ids)
    done = 0

    async def fetch_one(oid: str) -> None:
        nonlocal done
        async with local_sem:
            url = f"{PIPEDRIVE_API}/organizations/{oid}"
            try:
                r = await pd_get_with_retry(
                    client,
                    url,
                    None,
                    label=f"org:{oid}",
                    request_timeout=request_timeout,
                    max_total_time=180.0,   # org calls können zäher sein
                )
                if r.status_code == 200:
                    payload = r.json() or {}
                    data = payload.get("data")
                    if data:
                        results[str(oid)] = data
                elif r.status_code in (401, 403):
                    print(f"[fetch_orgs_bulk] AUTH problem for org {oid}: HTTP {r.status_code}")
                # 404 ok (org deleted), einfach überspringen

            except Exception as e:
                # Nicht crashen lassen, nur loggen
                print(f"[fetch_orgs_bulk] oid={oid} ERROR: {type(e).__name__}: {e}")

            done += 1
            if done % 50 == 0 or done == total:
                print(f"[fetch_orgs_bulk] progress {done}/{total}")

    await asyncio.gather(*(fetch_one(str(oid)) for oid in org_ids))
    return results


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

# =============================================================================
# STREAM FILTER
# =============================================================================
import urllib.parse
from typing import AsyncGenerator, List, Optional

async def stream_person_ids_by_filter_cursor(
    filter_id: int,
    page_limit: int = 200,
    job_obj=None,
    label: str = "Filter",
) -> AsyncGenerator[List[str], None]:
    """
    Streamt Personen-IDs eines Pipedrive-Filters seitenweise (API v2, cursor-basiert).
    Endpoint: GET /persons?filter_id=...&limit=...&cursor=...

    Yields: Liste von Person-IDs (strings) pro Seite.
    """
    client = http_client()
    cursor: Optional[str] = None

    while True:
        base = f"{PIPEDRIVE_API}/persons?filter_id={int(filter_id)}&limit={int(page_limit)}"
        url = append_token(base)
        if cursor:
            url += f"&cursor={urllib.parse.quote(str(cursor))}"

        payload, status, err = await pd_get_json_with_retry(
            client,
            url,
            get_headers(),
            label=f"persons_filter[{filter_id}]",
            retries=10,
            base_delay=0.8,
            request_timeout=30.0,
            max_total_time=180.0,
        )

        if status != 200 or not payload:
            print(f"[stream_person_ids_by_filter_cursor] WARN status={status} filter={filter_id} err={err}")
            break

        data = payload.get("data") or []
        if not data:
            break

        ids: List[str] = []
        for p in data:
            if isinstance(p, dict) and p.get("id") is not None:
                ids.append(str(p["id"]))

        if job_obj:
            job_obj.detail = f"IDs geladen: +{len(ids)}"
        if ids:
            yield ids

        additional = payload.get("additional_data") or {}
        next_cursor = additional.get("next_cursor")
        if not next_cursor:
            pagination = additional.get("pagination") or {}
            next_cursor = pagination.get("next_cursor")

        if not next_cursor:
            break

        cursor = str(next_cursor)

    
from typing import AsyncGenerator, List, Optional, Set, Dict
import asyncio

import urllib.parse
from typing import AsyncGenerator, List, Optional

async def stream_person_ids_by_filter(
    filter_id: int,
    *,
    page_limit: int = 200,     # /persons limit kann i.d.R. 200 sein
    max_pages: int = 2000,     # Safety
    job_obj=None,
    label_prefix: str = "persons/filter"
) -> AsyncGenerator[List[str], None]:
    """
    Streamt Person-IDs aus einem Pipedrive Filter (API v2) cursor-basiert.
    Liefert pro Seite: Liste[str] von Person IDs.
    """
    client = http_client()
    cursor: Optional[str] = None
    pages = 0
    seen_cursors: set[str] = set()

    while True:
        pages += 1
        if pages > max_pages:
            print(f"[WARN] stream_person_ids_by_filter abort: max_pages={max_pages}")
            break

        base = f"{PIPEDRIVE_API}/persons?filter_id={int(filter_id)}&limit={int(page_limit)}"
        url = append_token(base)
        if cursor:
            url += f"&cursor={urllib.parse.quote(str(cursor))}"

        r = await pd_get_with_retry(
            client,
            url,
            None,
            label=f"{label_prefix} filter={filter_id} page={pages}",
            retries=10,
            request_timeout=30.0,
            max_total_time=180.0,
        )

        if r.status_code != 200:
            print(f"[WARN] /persons?filter_id={filter_id} HTTP {r.status_code} {r.text[:200]}")
            break

        payload = r.json() or {}
        data = payload.get("data") or []
        if not data:
            break

        ids: List[str] = []
        for p in data:
            if isinstance(p, dict) and p.get("id") is not None:
                ids.append(str(p["id"]))

        if ids:
            yield ids

        if job_obj and pages % 5 == 0:
            job_obj.phase = f"Suche Personen (Filter {filter_id})"
            job_obj.percent = max(int(getattr(job_obj, "percent", 0) or 0), 10)

        ad = payload.get("additional_data") or {}
        next_cursor = ad.get("next_cursor") or (ad.get("pagination") or {}).get("next_cursor")
        if not next_cursor:
            break

        next_cursor = str(next_cursor)

        # Cursor loop guard
        if cursor and next_cursor == cursor:
            print(f"[WARN] cursor no progress (same cursor) filter={filter_id}")
            break
        if next_cursor in seen_cursors:
            print(f"[WARN] cursor repeated filter={filter_id}")
            break

        seen_cursors.add(next_cursor)
        cursor = next_cursor

from typing import List, Optional, Set

async def fetch_person_details_for_batch_from_filter(
    filter_id: int,
    batch_value: str,
    *,
    page_limit: int = 200,
    detail_concurrency: int = 3,
    job_obj=None,
    chunk_size: int = 120,
) -> List[dict]:
    """
    Schnellste saubere Variante für "Batch-ID neu":
    - streamt IDs aus Filter (3024)
    - lädt Details chunked
    - filtert exakt auf Custom Field 'Batch-ID neu'
    """
    batch_value = str(batch_value).strip()
    if not batch_value:
        return []

    FIELD_BATCH = PD_PERSON_FIELDS["Batch ID"]  # muss auf 'Batch-ID neu' Key zeigen!

    matched: List[dict] = []
    buffer: List[str] = []

    async def flush_buffer():
        nonlocal matched, buffer
        if not buffer:
            return
        details = await fetch_person_details_many(buffer, job_obj=job_obj, concurrency=detail_concurrency)
        for p in details:
            if not isinstance(p, dict) or p.get("_error") or p.get("id") is None:
                continue
            bval = str(cf_value(p, FIELD_BATCH) or "").strip()
            if bval == batch_value:
                matched.append(p)
        buffer = []
        await asyncio.sleep(0.3)  # glättet 429

    total_seen = 0
    async for ids in stream_person_ids_by_filter(filter_id, page_limit=page_limit, job_obj=job_obj):
        for pid in ids:
            buffer.append(pid)
            total_seen += 1
            if len(buffer) >= chunk_size:
                if job_obj:
                    job_obj.phase = f"Prüfe Batch-ID (Details-Check) ({total_seen})"
                    job_obj.percent = max(int(getattr(job_obj, "percent", 0) or 0), 18)
                await flush_buffer()

    # rest
    await flush_buffer()

    print(f"[NF] filter={filter_id} batch={batch_value} matched={len(matched)}")
    return matched

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
async def fetch_person_details(person_ids: List[str], job_obj=None) -> List[dict]:
    if not person_ids:
        return []

    if job_obj:
        job_obj.phase = f"Lade Personendetails (0/{len(person_ids)})"
        job_obj.percent = 25

    # Pass 1
    lookup = await fetch_person_details_many(person_ids, job_obj=job_obj, concurrency=3, request_timeout=25.0)
    missing = [str(pid) for pid in person_ids if str(pid) not in lookup]
    print(f"[fetch_person_details] pass1 got={len(lookup)} missing={len(missing)}")

    # Pass 2
    if missing:
        if job_obj:
            job_obj.phase = f"Nachladen fehlender Personendetails (pass2) ({len(missing)})"
            job_obj.percent = max(job_obj.percent, 66)

        lookup2 = await fetch_person_details_many(missing, job_obj=job_obj, concurrency=2, request_timeout=30.0)
        lookup.update(lookup2)
        missing = [str(pid) for pid in person_ids if str(pid) not in lookup]
        print(f"[fetch_person_details] pass2 added={len(lookup2)} still_missing={len(missing)}")

    # Pass 3 (ultra-konservativ)
    if missing:
        if job_obj:
            job_obj.phase = f"Nachladen fehlender Personendetails (pass3, langsam) ({len(missing)})"
            job_obj.percent = max(job_obj.percent, 72)

        lookup3 = await fetch_person_details_many(missing, job_obj=job_obj, concurrency=1, request_timeout=35.0)
        lookup.update(lookup3)

        missing2 = [str(pid) for pid in person_ids if str(pid) not in lookup]
        print(f"[fetch_person_details] pass3 added={len(lookup3)} still_missing={len(missing2)}")
        if missing2:
            print(f"[fetch_person_details] STILL missing examples: {missing2[:30]}")

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

from typing import Any

def _extract_custom_fields_blob(p: dict) -> dict:
    if not isinstance(p, dict):
        return {}
    blob: dict = {}

    cf = p.get("custom_fields")
    if isinstance(cf, dict):
        blob.update(cf)
    elif isinstance(cf, list):
        for it in cf:
            if isinstance(it, dict):
                k = it.get("key") or it.get("id")
                if k:
                    blob[str(k)] = it.get("value")

    # zusätzlich: top-level keys, die wie field keys aussehen (hex)
    for k, v in p.items():
        if isinstance(k, str) and len(k) >= 16 and all(ch in "0123456789abcdef" for ch in k.lower()):
            blob.setdefault(k, v)

    return blob

def cf_value(p: dict, field_key: str) -> Any:
    if not field_key or not isinstance(p, dict):
        return None
    if field_key in p:
        return p.get(field_key)
    blob = _extract_custom_fields_blob(p)
    return blob.get(field_key)


def cf_value(p: dict, field_key: str) -> Any:
    """
    Liest einen Custom Field Value robust aus Person.
    """
    if not field_key:
        return None
    if not isinstance(p, dict):
        return None

    # erst direkt (falls top-level)
    if field_key in p:
        return p.get(field_key)

    blob = _extract_custom_fields_blob(p)
    return blob.get(field_key)

def cf_value_v2(item: dict, field_key: str):
    """
    Liest benutzerdefinierte Felder gemäß API v2 (Top-Level Felder).
    Fällt auf API v1 Struktur zurück, falls notwendig.
    Wird ausschließlich für REFRESH benutzt.
    """
    if not item or not isinstance(item, dict):
        return None

    # API v2: Custom Fields liegen direkt auf der Person
    if field_key in item:
        return item.get(field_key)

    # API v1 fallback (ältere Struktur)
    custom = item.get("custom_fields") or item.get("custom_fields_data")
    if isinstance(custom, dict):
        return custom.get(field_key)

    return None

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


from typing import List, Set

async def fetch_person_details_for_batches_from_search(
    batch_ids: List[str],
    *,
    search_page_limit: int = 100,
    detail_concurrency: int = 4,
    job_obj=None,
) -> List[dict]:
    """
    1) Holt candidate Person IDs per persons/search (indexbasiert)
    2) Lädt /persons/{id} genau EINMAL für diese Kandidaten
    3) Filtert exakt nach Custom Field Batch ID
    4) Gibt die *vollen Person-Details* der Matches zurück
    """
    want: Set[str] = {str(b).strip() for b in (batch_ids or []) if str(b).strip()}
    if not want:
        return []

    # candidates via search
    candidate_ids: List[str] = []
    seen: Set[str] = set()

    for b in list(want)[:2]:
        items = await stream_person_items_by_batch_id_v2(
            b,
            page_limit=min(search_page_limit, 100),
            job_obj=job_obj
        )
        for it in items:
            pid = (it or {}).get("id")
            if pid is None:
                continue
            spid = str(pid)
            if spid in seen:
                continue
            seen.add(spid)
            candidate_ids.append(spid)

    if not candidate_ids:
        return []

    if job_obj:
        job_obj.phase = f"Prüfe Batch-ID (Details-Check) ({len(candidate_ids)})"
        job_obj.percent = max(int(getattr(job_obj, "percent", 0) or 0), 18)

    # details once
    details_all = await fetch_person_details_many(
        candidate_ids,
        job_obj=job_obj,
        concurrency=detail_concurrency
    )

    FIELD_BATCH_ID = PD_PERSON_FIELDS["Batch ID"]

    out: List[dict] = []
    for p in details_all:
        if not isinstance(p, dict) or p.get("_error") or p.get("id") is None:
            continue
        bval = str(cf_value(p, FIELD_BATCH_ID) or "").strip()
        if bval in want:
            out.append(p)

    print(f"[NF] candidates={len(candidate_ids)} details_ok={sum(1 for x in details_all if isinstance(x, dict) and not x.get('_error'))} matched={len(out)}")
    return out


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

PD_SEM = asyncio.Semaphore(3)  # wenn du sehr viele 429 bekommst -> 1

def _retry_after_seconds(resp: httpx.Response) -> float:
    ra = (resp.headers or {}).get("Retry-After")
    if not ra:
        return 0.0
    try:
        return float(ra)
    except Exception:
        return 0.0

async def pd_get_json_with_retry(
    client: httpx.AsyncClient,
    url: str,
    headers: dict,
    *,
    label: str = "",
    retries: int = 10,
    base_delay: float = 0.8,
    request_timeout: float = 30.0,
    max_total_time: float = 180.0,
    sem: asyncio.Semaphore = PD_SEM,
) -> Tuple[Optional[dict], int, Optional[str]]:
    """
    Liefert (json_payload|None, http_status|0, error|None).
    """
    delay = base_delay
    t0 = time.monotonic()
    last_err = None

    for attempt in range(1, retries + 1):
        if (time.monotonic() - t0) > max_total_time:
            return None, 0, f"TIMEOUT(total) {label} after {attempt-1} attempts"

        try:
            async with sem:
                r = await asyncio.wait_for(
                    client.get(url, headers=headers, timeout=request_timeout),
                    timeout=request_timeout + 2.0,
                )

            status = r.status_code

            if status == 200:
                try:
                    return r.json() or {}, status, None
                except Exception as e:
                    return None, status, f"JSON decode error: {e}"

            if status in (400, 401, 403, 404):
                return None, status, (r.text[:200] if r.text else f"HTTP {status}")

            if status == 429 or (500 <= status <= 599):
                ra = _retry_after_seconds(r)
                sleep_s = min(max(delay, ra) + random.uniform(0, 0.35), 30.0)
                print(f"[pd_get_json_with_retry] {label} HTTP {status} attempt={attempt}/{retries} sleep={sleep_s:.2f}s")
                await asyncio.sleep(sleep_s)
                delay = min(delay * 1.8, 30.0)
                continue

            # sonstiger status -> konservativ retry
            last_err = f"HTTP {status}: {(r.text or '')[:200]}"
            await asyncio.sleep(min(delay + random.uniform(0, 0.2), 10.0))
            delay = min(delay * 1.6, 30.0)

        except asyncio.TimeoutError:
            last_err = f"REQ_TIMEOUT({request_timeout}s)"
            await asyncio.sleep(min(delay + random.uniform(0, 0.35), 10.0))
            delay = min(delay * 1.8, 30.0)
        except Exception as e:
            last_err = f"EXC: {e}"
            await asyncio.sleep(min(delay + random.uniform(0, 0.35), 10.0))
            delay = min(delay * 1.8, 30.0)

    return None, 0, f"FAILED {label}: {last_err}"

import asyncio
import urllib.parse
from typing import List, Optional
import asyncio, time, random, urllib.parse
from typing import List, Dict, Optional

# Global limiter: ~6 Requests/Sekunde gesamt (stell das ggf. höher)
_PD_RATE_LOCK = asyncio.Lock()
_PD_NEXT_TS = 0.0

async def pd_global_rate(limit_per_sec: float = 6.0):
    global _PD_NEXT_TS
    min_interval = 1.0 / max(0.1, limit_per_sec)
    async with _PD_RATE_LOCK:
        now = time.monotonic()
        wait = _PD_NEXT_TS - now
        if wait > 0:
            await asyncio.sleep(wait)
        _PD_NEXT_TS = time.monotonic() + min_interval


import asyncio
import urllib.parse
from typing import List, Dict, Any

async def fetch_person_details_many(
    person_ids: List[str],
    job_obj=None,
    concurrency: int = 2,          # bewusst niedrig wegen 429
    request_timeout: float = 30.0,
) -> List[dict]:
    """
    Lädt Personendetails via /persons/{id} (API v2) für eine Liste von IDs.

    - Nutzt globale Drosselung (pd_throttle), um 429 zu vermeiden.
    - concurrency klein halten (1–2), damit Pipedrive nicht überfahren wird.
    - Bei Fehlern werden Dummy-Dicts mit "_error" zurückgegeben.
    """

    ids = [str(x).strip() for x in (person_ids or []) if str(x).strip()]
    total = len(ids)
    if total == 0:
        return []

    sem = asyncio.Semaphore(max(1, int(concurrency)))
    out: List[dict] = [None] * total  # Reihenfolge beibehalten

    async def fetch_one(idx: int, pid: str):
        url = append_token(f"{PIPEDRIVE_API}/persons/{urllib.parse.quote(pid)}")

        async with sem:
            # 🔹 globale Drosselung: max ~3–4 Requests/Sek
            await pd_throttle(0.3)

            try:
                r = await pd_get_with_retry(
                    http_client(),
                    url,
                    None,
                    label=f"person:{pid}",
                    request_timeout=request_timeout,
                    max_total_time=240.0,
                )
            except Exception as e:
                out[idx] = {"id": pid, "_error": f"EXC: {e}"}
                return

        if r.status_code != 200:
            out[idx] = {"id": pid, "_error": f"HTTP {r.status_code}: {r.text[:200]}"}
            return

        payload = r.json() or {}
        data = payload.get("data")
        if not isinstance(data, dict):
            out[idx] = {"id": pid, "_error": "No data in response"}
            return

        out[idx] = data

    # Fortschritt für UI
    done_counter = 0
    lock = asyncio.Lock()

    async def wrapped(idx: int, pid: str):
        nonlocal done_counter
        await fetch_one(idx, pid)
        async with lock:
            done_counter += 1
            if job_obj and (done_counter == 1 or done_counter % 50 == 0 or done_counter == total):
                job_obj.phase = f"Lade Personendetails ({done_counter}/{total})"
                job_obj.percent = max(int(getattr(job_obj, "percent", 0) or 0), 20)

    await asyncio.gather(*(wrapped(i, pid) for i, pid in enumerate(ids)))

    # None-Einträge absichern
    cleaned: List[dict] = []
    for i, pid in enumerate(ids):
        item = out[i]
        if item is None:
            cleaned.append({"id": pid, "_error": "Unknown error (None result)"})
        else:
            cleaned.append(item)

    return cleaned


from typing import List, Dict, Any, Optional
import asyncio


from typing import List, Set, Dict

async def person_ids_in_filter_v2(filter_id: int, limit: int = 200, max_pages: int = 200) -> List[str]:
    """Holt nur Person-IDs aus /persons?filter_id=... (ohne Custom Fields zu erwarten)."""
    persons = await fetch_persons_by_filter_id_v2(filter_id=filter_id, limit=limit, max_pages=max_pages)
    ids: List[str] = []
    seen: Set[str] = set()
    for p in persons:
        if not isinstance(p, dict):
            continue
        pid = p.get("id")
        if pid is None:
            continue
        spid = str(pid)
        if spid in seen:
            continue
        seen.add(spid)
        ids.append(spid)
    return ids


async def filter_person_ids_by_batch_via_details(
    person_ids: List[str],
    batch_ids: List[str],
    *,
    concurrency: int = 2,
) -> List[str]:
    """
    Prüft Batch-ID exakt über /persons/{id} (da sind Custom Fields zuverlässig).
    Gibt nur IDs zurück, deren FIELD_BATCH_ID in batch_ids liegt.
    """
    want: Set[str] = {str(b).strip() for b in (batch_ids or []) if str(b).strip()}
    if not want or not person_ids:
        return []

    FIELD_BATCH_ID = PD_PERSON_FIELDS["Batch ID"]

    # Details laden (nutzt deine bestehende Funktion, die bereits retry/limits hat)
    details_all = await fetch_person_details_many(person_ids, concurrency=concurrency)

    out: List[str] = []
    for p in details_all:
        if not isinstance(p, dict) or p.get("_error") or p.get("id") is None:
            continue
        bval = str(cf_value(p, FIELD_BATCH_ID) or "").strip()
        if bval in want:
            out.append(str(p["id"]))
    return out

import urllib.parse
from typing import Optional, List

async def stream_person_items_by_batch_id_v2(
    batch_id: str,
    page_limit: int = 100,   # API search limit <= 100
    job_obj=None,
) -> List[dict]:
    """
    Holt Personen-Search-Items über API v2:
      GET /persons/search?term=<batch_id>&fields=custom_fields&limit<=100&cursor=...

    Rückgabe: Liste der "item"-Dicts (aus data.items[].item)
    """
    batch_id = (batch_id or "").strip()
    if not batch_id:
        return []

    limit = min(int(page_limit or 100), 100)

    cursor: Optional[str] = None
    page = 0

    results: List[dict] = []
    seen_ids: set[str] = set()

    while True:
        page += 1

        base_url = (
            f"{PIPEDRIVE_API}/persons/search?"
            f"term={urllib.parse.quote(batch_id)}"
            f"&fields=custom_fields"
            f"&limit={limit}"
        )
        url = append_token(base_url)
        if cursor:
            url += f"&cursor={urllib.parse.quote(cursor)}"

        if job_obj and page % 2 == 1:
            job_obj.phase = f"Suche Personen (Batch {batch_id})"
            job_obj.percent = max(int(getattr(job_obj, "percent", 0) or 0), 10)

        r = await pd_get_with_retry(
            http_client(),
            url,
            None,
            label=f"persons_search batch={batch_id} page={page}",
            request_timeout=30.0,
            max_total_time=180.0,
        )

        if r.status_code != 200:
            print(f"[WARN] Batch {batch_id} Search Fehler: HTTP {r.status_code} {r.text[:200]}")
            break

        payload = r.json() or {}
        raw_items = (payload.get("data") or {}).get("items") or []
        if not raw_items:
            break

        for it in raw_items:
            if not isinstance(it, dict):
                continue
            item = it.get("item")
            if not isinstance(item, dict):
                continue
            pid = item.get("id")
            if pid is None:
                continue
            spid = str(pid)
            if spid in seen_ids:
                continue
            seen_ids.add(spid)
            results.append(item)

        ad = payload.get("additional_data") or {}
        cursor = ad.get("next_cursor") or (ad.get("pagination") or {}).get("next_cursor")
        if not cursor:
            break

    print(f"[INFO] Batch {batch_id}: {len(results)} eindeutige Personen (aus search) gesamt")
    return results



from typing import List

async def fetch_person_details(person_ids: List[str], job_obj=None) -> List[dict]:
    if not person_ids:
        return []

    if job_obj:
        job_obj.phase = f"Lade Personendetails (0/{len(person_ids)})"
        job_obj.percent = 25

    # Pass 1
    lookup = await fetch_person_details_many(person_ids, job_obj=job_obj, concurrency=2)

    missing = [str(pid) for pid in person_ids if str(pid) not in lookup]
    print(f"[fetch_person_details] pass1 got={len(lookup)} missing={len(missing)}")

    # Pass 2 (noch konservativer)
    if missing:
        if job_obj:
            job_obj.phase = f"Nachladen fehlender Personendetails ({len(missing)})"
            job_obj.percent = max(job_obj.percent, 66)

        lookup2 = await fetch_person_details_many(missing, job_obj=job_obj, concurrency=1)
        lookup.update(lookup2)

        missing2 = [str(pid) for pid in person_ids if str(pid) not in lookup]
        print(f"[fetch_person_details] pass2 added={len(lookup2)} still_missing={len(missing2)}")
        if missing2:
            print(f"[fetch_person_details] STILL missing examples: {missing2[:30]}")

    # ordered
    ordered: List[dict] = []
    for pid in person_ids:
        p = lookup.get(str(pid))
        if p:
            ordered.append(p)

    return ordered



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
import pandas as pd
from typing import Dict, List
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

def sanitize(v) -> str:
    if v is None:
        return ""
    s = str(v)
    return "" if s.lower() == "nan" else s.strip()

def split_name(first: Optional[str], last: Optional[str], full: Optional[str]) -> tuple[str, str]:
    first = (first or "").strip()
    last = (last or "").strip()
    if first or last:
        return first, last
    parts = (full or "").strip().split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])

def extract_org_id_from_person(p: dict) -> str:
    org = p.get("org_id")
    if isinstance(org, dict):
        return sanitize(org.get("value") or org.get("id") or "")
    return sanitize(org or "")

async def fetch_orgs_bulk(org_ids: List[str], *, concurrency: int = 2) -> Dict[str, dict]:
    if not org_ids:
        return {}
    client = http_client()
    local_sem = asyncio.Semaphore(concurrency)
    out: Dict[str, dict] = {}

    async def one(oid: str):
        async with local_sem:
            url = append_token(f"{PIPEDRIVE_API}/organizations/{oid}")
            payload, status, err = await pd_get_json_with_retry(client, url, get_headers(), label=f"org:{oid}", retries=10)
            if status == 200 and payload:
                data = payload.get("data")
                if data:
                    out[str(oid)] = data
            elif status not in (404,):
                # 404 ok (deleted)
                pass

    await asyncio.gather(*(one(str(x)) for x in org_ids))
    return out

async def _build_nf_master_final(
    selected: List[dict],
    campaign: str,
    batch_id_label: str = "",
    batch_id: Optional[str] = None,
    job_obj=None,
) -> pd.DataFrame:
    """
    Baut nf_master_final.
    Enthält:
      - Exportspalten (NF_EXPORT_COLUMNS)
      - zusätzlich interne Spalten für Regeln:
          * Organisationsart
          * Datum nächste Aktivität
    """

    # Person custom field keys
    FIELD_BATCH_ID      = PD_PERSON_FIELDS["Batch ID"]
    FIELD_PROSPECT_ID   = PD_PERSON_FIELDS["Prospect ID"]
    FIELD_GENDER        = PD_PERSON_FIELDS["Person Geschlecht"]
    FIELD_TITLE         = PD_PERSON_FIELDS["Person Titel"]
    FIELD_POSITION      = PD_PERSON_FIELDS["Person Position"]
    FIELD_XING          = PD_PERSON_FIELDS["XING Profil"]
    FIELD_LINKEDIN      = PD_PERSON_FIELDS["LinkedIn URL"]

    # Org custom field keys
    FIELD_ORG_ART       = PD_ORG_FIELDS["Organisationsart"]

    if (not batch_id_label) and batch_id:
        batch_id_label = batch_id

    # --- Org IDs sammeln
    unique_org_ids: List[str] = []
    seen: set[str] = set()
    for p in selected:
        oid = extract_org_id_from_person(p)
        if oid and oid not in seen:
            seen.add(oid)
            unique_org_ids.append(oid)

    # --- Orgs bulk laden
    org_lookup: Dict[str, dict] = {}
    if unique_org_ids:
        try:
            if job_obj:
                job_obj.phase = f"Lade Organisationsdetails ({len(unique_org_ids)})"
                job_obj.percent = max(job_obj.percent, 55)
            org_lookup = await fetch_orgs_bulk(unique_org_ids, concurrency=2)
        except Exception as e:
            print(f"[NF][WARN] fetch_orgs_bulk failed: {type(e).__name__}: {e}")
            org_lookup = {}

    def org_obj_for_person(p: dict) -> dict:
        oid = extract_org_id_from_person(p)
        if not oid:
            return {}
        o = org_lookup.get(str(oid))
        return o if isinstance(o, dict) else {}

    def org_name_for_person(p: dict) -> str:
        o = org_obj_for_person(p)
        if o.get("name"):
            return sanitize(o.get("name"))

        org = p.get("org_id")
        if isinstance(org, dict):
            return sanitize(org.get("name") or "")

        return ""

    def org_art_for_person(p: dict) -> str:
        o = org_obj_for_person(p)
        if not o:
            return ""
        return sanitize(o.get(FIELD_ORG_ART))

    def next_activity_for_person(p: dict) -> str:
        return sanitize(p.get("next_activity_date"))

    
    def primary_email(p: dict) -> str:
        """
        Pipedrive v2 persons/search liefert oft:
          - primary_email: "a@b.de"
          - emails: [{"value": "...", "primary": true}, ...] ODER ["a@b.de", ...]
        Und /persons/{id} liefert teils wieder "email".
        """
        if not isinstance(p, dict):
            return ""
    
        # 1) primary_email (v2 search)
        pe = p.get("primary_email")
        if isinstance(pe, str) and pe.strip():
            return sanitize(pe)
    
        # 2) emails (v2 search)
        emails = p.get("emails")
        if isinstance(emails, list) and emails:
            # primary zuerst
            prim = None
            for e in emails:
                if isinstance(e, dict) and e.get("primary") is True:
                    prim = e
                    break
            e0 = prim or emails[0]
            if isinstance(e0, dict):
                return sanitize(e0.get("value") or e0.get("email") or "")
            if isinstance(e0, str):
                return sanitize(e0)
        if isinstance(emails, dict):
            return sanitize(emails.get("value") or emails.get("email") or "")
    
        # 3) email (kommt oft aus /persons/{id})
        email = p.get("email")
        if isinstance(email, list) and email:
            prim = None
            for e in email:
                if isinstance(e, dict) and e.get("primary") is True:
                    prim = e
                    break
            e0 = prim or email[0]
            if isinstance(e0, dict):
                return sanitize(e0.get("value") or e0.get("email") or "")
            if isinstance(e0, str):
                return sanitize(e0)
        if isinstance(email, dict):
            return sanitize(email.get("value") or email.get("email") or "")
        if isinstance(email, str) and email.strip():
            return sanitize(email)
    
        return ""


    
    rows: List[dict] = []
    total = len(selected)

    for idx, p in enumerate(selected, start=1):
        if job_obj and (idx == 1 or idx % 50 == 0 or idx == total):
            job_obj.phase = f"Baue Master ({idx}/{total})"
            job_obj.percent = max(job_obj.percent, 70)

        person_id = sanitize(p.get("id"))
        org_id = extract_org_id_from_person(p)

        first_name = sanitize(p.get("first_name"))
        last_name = sanitize(p.get("last_name"))
        if not first_name and not last_name:
            fn, ln = split_name(None, None, sanitize(p.get("name")))
            first_name, last_name = fn, ln

        prospect_id = sanitize(cf_value(p, FIELD_PROSPECT_ID))
        gender_raw  = sanitize(cf_value(p, FIELD_GENDER))
        title_raw   = sanitize(cf_value(p, FIELD_TITLE))
        pos_raw     = sanitize(cf_value(p, FIELD_POSITION))
        xing_raw    = sanitize(cf_value(p, FIELD_XING))
        li_raw      = sanitize(cf_value(p, FIELD_LINKEDIN))

        gender = GENDER_OPTION_MAP.get(gender_raw, gender_raw)

        # Für den Export soll immer die vom Nutzer angegebene Export-Batch-ID verwendet werden
        batch_out = sanitize(batch_id_label or (batch_id or ""))

        rows.append({
            # Export-Spalten
            "Batch ID": batch_out,
            "Channel": DEFAULT_CHANNEL,
            "Cold-Mailing Import": sanitize(campaign),
            "Prospect ID": prospect_id,
            "Organisation ID": sanitize(org_id),
            "Organisation Name": sanitize(org_name_for_person(p)),
            "Person ID": person_id,
            "Person Vorname": first_name,
            "Person Nachname": last_name,
            "Person Titel": title_raw,
            "Person Geschlecht": gender,
            "Person Position": pos_raw,
            "Person E-Mail": primary_email(p),
            "XING Profil": xing_raw,
            "LinkedIn URL": li_raw,

            # Interne Spalten für Regeln/Log
            "Organisationsart": org_art_for_person(p),
            "Datum nächste Aktivität": next_activity_for_person(p),
        })

    df_final = pd.DataFrame(rows)

    # ensure export columns exist in df_final
    for c in NF_EXPORT_COLUMNS:
        if c not in df_final.columns:
            df_final[c] = ""

    # ensure internal columns exist
    for extra in ["Organisationsart", "Datum nächste Aktivität"]:
        if extra not in df_final.columns:
            df_final[extra] = ""

    # finale Reihenfolge: Exportspalten zuerst, dann interne
    ordered_cols = NF_EXPORT_COLUMNS + ["Organisationsart", "Datum nächste Aktivität"]
    df_final = df_final.reindex(columns=ordered_cols, fill_value="")

    return df_final


# =============================================================================
# REFRESH - MASTER
# =============================================================================
async def _build_refresh_master_final(
    selected: List[dict],
    campaign: str,
    batch_id: str,
    job_obj=None,
) -> pd.DataFrame:
    """
    Baut rf_master_final für Refresh.

    Intern wird _build_nf_master_final genutzt, aber vorhandene Batch-ID-Werte
    an der Person werden ignoriert, so dass für den Export immer die vom Nutzer
    übergebene Batch-ID verwendet wird.
    """
    batch_id = sanitize(batch_id) or ""
    field_batch = PD_PERSON_FIELDS.get("Batch ID")

    # Kopie der Personen ohne vorhandene Batch-ID
    cleaned: List[dict] = []
    for p in selected:
        if not isinstance(p, dict):
            continue
        q = dict(p)
        if field_batch:
            # Top-Level-Field entfernen
            q.pop(field_batch, None)

            # custom_fields bereinigen
            cf = q.get("custom_fields")
            if isinstance(cf, dict):
                cf = dict(cf)
                cf.pop(field_batch, None)
                q["custom_fields"] = cf
        cleaned.append(q)

    df = await _build_nf_master_final(
        cleaned,
        campaign=campaign,
        batch_id_label=batch_id,
        batch_id=batch_id,
        job_obj=job_obj,
    )
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
# -----------------------------------------------------------------------------
# Standard-Spalten für Delete-Logs (damit Tabellen auch leer korrekt zurückgesetzt werden)
# -----------------------------------------------------------------------------
DELETE_LOG_COLUMNS = [
    "reason",
    "Kontakt ID",
    "Name",
    "Organisation ID",
    "Organisationsname",
    "Grund",
    "extra",
    "id",
    "name",
    "org_name",
]

async def _reconcile(prefix: str, job_obj=None) -> None:
    """
    Abgleich:
      1) max 2 Kontakte pro Organisation
      2) Organisationsart gesetzt -> löschen
      3) Orga-Fuzzy >=95% gegen Filter 1245/851/1521 -> löschen
      4) Person-ID bereits in Filtern 1216/1708 -> löschen
      5) Datum nächste Aktivität: in Zukunft ODER innerhalb der letzten 3 Monate -> löschen

    Schreibt:
      - <prefix>_master_ready
      - <prefix>_delete_log
      - nf_excluded (Summary für Bulletpoints)
    """
    t = tables(prefix)
    if job_obj:
        job_obj.detail = f"Abgleich gestartet ({prefix})"
    df = await load_df_text(t["final"])

    # erwartete Spalten im nf_master_final
    col_pid = "Person ID"
    col_orgid = "Organisation ID"
    col_orgname = "Organisation Name"
    col_orgtype = "Organisationsart"
    col_next = "Datum nächste Aktivität"

    if df.empty:
        await save_df_text(pd.DataFrame(), t["ready"])
        await save_df_text(pd.DataFrame(columns=DELETE_LOG_COLUMNS), t["log"])
        # Bulletpoints IMMER schreiben (auch 0)
        await save_df_text(pd.DataFrame([
            {"Grund": "Max 2 Kontakte pro Organisation", "Anzahl": 0},
            {"Grund": "Datum nächste Aktivität steht an bzw. liegt in naher Vergangenheit", "Anzahl": 0},
        ]), t["excluded"])
        return

    def flatten(v):
        if v is None:
            return ""
        if isinstance(v, float) and pd.isna(v):
            return ""
        if isinstance(v, list):
            return flatten(v[0] if v else "")
        if isinstance(v, dict):
            return flatten(v.get("value") or v.get("label") or v.get("name") or v.get("id") or "")
        return str(v).strip()

    # alles einmal säubern
    df = df.replace({None: "", np.nan: ""}).copy()
    for c in df.columns:
        df[c] = df[c].map(flatten)

    # Debug: sind Spalten überhaupt gefüllt?
    if col_orgtype in df.columns:
        filled_orgtype = int((df[col_orgtype].astype(str).str.strip() != "").sum())
        print(f"[reconcile] Organisationsart gefüllt: {filled_orgtype}/{len(df)}")
    else:
        print(f"[reconcile][WARN] Spalte fehlt: {col_orgtype}")

    if col_next in df.columns:
        filled_next = int((df[col_next].astype(str).str.strip() != "").sum())
        print(f"[reconcile] Datum nächste Aktivität gefüllt: {filled_next}/{len(df)}")
    else:
        print(f"[reconcile][WARN] Spalte fehlt: {col_next}")

    delete_rows: list[dict] = []

    def log_drop(row: pd.Series, reason: str, extra: str):
        kontakt_id = flatten(row.get(col_pid))
        name = (flatten(row.get("Person Vorname")) + " " + flatten(row.get("Person Nachname"))).strip()
        org_id = flatten(row.get(col_orgid))
        org_name = flatten(row.get(col_orgname))
        delete_rows.append({
            "reason": reason,
            "Kontakt ID": kontakt_id,
            "Name": name,
            "Organisation ID": org_id,
            "Organisationsname": org_name,
            "Grund": extra,
            "extra": extra,
            "id": kontakt_id,
            "name": name,
            "org_name": org_name,
        })

    # (2) Organisationsart gesetzt -> raus
    if col_orgtype in df.columns:
        mask = df[col_orgtype].astype(str).str.strip() != ""
        removed = df[mask]
        for _, r in removed.iterrows():
            log_drop(r, "org_art_not_empty", "Organisationsart ist gesetzt")
        df = df[~mask]

    # (5) Datum nächste Aktivität -> raus
    if col_next in df.columns:
        mask = df[col_next].astype(str).map(lambda x: is_forbidden_activity_date(x if x else None))
        removed = df[mask]
        for _, r in removed.iterrows():
            log_drop(r, "forbidden_activity_date", f"Datum nächste Aktivität gesperrt: {flatten(r.get(col_next))}")
        df = df[~mask]

    # (3) FUZZY Orga >=95% -> raus
    buckets_all = await _fetch_org_names_for_filter_capped(PAGE_LIMIT, MAX_ORG_NAMES, MAX_ORG_BUCKET)

    drop_idx = []
    fuzzy_checked = 0
    if job_obj:
        job_obj.detail = "Fuzzy-Abgleich Organisationen (>=95%) …"
    for idx, row in df.iterrows():
        name_clean = flatten(row.get(col_orgname))
        norm = normalize_name(name_clean)
        if not norm:
            continue

        fuzzy_checked += 1
        if job_obj and (fuzzy_checked % 200 == 0):
            job_obj.detail = f"Fuzzy-Abgleich … geprüft {fuzzy_checked}" 

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
        if score >= 95:
            log_drop(row, "org_match_95", f"Orga ähnlich {score}% zu '{best_name}'")
            drop_idx.append(idx)

    if drop_idx:
        df = df.drop(drop_idx)
    if job_obj:
        try:
            job_obj.stats["fuzzy_checked"] = int(fuzzy_checked)
            job_obj.stats["fuzzy_removed"] = int(len(drop_idx))
        except Exception:
            pass

    # (4) Person-ID bereits in Filtern 1216/1708 -> raus
  
    suspect_ids: set[str] = set()
    for fid in (1216, 1708):
        async for ids in stream_person_ids_by_filter_cursor(fid, page_limit=PAGE_LIMIT, job_obj=job_obj, label="Kontaktierte Personen"):
            suspect_ids.update(map(str, ids))

    if col_pid in df.columns and suspect_ids:
        mask = df[col_pid].astype(str).isin(suspect_ids)
        removed = df[mask]
        for _, r in removed.iterrows():
            log_drop(r, "person_id_match", "Person bereits kontaktiert (Filter 1216/1708)")
        df = df[~mask]

    # (1) Max 2 Kontakte pro Organisation -> raus
    limit = int(PER_ORG_DEFAULT_LIMIT or 2)
    if col_orgid in df.columns and col_pid in df.columns:
        df = df.copy()
        df["_orgid_sort"] = df[col_orgid].astype(str)
        df["_pid_sort"] = pd.to_numeric(df[col_pid], errors="coerce").fillna(10**18).astype(np.int64)
        df = df.sort_values(by=["_orgid_sort", "_pid_sort"], kind="mergesort")

        over = df.groupby("_orgid_sort").cumcount() >= limit
        removed = df[over]
        for _, r in removed.iterrows():
            log_drop(r, "per_org_limit", f"Max {limit} Kontakte pro Organisation")
        df = df[~over].drop(columns=["_orgid_sort", "_pid_sort"], errors="ignore")

    # READY + LOG speichern
    ready_df = df.drop(columns=[c for c in ["_orgid_sort", "_pid_sort"] if c in df.columns], errors="ignore")
    log_df = pd.DataFrame(delete_rows, columns=DELETE_LOG_COLUMNS)

    await save_df_text(ready_df, t["ready"])
    await save_df_text(log_df, t["log"])

    # SUMMARY IMMER schreiben (auch wenn log leer)
    def cnt(reason: str) -> int:
        if log_df.empty or "reason" not in log_df.columns:
            return 0
        return int((log_df["reason"] == reason).sum())

    summary_rows = [
        {"Grund": "Max 2 Kontakte pro Organisation", "Anzahl": cnt("per_org_limit")},
        {"Grund": "Organisationsart ist gesetzt", "Anzahl": cnt("org_art_not_empty")},
        {"Grund": "Datum nächste Aktivität steht an bzw. liegt in naher Vergangenheit", "Anzahl": cnt("forbidden_activity_date")},
        {"Grund": "Organisation ähnlich ≥95%", "Anzahl": cnt("org_match_95")},
        {"Grund": "Person bereits kontaktiert (Filter 1216/1708)", "Anzahl": cnt("person_id_match")},
    ]
    await save_df_text(pd.DataFrame(summary_rows), t["excluded"])

# =============================================================================
# NACHFASS - LOGIK
# =============================================================================
from typing import Optional, List, Set
from collections import Counter

async def run_nachfass_job(
    job_obj,
    job_id: str,
    campaign: str,
    filters: Optional[List[int]] = None,   # enthält normalerweise [FILTER_NACHFASS]
    nf_batch_ids: Optional[List[str]] = None,
):
    """
    Nachfass-Job (schnell):
      1) Personen über Filter 3024 laden (/persons?filter_id=...)
      2) lokal auf Batch-ID-Field filtern (kein /persons/{id} mehr!)
      3) nf_master_final bauen
      4) _reconcile("nf") ausführen
      5) Excel aus nf_master_ready erzeugen
    """

    try:
        # ------------------------------
        # A) Batch IDs aus UI normalisieren
        # ------------------------------
        user_batches_raw = [str(b).strip() for b in (nf_batch_ids or []) if str(b).strip()]
        if not user_batches_raw:
            job_obj.done = True
            job_obj.error = "Keine Batch-ID angegeben."
            job_obj.phase = "Fertig (leer)"
            job_obj.percent = 100
            return

        user_batches_raw = user_batches_raw[:2]

        user_batch_set: Set[str] = set()
        for b in user_batches_raw:
            b = b.strip()
            if not b:
                continue
            # nur Ziffern nehmen, falls jemand "B443" eingibt
            digits = "".join(ch for ch in b if ch.isdigit())
            if digits:
                user_batch_set.add(digits)
            else:
                user_batch_set.add(b)

        print("[NF] run_nachfass_job: Batch-IDs (normalisiert) =", sorted(user_batch_set))

        if not user_batch_set:
            job_obj.done = True
            job_obj.error = "Keine gültige Batch-ID angegeben."
            job_obj.phase = "Fertig (leer)"
            job_obj.percent = 100
            return

        # ------------------------------
        # B) Personen über Filter laden
        # ------------------------------
        filter_id = int((filters or [FILTER_NACHFASS])[0])

        job_obj.phase = f"Suche Personen (Filter {filter_id})"
        job_obj.percent = 8

        persons_all = await fetch_persons_by_filter_id_v2(
            filter_id=filter_id,
            limit=PAGE_LIMIT,
            job_obj=job_obj,
            max_pages=None,
            max_empty_growth=2,
        )

        total_filter = len(persons_all)
        job_obj.stats["total"] = int(total_filter)
        print(f"[NF] Personen im Nachfass-Filter {filter_id}: {total_filter}")

        if total_filter == 0:
            job_obj.done = True
            job_obj.error = "Keine Personen im Nachfass-Filter gefunden."
            job_obj.phase = "Fertig (leer)"
            job_obj.percent = 100
            return

        # ------------------------------
        # C) Lokal nach Batch-ID-Feld filtern + LOGGING
        # ------------------------------
        job_obj.phase = "Prüfe Batch-ID (Details-Check)"
        job_obj.percent = 18

        FIELD_BATCH_ID = PD_PERSON_FIELDS["Batch ID"]   # zeigt auf dein neues Integer-Feld

        def get_batch_value(p: dict) -> str:
            v = cf_value(p, FIELD_BATCH_ID)
            if v is None:
                return ""
            s = str(v).strip()
            digits = "".join(ch for ch in s if ch.isdigit())
            return digits or s

        selected: List[dict] = []
        has_batch_count = 0
        batch_counter: Counter[str] = Counter()

        for p in persons_all:
            bval = get_batch_value(p)
            if bval:
                has_batch_count += 1
                batch_counter[bval] += 1

                if bval in user_batch_set:
                    selected.append(p)

        print(f"[NF] Personen mit irgendeiner Batch-ID im Filter {filter_id}: {has_batch_count}")
        print(f"[NF] Treffer nach Batch-ID-Filter ({sorted(user_batch_set)}): {len(selected)}")
        job_obj.stats["with_any_batch"] = int(has_batch_count)
        job_obj.stats["selected"] = int(len(selected))

        # Top 10 Batch-Werte im Filter für Debug
        if batch_counter:
            top10 = batch_counter.most_common(10)
            print("[NF] Häufigste Batch-ID-Werte im Filter (Top 10):", top10)

        if not selected:
            job_obj.done = True
            job_obj.error = "Keine Personen zur Batch-ID gefunden."
            job_obj.phase = "Fertig (leer)"
            job_obj.percent = 100
            return

        # ------------------------------
        # D) Master bauen (ohne zusätzliche Detail-Calls)
        # ------------------------------
        job_obj.phase = "Baue Master (nf_master_final)"
        job_obj.percent = 55

        export_batch_id = sanitize(getattr(job_obj, "batch_id", "") or "")
        if not export_batch_id:
            # Fallback (sollte selten passieren): nutze Eingabe-Batch-IDs
            export_batch_id = ",".join(sorted(user_batch_set))
        batch_label_for_export = export_batch_id

        master_final = await _build_nf_master_final(
            selected,
            campaign=campaign,
            batch_id_label=batch_label_for_export,
            batch_id=None,
            job_obj=job_obj,
        )

        # ------------------------------
        # E) Master in DB speichern
        # ------------------------------
        job_obj.phase = "Speichere Master (DB)"
        job_obj.percent = 70

        t = tables("nf")
        await save_df_text(master_final, t["final"])

        # ------------------------------
        # F) Abgleich (Orga, Dubletten, etc.)
        # ------------------------------
        job_obj.phase = "Abgleich (nf -> ready/log)"
        job_obj.percent = 80
        await reconcile_with_progress(job_obj, "nf", start_percent=72, end_percent=85)
        # ------------------------------
        # G) Excel aus Ready-Tabelle
        # ------------------------------
        job_obj.phase = "Erzeuge Excel"
        job_obj.percent = 90

        ready_df = await load_df_text(t["ready"])
        export_df = build_nf_export(ready_df)

        safe_campaign = slugify_filename(campaign or "Nachfass")

        out_path = export_to_excel(
            export_df,
            prefix=safe_campaign,       # <-- Kampagnenname als Dateiname
            job_id=job_id
        )
        
        job_obj.path = out_path
        job_obj.filename_base = safe_campaign

        job_obj.error = None
        job_obj.phase = "Fertig"
        job_obj.percent = 100
        # --- API COUNTERS sichern ---
        try:
            job_obj.api_calls = dict(REQUEST_COUNTER)
        except:
            job_obj.api_calls = {}
    
        # Counter zurücksetzen für den nächsten Job
        for k in REQUEST_COUNTER:
            REQUEST_COUNTER[k] = 0
        job_obj.done = True
        

    except Exception as e:
        job_obj.done = True
        job_obj.error = f"{type(e).__name__}: {e}"
        job_obj.phase = "Fehler"
        job_obj.percent = 100
        print(f"[NF][ERROR] Job failed: {job_obj.error}")
        return

# =============================================================================
# Excel-Export-Helfer – FINAL MODUL 3
# =============================================================================
import pandas as pd

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
    Erzwingt die finale Export-Struktur (Spalten + Reihenfolge).
    Fehlende Spalten werden angelegt.
    """
    out = pd.DataFrame()

    for col in NF_EXPORT_COLUMNS:
        out[col] = df[col] if col in df.columns else ""

    # IDs als string, nan raus
    for c in ("Person ID", "Organisation ID"):
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


def export_to_excel(df: pd.DataFrame, prefix: str, job_id: str) -> str:
    """
    Schreibt den Nachfass-Excel Export nach ./exports und gibt den Dateipfad zurück.
    Nutzt eure bestehende Bytes-Funktion _df_to_excel_bytes_nf(df).
    """
    os.makedirs("exports", exist_ok=True)

    safe_prefix = "".join(c for c in (prefix or "export") if c.isalnum() or c in ("_", "-")).strip() or "export"
    safe_job = "".join(c for c in (job_id or "") if c.isalnum() or c in ("_", "-")).strip() or uuid.uuid4().hex[:8]
    out_path = os.path.join("exports", f"{safe_prefix}_{safe_job}.xlsx")

    data = _df_to_excel_bytes_nf(df)
    with open(out_path, "wb") as f:
        f.write(data)

    print(f"[export_to_excel] wrote: {out_path} bytes={len(data)} rows={len(df)} cols={len(df.columns)}")
    return out_path

# =============================================================================
# JOB-VERWALTUNG & FORTSCHRITT
# =============================================================================
class Job:
    def __init__(self) -> None:
        now_ms = int(time.time() * 1000)

        # intern (mit "touch" auf Setter)
        self._phase = "Warten …"
        self._percent = 0
        self._detail = ""

        # Heartbeat / UI
        self.last_update_ms: int = now_ms
        self.heartbeat: int = 0

        self.done = False
        self.error: Optional[str] = None
        self.path: Optional[str] = None

        # optionale Metadaten / Zähler für UI
        self.stats: Dict[str, int] = {}
        self.total_rows: int = 0

        self.filename_base: str = "BatchFlow_Export"
        self.excel_bytes: Optional[bytes] = None

    def _touch(self):
        self.last_update_ms = int(time.time() * 1000)
        self.heartbeat += 1

    @property
    def phase(self) -> str:
        return self._phase

    @phase.setter
    def phase(self, v: str):
        self._phase = str(v) if v is not None else ""
        self._touch()

    @property
    def percent(self) -> int:
        return int(self._percent or 0)

    @percent.setter
    def percent(self, v):
        try:
            self._percent = int(v or 0)
        except Exception:
            self._percent = 0
        self._touch()

    @property
    def detail(self) -> str:
        return self._detail

    @detail.setter
    def detail(self, v: str):
        self._detail = str(v) if v is not None else ""
        self._touch()


JOBS: Dict[str, Job] = {}

# =============================================================================
# RECONCILE MIT FORTSCHRITT
# =============================================================================
async def reconcile_with_progress(job: "Job", prefix: str, start_percent: int = 55, end_percent: int = 75):
    """Führt _reconcile(prefix) aus und aktualisiert dabei Fortschritt/Infos.

    start_percent/end_percent steuern nur den Fortschrittsbereich innerhalb des Gesamtjobs.
    """
    try:
        sp = int(start_percent)
        ep = int(end_percent)
        if ep < sp:
            sp, ep = ep, sp

        # kleine Unterphasen, die in den Bereich [sp, ep] gemappt werden
        def p(x: float) -> int:
            return max(0, min(100, int(sp + (ep - sp) * x)))

        job.phase = "Vorbereitung läuft …"
        job.percent = max(job.percent, p(0.10))
        await asyncio.sleep(0)  # Yield

        job.phase = "Lade Vergleichsdaten …"
        job.percent = max(job.percent, p(0.35))
        await asyncio.sleep(0)  # Yield

        job.detail = f"Regeln anwenden ({prefix}) …"
        await _reconcile(prefix, job_obj=job)

        # --- UI-Infos (Entfernte Datensätze / Export-Menge) ---
        # WICHTIG: "excluded" = Anzahl ENTFERNTER DATENSÄTZE (nicht Summary-Zeilen)
        try:
            dlog = await load_df_text(f"{prefix}_delete_log")
            removed_n = int(len(dlog))
        except Exception:
            removed_n = int(job.stats.get("delete_log", 0) or 0)

        job.stats["excluded"] = removed_n
        job.stats["delete_log"] = removed_n

        try:
            t = tables(prefix)
            ready = await load_df_text(t["ready"])
            job.stats["ready"] = int(len(ready))
        except Exception:
            job.stats["ready"] = int(job.stats.get("ready", 0) or 0)

        job.phase = "Abgleich abgeschlossen"
        job.percent = max(job.percent, p(1.00))

    except Exception as e:
        job.error = f"Fehler beim Abgleich: {e}"
        job.phase = "Fehler"
        job.percent = 100
        job.done = True
# =============================================================================
# /refresh/options – Variante B (zeigt auch Fachbereiche mit count = 0)
# =============================================================================
@app.get("/refresh/options")
async def refresh_options():

    field_key = PD_PERSON_FIELDS.get("Fachbereich - Kampagne")
    if not field_key:
        return JSONResponse({"options": []})

    # -------------------------------------------------------------------------
    # 1) Personen laden (Turbo: große Pages + fewer max_pages)
    # -------------------------------------------------------------------------
    persons = await fetch_persons_by_filter_id_v2(
        filter_id=FILTER_REFRESH,
        limit=500,
        job_obj=None,
        max_pages=60,
        max_empty_growth=2,
    )

    # -------------------------------------------------------------------------
    # 2) Alle möglichen Fachbereichs-Labels laden
    # -------------------------------------------------------------------------
    label_map = await get_fachbereich_label_map()

    # Liste aller verfügbaren Dropdown-Werte (z. B. "marketing", "crm", ...)
    all_possible_values = list(label_map.keys())

    # Ergebnisse
    per_fach_counts: Dict[str, int] = {k: 0 for k in all_possible_values}
    fach_org_counts: Dict[str, Dict[str, int]] = {k: {} for k in all_possible_values}

    # -------------------------------------------------------------------------
    # 3) Personen bereinigen wie im Export:
    #    - Fachbereich muss gesetzt sein
    #    - Next activity darf nicht blockiert sein
    #    - Max 2 Kontakte pro Organisation
    # -------------------------------------------------------------------------
    for p in persons:
        if not isinstance(p, dict):
            continue

        fach_val = sanitize(cf_value_v2(p, field_key))
        if not fach_val:
            continue
        if fach_val not in all_possible_values:
            continue

        org_id = extract_org_id_from_person(p)
        if not org_id:
            continue

        next_date = sanitize(p.get("next_activity_date"))
        if next_date and is_forbidden_activity_date(next_date):
            continue

        # Organisationslimit: max 2 Kontakte
        orgs = fach_org_counts[fach_val]
        if orgs.get(org_id, 0) >= PER_ORG_DEFAULT_LIMIT:
            continue

        orgs[org_id] = orgs.get(org_id, 0) + 1
        per_fach_counts[fach_val] += 1

    # -------------------------------------------------------------------------
    # 4) Ausgabe sortieren + auch 0-Werte anzeigen
    # -------------------------------------------------------------------------
    options = []
    for fach_val in all_possible_values:
        label = label_map.get(fach_val) or fach_val
        count = per_fach_counts.get(fach_val, 0)
        options.append({
            "value": fach_val,
            "label": label,
            "count": count,
        })

    options.sort(key=lambda o: o["label"].lower())
    return JSONResponse({"options": options})

# =============================================================================
# NEUKONTAKTE - OPTIONS
# =============================================================================
# =============================================================================
# /neukontakte/options – identisch zu /refresh/options
# =============================================================================
@app.get("/neukontakte/options")
async def neukontakte_options():

    field_key = PD_PERSON_FIELDS.get("Fachbereich - Kampagne")
    if not field_key:
        return JSONResponse({"options": []})

    # -------------------------------------------------------------------------
    # 1) Personen laden (identisch zu Refresh, anderer Filter)
    # -------------------------------------------------------------------------
    persons = await fetch_persons_by_filter_id_v2(
        filter_id=FILTER_NEUKONTAKTE,
        limit=500,
        job_obj=None,
        max_pages=60,
        max_empty_growth=2,
    )

    # -------------------------------------------------------------------------
    # 2) Alle möglichen Fachbereichs-Labels laden
    # -------------------------------------------------------------------------
    label_map = await get_fachbereich_label_map()
    all_possible_values = list(label_map.keys())

    per_fach_counts: Dict[str, int] = {k: 0 for k in all_possible_values}
    fach_org_counts: Dict[str, Dict[str, int]] = {k: {} for k in all_possible_values}

    # -------------------------------------------------------------------------
    # 3) Personen bereinigen (EXAKT wie bei Refresh!)
    # -------------------------------------------------------------------------
    for p in persons:
        if not isinstance(p, dict):
            continue

        fach_val = sanitize(cf_value_v2(p, field_key))
        if not fach_val:
            continue
        if fach_val not in all_possible_values:
            continue

        org_id = extract_org_id_from_person(p)
        if not org_id:
            continue

        next_date = sanitize(p.get("next_activity_date"))
        if next_date and is_forbidden_activity_date(next_date):
            continue

        # Organisationslimit: max 2 Kontakte
        orgs = fach_org_counts[fach_val]
        if orgs.get(org_id, 0) >= PER_ORG_DEFAULT_LIMIT:
            continue

        orgs[org_id] = orgs.get(org_id, 0) + 1
        per_fach_counts[fach_val] += 1

    # -------------------------------------------------------------------------
    # 4) Ausgabe – sortiert, inkl. 0-Werte
    # -------------------------------------------------------------------------
    options = []
    for fach_val in all_possible_values:
        label = label_map.get(fach_val) or fach_val
        count = per_fach_counts.get(fach_val, 0)
        options.append({
            "value": fach_val,
            "label": label,
            "count": count,
        })

    options.sort(key=lambda o: o["label"].lower())
    return JSONResponse({"options": options})


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
    # Reset entfernte Datensätze / Löschlog für neuen Lauf
    t = tables("nk")
    await save_df_text(pd.DataFrame(columns=DELETE_LOG_COLUMNS), t["log"])
    await save_df_text(pd.DataFrame(columns=["Grund","Anzahl"]), t["excluded"])
    job.phase = "Initialisiere …"
    job.percent = 1
    job.filename_base = slugify_filename(campaign or "BatchFlow_Export")

    async def update_progress(phase: str, percent: int):
        job.phase = phase
        job.percent = min(100, max(0, percent))
        await asyncio.sleep(0)

    async def _run():
        try:
            # 1️⃣ Personen aus Filter laden
            job.phase = f"Lade Neukontakte-Kandidaten (Filter {FILTER_NEUKONTAKTE})"
            job.percent = 5
            job.detail = "Starte Abruf …"

            persons_all = await fetch_persons_by_filter_id_v2(
                filter_id=FILTER_NEUKONTAKTE,
                limit=PAGE_LIMIT,
                job_obj=job,
                max_pages=120,
                max_empty_growth=2,
            )

            field_key = PD_PERSON_FIELDS.get("Fachbereich - Kampagne")
            target_fach = sanitize(fachbereich)

            job.phase = "Filtere nach Fachbereich / Regeln …"
            job.percent = 35
            job.detail = f"Fachbereich: {target_fach or '–'}"

            selected: List[dict] = []
            org_counts: Dict[str, int] = {}

            for p in persons_all:
                if not isinstance(p, dict) or not field_key:
                    continue

                fach_val = sanitize(cf_value_v2(p, field_key))
                if not fach_val or fach_val != target_fach:
                    continue

                org_id = extract_org_id_from_person(p)
                if not org_id:
                    continue

                # Datum nächste Aktivität blockieren
                next_date = sanitize(p.get("next_activity_date"))
                if next_date and is_forbidden_activity_date(next_date):
                    continue

                # Max Kontakte pro Organisation
                cur = org_counts.get(org_id, 0)
                if cur >= int(per_org_limit or PER_ORG_DEFAULT_LIMIT or 2):
                    continue
                org_counts[org_id] = cur + 1

                selected.append(p)

                if take_count is not None and take_count > 0 and len(selected) >= take_count:
                    break

            job.stats["selected"] = int(len(selected))

            # 2️⃣ Master erstellen
            job.phase = f"Baue Master (Auswahl={len(selected)})"
            job.percent = 55
            job.detail = "Erzeuge nk_master_final …"

            batch = sanitize(batch_id or "")
            camp = sanitize(campaign or "")

            master_final = await _build_refresh_master_final(
                selected,
                campaign=camp,
                batch_id=batch,
                job_obj=job,
            )

            # 3️⃣ Master speichern
            job.phase = "Speichere Master (DB)"
            job.percent = 65
            t = tables("nk")
            await save_df_text(master_final, t["final"])

            # 4️⃣ Abgleich wie Refresh/Nachfass
            job.phase = "Abgleich (nk -> ready/log)"
            job.percent = 75
            job.detail = "Regeln anwenden …"
            await reconcile_with_progress(job, "nk", start_percent=60, end_percent=78)

            # 5️⃣ Excel-Export (ohne Hyperlinks)
            job.phase = "Erzeuge Excel-Datei …"
            job.percent = 90
            job.detail = "Bereite Export-Datei vor …"

            ready_df = await load_df_text(t["ready"])
            export_df = build_nf_export(ready_df)

            out_path = export_to_excel(export_df, prefix="neukontakte_export", job_id=job_id)
            job.path = out_path
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
# Neukontakte - OPTIONS
# =============================================================================

# =============================================================================
# EXPORT-FORTSCHRITT & DOWNLOAD-ENDPUNKTE REFRESH (KORRIGIERT)
# =============================================================================
@app.post("/refresh/export_start")
async def refresh_export_start(
    fachbereich: str = Body(...),
    take_count: Optional[int] = Body(None),
    batch_id: Optional[str] = Body(None),
    campaign: Optional[str] = Body(None),
    per_org_limit: int = Body(PER_ORG_DEFAULT_LIMIT),
):
    """
    Startet den Refresh-Export:
      - liest Personen aus FILTER_REFRESH
      - filtert nach Fachbereich
      - begrenzt pro Organisation und optional auf eine Gesamtanzahl
      - schreibt rf_master_final → rf_master_ready / rf_delete_log
      - erzeugt eine Excel-Datei wie beim Nachfass (ohne Hyperlinks!)
    """
    job_id = str(uuid.uuid4())
    job = Job()
    JOBS[job_id] = job
    # Reset entfernte Datensätze / Löschlog für neuen Lauf
    t = tables("rf")
    await save_df_text(pd.DataFrame(columns=DELETE_LOG_COLUMNS), t["log"])
    await save_df_text(pd.DataFrame(columns=["Grund","Anzahl"]), t["excluded"])
    job.phase = "Initialisiere Neukontakte …"
    job.percent = 1
    job.filename_base = (campaign or "Neukontakte_Export")

    async def _run():
        try:
            # 1️⃣ Personen aus Filter laden
            job.phase = f"Lade Refresh-Kandidaten (Filter {FILTER_REFRESH})"
            job.percent = 5

            persons_all = await fetch_persons_by_filter_id_v2(
                filter_id=FILTER_REFRESH,
                limit=PAGE_LIMIT,
                job_obj=job,
                max_pages=120,
                max_empty_growth=2,
            )

            field_key = PD_PERSON_FIELDS.get("Fachbereich - Kampagne")
            target_fach = sanitize(fachbereich)

            selected: List[dict] = []
            org_counts: Dict[str, int] = {}

            job.phase = "Filtere nach Fachbereich / Regeln …"
            job.percent = 30

            total = len(persons_all) or 1
            job.stats["total"] = int(len(persons_all))
            for idx, p in enumerate(persons_all, start=1):
                if idx == 1 or idx % 200 == 0 or idx == total:
                    job.percent = max(job.percent, min(60, 30 + int(idx / total * 20)))

                if not isinstance(p, dict) or not field_key:
                    continue

                fach_val = sanitize(cf_value_v2(p, field_key))
                if not fach_val or fach_val != target_fach:
                    continue

                org_id = extract_org_id_from_person(p)
                if not org_id:
                    continue

                # Datum nächste Aktivität blockieren wie Nachfass
                next_date = sanitize(p.get("next_activity_date"))
                if next_date and is_forbidden_activity_date(next_date):
                    continue

                # Max 2 Kontakte pro Organisation
                cur = org_counts.get(org_id, 0)
                if cur >= int(per_org_limit or PER_ORG_DEFAULT_LIMIT or 2):
                    continue
                org_counts[org_id] = cur + 1

                selected.append(p)

                if take_count is not None and take_count > 0 and len(selected) >= take_count:
                    break

            job.stats["selected"] = int(len(selected))

            # 2️⃣ Master erstellen
            job.phase = f"Baue Master (Auswahl={len(selected)})"
            job.percent = 60

            batch = sanitize(batch_id or "")
            camp = sanitize(campaign or "")

            master_final = await _build_refresh_master_final(
                selected,
                campaign=camp,
                batch_id=batch,
                job_obj=job,
            )

            # 3️⃣ Master speichern
            job.phase = "Speichere Master (DB)"
            job.percent = 70
            t = tables("rf")
            await save_df_text(master_final, t["final"])

            # 4️⃣ Abgleich wie Nachfass
            job.phase = "Abgleich (rf -> ready/log)"
            job.percent = 80
            await reconcile_with_progress(job, "rf", start_percent=60, end_percent=78)
            # 5️⃣ Excel-Export OHNE Hyperlinks — exakt wie Nachfass
            job.phase = "Erzeuge Excel-Datei …"
            job.percent = 90

            ready_df = await load_df_text(t["ready"])
            export_df = build_nf_export(ready_df)

            # 🔥 URLs als reinen Text markieren → Excel erzeugt KEINE Hyperlinks
            for col in export_df.columns:
                if "http" in col.lower() or "linkedin" in col.lower() or "xing" in col.lower():
                    export_df[col] = export_df[col].astype(str).apply(
                        lambda v: "'" + v if v.startswith("http") else v
                    )

            # 🔥 Export mit NACHFASS-Logik (keine Hyperlinks, stabil, sauber)
            out_path = export_to_excel(export_df, prefix="refresh_export", job_id=job_id)
            job.path = out_path
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
# EXPORT-FORTSCHRITT – NEUKONTAKTE (FINAL & KORREKT)
# =============================================================================
@app.get("/neukontakte/export_progress")
async def neukontakte_export_progress(job_id: str = Query(...)):

    job = JOBS.get(job_id)
    if not job:
        return JSONResponse(
            {"error": "Job nicht gefunden"},
            status_code=404
        )

    return JSONResponse({
        "phase": str(job.phase),
        "percent": int(job.percent),
        "done": bool(job.done),
        "error": str(job.error) if job.error else None,
        "stats": dict(getattr(job, "stats", {}) or {}),
        "detail": str(getattr(job, "detail", "") or ""),
        "last_update_ms": int(getattr(job, "last_update_ms", 0) or 0),
        "heartbeat": int(getattr(job, "heartbeat", 0) or 0),

        # optional – falls später genutzt
        "note_org_limit": getattr(job, "note_org_limit", 0),
        "note_date_invalid": getattr(job, "note_date_invalid", 0),
    })


# =============================================================================
# EXPORT-DOWNLOAD – NEUKONTAKTE (FEHLTE → JETZT FINAL)
# =============================================================================
@app.get("/neukontakte/export_download")
async def neukontakte_export_download(job_id: str = Query(...)):

    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job nicht gefunden")

    if not job.done:
        raise HTTPException(status_code=400, detail="Export noch nicht abgeschlossen")

    if job.error:
        raise HTTPException(status_code=400, detail=job.error)

    if not job.path or not os.path.exists(job.path):
        raise HTTPException(status_code=404, detail="Exportdatei nicht gefunden")

    return FileResponse(
        job.path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"{slugify_filename(job.filename_base or 'Neukontakte_Export')}.xlsx"
    )

# =============================================================================
# Frontend - Startseite
# =============================================================================
@app.get("/campaign", response_class=HTMLResponse)
async def campaign_home():
    return HTMLResponse("""<!doctype html>
<html lang="de" xmlns:mso="urn:schemas-microsoft-com:office:office" xmlns:msdt="uuid:C2F41010-65B3-11d1-A29F-00AA00C14882">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>BatchFlow – Kampagne wählen</title>

<style>
/* =========================
   DESIGN ONLY – SAFE
   ========================= */
body{
  margin:0;
  background:#f7f9fc;
  color:#0f172a;
  font-family:Inter,system-ui,-apple-system,BlinkMacSystemFont,sans-serif;
}

/* Header */
header{
  background:#ffffff;
  border-bottom:1px solid #e5e9f0;
}
.hwrap{
  max-width:1200px;
  margin:0 auto;
  padding:22px 24px;
  display:flex;
  align-items:center;
  justify-content:space-between;
}
.hleft{
  display:flex;
  align-items:center;
  gap:16px;
}
.hleft img{
  height:48px; /* bewusst größer */
}
.hleft span{
  font-size:18px;
  font-weight:600;
}

/* Content */
.container{
  max-width:1100px;
  margin:72px auto;
  padding:0 24px;
  display:grid;
  grid-template-columns:repeat(auto-fit,minmax(280px,1fr));
  gap:32px;
}

/* Cards */
.card{
  background:#ffffff;
  border:1px solid #e5e9f0;
  border-radius:20px;
  padding:36px;
  box-shadow:
    0 14px 32px rgba(15,23,42,.06),
    0 6px 12px rgba(15,23,42,.04);
  display:flex;
  flex-direction:column;
  justify-content:space-between;
  transition:transform .15s ease, box-shadow .15s ease;
}
.card:hover{
  transform:translateY(-2px);
  box-shadow:
    0 20px 40px rgba(15,23,42,.08),
    0 10px 18px rgba(15,23,42,.05);
}
.card h2{
  margin-bottom:24px;
  font-size:22px;
}
.card p{
  margin:0 0 26px;
  color:#475569;
  font-size:15px;
}

/* Button */
.btn{
  margin-top:36px;
  background:#0ea5e9;
  border:none;
  color:#ffffff;
  border-radius:999px;
  padding:12px 24px;
  font-weight:600;
  font-size:14px;
  cursor:pointer;
  box-shadow:0 6px 14px rgba(14,165,233,.35);
}
.btn:hover{
  background:#0284c7;
}

</style>

<!--[if gte mso 9]><xml>
<mso:CustomDocumentProperties>
<mso:_dlc_DocId msdt:dt="string">WETVQW7WMXWY-1237663653-109692</mso:_dlc_DocId>
<mso:_dlc_DocIdItemGuid msdt:dt="string">b6722014-ecb9-4557-9bd3-8e7b4fb60a7c</mso:_dlc_DocIdItemGuid>
<mso:_dlc_DocIdUrl msdt:dt="string">https://bizforward.sharepoint.com/sites/bizforwardintern/_layouts/15/DocIdRedir.aspx?ID=WETVQW7WMXWY-1237663653-109692, WETVQW7WMXWY-1237663653-109692</mso:_dlc_DocIdUrl>
</mso:CustomDocumentProperties>
</xml><![endif]-->
</head>

<body>

<header>
  <div class="hwrap">
    <div class="hleft">
      <img src="/static/bizforward-Logo-Clean-2024.svg" alt="bizforward">
      <span>BatchFlow</span>
    </div>
  </div>
</header>

<div class="container">

  <div class="card">
    <div>
      <h2>Neukontakte</h2>
      <p>Noch nicht angeschriebene Kontakte auswählen</p>
    </div>
    <a class="btn" href="/neukontakte">Öffnen</a>
  </div>

  <div class="card">
    <div>
      <h2>Nachfass</h2>
      <p>Nachfassen anhand einer Batch ID (Filter 3024)</p>
    </div>
    <a class="btn" href="/nachfass">Öffnen</a>
  </div>

  <div class="card">
    <div>
      <h2>Refresh</h2>
      <p>Kontakte anhand eines Fachbereiches auswählen (Filter 4444)</p>
    </div>
    <a class="btn" href="/refresh">Öffnen</a>
  </div>

</div>

</body>
</html>
""")

# =============================================================================
# Frontend - Neukontakte
# =============================================================================
@app.get("/neukontakte", response_class=HTMLResponse)
async def neukontakte_page(request: Request):
    authed = bool(user_tokens.get("default") or PD_API_TOKEN)
    authed_html = "<span class='muted'>angemeldet</span>" if authed else "<a href='/login'>Anmelden</a>"

    return HTMLResponse(
r"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Neukontakte – BatchFlow</title>

<style>
/* =========================
   BASE
   ========================= */
body{
  margin:0;
  background:#f7f9fc;
  color:#0f172a;
  font:16px/1.6 Inter,system-ui,-apple-system,BlinkMacSystemFont,sans-serif;
}

/* =========================
   HEADER
   ========================= */
header{
  background:#fff;
  border-bottom:1px solid #e5e9f0;
}
.hwrap{
  max-width:1200px;
  margin:0 auto;
  padding:22px 24px;
  display:flex;
  align-items:center;
  justify-content:space-between;
}
.hleft{
  display:flex;
  align-items:center;
  gap:14px;
}
.hleft img{ height:48px; }
.hcenter{ font-size:18px;font-weight:600; }
.hright{ font-size:14px; }

/* =========================
   LAYOUT
   ========================= */
main{
  max-width:760px;
  margin:56px auto;
  padding:0 24px;
}

/* =========================
   CARD
   ========================= */
.card{
  background:#fff;
  border:1px solid #e5e9f0;
  border-radius:24px;
  padding:40px;
  box-shadow:
    0 20px 40px rgba(15,23,42,.06),
    0 8px 16px rgba(15,23,42,.04);
  margin-bottom:48px;
}
.card h2{ margin:0 0 6px 0; }
.card p.lead{
  margin:0 0 32px 0;
  color:#475569;
  font-size:15px;
}

/* =========================
   GRID (ruhig & harmonisch)
   ========================= */
.grid{
  display:grid;
  grid-template-columns:1fr 1fr;
  gap:32px;
}
.col-12{ grid-column: span 2; }
/* =========================
   META ROW (Anzahl + Batch)
   ========================= */
.meta-row{
  grid-column: span 2;
  display:flex;
  gap:24px;
  align-items:flex-start;
}

.meta-field{
  flex:1;
}

.meta-field input{
  max-width:240px;
}

.meta-field small{
  font-size:12px;
  color:#94a3b8;
}

/* =========================
   FORM
   ========================= */
label{
  display:block;
  font-weight:600;
  margin-bottom:8px;
  font-size:14px;
}
small{
  display:block;
  margin-top:6px;
  font-size:13px;
  color:#64748b;
}

input,select{
  width:100%;
  padding:14px 16px;
  border-radius:14px;
  border:1px solid #dbe3ec;
  background:#f8fafc;
  font-size:15px;
}
input:focus,select:focus{
  outline:none;
  background:#fff;
  border-color:#0ea5e9;
  box-shadow:0 0 0 3px rgba(14,165,233,.15);
}

/* Fachbereich hervorheben */
.fachbereich{
  padding:24px;
  border-radius:18px;
  background:#f8fafc;
  border:1px dashed #cbd5e1;
}

/* =========================
   CTA
   ========================= */
.cta-row{
  margin-top:48px;
  padding-top:28px;
  border-top:1px solid #e5e9f0;
  display:flex;
  justify-content:flex-end;
}
.btn{
  background:#0ea5e9;
  border:none;
  color:#fff;
  border-radius:999px;
  padding:14px 28px;
  font-weight:600;
  font-size:15px;
  cursor:pointer;
  box-shadow:0 10px 20px rgba(14,165,233,.35);
}
.btn:hover{ background:#0284c7; }
.btn:disabled{ opacity:.5; cursor:not-allowed; }

/* =========================
   LOADER FACHBEREICH
   ========================= */
#fb-loading-box{ display:none; margin-bottom:14px; }
#fb-loading-text{ font-size:13px;color:#0a66c2;margin-bottom:6px; }
#fb-loading-bar-wrap{
  width:100%;height:8px;background:#e5e9f0;border-radius:999px;overflow:hidden;
}
#fb-loading-bar{
  height:8px;width:0%;
  background:linear-gradient(90deg,#0ea5e9,#38bdf8);
}

/* =========================
   TABLE
   ========================= */
table{ width:100%; border-collapse:collapse; }
thead{ background:#f8fafc; }
th{
  font-size:12px;
  font-weight:600;
  color:#64748b;
  padding:14px 16px;
  text-align:left;
}
td{
  padding:14px 16px;
  border-top:1px solid #e5e9f0;
  font-size:14px;
}
tbody tr:hover{ background:#f1f5f9; }

/* =========================
   OVERLAY
   ========================= */
#overlay{
  display:none;
  position:fixed;
  inset:0;
  background:rgba(247,249,252,.8);
  backdrop-filter:blur(3px);
  z-index:9999;
  align-items:center;
  justify-content:center;
  flex-direction:column;
  gap:12px;
}
.barwrap{
  width:min(520px,90vw);
  height:10px;
  border-radius:999px;
  background:#e5e9f0;
  overflow:hidden;
}
.bar{
  height:100%;
  width:0%;
  background:linear-gradient(90deg,#0ea5e9,#38bdf8);
}

/* ---------- Live/Heartbeat ---------- */
.pulse-dot{width:8px;height:8px;border-radius:50%;background:var(--primary);display:inline-block;animation:pulse 1.2s infinite;}
@keyframes pulse{0%{transform:scale(.8);opacity:.5}50%{transform:scale(1);opacity:1}100%{transform:scale(.8);opacity:.5}}
.bar-indet{position:relative;overflow:hidden;}
.bar-indet::before{content:"";position:absolute;inset:0;background:linear-gradient(90deg,rgba(255,255,255,0) 0%,rgba(255,255,255,.55) 50%,rgba(255,255,255,0) 100%);transform:translateX(-100%);animation:indet 1.1s infinite;}
@keyframes indet{0%{transform:translateX(-100%)}100%{transform:translateX(100%)}}



/* --- Overlay (neu) --- */
#overlay{display:none;position:fixed;inset:0;background:rgba(15,23,42,.35);align-items:center;justify-content:center;z-index:100}
#overlay .box{background:var(--card);border:1px solid var(--line);border-radius:18px;padding:18px 20px;width:min(520px,92vw);box-shadow:var(--shadow2)}
#overlay-phase{font-weight:800}
#overlay-bar-wrap{width:100%;height:10px;border-radius:999px;background:var(--line);overflow:hidden;margin-top:10px}
#overlay-bar{height:100%;width:0%;background:linear-gradient(90deg,var(--brand),var(--brand2));transition:width .25s linear}

/* --- Statusbar (neu) --- */
.statusbar{position:fixed;left:0;right:0;bottom:0;background:rgba(247,249,252,.9);backdrop-filter:blur(8px);border-top:1px solid var(--line);padding:10px 14px;z-index:50}
.status-inner{max-width:1100px;margin:0 auto}
.status-top{display:flex;align-items:center;justify-content:space-between;gap:14px}
#status-phase{font-weight:800}
#status-percent{color:var(--muted);font-weight:800}

</style>
</head>

<body>

<header>
  <div class="hwrap">
    <div class="hleft">
      <img src="/static/bizforward-Logo-Clean-2024.svg" alt="bizforward">
      <a href="/campaign" style="color:#0a66c2;text-decoration:none;font-size:14px">
        Kampagne wählen
      </a>
    </div>
    <div class="hcenter">Neukontakte</div>
    <div class="hright">""" + authed_html + r"""</div>
  </div>
</header>

<main>

<!-- =========================
     FORM
     ========================= -->
<section class="card">
  <h2>Schritt 1 – Neukontakte auswählen</h2>
  <p class="lead">
    Wähle einen Fachbereich und definiere, wie viele neue Kontakte für den Erstkontakt exportiert werden sollen.
  </p>

  <div class="grid">

    <div class="col-12 fachbereich">
      <label>Fachbereich</label>

      <div id="fb-loading-box">
        <div id="fb-loading-text">Fachbereiche werden geladen … bitte warten.</div>
        <div id="fb-loading-bar-wrap"><div id="fb-loading-bar"></div></div>
      </div>

      <select id="fachbereich">
        <option value="">– bitte auswählen –</option>
      </select>

      <small>Quelle aus Pipedrive – nur noch nicht kontaktierte Personen.</small>
    </div>
    <div class="meta-row">
      <div class="meta-field">
        <label>Anzahl Kontakte</label>
        <input id="take_count" type="number" min="1" placeholder="alle">
        <small>Optional</small>
      </div>
    
      <div class="meta-field">
        <label>Batch ID</label>
        <input id="batch_id" placeholder="xxx">
        <small>Intern</small>
      </div>
    </div>



    <div class="col-12">
      <label>Kampagnenname (für Cold Mailing)</label>
      <input id="campaign" placeholder="z. B. Frühling 2025">
    </div>

  </div>

  <div class="cta-row">
    <button class="btn" id="btnExport" disabled>
      Abgleich & Download
    </button>
  </div>
</section>

<!-- =========================
     EXCLUDED
     ========================= -->
<section class="card">
  <h3>Entfernte Datensätze</h3>

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
      <tr>
        <td colspan="5" style="text-align:center;color:#94a3b8">
          Noch keine Daten geladen
        </td>
      </tr>
    </tbody>
  </table>
</section>

</main>

<!-- =========================
     OVERLAY
     ========================= -->
<div id="overlay">
  <div class="box">
    <div id="overlay-phase">Bitte warten …</div>
    <div id="overlay-detail" class="muted" style="margin-top:6px;font-size:14px"></div>
    <div id="overlay-bar-wrap"><div id="overlay-bar"></div></div>
  </div>
</div>

<script>
const el = (id)=>document.getElementById(id);
const clampPct = (p)=>Math.min(100, Math.max(0, parseInt(p||0,10)));

function setIndeterminate(on){
  const track = el("status-bar-wrap");
  if(track) track.classList.toggle("bar-indet", !!on);
  const ov = el("overlay-bar-wrap");
  if(ov) ov.classList.toggle("bar-indet", !!on);
}

function formatTime(ms){
  try{
    const d = new Date(ms);
    return d.toLocaleTimeString('de-DE', {hour:'2-digit', minute:'2-digit', second:'2-digit'});
  }catch(e){ return ""; }
}

function showOverlay(msg){
  const ov = el("overlay");
  if(!ov) return;
  if(el("overlay-phase")) el("overlay-phase").textContent = msg || "Bitte warten …";
  if(el("overlay-detail")) el("overlay-detail").textContent = "";
  ov.style.display = "flex";
}
function hideOverlay(){ const ov = el("overlay"); if(ov) ov.style.display = "none"; }
function setOverlayProgress(p){
  const b = el("overlay-bar");
  if(b) b.style.width = clampPct(p) + "%";
}

function statsHtml(stats){
  if(!stats) return "";
  const items = [];
  const push = (label, val)=>{ if(val===0 || val) items.push(`<span class="status-pill">${label}: <b>${val}</b></span>`); };

  push("Geladen", stats.total);
  push("Mit Batch", stats.with_any_batch);
  push("Ausgewählt", stats.selected);
  push("Entfernt", stats.removed);
  push("Exportiert", stats.exported);
  push("Löschlog", stats.delete_log);
  push("Fuzzy geprüft", stats.fuzzy_checked);
  push("Fuzzy entfernt", stats.fuzzy_removed);

  return items.join(" ");
}

function showStatus(phase, pct, stats){
  if(el("status-phase")) el("status-phase").textContent = phase || "";
  if(el("status-percent")) el("status-percent").textContent = clampPct(pct) + "%";
  const fill = el("status-bar");
  if(fill) fill.style.width = clampPct(pct) + "%";
  const info = el("status-info");
  if(info) info.innerHTML = statsHtml(stats);
}

function updateHeartbeat(state){
  const hb = el("status-meta");
  if(!hb) return;

  const lu = state?.last_update_ms || Date.now();
  const age = Date.now() - lu;

  let msg = `Letztes Update: ${formatTime(lu)}`;
  let indet = false;
  let pulse = false;

  if(state?.running && age > 4500){
    pulse = true;
    indet = true;
    msg = `Letztes Update: ${formatTime(lu)} · Noch aktiv …`;
  }
  if(state?.running && age > 25000){
    pulse = true;
    indet = true;
    msg = `Dauert länger als üblich · läuft weiter · Letztes Update: ${formatTime(lu)}`;
  }

  setIndeterminate(indet);
  hb.innerHTML = `${pulse ? '<span class="pulse-dot"></span>' : ''}<span>${msg}</span>`;
}

async function loadExcluded(){
  try{
    const r = await fetch("/neukontakte/excluded/json");
    if(!r.ok) return;
    const data = await r.json();
    const rows = data.rows || [];
    const summary = data.summary || [];
    const pick = (obj, keys) => {
      for(const k of keys){
        const v = obj && obj[k];
        if(v !== undefined && v !== null && String(v).trim() !== "") return v;
      }
      return "";
    };
    const body = el("excluded-table-body");
    if(body){
      if(rows.length===0){
        body.innerHTML = `<tr><td colspan="5" style="text-align:center;color:#94a3b8">Noch keine Daten</td></tr>`;
      }else{
        body.innerHTML = rows.map(x=>`
          <tr>
            <td>${pick(x, ["person_id","Kontakt ID","contact_id","id"])}</td>
            <td>${pick(x, ["name","Name"])}</td>
            <td>${pick(x, ["org_id","Organisation ID","organization_id"])}</td>
            <td>${pick(x, ["org_name","Organisationsname","organisation_name"])}</td>
            <td>${pick(x, ["reason","Grund","grund","extra","label"])}</td>
          </tr>`).join("");
      }
    }
    const sumEl = el("excluded-summary");
    if(sumEl){
      if(summary.length===0){
        sumEl.innerHTML = "";
      }else{
        sumEl.innerHTML = summary.map(s=>{
          const cnt = pick(s, ["count","Anzahl","anzahl"]);
          const lbl = pick(s, ["label","Grund","grund","reason"]);
          if(String(lbl).trim()==="") return "";
          return `<li><b>${cnt}</b> – ${lbl}</li>`;
        }).join("");
      }
    }
  }catch(e){ /* ignore */ }
}

async function loadOptions(){
  const box = el("fb-loading-box");
  const bar = el("fb-loading-bar");
  const txt = el("fb-loading-text");
  if(box) box.style.display = "block";
  if(bar) bar.style.width = "0%";
  if(txt) txt.textContent = "Fachbereiche werden geladen … bitte warten.";

  let p=0;
  const interval=setInterval(()=>{
    p=Math.min(p+6,90);
    if(bar) bar.style.width = p+"%";
  },180);

  try{
    const r = await fetch("/neukontakte/options");
    if(!r.ok) throw new Error("HTTP "+r.status);
    const data = await r.json();
    clearInterval(interval);
    if(bar) bar.style.width = "100%";
    setTimeout(()=>{ if(box) box.style.display="none"; }, 250);

    const sel = el("fachbereich");
    if(sel){
      sel.innerHTML = '<option value="">– bitte auswählen –</option>';
      (data.options||[]).forEach(o=>{
        const opt = document.createElement("option");
        opt.value = o.value;
        opt.textContent = (o.count === undefined ? o.label : (o.label + " (" + o.count + ")"));
        sel.appendChild(opt);
      });
      sel.onchange = ()=>{
        const btn = el("btnExport");
        if(btn) btn.disabled = !sel.value;
      };
    }
  }catch(e){
    clearInterval(interval);
    if(bar) bar.style.width = "100%";
    setTimeout(()=>{ if(box) box.style.display="none"; }, 250);
    alert("Fachbereiche konnten nicht geladen werden.");
  }
}

async function startExport(){
  const btn = el("btnExport");
  try{
    if(btn){ btn.disabled = true; btn.style.opacity="0.7"; btn.style.pointerEvents="none"; }
    showOverlay("Starte Abgleich …");
    showStatus("Starte …", 1, {});
    updateHeartbeat({running:true, last_update_ms: Date.now()});
    setOverlayProgress(3);

    
    const fb = el('fachbereich')?.value || "";
    if(!fb) return alert('Bitte Fachbereich wählen');

    const batchVal = (el("batch_id")?.value||"").trim();
    if(!batchVal) return alert("Bitte Batch ID eintragen.");

    const payload = {
      fachbereich: fb,
      take_count: (el("take_count")?.value||"").trim(),
      batch_id: batchVal,
      campaign: (el("campaign")?.value||"").trim()
    };


    await loadExcluded(); // clears current view (backend tables are cleared on export_start)
    const r = await fetch("/neukontakte/export_start", {
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body: JSON.stringify(payload)
    });
    if(!r.ok){
      const t = await r.text();
      throw new Error("Serverfehler ("+r.status+"): " + t);
    }
    const res = await r.json();
    const job_id = res.job_id;
    if(!job_id) throw new Error("Kein job_id vom Server erhalten.");

    let last = Date.now();
    while(true){
      await new Promise(r=>setTimeout(r, 650));
      const pr = await fetch("/neukontakte/export_progress?job_id="+encodeURIComponent(job_id));
      if(!pr.ok){
        const t = await pr.text();
        throw new Error("Progress-Fehler ("+pr.status+"): " + t);
      }
      const s = await pr.json();
      if(s.error) throw new Error(s.error);

      const pct = clampPct(s.progress || 0);
      showOverlay((s.phase || "Bitte warten …") + " (" + pct + "%)");
      if(el("overlay-detail")) el("overlay-detail").textContent = s.detail || "";
      setOverlayProgress(pct);
      showStatus(s.phase || "läuft …", pct, s.stats||{});

      const lu = s.last_update_ms || Date.now();
      updateHeartbeat({running: !s.done, last_update_ms: lu});

      if(s.done){
        // Download
        window.open("/neukontakte/export_download?job_id="+encodeURIComponent(job_id), "_blank");
        hideOverlay();
        await loadExcluded();
        showStatus("Fertig – Download gestartet", 100, s.stats||{});
        return;
      }
    }
  }catch(e){
    console.error(e);
    hideOverlay();
    showStatus("Fehler", 100, {});
    alert(e?.message || String(e));
  }finally{
    if(btn){ btn.disabled=false; btn.style.opacity="1"; btn.style.pointerEvents="auto"; }
  }
}

el('btnExport').onclick = startExport;
loadOptions();
showStatus("Bereit", 0, {});
updateHeartbeat({running:false, last_update_ms: Date.now()});
loadExcluded();
</script>



<!-- Statusbar -->
<div class="statusbar">
  <div class="status-inner">
    <div class="status-top">
      <div id="status-phase">Bereit</div>
      <div id="status-percent">0%</div>
    </div>
    <div id="status-bar-wrap"><div id="status-bar"></div></div>
    <div id="status-info"></div>
    <div id="status-meta" class="muted" style="margin-top:6px;display:flex;gap:10px;align-items:center;"></div>
  </div>
</div>

</body>
</html>
"""
    )

# =============================================================================
# Frontend – Nachfass (DESIGN identisch zu Neukontakte)
# =============================================================================

# =============================================================================
# Frontend – Nachfass (Layout wie Neukontakte + Statusbar)
# =============================================================================
@app.get("/nachfass", response_class=HTMLResponse)
async def nachfass_page(request: Request):
    authed = bool(user_tokens.get("default") or PD_API_TOKEN)
    authed_html = "<span class='muted'>angemeldet</span>" if authed else "<a href='/login'>Anmelden</a>"

    return HTMLResponse(
r"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Nachfass – BatchFlow</title>

<style>
:root{
  --bg:#f7f9fc;
  --card:#fff;
  --text:#0f172a;
  --muted:#64748b;
  --line:#e5e9f0;
  --brand:#0ea5e9;
  --brand2:#38bdf8;
  --shadow1:0 14px 32px rgba(15,23,42,.06);
  --shadow2:0 6px 12px rgba(15,23,42,.04);
  --radius:22px;
}

*{box-sizing:border-box;}
body{margin:0;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Cantarell,Noto Sans,sans-serif;}

header{
  position:sticky;top:0;z-index:10;
  background:#fff;border-bottom:1px solid var(--line);
}
.hwrap{
  max-width:1200px;margin:0 auto;
  padding:18px 22px;
  display:flex;align-items:center;justify-content:space-between;
}
.hleft{display:flex;align-items:center;gap:14px;}
.hleft img{height:26px;}
.hcenter{font-weight:700;letter-spacing:.2px;}
.hright{font-size:14px;color:var(--muted);}
.hright a{color:var(--brand);text-decoration:none;}
.hright a:hover{text-decoration:underline;}

main{max-width:1200px;margin:24px auto;padding:0 22px 120px;}

.card{
  background:var(--card);
  border:1px solid var(--line);
  border-radius:var(--radius);
  padding:36px;
  box-shadow:var(--shadow1),var(--shadow2);
  margin-bottom:20px;
}
.card h2{margin:0 0 10px 0;font-size:28px;}
.card h3{margin:0 0 8px 0;font-size:18px;}
.card p{margin:0 0 22px 0;color:var(--muted);font-size:14px;line-height:1.35;}
.muted{color:var(--muted);}

.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:20px;row-gap:22px;}
.col-12{grid-column:span 12;}
.col-6{grid-column:span 6;}
@media (max-width:840px){.col-6{grid-column:span 12;}}

label{display:block;font-weight:600;font-size:13px;color:#334155;margin-bottom:8px;}
input,select,textarea{
  width:100%;
  padding:14px 16px;
  border:1px solid var(--line);
  border-radius:14px;
  font-size:14px;
  background:#fff;
  outline:none;
}
input:focus,select:focus,textarea:focus{border-color:#bae6fd;box-shadow:0 0 0 4px rgba(56,189,248,.18);}
small{display:block;color:var(--muted);font-size:12px;margin-top:6px;}

.btn{
  background:var(--brand);
  color:#fff;
  border:none;
  border-radius:999px;
  padding:14px 28px;
  font-weight:700;
  cursor:pointer;
  box-shadow:0 12px 24px rgba(14,165,233,.18);
}
.btn:disabled{opacity:.55;cursor:not-allowed;box-shadow:none;}
.btn:hover:not(:disabled){filter:brightness(.98);}

table{width:100%;border-collapse:collapse;margin-top:12px;font-size:14px;}
thead{background:#f8fafc;}
th,td{padding:14px 16px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top;}
th{font-size:12px;font-weight:700;color:var(--muted);white-space:nowrap;}
.badge{display:inline-block;padding:4px 10px;border-radius:999px;background:#e0f2fe;color:#0369a1;font-size:12px;font-weight:700;}

#overlay{
  display:none;position:fixed;inset:0;z-index:50;
  background:rgba(15,23,42,.28);
  align-items:center;justify-content:center;
}
#overlay .box{
  width:min(520px,92vw);
  background:#fff;border:1px solid var(--line);
  border-radius:18px;padding:22px 22px 18px;
  box-shadow:0 18px 48px rgba(15,23,42,.18);
}
#overlay-phase{font-weight:700;margin-bottom:10px;}
#overlay-bar-wrap{width:100%;height:10px;border-radius:999px;background:var(--line);overflow:hidden;}
#overlay-bar{height:100%;width:0%;background:linear-gradient(90deg,var(--brand),var(--brand2));transition:width .25s linear;}

.statusbar{
  position:fixed;left:0;right:0;bottom:0;z-index:40;
  background:#fff;border-top:1px solid var(--line);
}
.status-inner{
  max-width:1200px;margin:0 auto;padding:12px 22px;
}
.status-top{display:flex;align-items:center;justify-content:space-between;gap:14px;}
#status-phase{font-weight:700;}
#status-percent{color:var(--muted);font-weight:700;}
#status-bar-wrap{width:100%;height:8px;border-radius:999px;background:var(--line);overflow:hidden;margin-top:8px;}
#status-bar{height:100%;width:0%;background:linear-gradient(90deg,var(--brand),var(--brand2));transition:width .25s linear;}
#status-info{margin-top:8px;color:#334155;font-size:13px;}
.status-pill{display:inline-flex;gap:6px;align-items:center;margin-right:12px;}
.status-pill b{font-weight:800;}

/* ---------- Live/Heartbeat ---------- */
.pulse-dot{width:8px;height:8px;border-radius:50%;background:var(--primary);display:inline-block;animation:pulse 1.2s infinite;}
@keyframes pulse{0%{transform:scale(.8);opacity:.5}50%{transform:scale(1);opacity:1}100%{transform:scale(.8);opacity:.5}}
.bar-indet{position:relative;overflow:hidden;}
.bar-indet::before{content:"";position:absolute;inset:0;background:linear-gradient(90deg,rgba(255,255,255,0) 0%,rgba(255,255,255,.55) 50%,rgba(255,255,255,0) 100%);transform:translateX(-100%);animation:indet 1.1s infinite;}
@keyframes indet{0%{transform:translateX(-100%)}100%{transform:translateX(100%)}}

</style>

</head>

<body>
<header>
  <div class="hwrap">
    <div class="hleft">
      <img src="/static/bizforward-Logo-Clean-2024.svg" alt="bizforward">
      <a href="/campaign" style="color:#0a66c2;text-decoration:none;font-size:14px">Kampagne wählen</a>
    </div>
    <div class="hcenter">Nachfass</div>
    <div class="hright">""" + authed_html + r"""</div>
  </div>
</header>

<main>
  <section class="card">
    <h2>Schritt 1 – Nachfass auswählen</h2>
    <p>Trage die Batch IDs ein, die nachgefasst werden sollen, und definiere die Export Batch ID für die Excel-Datei.</p>

    <div class="grid">
      <div class="col-12">
        <label>Batch IDs (Nachfass)</label>
        <textarea id="nf_batch_ids" rows="2" placeholder="z. B. 123, 124, 125"></textarea>
        <small>Mehrere Werte mit Komma, Leerzeichen oder Zeilenumbruch trennen.</small>
      </div>

      <div class="col-6">
        <label>Export Batch ID</label>
        <input id="batch_id" placeholder="xxx">
        <small>Diese ID wird in Excel in die erste Spalte („Batch ID“) geschrieben.</small>
      </div>

      <div class="col-6">
        <label>Kampagnenname (für Cold Mailing)</label>
        <input id="campaign" placeholder="z. B. Frühling 2025">
        <small>Optional – wird für Dateiname/Export genutzt.</small>
      </div>

      <div class="col-12" style="display:flex;justify-content:flex-end;gap:12px;align-items:center;margin-top:8px">
        <button class="btn" id="btnExportNf">Abgleich & Download</button>
      </div>
    </div>
  </section>

  <section class="card">
    <h3>Entfernte Datensätze</h3>
    <p class="muted" style="margin-bottom:12px">Hier siehst du jederzeit, welche Datensätze im Nachfassprozess entfernt wurden (inkl. Grund).</p>
    <div id="excluded-summary" class="muted" style="margin:0 0 10px 0"></div>

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
        <tr><td colspan="5" style="text-align:center;color:#94a3b8">Noch keine Daten geladen</td></tr>
      </tbody>
    </table>
  </section>
</main>

<!-- Overlay -->
<div id="overlay">
  <div class="box">
    <div id="overlay-phase"></div>
    <div id="overlay-detail" class="muted" style="margin:-2px 0 10px 0;font-size:13px;"></div>
    <div id="overlay-detail" class="muted" style="margin:-2px 0 10px 0;font-size:13px;"></div>
    <div id="overlay-bar-wrap"><div id="overlay-bar"></div></div>
  </div>
</div>

<!-- Statusbar -->
<div class="statusbar">
  <div class="status-inner">
    <div class="status-top">
      <div id="status-phase">Bereit</div>
      <div id="status-percent">0%</div>
    </div>
    <div id="status-bar-wrap"><div id="status-bar"></div></div>
    <div id="status-info"></div>
    <div id="status-meta" class="muted" style="margin-top:6px;display:flex;gap:10px;align-items:center;"></div>
    <div id="status-meta" class="muted" style="margin-top:6px;display:flex;gap:10px;align-items:center;"></div>
  </div>
</div>


<script>
const el = (id)=>document.getElementById(id);
const clampPct = (p)=>Math.min(100, Math.max(0, parseInt(p||0,10)));

function setIndeterminate(on){
  const track = el("status-bar-wrap");
  if(track) track.classList.toggle("bar-indet", !!on);
  const ov = el("overlay-bar-wrap");
  if(ov) ov.classList.toggle("bar-indet", !!on);
}

function formatTime(ms){
  try{
    const d = new Date(ms);
    return d.toLocaleTimeString('de-DE', {hour:'2-digit', minute:'2-digit', second:'2-digit'});
  }catch(e){ return ""; }
}

function showOverlay(msg){
  const ov = el("overlay");
  if(!ov) return;
  if(el("overlay-phase")) el("overlay-phase").textContent = msg || "Bitte warten …";
  if(el("overlay-detail")) el("overlay-detail").textContent = "";
  ov.style.display = "flex";
}
function hideOverlay(){ const ov = el("overlay"); if(ov) ov.style.display = "none"; }
function setOverlayProgress(p){
  const b = el("overlay-bar");
  if(b) b.style.width = clampPct(p) + "%";
}

function statsHtml(stats){
  if(!stats) return "";
  const items = [];
  const push = (label, val)=>{ if(val===0 || val) items.push(`<span class="status-pill">${label}: <b>${val}</b></span>`); };

  push("Geladen", stats.total);
  push("Mit Batch", stats.with_any_batch);
  push("Ausgewählt", stats.selected);
  push("Entfernt", stats.removed);
  push("Exportiert", stats.exported);
  push("Löschlog", stats.delete_log);
  push("Fuzzy geprüft", stats.fuzzy_checked);
  push("Fuzzy entfernt", stats.fuzzy_removed);

  return items.join(" ");
}

function showStatus(phase, pct, stats){
  if(el("status-phase")) el("status-phase").textContent = phase || "";
  if(el("status-percent")) el("status-percent").textContent = clampPct(pct) + "%";
  const fill = el("status-bar");
  if(fill) fill.style.width = clampPct(pct) + "%";
  const info = el("status-info");
  if(info) info.innerHTML = statsHtml(stats);
}

function updateHeartbeat(state){
  const hb = el("status-meta");
  if(!hb) return;

  const lu = state?.last_update_ms || Date.now();
  const age = Date.now() - lu;

  let msg = `Letztes Update: ${formatTime(lu)}`;
  let indet = false;
  let pulse = false;

  if(state?.running && age > 4500){
    pulse = true;
    indet = true;
    msg = `Letztes Update: ${formatTime(lu)} · Noch aktiv …`;
  }
  if(state?.running && age > 25000){
    pulse = true;
    indet = true;
    msg = `Dauert länger als üblich · läuft weiter · Letztes Update: ${formatTime(lu)}`;
  }

  setIndeterminate(indet);
  hb.innerHTML = `${pulse ? '<span class="pulse-dot"></span>' : ''}<span>${msg}</span>`;
}

async function loadExcluded(){
  try{
    const r = await fetch("/nachfass/excluded/json");
    if(!r.ok) return;
    const data = await r.json();
    const rows = data.rows || [];
    const summary = data.summary || [];
    const pick = (obj, keys) => {
      for(const k of keys){
        const v = obj && obj[k];
        if(v !== undefined && v !== null && String(v).trim() !== "") return v;
      }
      return "";
    };
    const body = el("excluded-table-body");
    if(body){
      if(rows.length===0){
        body.innerHTML = `<tr><td colspan="5" style="text-align:center;color:#94a3b8">Noch keine Daten</td></tr>`;
      }else{
        body.innerHTML = rows.map(x=>`
          <tr>
            <td>${pick(x, ["person_id","Kontakt ID","contact_id","id"])}</td>
            <td>${pick(x, ["name","Name"])}</td>
            <td>${pick(x, ["org_id","Organisation ID","organization_id"])}</td>
            <td>${pick(x, ["org_name","Organisationsname","organisation_name"])}</td>
            <td>${pick(x, ["reason","Grund","grund","extra","label"])}</td>
          </tr>`).join("");
      }
    }
    const sumEl = el("excluded-summary");
    if(sumEl){
      if(summary.length===0){
        sumEl.innerHTML = "";
      }else{
        sumEl.innerHTML = summary.map(s=>{
          const cnt = pick(s, ["count","Anzahl","anzahl"]);
          const lbl = pick(s, ["label","Grund","grund","reason"]);
          if(String(lbl).trim()==="") return "";
          return `<li><b>${cnt}</b> – ${lbl}</li>`;
        }).join("");
      }
    }
  }catch(e){ /* ignore */ }
}

async function startExport(){
  const btn = el("btnExportNf");
  try{
    if(btn){ btn.disabled = true; btn.style.opacity="0.7"; btn.style.pointerEvents="none"; }
    showOverlay("Starte Abgleich …");
    showStatus("Starte …", 1, {});
    updateHeartbeat({running:true, last_update_ms: Date.now()});
    setOverlayProgress(3);

    
    const nf_raw = (el("nf_batch_ids")?.value || "").trim();
    if(!nf_raw) return alert("Bitte mindestens eine Batch ID eintragen.");
    const nf_batch_ids = nf_raw.split(/[,\s\n\r]+/).map(x=>x.trim()).filter(Boolean);
    const batchVal = (el("batch_id")?.value||"").trim(); // Export Batch ID in UI
    if(!batchVal) return alert("Bitte Export Batch ID eintragen.");
    const payload = { nf_batch_ids, batch_id: batchVal, campaign: (el("campaign")?.value||"").trim() };


    await loadExcluded(); // clears current view (backend tables are cleared on export_start)
    const r = await fetch("/nachfass/export_start", {
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body: JSON.stringify(payload)
    });
    if(!r.ok){
      const t = await r.text();
      throw new Error("Serverfehler ("+r.status+"): " + t);
    }
    const res = await r.json();
    const job_id = res.job_id;
    if(!job_id) throw new Error("Kein job_id vom Server erhalten.");

    let last = Date.now();
    while(true){
      await new Promise(r=>setTimeout(r, 650));
      const pr = await fetch("/nachfass/export_progress?job_id="+encodeURIComponent(job_id));
      if(!pr.ok){
        const t = await pr.text();
        throw new Error("Progress-Fehler ("+pr.status+"): " + t);
      }
      const s = await pr.json();
      if(s.error) throw new Error(s.error);

      const pct = clampPct(s.progress || 0);
      showOverlay((s.phase || "Bitte warten …") + " (" + pct + "%)");
      if(el("overlay-detail")) el("overlay-detail").textContent = s.detail || "";
      setOverlayProgress(pct);
      showStatus(s.phase || "läuft …", pct, s.stats||{});

      const lu = s.last_update_ms || Date.now();
      updateHeartbeat({running: !s.done, last_update_ms: lu});

      if(s.done){
        // Download
        window.open("/nachfass/export_download?job_id="+encodeURIComponent(job_id), "_blank");
        hideOverlay();
        await loadExcluded();
        showStatus("Fertig – Download gestartet", 100, s.stats||{});
        return;
      }
    }
  }catch(e){
    console.error(e);
    hideOverlay();
    showStatus("Fehler", 100, {});
    alert(e?.message || String(e));
  }finally{
    if(btn){ btn.disabled=false; btn.style.opacity="1"; btn.style.pointerEvents="auto"; }
  }
}

el('btnExportNf').onclick = startExport;
showStatus("Bereit", 0, {});
updateHeartbeat({running:false, last_update_ms: Date.now()});
loadExcluded();
</script>


<script>
function parseBatchIds(raw){
  return (raw||"")
    .split(/[\s,;]+/g)
    .map(s=>s.trim())
    .filter(Boolean);
}

function resetExcludedUI(msg){
  el("excluded-summary").innerHTML = `<span class='muted'>${msg||"Noch kein Abgleich gestartet."}</span>`;
  el("excluded-table-body").innerHTML = `<tr><td colspan="5" style="text-align:center;color:#94a3b8">${msg||"Noch kein Abgleich gestartet."}</td></tr>`;
}

async function loadExcluded(){
  try{
    const res = await fetch("/nachfass/excluded/json");
    const data = await res.json();

    // Summary
    const sum = (data.summary||[])
      .filter(x => (x.Anzahl||0) > 0)
      .map(x => `<li><b>${x.Anzahl}</b> – ${x.Grund}</li>`)
      .join("");
    el("excluded-summary").innerHTML = sum ? `<ul style="margin:0;padding-left:18px">${sum}</ul>` : "<span class='muted'>Keine Datensätze ausgeschlossen.</span>";

    // Rows
    const tb = el("excluded-table-body");
    const rows = data.rows || [];
    if(!rows.length){
      tb.innerHTML = `<tr><td colspan="5" style="text-align:center;color:#94a3b8">Keine entfernten Datensätze</td></tr>`;
      return;
    }
    tb.innerHTML = rows.map(r => `
      <tr>
        <td>${r["Kontakt ID"]||""}</td>
        <td>${r["Name"]||""}</td>
        <td>${r["Organisation ID"]||""}</td>
        <td>${r["Organisationsname"]||""}</td>
        <td>${r["Grund"]||""}</td>
      </tr>
    `).join("");
  }catch(e){
    console.warn("loadExcluded failed", e);
  }
}

async function startExport(){
  const btn = document.getElementById("btnExportNf");
  try{
    if(btn){ btn.disabled = true; btn.style.opacity = "0.7"; btn.style.pointerEvents = "none"; }
    const ids = parseBatchIds(el("nf_batch_ids")?.value || el("batch_ids")?.value || "");
    // nach Prozess unterschiedlich: Nachfass nutzt nf_batch_ids, NK/RF nutzen batch_ids
    // Validierung je nach vorhandenen Feldern:
    const isNachfass = !!document.getElementById("nf_batch_ids");
    if(isNachfass && !ids.length) return alert("Bitte mindestens eine Batch ID eintragen.");
    const batchField = document.getElementById("batch_id");
    const batchVal = (batchField ? (batchField.value||"").trim() : "");
    if(!batchVal) return alert("Bitte Batch ID eintragen.");

    showOverlay("Starte Abgleich …");
    setOverlayProgress(0);
    setIndeterminate(true);
    showStatus("Starte Abgleich …", 0, {});

    const payload = isNachfass
      ? { nf_batch_ids: ids, batch_id: batchVal, campaign: (el("campaign").value||"").trim() }
      : { filter_id: (el("filter_id")?.value || "").trim(), take_count: (el("take_count")?.value||"").trim(), batch_id: batchVal, campaign: (el("campaign").value||"").trim() };

    // Bei NK/RF kommt Fachbereich aus select:
    if(!isNachfass){
      const fb = el("fachbereich")?.value || "";
      payload.fachbereich = fb;
    }

    const r = await fetch("/nachfass/export_start", {
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body: JSON.stringify(payload)
    });
    if(!r.ok){
      const txt = await r.text();
      throw new Error("Serverfehler ("+r.status+"): " + txt);
    }
    const res = await r.json();
    const job_id = res.job_id;
    if(!job_id) throw new Error("Kein job_id vom Server erhalten.");

    while(true){
      await new Promise(r=>setTimeout(r, 650));
      const pr = await fetch("/nachfass/export_progress?job_id="+encodeURIComponent(job_id));
      if(!pr.ok){
        const txt = await pr.text();
        throw new Error("Progress-Fehler ("+pr.status+"): " + txt);
      }
      const s = await pr.json();

      if(s.error){
        setIndeterminate(false);
        hideOverlay();
        showStatus("Fehler", 100, s.stats||{});
        alert(s.error);
        return;
      }

      // UI update
      el("overlay-phase").textContent = `${s.phase}${s.detail ? " · " + s.detail : ""} (${s.percent}%)`;
      if(el("overlay-detail")) el("overlay-detail").textContent = s.detail || "";
      updateHeartbeat(s);
      setOverlayProgress(s.percent);
      showStatus(s.phase, s.percent, s.stats||{});

      if(s.download_ready){
        setIndeterminate(false);
        window.open("/nachfass/export_download?job_id="+encodeURIComponent(job_id), "_blank");
        hideOverlay();
        await loadExcluded();
        showStatus("Fertig", 100, s.stats||{});
        const card = document.querySelectorAll("section.card")[1];
        if(card) card.scrollIntoView({behavior:"smooth"});
        return;
      }
    }
  }catch(e){
    console.error(e);
    setIndeterminate(false);
    hideOverlay();
    showStatus("Fehler", 100, {});
    alert(e?.message || String(e));
  }finally{
    if(btn){ btn.disabled = false; btn.style.opacity = "1"; btn.style.pointerEvents = "auto"; }
  }
}

window.addEventListenerwindow.addEventListener("load", ()=>resetExcludedUI("Noch kein Abgleich gestartet."));
el("btnExportNf").onclick = startExport;
</script>

</body>
</html>
"""
    )



# =============================================================================
# Frontend – Refresh (Layout wie Neukontakte + Statusbar)
# =============================================================================
@app.get("/refresh", response_class=HTMLResponse)
async def refresh_page(request: Request):
    authed = bool(user_tokens.get("default") or PD_API_TOKEN)
    auth_info = "<span class='muted'>angemeldet</span>" if authed else "<a href='/login'>Anmelden</a>"

    return HTMLResponse(r"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Refresh – BatchFlow</title>

<style>
:root{
  --bg:#f7f9fc;
  --card:#fff;
  --text:#0f172a;
  --muted:#64748b;
  --line:#e5e9f0;
  --brand:#0ea5e9;
  --brand2:#38bdf8;
  --shadow1:0 14px 32px rgba(15,23,42,.06);
  --shadow2:0 6px 12px rgba(15,23,42,.04);
  --radius:22px;
}

*{box-sizing:border-box;}
body{margin:0;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Cantarell,Noto Sans,sans-serif;}

header{
  position:sticky;top:0;z-index:10;
  background:#fff;border-bottom:1px solid var(--line);
}
.hwrap{
  max-width:1200px;margin:0 auto;
  padding:18px 22px;
  display:flex;align-items:center;justify-content:space-between;
}
.hleft{display:flex;align-items:center;gap:14px;}
.hleft img{height:26px;}
.hcenter{font-weight:700;letter-spacing:.2px;}
.hright{font-size:14px;color:var(--muted);}
.hright a{color:var(--brand);text-decoration:none;}
.hright a:hover{text-decoration:underline;}

main{max-width:1200px;margin:24px auto;padding:0 22px 120px;}

.card{
  background:var(--card);
  border:1px solid var(--line);
  border-radius:var(--radius);
  padding:36px;
  box-shadow:var(--shadow1),var(--shadow2);
  margin-bottom:20px;
}
.card h2{margin:0 0 10px 0;font-size:28px;}
.card h3{margin:0 0 8px 0;font-size:18px;}
.card p{margin:0 0 22px 0;color:var(--muted);font-size:14px;line-height:1.35;}
.muted{color:var(--muted);}

.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:20px;row-gap:22px;}
.col-12{grid-column:span 12;}
.col-6{grid-column:span 6;}
@media (max-width:840px){.col-6{grid-column:span 12;}}

label{display:block;font-weight:600;font-size:13px;color:#334155;margin-bottom:8px;}
input,select,textarea{
  width:100%;
  padding:14px 16px;
  border:1px solid var(--line);
  border-radius:14px;
  font-size:14px;
  background:#fff;
  outline:none;
}
input:focus,select:focus,textarea:focus{border-color:#bae6fd;box-shadow:0 0 0 4px rgba(56,189,248,.18);}
small{display:block;color:var(--muted);font-size:12px;margin-top:6px;}

.btn{
  background:var(--brand);
  color:#fff;
  border:none;
  border-radius:999px;
  padding:14px 28px;
  font-weight:700;
  cursor:pointer;
  box-shadow:0 12px 24px rgba(14,165,233,.18);
}
.btn:disabled{opacity:.55;cursor:not-allowed;box-shadow:none;}
.btn:hover:not(:disabled){filter:brightness(.98);}

table{width:100%;border-collapse:collapse;margin-top:12px;font-size:14px;}
thead{background:#f8fafc;}
th,td{padding:14px 16px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top;}
th{font-size:12px;font-weight:700;color:var(--muted);white-space:nowrap;}
.badge{display:inline-block;padding:4px 10px;border-radius:999px;background:#e0f2fe;color:#0369a1;font-size:12px;font-weight:700;}

#overlay{
  display:none;position:fixed;inset:0;z-index:50;
  background:rgba(15,23,42,.28);
  align-items:center;justify-content:center;
}
#overlay .box{
  width:min(520px,92vw);
  background:#fff;border:1px solid var(--line);
  border-radius:18px;padding:22px 22px 18px;
  box-shadow:0 18px 48px rgba(15,23,42,.18);
}
#overlay-phase{font-weight:700;margin-bottom:10px;}
#overlay-bar-wrap{width:100%;height:10px;border-radius:999px;background:var(--line);overflow:hidden;}
#overlay-bar{height:100%;width:0%;background:linear-gradient(90deg,var(--brand),var(--brand2));transition:width .25s linear;}

.statusbar{
  position:fixed;left:0;right:0;bottom:0;z-index:40;
  background:#fff;border-top:1px solid var(--line);
}
.status-inner{
  max-width:1200px;margin:0 auto;padding:12px 22px;
}
.status-top{display:flex;align-items:center;justify-content:space-between;gap:14px;}
#status-phase{font-weight:700;}
#status-percent{color:var(--muted);font-weight:700;}
#status-bar-wrap{width:100%;height:8px;border-radius:999px;background:var(--line);overflow:hidden;margin-top:8px;}
#status-bar{height:100%;width:0%;background:linear-gradient(90deg,var(--brand),var(--brand2));transition:width .25s linear;}
#status-info{margin-top:8px;color:#334155;font-size:13px;}
.status-pill{display:inline-flex;gap:6px;align-items:center;margin-right:12px;}
.status-pill b{font-weight:800;}

/* ---------- Live/Heartbeat ---------- */
.pulse-dot{width:8px;height:8px;border-radius:50%;background:var(--primary);display:inline-block;animation:pulse 1.2s infinite;}
@keyframes pulse{0%{transform:scale(.8);opacity:.5}50%{transform:scale(1);opacity:1}100%{transform:scale(.8);opacity:.5}}
.bar-indet{position:relative;overflow:hidden;}
.bar-indet::before{content:"";position:absolute;inset:0;background:linear-gradient(90deg,rgba(255,255,255,0) 0%,rgba(255,255,255,.55) 50%,rgba(255,255,255,0) 100%);transform:translateX(-100%);animation:indet 1.1s infinite;}
@keyframes indet{0%{transform:translateX(-100%)}100%{transform:translateX(100%)}}

</style>

</head>

<body>
<header>
  <div class="hwrap">
    <div class="hleft">
      <img src="/static/bizforward-Logo-Clean-2024.svg" alt="bizforward">
      <a href="/campaign" style="color:#0a66c2;text-decoration:none;font-size:14px">Kampagne wählen</a>
    </div>
    <div class="hcenter">Refresh</div>
    <div class="hright">""" + auth_info + r"""</div>
  </div>
</header>

<main>
  <section class="card">
    <h2>Schritt 1 – Refresh auswählen</h2>
    <p>Wähle einen Fachbereich und definiere, wie viele Kontakte exportiert werden sollen.</p>

    <div class="grid">
      <div class="col-12">
        <label>Fachbereich</label>

        <div id="fb-loading-box" style="display:none;margin:8px 0 14px 0">
          <div id="fb-loading-text" class="muted" style="font-size:13px;margin-bottom:6px">
            Fachbereiche werden geladen … bitte warten.
          </div>
          <div id="fb-loading-bar-wrap" style="width:100%;height:10px;border-radius:999px;background:var(--line);overflow:hidden">
            <div id="fb-loading-bar" style="height:100%;width:0%;background:linear-gradient(90deg,var(--brand),var(--brand2));transition:width .25s linear"></div>
          </div>
        </div>

        <select id="fachbereich">
          <option value="">– bitte auswählen –</option>
        </select>
        <small>Quelle aus Pipedrive – Refresh Filter.</small>
      </div>

      <div class="col-6">
        <label>Anzahl Kontakte</label>
        <input id="take_count" type="number" min="1" placeholder="alle">
        <small>Optional</small>
      </div>

      <div class="col-6">
        <label>Batch ID</label>
        <input id="batch_id" placeholder="xxx">
        <small>Intern</small>
      </div>

      <div class="col-12">
        <label>Kampagnenname (für Cold Mailing)</label>
        <input id="campaign" placeholder="z. B. Frühling 2025">
      </div>

      <div class="col-12" style="display:flex;justify-content:flex-end;margin-top:8px">
        <button class="btn" id="btnExportRf" disabled>Abgleich & Download</button>
      </div>
    </div>
  </section>

  <section class="card">
    <h3>Entfernte Datensätze</h3>
    <p class="muted" style="margin-bottom:12px">Hier siehst du jederzeit, welche Datensätze im Refreshprozess entfernt wurden (inkl. Grund).</p>
    <div id="excluded-summary" class="muted" style="margin:0 0 10px 0"></div>

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
        <tr><td colspan="5" style="text-align:center;color:#94a3b8">Noch keine Daten geladen</td></tr>
      </tbody>
    </table>
  </section>
</main>

<!-- Overlay -->
<div id="overlay">
  <div class="box">
    <div id="overlay-phase"></div>
    <div id="overlay-detail" class="muted" style="margin:-2px 0 10px 0;font-size:13px;"></div>
    <div id="overlay-bar-wrap"><div id="overlay-bar"></div></div>
  </div>
</div>

<!-- Statusbar -->
<div class="statusbar">
  <div class="status-inner">
    <div class="status-top">
      <div id="status-phase">Bereit</div>
      <div id="status-percent">0%</div>
    </div>
    <div id="status-bar-wrap"><div id="status-bar"></div></div>
    <div id="status-info"></div>
    <div id="status-meta" class="muted" style="margin-top:6px;display:flex;gap:10px;align-items:center;"></div>
  </div>
</div>


<script>
const el = (id)=>document.getElementById(id);
const clampPct = (p)=>Math.min(100, Math.max(0, parseInt(p||0,10)));

function setIndeterminate(on){
  const track = el("status-bar-wrap");
  if(track) track.classList.toggle("bar-indet", !!on);
  const ov = el("overlay-bar-wrap");
  if(ov) ov.classList.toggle("bar-indet", !!on);
}

function formatTime(ms){
  try{
    const d = new Date(ms);
    return d.toLocaleTimeString('de-DE', {hour:'2-digit', minute:'2-digit', second:'2-digit'});
  }catch(e){ return ""; }
}

function showOverlay(msg){
  const ov = el("overlay");
  if(!ov) return;
  if(el("overlay-phase")) el("overlay-phase").textContent = msg || "Bitte warten …";
  if(el("overlay-detail")) el("overlay-detail").textContent = "";
  ov.style.display = "flex";
}
function hideOverlay(){ const ov = el("overlay"); if(ov) ov.style.display = "none"; }
function setOverlayProgress(p){
  const b = el("overlay-bar");
  if(b) b.style.width = clampPct(p) + "%";
}

function statsHtml(stats){
  if(!stats) return "";
  const items = [];
  const push = (label, val)=>{ if(val===0 || val) items.push(`<span class="status-pill">${label}: <b>${val}</b></span>`); };

  push("Geladen", stats.total);
  push("Mit Batch", stats.with_any_batch);
  push("Ausgewählt", stats.selected);
  push("Entfernt", stats.removed);
  push("Exportiert", stats.exported);
  push("Löschlog", stats.delete_log);
  push("Fuzzy geprüft", stats.fuzzy_checked);
  push("Fuzzy entfernt", stats.fuzzy_removed);

  return items.join(" ");
}

function showStatus(phase, pct, stats){
  if(el("status-phase")) el("status-phase").textContent = phase || "";
  if(el("status-percent")) el("status-percent").textContent = clampPct(pct) + "%";
  const fill = el("status-bar");
  if(fill) fill.style.width = clampPct(pct) + "%";
  const info = el("status-info");
  if(info) info.innerHTML = statsHtml(stats);
}

function updateHeartbeat(state){
  const hb = el("status-meta");
  if(!hb) return;

  const lu = state?.last_update_ms || Date.now();
  const age = Date.now() - lu;

  let msg = `Letztes Update: ${formatTime(lu)}`;
  let indet = false;
  let pulse = false;

  if(state?.running && age > 4500){
    pulse = true;
    indet = true;
    msg = `Letztes Update: ${formatTime(lu)} · Noch aktiv …`;
  }
  if(state?.running && age > 25000){
    pulse = true;
    indet = true;
    msg = `Dauert länger als üblich · läuft weiter · Letztes Update: ${formatTime(lu)}`;
  }

  setIndeterminate(indet);
  hb.innerHTML = `${pulse ? '<span class="pulse-dot"></span>' : ''}<span>${msg}</span>`;
}

async function loadExcluded(){
  try{
    const r = await fetch("/refresh/excluded/json");
    if(!r.ok) return;
    const data = await r.json();
    const rows = data.rows || [];
    const summary = data.summary || [];
    const pick = (obj, keys) => {
      for(const k of keys){
        const v = obj && obj[k];
        if(v !== undefined && v !== null && String(v).trim() !== "") return v;
      }
      return "";
    };
    const body = el("excluded-table-body");
    if(body){
      if(rows.length===0){
        body.innerHTML = `<tr><td colspan="5" style="text-align:center;color:#94a3b8">Noch keine Daten</td></tr>`;
      }else{
        body.innerHTML = rows.map(x=>`
          <tr>
            <td>${pick(x, ["person_id","Kontakt ID","contact_id","id"])}</td>
            <td>${pick(x, ["name","Name"])}</td>
            <td>${pick(x, ["org_id","Organisation ID","organization_id"])}</td>
            <td>${pick(x, ["org_name","Organisationsname","organisation_name"])}</td>
            <td>${pick(x, ["reason","Grund","grund","extra","label"])}</td>
          </tr>`).join("");
      }
    }
    const sumEl = el("excluded-summary");
    if(sumEl){
      if(summary.length===0){
        sumEl.innerHTML = "";
      }else{
        sumEl.innerHTML = summary.map(s=>{
          const cnt = pick(s, ["count","Anzahl","anzahl"]);
          const lbl = pick(s, ["label","Grund","grund","reason"]);
          if(String(lbl).trim()==="") return "";
          return `<li><b>${cnt}</b> – ${lbl}</li>`;
        }).join("");
      }
    }
  }catch(e){ /* ignore */ }
}

async function loadOptions(){
  const box = el("fb-loading-box");
  const bar = el("fb-loading-bar");
  const txt = el("fb-loading-text");
  if(box) box.style.display = "block";
  if(bar) bar.style.width = "0%";
  if(txt) txt.textContent = "Fachbereiche werden geladen … bitte warten.";

  let p=0;
  const interval=setInterval(()=>{
    p=Math.min(p+6,90);
    if(bar) bar.style.width = p+"%";
  },180);

  try{
    const r = await fetch("/refresh/options");
    if(!r.ok) throw new Error("HTTP "+r.status);
    const data = await r.json();
    clearInterval(interval);
    if(bar) bar.style.width = "100%";
    setTimeout(()=>{ if(box) box.style.display="none"; }, 250);

    const sel = el("fachbereich");
    if(sel){
      sel.innerHTML = '<option value="">– bitte auswählen –</option>';
      (data.options||[]).forEach(o=>{
        const opt = document.createElement("option");
        opt.value = o.value;
        opt.textContent = (o.count === undefined ? o.label : (o.label + " (" + o.count + ")"));
        sel.appendChild(opt);
      });
      sel.onchange = ()=>{
        const btn = el("btnExportRf");
        if(btn) btn.disabled = !sel.value;
      };
    }
  }catch(e){
    clearInterval(interval);
    if(bar) bar.style.width = "100%";
    setTimeout(()=>{ if(box) box.style.display="none"; }, 250);
    alert("Fachbereiche konnten nicht geladen werden.");
  }
}

async function startExport(){
  const btn = el("btnExportRf");
  try{
    if(btn){ btn.disabled = true; btn.style.opacity="0.7"; btn.style.pointerEvents="none"; }
    showOverlay("Starte Abgleich …");
    showStatus("Starte …", 1, {});
    updateHeartbeat({running:true, last_update_ms: Date.now()});
    setOverlayProgress(3);

    
    const fb = el('fachbereich')?.value || "";
    if(!fb) return alert('Bitte Fachbereich wählen');

    const batchVal = (el("batch_id")?.value||"").trim();
    if(!batchVal) return alert("Bitte Batch ID eintragen.");

    const payload = {
      fachbereich: fb,
      take_count: (el("take_count")?.value||"").trim(),
      batch_id: batchVal,
      campaign: (el("campaign")?.value||"").trim()
    };


    await loadExcluded(); // clears current view (backend tables are cleared on export_start)
    const r = await fetch("/refresh/export_start", {
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body: JSON.stringify(payload)
    });
    if(!r.ok){
      const t = await r.text();
      throw new Error("Serverfehler ("+r.status+"): " + t);
    }
    const res = await r.json();
    const job_id = res.job_id;
    if(!job_id) throw new Error("Kein job_id vom Server erhalten.");

    let last = Date.now();
    while(true){
      await new Promise(r=>setTimeout(r, 650));
      const pr = await fetch("/refresh/export_progress?job_id="+encodeURIComponent(job_id));
      if(!pr.ok){
        const t = await pr.text();
        throw new Error("Progress-Fehler ("+pr.status+"): " + t);
      }
      const s = await pr.json();
      if(s.error) throw new Error(s.error);

      const pct = clampPct(s.progress || 0);
      showOverlay((s.phase || "Bitte warten …") + " (" + pct + "%)");
      if(el("overlay-detail")) el("overlay-detail").textContent = s.detail || "";
      setOverlayProgress(pct);
      showStatus(s.phase || "läuft …", pct, s.stats||{});

      const lu = s.last_update_ms || Date.now();
      updateHeartbeat({running: !s.done, last_update_ms: lu});

      if(s.done){
        // Download
        window.open("/refresh/export_download?job_id="+encodeURIComponent(job_id), "_blank");
        hideOverlay();
        await loadExcluded();
        showStatus("Fertig – Download gestartet", 100, s.stats||{});
        return;
      }
    }
  }catch(e){
    console.error(e);
    hideOverlay();
    showStatus("Fehler", 100, {});
    alert(e?.message || String(e));
  }finally{
    if(btn){ btn.disabled=false; btn.style.opacity="1"; btn.style.pointerEvents="auto"; }
  }
}

el('btnExportRf').onclick = startExport;
loadOptions();
showStatus("Bereit", 0, {});
updateHeartbeat({running:false, last_update_ms: Date.now()});
loadExcluded();
</script>


<script>
function setFbLoading(pct, text){
  const box = el("fb-loading-box");
  const bar = el("fb-loading-bar");
  const t = el("fb-loading-text");
  box.style.display = "block";
  bar.style.width = clampPct(pct) + "%";
  if(text) t.textContent = text;
}

async function loadOptions(){
  const sel = el("fachbereich");
  sel.innerHTML = '<option value="">– bitte auswählen –</option>';
  el("btnExportRf").disabled = true;

  setFbLoading(10, "Fachbereiche werden geladen … bitte warten.");
  showStatus("Fachbereiche laden …", 10, {});

  for(let attempt=1; attempt<=3; attempt++) {
    try {
      setFbLoading(10 + attempt*10);
      const r = await fetch("/refresh/options");
      const data = await r.json();
      (data.options || []).forEach(o=>{
        const opt = document.createElement("option");
        opt.value = o.value;
        opt.textContent = `${o.label} (${o.count})`;
        sel.appendChild(opt);
      });
      setFbLoading(100, "Fachbereiche geladen.");
      showStatus("Bereit", 0, {});
      setTimeout(()=> el("fb-loading-box").style.display="none", 300);
      sel.onchange = ()=> el("btnExportRf").disabled = !sel.value;
      return;
    } catch(e) {
      console.warn("loadOptions attempt failed", attempt, e);
      setFbLoading(10 + attempt*15, "Laden fehlgeschlagen – erneuter Versuch …");
      showStatus("Fachbereiche laden …", 10 + attempt*15, {});
      await new Promise(r=>setTimeout(r, 400));
    }
  }

  setFbLoading(100, "Fachbereiche konnten nicht geladen werden.");
  showStatus("Fehler beim Laden der Fachbereiche", 100, {});
}

async function loadExcluded(){
  try{
    const res = await fetch("/nachfass/excluded/json");
    const data = await res.json();

    const sum = (data.summary||[])
      .filter(x => (x.Anzahl||0) > 0)
      .map(x => `<li><b>${x.Anzahl}</b> – ${x.Grund}</li>`)
      .join("");
    el("excluded-summary").innerHTML = sum ? `<ul style="margin:0;padding-left:18px">${sum}</ul>` : "<span class='muted'>Keine Datensätze ausgeschlossen.</span>";

    const tb = el("excluded-table-body");
    const rows = data.rows || [];
    if(!rows.length){
      tb.innerHTML = `<tr><td colspan="5" style="text-align:center;color:#94a3b8">Keine entfernten Datensätze</td></tr>`;
      return;
    }
    tb.innerHTML = rows.map(r => `
      <tr>
        <td>${r["Kontakt ID"]||""}</td>
        <td>${r["Name"]||""}</td>
        <td>${r["Organisation ID"]||""}</td>
        <td>${r["Organisationsname"]||""}</td>
        <td>${r["Grund"]||""}</td>
      </tr>
    `).join("");
  }catch(e){
    console.warn("loadExcluded failed", e);
  }
}

async function startExport(){
  const btn = document.getElementById("btnExportRf");
  try{
    if(btn){ btn.disabled = true; btn.style.opacity = "0.7"; btn.style.pointerEvents = "none"; }
    const ids = parseBatchIds(el("nf_batch_ids")?.value || el("batch_ids")?.value || "");
    // nach Prozess unterschiedlich: Nachfass nutzt nf_batch_ids, NK/RF nutzen batch_ids
    // Validierung je nach vorhandenen Feldern:
    const isNachfass = !!document.getElementById("nf_batch_ids");
    if(isNachfass && !ids.length) return alert("Bitte mindestens eine Batch ID eintragen.");
    const batchField = document.getElementById("batch_id");
    const batchVal = (batchField ? (batchField.value||"").trim() : "");
    if(!batchVal) return alert("Bitte Batch ID eintragen.");

    showOverlay("Starte Abgleich …");
    setOverlayProgress(0);
    setIndeterminate(true);
    showStatus("Starte Abgleich …", 0, {});

    const payload = isNachfass
      ? { nf_batch_ids: ids, batch_id: batchVal, campaign: (el("campaign").value||"").trim() }
      : { filter_id: (el("filter_id")?.value || "").trim(), take_count: (el("take_count")?.value||"").trim(), batch_id: batchVal, campaign: (el("campaign").value||"").trim() };

    // Bei NK/RF kommt Fachbereich aus select:
    if(!isNachfass){
      const fb = el("fachbereich")?.value || "";
      payload.fachbereich = fb;
    }

    const r = await fetch("/refresh/export_start", {
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body: JSON.stringify(payload)
    });
    if(!r.ok){
      const txt = await r.text();
      throw new Error("Serverfehler ("+r.status+"): " + txt);
    }
    const res = await r.json();
    const job_id = res.job_id;
    if(!job_id) throw new Error("Kein job_id vom Server erhalten.");

    while(true){
      await new Promise(r=>setTimeout(r, 650));
      const pr = await fetch("/refresh/export_progress?job_id="+encodeURIComponent(job_id));
      if(!pr.ok){
        const txt = await pr.text();
        throw new Error("Progress-Fehler ("+pr.status+"): " + txt);
      }
      const s = await pr.json();

      if(s.error){
        setIndeterminate(false);
        hideOverlay();
        showStatus("Fehler", 100, s.stats||{});
        alert(s.error);
        return;
      }

      // UI update
      el("overlay-phase").textContent = `${s.phase}${s.detail ? " · " + s.detail : ""} (${s.percent}%)`;
      if(el("overlay-detail")) el("overlay-detail").textContent = s.detail || "";
      updateHeartbeat(s);
      setOverlayProgress(s.percent);
      showStatus(s.phase, s.percent, s.stats||{});

      if(s.download_ready){
        setIndeterminate(false);
        window.open("/refresh/export_download?job_id="+encodeURIComponent(job_id), "_blank");
        hideOverlay();
        await loadExcluded();
        showStatus("Fertig", 100, s.stats||{});
        const card = document.querySelectorAll("section.card")[1];
        if(card) card.scrollIntoView({behavior:"smooth"});
        return;
      }
    }
  }catch(e){
    console.error(e);
    setIndeterminate(false);
    hideOverlay();
    showStatus("Fehler", 100, {});
    alert(e?.message || String(e));
  }finally{
    if(btn){ btn.disabled = false; btn.style.opacity = "1"; btn.style.pointerEvents = "auto"; }
  }
}

window.addEventListenerwindow.addEventListener("load", async ()=>{
  await loadOptions();
  resetExcludedUI("Noch kein Abgleich gestartet.");
});
el("btnExportRf").onclick = startExport;
</script>

</body>
</html>
""")


@app.get("/refresh/summary", response_class=HTMLResponse)
async def refresh_summary(job_id: str = Query(...)):
    ready = await load_df_text("rf_master_ready")
    log   = await load_df_text("rf_delete_log")

    def count(df: pd.DataFrame, reason_keys: list) -> int:
        if df.empty or "reason" not in df.columns:
            return 0
        keys = [k.lower() for k in reason_keys]
        return int(df["reason"].astype(str).str.lower().isin(keys).sum())

    total   = len(ready)
    cnt_org = count(log, ["org_match_95"])
    cnt_pid = count(log, ["person_id_match"])
    removed = cnt_org + cnt_pid

    if not log.empty:
        view = log.tail(50).copy()
        view["Grund"] = view.apply(lambda r: f"{r['reason']} – {r['extra']}", axis=1)
        table_html = view[["id","name","org_name","Grund"]].to_html(index=False, border=0)
    else:
        table_html = "<i>Keine entfernt</i>"

    html = f"""
    <!doctype html><html lang="de"><head><meta charset="utf-8"/><title>Refresh – Ergebnis</title></head>
    <body style="font-family:Inter,sans-serif;max-width:1100px;margin:30px auto;padding:0 20px">
      <h2>Refresh – Ergebnis</h2>
      <ul>
        <li>Gesamt exportierte Zeilen: <b>{total}</b></li>
        <li>Organisationen ≥95 % Ähnlichkeit entfernt: <b>{cnt_org}</b></li>
        <li>Bereits kontaktierte Personen entfernt: <b>{cnt_pid}</b></li>
        <li><b>Gesamt entfernt: {removed}</b></li>
      </ul>
      <h3>Letzte Ausschlüsse</h3>
      {table_html}
      <p><a href="/campaign">Zur Übersicht</a></p>
    </body></html>"""
    return HTMLResponse(html)


# =============================================================================
# Refresh – Excluded JSON / HTML
# =============================================================================

async def build_excluded_payload(prefix: str) -> dict:
    """Einheitliches Payload für 'Entfernte Datensätze' (Summary + Zeilen).

    Summary wird primär aus *_delete_log berechnet (tatsächlich entfernte Datensätze),
    fallback auf *_excluded (falls delete_log leer ist).
    """
    def flatten(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        if isinstance(v, list):
            return flatten(v[0] if v else "")
        if isinstance(v, dict):
            return flatten(v.get("value") or v.get("label") or v.get("name") or v.get("id") or "")
        return str(v)

    excluded_df = await load_df_text(f"{prefix}_excluded")
    deleted_df  = await load_df_text(f"{prefix}_delete_log")

    # Summary
    summary = []
    if not deleted_df.empty:
        base_col = None
        for c in ("Grund", "reason", "extra"):
            if c in deleted_df.columns:
                base_col = c
                break
        if base_col:
            s = deleted_df[base_col].fillna("").astype(str).map(lambda x: x.strip())
            s = s[(s != "") & (s.str.lower() != "nan")]
            for grund, cnt in s.value_counts().items():
                summary.append({"label": flatten(grund), "count": int(cnt), "Grund": flatten(grund), "Anzahl": int(cnt)})

    if (not summary) and (not excluded_df.empty):
        for _, r in excluded_df.iterrows():
            cnt0 = int(r.get("Anzahl") or 0)
            if cnt0 > 0:
                g0 = flatten(r.get("Grund"))
                summary.append({"label": g0, "count": cnt0, "Grund": g0, "Anzahl": cnt0})

    # Rows
    rows = []
    if not deleted_df.empty:
        deleted_df = deleted_df.replace({None: "", np.nan: ""})
        for _, r in deleted_df.iterrows():
            rows.append({
                "Kontakt ID": flatten(r.get("Kontakt ID") or r.get("id")),
                "Name": flatten(r.get("Name") or r.get("name")),
                "Organisation ID": flatten(r.get("Organisation ID")),
                "Organisationsname": flatten(r.get("Organisationsname") or r.get("org_name")),
                "Grund": flatten(r.get("Grund") or r.get("reason") or r.get("extra")),
            })

    return {"summary": summary, "total": len(rows), "rows": rows}


@app.get("/refresh/excluded/json")
async def refresh_excluded_json():
    return JSONResponse(await build_excluded_payload("rf"))


@app.get("/refresh/excluded", response_class=HTMLResponse)
async def refresh_excluded():
    html = r"""
    <!DOCTYPE html><html lang="de"><head><meta charset="UTF-8">
    <title>Refresh – Nicht berücksichtigte Datensätze</title>
    <style>
      body{font-family:Inter,sans-serif;margin:30px auto;max-width:1100px;
           padding:0 20px;background:#f6f8fb;}
      table{width:100%;border-collapse:collapse}
      th,td{border-bottom:1px solid #e5e7eb;padding:8px 10px;text-align:left}
      th{background:#f1f5f9;font-weight:600}
      tr:hover td{background:#f9fafb}
      .center{text-align:center;color:#6b7280}
    </style></head><body>
    <h2>Refresh – Nicht berücksichtigte Datensätze</h2>
    <table><thead><tr>
      <th>Kontakt ID</th><th>Name</th><th>Organisation ID</th>
      <th>Organisationsname</th><th>Grund</th>
    </tr></thead><tbody id="excluded-table-body">
      <tr><td colspan="5" class="center">Lade Daten…</td></tr>
    </tbody></table>
    <script>
      async function loadExcluded(){
        const r=await fetch('/refresh/excluded/json');
        const data=await r.json();
        const body=document.getElementById('excluded-table-body'); body.innerHTML='';
        if(!data.rows||!data.rows.length){
          body.innerHTML='<tr><td colspan="5" class="center">Keine Datensätze ausgeschlossen</td></tr>'; return;
        }
        for(const row of data.rows){
          const tr=document.createElement('tr');
          tr.innerHTML=`<td>${row["Kontakt ID"]||""}</td>
                        <td>${row["Name"]||""}</td>
                        <td>${row["Organisation ID"]||""}</td>
                        <td>${row["Organisationsname"]||""}</td>
                        <td>${row["Grund"]||""}</td>`;
          body.appendChild(tr);
        }
      }
      loadExcluded();
    </script></body></html>"""
    return HTMLResponse(html)

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
    log   = await load_df_text("nf_delete_log")

    # Sicherheits-Normalisierung
    for col in ready.columns:
        ready[col] = ready[col].astype(str)

    total = len(ready)

    def count_reason(df, keys):
        if df.empty: return 0
        return df["reason"].astype(str).str.lower().isin([k.lower() for k in keys]).sum()

    cnt_org95 = count_reason(log, ["org_match_95"])
    cnt_pid   = count_reason(log, ["person_id_match"])
    cnt_orgart = count_reason(log, ["org_art_not_empty"])
    cnt_nextact = count_reason(log, ["forbidden_activity_date"])

    removed = cnt_org95 + cnt_pid + cnt_orgart + cnt_nextact

    # Letzte 50 Details
    if not log.empty:
        view = log.tail(50).copy()
        view["Grund"] = view["reason"].astype(str) + " – " + view["extra"].astype(str)
        table_html = view[["id","name","org_name","Grund"]].to_html(index=False, border=0)
    else:
        table_html = "<i>Keine entfernt</i>"

    html = f"""
    <!doctype html><html><body style='font-family:Inter;max-width:900px;margin:40px auto'>
    <h2>Nachfass – Ergebnis</h2>
    <ul>
      <li>Exportierte Zeilen: <b>{total}</b></li>
      <li>Orga ≥95% Ähnlichkeit: <b>{cnt_org95}</b></li>
      <li>Person-ID Dubletten: <b>{cnt_pid}</b></li>
      <li>Organisationsart gefüllt: <b>{cnt_orgart}</b></li>
      <li>Nächste Aktivität blockiert: <b>{cnt_nextact}</b></li>
      <li><b>Gesamt entfernt: {removed}</b></li>
    </ul>

    <h3>Letzte 50 entfernte Datensätze</h3>
    {table_html}

    <p><a href='/campaign'>Zur Übersicht</a></p>
    </body></html>
    """
    return HTMLResponse(html)

# =============================================================================
# EXCLUDED + SUMMARY + DEBUG (FINAL & KOMPATIBEL)
# =============================================================================
@app.get("/nachfass/excluded/json")
async def nachfass_excluded_json():
    return JSONResponse(await build_excluded_payload("nf"))

@app.get("/neukontakte/excluded/json")
async def neukontakte_excluded_json():
    return JSONResponse(await build_excluded_payload("nk"))

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
# REFRESH – SUMMARY-SEITE (analog Nachfass)
# =============================================================================
@app.get("/refresh/summary", response_class=HTMLResponse)
async def refresh_summary(job_id: str = Query(...)):
    ready = await load_df_text("rf_master_ready")
    log   = await load_df_text("rf_delete_log")

    total = len(ready)

    def count_reason(df, keys):
        if df.empty: return 0
        return df["reason"].astype(str).str.lower().isin([k.lower() for k in keys]).sum()

    cnt_org95 = count_reason(log, ["org_match_95"])
    cnt_pid   = count_reason(log, ["person_id_match"])
    cnt_orgart = count_reason(log, ["org_art_not_empty"])
    cnt_nextact = count_reason(log, ["forbidden_activity_date"])

    removed = cnt_org95 + cnt_pid + cnt_orgart + cnt_nextact

    if not log.empty:
        view = log.tail(50).copy()
        view["Grund"] = view["reason"].astype(str) + " – " + view["extra"].astype(str)
        table_html = view[["id","name","org_name","Grund"]].to_html(index=False, border=0)
    else:
        table_html = "<i>Keine entfernt</i>"

    html = f"""
    <!doctype html><html><body style='font-family:Inter;max-width:900px;margin:40px auto'>
    <h2>Refresh – Ergebnis</h2>
    <ul>
      <li>Exportierte Zeilen: <b>{total}</b></li>
      <li>Orga ≥95% Ähnlichkeit: <b>{cnt_org95}</b></li>
      <li>Person-ID Dubletten: <b>{cnt_pid}</b></li>
      <li>Organisationsart gefüllt: <b>{cnt_orgart}</b></li>
      <li>Nächste Aktivität blockiert: <b>{cnt_nextact}</b></li>
      <li><b>Gesamt entfernt: {removed}</b></li>
    </ul>

    <h3>Letzte 50 entfernte Datensätze</h3>
    {table_html}

    <p><a href='/campaign'>Zur Übersicht</a></p>
    </body></html>
    """
    return HTMLResponse(html)

# =============================================================================
# REFRESH – Excluded JSON
# =============================================================================
@app.get("/refresh/excluded/json2")
async def refresh_excluded_json():

    # -----------------------------------------------------
    # Hilfsfunktionen
    # -----------------------------------------------------
    def flat(v):
        if v is None: return ""
        if isinstance(v, float) and pd.isna(v): return ""
        return str(v)

    # -----------------------------------------------------
    # Tabellen laden
    # -----------------------------------------------------
    excluded_df = await load_df_text("rf_excluded")     # 2-Kontakte-Regel
    delete_df   = await load_df_text("rf_delete_log")   # Fuzzy / ID / Activity / OrgaArt

    summary = []
    rows = []

    # -----------------------------------------------------
    # 1) 2-Kontakte-Regel (rf_excluded)
    # -----------------------------------------------------
    if not excluded_df.empty:
        for _, r in excluded_df.iterrows():
            summary.append({
                "Grund": flat(r.get("Grund")),
                "Anzahl": int(r.get("Anzahl") or 0)
            })

    # -----------------------------------------------------
    # 2) Abgleich-Ausschlüsse (rf_delete_log)
    #    Gründe enthalten u.a.:
    #    - org_match_95
    #    - person_id_match
    #    - forbidden_activity_date
    #    - org_art_not_empty
    # -----------------------------------------------------
    if not delete_df.empty:

        # Tabellenzeilen für UI
        for _, r in delete_df.iterrows():
            rows.append({
                "Kontakt ID":        flat(r.get("id") or r.get("Kontakt ID")),
                "Name":              flat(r.get("name") or r.get("Name")),
                "Organisation ID":   flat(r.get("org_id") or r.get("Organisation ID")),
                "Organisationsname": flat(r.get("org_name") or r.get("Organisationsname")),
                "Grund":             flat(r.get("Grund") or r.get("extra") or r.get("reason")),
            })

        # Gruppierung nach Gründen
        reason_counts = (
            delete_df["reason"]
            .fillna("")
            .astype(str)
            .str.lower()
            .value_counts()
        )

        # Hilfsfunktion für Summen
        def add_reason(label, keys):
            count = int(sum(reason_counts.get(k.lower(), 0) for k in keys))
            if count > 0:
                summary.append({"Grund": label, "Anzahl": count})

        add_reason("Fuzzy Orga ≥95%", ["org_match_95"])
        add_reason("Personen-ID Dublette", ["person_id_match"])
        add_reason("Nächste Aktivität blockiert", ["forbidden_activity_date"])
        add_reason("Organisationsart gefüllt", ["org_art_not_empty"])

    # -----------------------------------------------------
    # Falls GAR nichts ausgeschlossen wurde → Hinweis
    # -----------------------------------------------------
    if not summary:
        summary.append({"Grund": "Keine Datensätze ausgeschlossen", "Anzahl": 0})

    return JSONResponse({
        "summary": summary,
        "total": len(rows),
        "rows": rows
    })

# =============================================================================
# REFRESH – Excluded HTML
# =============================================================================
@app.get("/refresh/excluded", response_class=HTMLResponse)
async def refresh_excluded():

    html = r"""
    <!DOCTYPE html>
    <html lang="de">
    <head>
      <meta charset="UTF-8">
      <title>Refresh – Nicht berücksichtigte Datensätze</title>
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
        .center { text-align: center; color: #6b7280; }
      </style>
    </head>
    <body>

      <h2>Refresh – Nicht berücksichtigte Datensätze</h2>

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
          <tr><td colspan="5" class="center">Lade Daten…</td></tr>
        </tbody>
      </table>

      <script>
        async function loadExcludedTable() {
          try {
            const r = await fetch('/refresh/excluded/json');
            const data = await r.json();
            const body = document.getElementById('excluded-table-body');
            body.innerHTML = '';

            if (!data.rows || !data.rows.length) {
              body.innerHTML = '<tr><td colspan="5" class="center">Keine Datensätze ausgeschlossen</td></tr>';
              return;
            }

            for (const row of data.rows) {
              const tr = document.createElement('tr');
              tr.innerHTML = `
                <td>${row["Kontakt ID"] || ""}</td>
                <td>${row["Name"] || ""}</td>
                <td>${row["Organisation ID"] || ""}</td>
                <td>${row["Organisationsname"] || ""}</td>
                <td>${row["Grund"] || ""}</td>
              `;
              body.appendChild(tr);
            }
          } catch (err) {
            console.error("Fehler bei excluded:", err);
            document.getElementById('excluded-table-body').innerHTML =
              '<tr><td colspan="5" class="center" style="color:red">Fehler beim Laden</td></tr>';
          }
        }

        loadExcludedTable();
      </script>

    </body>
    </html>
    """
    return HTMLResponse(html)


# =============================================================================
# MODUL 6 – FINALER JOB-/WORKFLOW FÜR NACHFASS (EXPORT/PROGRESS/DOWNLOAD)
# =============================================================================
from uuid import uuid4
from fastapi import Request
from fastapi.responses import JSONResponse
import asyncio

@app.post("/nachfass/export_start")
async def nachfass_export_start(req: Request):
    body = await req.json()

    nf_batch_ids = body.get("nf_batch_ids") or []
    batch_id     = body.get("batch_id") or ""
    campaign     = body.get("campaign") or ""

    # optional: falls Frontend keine Filter sendet -> Default auf FILTER_NACHFASS
    filters = body.get("filters") or [FILTER_NACHFASS]

    job_id = str(uuid4())
    job = Job()
    JOBS[job_id] = job
    # Reset entfernte Datensätze / Löschlog für neuen Lauf
    t = tables("nf")
    await save_df_text(pd.DataFrame(columns=DELETE_LOG_COLUMNS), t["log"])
    await save_df_text(pd.DataFrame(columns=["Grund","Anzahl"]), t["excluded"])

    # Job-Inputs speichern (optional, aber ok)
    job.nf_batch_ids = nf_batch_ids
    job.batch_id     = batch_id
    job.campaign     = campaign
    job.filters      = filters

    # WICHTIG: richtiger Aufruf (job_obj statt job=)
    asyncio.create_task(
        run_nachfass_job(
            job_obj=job,
            job_id=job_id,
            campaign=campaign,
            filters=filters,
            nf_batch_ids=nf_batch_ids,
        )
    )

    return JSONResponse({"job_id": job_id})


# =============================================================================
# Fortschritt abfragen
# =============================================================================
@app.get("/nachfass/export_progress")
async def nachfass_export_progress(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        return JSONResponse({"error": "Job nicht gefunden"}, status_code=404)

    has_file = bool(getattr(job, "path", None)) and os.path.exists(str(getattr(job, "path", "")))

    return JSONResponse({
        "phase": str(job.phase),
        "percent": int(job.percent),
        "done": bool(job.done),
        "error": str(job.error) if job.error else None,
        "stats": dict(getattr(job, "stats", {}) or {}),
        "detail": str(getattr(job, "detail", "") or ""),
        "last_update_ms": int(getattr(job, "last_update_ms", 0) or 0),
        "heartbeat": int(getattr(job, "heartbeat", 0) or 0),
        "has_file": has_file,
        "download_ready": bool(job.done) and (job.error is None) and has_file
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
# PROGRESS UND DOWNLOAD REFRESH
# =============================================================================
@app.get("/refresh/export_progress")
async def refresh_export_progress(job_id: str = Query(...)):
    job = JOBS.get(job_id)
    if not job:
        return JSONResponse({"error": "Job nicht gefunden"}, status_code=404)

    has_file = bool(getattr(job, "path", None)) and os.path.exists(str(getattr(job, "path", "")))

    return JSONResponse({
        "phase": str(job.phase),
        "percent": int(job.percent),
        "done": bool(job.done),
        "error": str(job.error) if job.error else None,
        "stats": dict(getattr(job, "stats", {}) or {}),
        "detail": str(getattr(job, "detail", "") or ""),
        "last_update_ms": int(getattr(job, "last_update_ms", 0) or 0),
        "heartbeat": int(getattr(job, "heartbeat", 0) or 0),
        "has_file": has_file,
        "download_ready": bool(job.done) and (job.error is None) and has_file
    })



# =============================================================================
# Download
# =============================================================================
@app.get("/nachfass/export_download")
async def nachfass_export_download(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        return JSONResponse({"error": "Job nicht gefunden"}, status_code=404)

    if job.error:
        return JSONResponse({"error": job.error}, status_code=400)

    if not getattr(job, "path", None):
        return JSONResponse({"error": "Keine Datei gefunden"}, status_code=404)

    return FileResponse(
        job.path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"{slugify_filename(job.filename_base or 'Nachfass_Export')}.xlsx"
    )


# =============================================================================
# Download – Refresh (analog zu Nachfass)
# =============================================================================
@app.get("/refresh/export_download")
async def refresh_export_download(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        return JSONResponse({"error": "Job nicht gefunden"}, status_code=404)

    if job.error:
        return JSONResponse({"error": job.error}, status_code=400)

    if not getattr(job, "path", None):
        return JSONResponse({"error": "Keine Datei gefunden"}, status_code=404)

    return FileResponse(
        job.path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"{slugify_filename(job.filename_base or 'Refresh_Export')}.xlsx"
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
