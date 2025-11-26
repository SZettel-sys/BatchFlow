# master_fixed_v2_part1.py — Teil 1/5
# Basis: BatchFlow (FastAPI + Pipedrive + Neon)
# Getestet für Render Free Tier (Python 3.12, 512 MB RAM)

import os, re, io, uuid, time, asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, AsyncGenerator
import numpy as np, pandas as pd, json, httpx, asyncpg
from rapidfuzz import fuzz, process
from fastapi import FastAPI, Request, Body, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware
from collections import defaultdict

fuzz.default_processor = lambda s: s  # kein Vor-Preprocessing

# -----------------------------------------------------------------------------
# App-Grundkonfiguration allgemein (start)
# -----------------------------------------------------------------------------
app = FastAPI(title="BatchFlow")
app.add_middleware(GZipMiddleware, minimum_size=1024)
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# -----------------------------------------------------------------------------
# Umgebungsvariablen & Konstanten setzen
# -----------------------------------------------------------------------------
PD_API_TOKEN = os.getenv("PD_API_TOKEN", "")
PIPEDRIVE_API = "https://api.pipedrive.com/v1"
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
    async with get_pool().acquire() as conn:
        await clear_table(conn, table)
        await ensure_table_text(conn, table, list(df.columns))
        if df.empty: return
        cols, cols_sql = list(df.columns), ", ".join(f'"{c}"' for c in df.columns)
        ph = ", ".join(f'${i}' for i in range(1, len(cols)+1))
        sql = f'INSERT INTO "{SCHEMA}"."{table}" ({cols_sql}) VALUES ({ph})'
        batch=[]
        async with conn.transaction():
            for _, row in df.iterrows():
                vals = ["" if pd.isna(v) else str(v) for v in row.tolist()]
                batch.append(vals)
                if len(batch)>=1000:
                    await conn.executemany(sql, batch); batch=[]
            if batch: await conn.executemany(sql, batch)

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


async def load_df_text(table:str)->pd.DataFrame:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(f'SELECT * FROM "{SCHEMA}"."{table}"')
    if not rows: return pd.DataFrame()
    cols=list(rows[0].keys())
    return pd.DataFrame([{c:r[c] for c in cols} for r in rows]).replace({"":np.nan})
# master_fixed_v2_part2.py — Teil 2/5
# Pipedrive-Anbindung, Streaming und Nachfass-Datenaufbau (parallel, performant)

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
# PERSONENFELDER (Cache)
# =============================================================================
async def get_person_fields() -> List[dict]:
    """Lädt Personenfelder aus Pipedrive (Cache wird genutzt)."""
    global _PERSON_FIELDS_CACHE
    if _PERSON_FIELDS_CACHE is not None:
        return _PERSON_FIELDS_CACHE
    url = append_token(f"{PIPEDRIVE_API}/personFields")
    r = await http_client().get(url, headers=get_headers())
    r.raise_for_status()
    _PERSON_FIELDS_CACHE = r.json().get("data") or []
    return _PERSON_FIELDS_CACHE

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
# STREAMING-FUNKTIONEN (mit Paging)
# =============================================================================

async def stream_organizations_by_filter(filter_id: int, page_limit: int = 500):
    """
    Holt Organisationen stabil per FILTER (nie Search).
    Verursacht keinen 429 und keinen 'term'-Fehler.
    """
    start = 0

    while True:
        url = append_token(
            f"{PIPEDRIVE_API}/organizations?filter_id={filter_id}&start={start}&limit={page_limit}"
        )

        r = await http_client().get(url, headers=get_headers())

        if r.status_code == 429:
            print(f"[WARN][ORG-STREAM] Rate limit 429 → warte 2 Sekunden…")
            await asyncio.sleep(2)
            continue

        if r.status_code != 200:
            print(f"[ERROR][ORG-STREAM] Filter {filter_id}: {r.text}")
            return

        data = (r.json().get("data") or {}).get("items") or []

        if not data:
            break

        yield data

        if len(data) < page_limit:
            break

        start += len(data)
        await asyncio.sleep(0.1)


async def stream_person_ids_by_filter(filter_id: int, page_limit: int = PAGE_LIMIT) -> AsyncGenerator[List[str], None]:
    """Streamt nur Personen-IDs – robust gegen Pipedrive-Listenstrukturen."""

    start = 0
    while True:
        url = append_token(
            f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={page_limit}&sort=id"
        )
        r = await http_client().get(url, headers=get_headers())

        if r.status_code != 200:
            raise Exception(f"Pipedrive Fehler IDs {filter_id}: {r.text}")

        raw = r.json().get("data") or []
        if not raw:
            break

        ids: List[str] = []

        # ------------------------------
        # ROBUSTE EXTRAKTION (FIX)
        # ------------------------------
        def extract(obj):
            """Extrahiert IDs aus dicts und Listen."""
            if obj is None:
                return
            if isinstance(obj, dict):
                _id = obj.get("id")
                if _id:
                    ids.append(str(_id))
            elif isinstance(obj, list):
                for sub in obj:
                    extract(sub)

        for entry in raw:
            extract(entry)

        if ids:
            yield ids

        if len(raw) < page_limit:
            break

        start += len(raw)

# =============================================================================
# Organisationen – Bucketing + Kappung (Performanceoptimiert)
# =============================================================================
async def _fetch_org_names_for_filter_capped(
    filter_id: int,
    page_limit: int,
    cap_total: int,
    cap_bucket: int
) -> Dict[str, List[str]]:
    """
    Holt Organisationsnamen aus einem Pipedrive-Filter, normalisiert sie und
    legt sie in Buckets nach Anfangsbuchstaben. Die Gesamtanzahl (cap_total)
    und die maximale Bucketgröße (cap_bucket) werden begrenzt.
    """
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
    sem = asyncio.Semaphore(6)  # gedrosselt, um 429 zu vermeiden

    async def fetch_one(bid: str):
        start = 0
        total = 0
        local: List[dict] = []

        async with sem:
            while True:
                url = append_token(
                    f"{PIPEDRIVE_API}/persons/search?"
                    f"term={bid}&fields=custom_fields&start={start}&limit={page_limit}"
                )
                r = await http_client().get(url, headers=get_headers())

                if r.status_code == 429:
                    print("[WARN] Rate limit erreicht, warte 2 Sekunden ...")
                    await asyncio.sleep(2)
                    continue

                if r.status_code != 200:
                    print(f"[WARN] Batch {bid} Fehler: {r.text}")
                    break

                raw_items = r.json().get("data", {}).get("items", []) or []
                if not raw_items:
                    break

                # ---------- ROBUSTE ITEMS-EXTRAKTION (FIX) ----------
                persons: List[dict] = []

                def add_item(obj):
                    # rekursiv durch Listen/Dicts laufen und "item" einsammeln
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
                # ---------------------------------------------------

                if not persons:
                    break

                local.extend(persons)
                total += len(persons)
                start += len(persons)

                if len(persons) < page_limit:
                    break

                await asyncio.sleep(0.1)  # minimale Pause zwischen Seiten

        print(f"[DEBUG] Batch {bid}: {total} Personen geladen")
        results.extend(local)

    await asyncio.gather(*(fetch_one(bid) for bid in batch_ids))
    print(f"[INFO] Alle Batch-IDs geladen: {len(results)} Personen gesamt")
    return results



# =============================================================================
# Nachfass – Aufbau Master (robust, progressiv & vollständig)
# =============================================================================
import asyncio
async def fetch_person_details(person_ids: List[str]) -> List[dict]:
    """Lädt vollständige Datensätze für Personen-IDs parallel (inkl. 429-Retry, Organisation & Dedup)."""

    results = []
    sem = asyncio.Semaphore(8)   # performant & sicher

    async def fetch_one(pid):
        retries = 5
        while retries > 0:
            try:
                async with sem:

                    # --- WICHTIG ---
                    # return_all_custom_fields=1     → liefert alle Custom-Felder inkl. Label
                    # include=organization,organization_fields → liefert vollständige Organisation (id+name)
                    url = append_token(
                        f"{PIPEDRIVE_API}/persons/{pid}"
                        "?return_all_custom_fields=1"
                        "&include=organization,organization_fields"
                    )

                    r = await http_client().get(url, headers=get_headers())

                    # Rate Limit (429)
                    if r.status_code == 429:
                        await asyncio.sleep(2)
                        retries -= 1
                        continue

                    # Erfolg
                    if r.status_code == 200:
                        data = r.json().get("data")
                        if data:
                            results.append(data)
                        break

                # Eventloop kurz freigeben
                await asyncio.sleep(0.05)

            except Exception:
                retries -= 1
                await asyncio.sleep(1)

    # Personen in stabile Chunks aufteilen
    chunks = [person_ids[i:i+100] for i in range(0, len(person_ids), 100)]
    print("[DEBUG] fetch: Starte mit IDs:", len(person_ids))

    for group in chunks:
        await asyncio.gather(*(fetch_one(pid) for pid in group))


    # --------------------------------------------------------------
    # DUPLIKAT-SCHUTZ → entfernt doppelte Personen-IDs
    # --------------------------------------------------------------
    unique = {}
    for p in results:
        unique[p["id"]] = p

    results = list(unique.values())

    print(f"[DEBUG] Vollständige Personendaten geladen (unique): {len(results)}")
    loaded_ids = {p["id"] for p in results}
    missing = set(person_ids) - loaded_ids

    print("[DEBUG] fetch: Vollständige geladen:", len(results))
    print("[DEBUG] fetch: Fehlende:", len(missing))
    print("[DEBUG] fetch: Fehlende IDs:", missing)
    return results


# -----------------------------------------------------------------------------
# INTERNER CACHE
# -----------------------------------------------------------------------------
_NEXT_ACTIVITY_KEY: Optional[str] = None
_LAST_ACTIVITY_KEY: Optional[str] = None
_BATCH_FIELD_KEY: Optional[str] = None

# -----------------------------------------------------------------------------
# PIPEDRIVE HILFSFUNKTIONEN
# -----------------------------------------------------------------------------
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
    Liefert Personen seitenweise (Paging) aus einem Pipedrive-Filter.
    Verwendung: async for chunk in stream_persons_by_filter(...): ...
    """
    start = 0
    while True:
        url = append_token(
            f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={page_limit}&sort=id"
        )
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive Fehler: {r.text}")
        data = (r.json() or {}).get("data") or []
        if not data:
            break
        yield data
        if len(data) < page_limit:
            break
        start += len(data)

# -----------------------------------------------------------------------------
# build_nf_master
# -----------------------------------------------------------------------------
async def _build_nf_master_final(
    nf_batch_ids: List[str],
    batch_id: str,
    campaign: str,
    job_obj=None
) -> pd.DataFrame:

    # ============================================================
    # Hilfsfunktionen
    # ============================================================

    def sanitize(v):
        """Konvertiert beliebige Pipedrive-Werte (list/dict/None/etc.) in Strings."""
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""

        # Strings mit JSON-Inhalt
        if isinstance(v, str):
            v2 = v.strip()
            if v2.startswith("[") and v2.endswith("]"):
                try:
                    parsed = json.loads(v2)
                    return sanitize(parsed)
                except:
                    return v2
            if v2.startswith("{") and v2.endswith("}"):
                try:
                    parsed = json.loads(v2)
                    return sanitize(parsed)
                except:
                    return v2
            return v2

        # Dict → gängige Keys extrahieren
        if isinstance(v, dict):
            return (
                sanitize(v.get("value"))
                or sanitize(v.get("label"))
                or sanitize(v.get("name"))
                or sanitize(v.get("id"))
                or ""
            )

        # Liste → nur erstes Element
        if isinstance(v, list):
            return sanitize(v[0]) if v else ""

        return str(v)

    def cf(p, key):
        return sanitize(p.get(key))

    # ============================================================
    # Personen laden (NEU: Filterbasiert)
    # ============================================================

    if job_obj:
        job_obj.phase = "Lade Nachfass-Kandidaten …"
        job_obj.percent = 10

    persons = await stream_persons_by_batch_id(
        "5ac34dad3ea917fdef4087caebf77ba275f87eec",   # Batch-ID-Feld
        nf_batch_ids
    )

    # ============================================================
    # Filterregeln
    # ============================================================

    today = datetime.now().date()

    def is_date_valid(raw):
        raw = sanitize(raw)
        if not raw:
            return True
        try:
            dt = datetime.fromisoformat(raw[:10]).date()
        except:
            return True
        if dt > today:
            return False
        if (today - dt).days <= 90:
            return False
        return True

    selected = []
    org_counter = defaultdict(int)
    count_org_limit = 0
    count_date_invalid = 0

    for p in persons:

        # ---------------- ORGANISATION sicher extrahieren ----------------
        org = p.get("organization")

        if isinstance(org, list):
            org = org[0] if org else {}

        if not isinstance(org, dict):
            org = {}

        org_id = sanitize(org.get("id"))

        # ---------------- Datum prüfen ----------------
        if not is_date_valid(p.get("next_activity_date")):
            count_date_invalid += 1
            continue

        # ---------------- Max. 2 pro Organisation ----------------
        if org_id:
            org_counter[org_id] += 1
            if org_counter[org_id] > 2:
                count_org_limit += 1
                continue

        selected.append(p)

    # ============================================================
    # Export-Zeilen erzeugen
    # ============================================================

    rows = []

    for p in selected:

        # -------- PERSON --------
        pid = sanitize(p.get("id"))
        first = sanitize(p.get("first_name"))
        last = sanitize(p.get("last_name"))
        fullname = sanitize(p.get("name"))

        if not first and not last and fullname:
            parts = fullname.split()
            first = " ".join(parts[:-1]) if len(parts) > 1 else parts[0]
            last = parts[-1] if len(parts) > 1 else ""

        # -------- ORGANISATION --------
        org = p.get("organization")
        if isinstance(org, list):
            org = org[0] if org else {}
        if not isinstance(org, dict):
            org = {}

        org_id = sanitize(org.get("id"))
        org_name = sanitize(org.get("name"))

        # -------- E-MAIL (verschachtelt flatten) --------
        email_field = p.get("email") or []
        emails_flat = []

        def flatten_email(x):
            if isinstance(x, dict):
                emails_flat.append(x)
            elif isinstance(x, list):
                for i in x:
                    flatten_email(i)

        flatten_email(email_field)

        email = ""
        for e in emails_flat:
            if e.get("primary"):
                email = sanitize(e.get("value"))
                break

        if not email and emails_flat:
            email = sanitize(emails_flat[0].get("value"))

        # -------- Exportzeile --------
        rows.append({
            "Batch ID": sanitize(batch_id),
            "Channel": DEFAULT_CHANNEL,
            "Cold-Mailing Import": sanitize(campaign),

            "Person ID": pid,
            "Person Vorname": first,
            "Person Nachname": last,
            "Person Titel": cf(p, "0343bc43a91159aaf33a463ca603dc5662422ea5"),
            "Person Geschlecht": cf(p, "c4f5f434cdb0cfce3f6d62ec7291188fe968ac72"),
            "Person Position": cf(p, "4585e5de11068a3bccf02d8b93c126bcf5c257ff"),
            "Person E-Mail": email,

            "Prospect ID": cf(p, "f9138f9040c44622808a4b8afda2b1b75ee5acd0"),

            "Organisation ID": org_id,
            "Organisation Name": org_name,

            "XING Profil": cf(p, "44ebb6feae2a670059bc5261001443a2878a2b43"),
            "LinkedIn URL": cf(p, "25563b12f847a280346bba40deaf527af82038cc"),
        })

    df = pd.DataFrame(rows).replace({None: ""})

    # ============================================================
    # Excluded speichern
    # ============================================================

    excluded = [
        {"Grund": "Max 2 Kontakte pro Organisation", "Anzahl": count_org_limit},
        {"Grund": "Datum nächste Aktivität steht an bzw. liegt in naher Vergangenheit", "Anzahl": count_date_invalid},
    ]
    await save_df_text(pd.DataFrame(excluded), "nf_excluded")

    # final speichern
    await save_df_text(df, "nf_master_final")

    if job_obj:
        job_obj.phase = "Nachfass-Master erstellt"
        job_obj.percent = 80

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
# Fuzzy-Bucket-Ladung (Orga-Dubletten-Abgleich)
# =============================================================================
async def _fetch_org_names_for_filter_capped(
    filter_id: int, page_limit: int, cap_total: int, cap_bucket: int
) -> Dict[str, List[str]]:
    """
    Holt Organisationsnamen aus Pipedrive-Filter.
    • normalized
    • alphabetisch gebucket
    • capped pro Bucket & Gesamtanzahl
    """
    buckets: Dict[str, List[str]] = {}
    total = 0

    async for chunk in stream_organizations_by_filter(filter_id, page_limit):
        for o in chunk:
            n = normalize_name(o.get("name") or "")
            if not n:
                continue

            b = bucket_key(n)
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
# _reconcile_nf
# =============================================================================
async def _reconcile(prefix: str) -> None:
    """
    Bereinigte und robuste Version des Nachfass-Abgleichs.
    Verhindert zuverlässig 'list' object has no attribute 'get'.
    """

    t = tables(prefix)
    df = await load_df_text(t["final"])

    if df.empty:
        await save_df_text(pd.DataFrame(), t["ready"])
        await save_df_text(pd.DataFrame(columns=[
            "reason", "Kontakt ID", "Name",
            "Organisation ID", "Organisationsname", "Grund"
        ]), t["log"])
        return

    col_pid = "Person ID"
    col_orgname = "Organisation Name"
    col_orgid = "Organisation ID"

    delete_rows = []
    drop_idx = []

    # =======================================================
    # UNIVERSAL SANITIZER – global & sicher
    # =======================================================
    def sanitize_cell(v):
        """Konvertiert jede DF-Zelle zu einem stabilen String."""
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""

        # JSON-Strings → dekodieren
        if isinstance(v, str):
            s = v.strip()
            if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
                try:
                    parsed = json.loads(s)
                    return sanitize_cell(parsed)
                except Exception:
                    return s
            return s

        # dict → value extrahieren
        if isinstance(v, dict):
            return (
                sanitize_cell(v.get("value")) or
                sanitize_cell(v.get("label")) or
                sanitize_cell(v.get("name")) or
                sanitize_cell(v.get("id")) or
                ""
            )

        # list → erstes Element extrahieren
        if isinstance(v, list):
            return sanitize_cell(v[0]) if v else ""

        return str(v)

    # =======================================================
    # Organisationsliste laden (für Fuzzy Matching)
    # =======================================================

    filter_ids_org = [1245, 851, 1521]
    buckets_all = {}
    total_collected = 0

    for fid in filter_ids_org:
        caps_left = MAX_ORG_NAMES - total_collected
        if caps_left <= 0:
            break

        sub = await _fetch_org_names_for_filter_capped(
            fid, PAGE_LIMIT, caps_left, MAX_ORG_BUCKET
        )

        for key, vals in sub.items():
            b = buckets_all.setdefault(key, [])
            for v in vals:
                if v not in b:
                    b.append(v)
                    total_collected += 1

    # =======================================================
    # 1) Fuzzy Matching auf Organisationen
    # =======================================================

    for idx, row in df.iterrows():

        name_raw = row.get(col_orgname, "")
        name_clean = sanitize_cell(name_raw)
        norm = normalize_name(name_clean)

        if not norm:
            continue

        b = bucket_key(norm)
        bucket = buckets_all.get(b)
        if not bucket:
            continue

        # Kandidaten für fuzzy
        near = [n for n in bucket if abs(len(n) - len(norm)) <= 4]
        if not near:
            continue

        best = process.extractOne(norm, near, scorer=fuzz.token_sort_ratio)

        if best and best[1] >= 95:

            # saubere Organisation ID
            orgid_clean = sanitize_cell(row.get(col_orgid))

            # Logging
            delete_rows.append({
                "reason": "org_match_95",
                "Kontakt ID": sanitize_cell(row.get(col_pid)),
                "Name": sanitize_cell(f"{row.get('Person Vorname','')} {row.get('Person Nachname','')}").strip(),
                "Organisation ID": orgid_clean,
                "Organisationsname": name_clean,
                "Grund": f"Ähnlichkeit {best[1]}% mit '{best[0]}'"
            })

            drop_idx.append(idx)

    df = df.drop(drop_idx)

    # =======================================================
    # 2) Personen bereits kontaktiert (Filter 1216/1708)
    # =======================================================

    suspect_ids = set()

    for f_id in (1216, 1708):
        async for ids in stream_person_ids_by_filter(f_id):
            suspect_ids.update(ids)

    mask = df[col_pid].astype(str).isin(suspect_ids)
    removed = df[mask]

    for _, r in removed.iterrows():

        delete_rows.append({
            "reason": "person_id_match",
            "Kontakt ID": sanitize_cell(r.get(col_pid)),
            "Name": sanitize_cell(
                f"{r.get('Person Vorname','')} {r.get('Person Nachname','')}"
            ).strip(),
            "Organisation ID": sanitize_cell(r.get(col_orgid)),
            "Organisationsname": sanitize_cell(r.get(col_orgname)),
            "Grund": "Bereits in Filter 1216/1708"
        })

    df = df[~mask]

    # =======================================================
    # 3) Speichern der Ergebnisse
    # =======================================================

    # READY (bereinigt)
    ready_df = df.applymap(sanitize_cell)
    await save_df_text(ready_df, t["ready"])

    # LOG (bereinigt)
    log_df = pd.DataFrame(delete_rows).replace({None: ""})
    log_df = log_df.applymap(sanitize_cell)
    await save_df_text(log_df, t["log"])


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
# /nachfass/export_download – FINAL (mit Kampagnennamen!)
# =============================================================================

@app.get("/nachfass/export_download")
async def nachfass_export_download(job_id: str = Query(...)):
    """
    Liefert den finalen Nachfass-Export als Excel-Download.
    Der Dateiname = Kampagnenname.xlsx
    """
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job nicht gefunden")

    try:
        # finalen Master laden
        df = await load_df_text("nf_master_final")

        if df.empty:
            raise FileNotFoundError("Keine Exportdaten vorhanden")

        # Export bauen
        export_df = build_nf_export(df)
        excel_bytes = _df_to_excel_bytes_nf(export_df)

        # Kampagnennamen als Dateiname
        filename = slugify_filename(job.filename_base or "Nachfass_Export") + ".xlsx"

        return StreamingResponse(
            io.BytesIO(excel_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        print(f"[ERROR] /nachfass/export_download: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
        await asyncio.sleep(0.2)

        job.phase = "Lade Vergleichsdaten …"; job.percent = 25
        await asyncio.sleep(0.2)

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
        await asyncio.sleep(0.05)

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
@app.get("/nachfass/export_download")
async def nachfass_export_download(job_id: str):
    """
    Liefert die exportierte Nachfass-Datei als Download zurück.
    """
    try:
        # DataFrame laden
        df = await load_df_text(tables("nf")["final"])

        if df is None or df.empty:
            raise FileNotFoundError("Keine Exportdaten gefunden")

        # Temporäre Excel-Datei erzeugen
        file_path = f"/tmp/nachfass_{job_id}.xlsx"
        df.to_excel(file_path, index=False)

        # Datei als Download zurückgeben
        return FileResponse(
            file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"nachfass_{job_id}.xlsx"
        )

    except FileNotFoundError:
        return JSONResponse({"detail": "Datei nicht gefunden"}, status_code=404)

    except Exception as e:
        print(f"[ERROR] /nachfass/export_download: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

# =============================================================================
# Kampagnenübersicht (Home)
# =============================================================================
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

    return HTMLResponse(f"""<!doctype html><html lang="de">
<head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Neukontakte – BatchFlow</title>
<style>
  body{{margin:0;background:#f6f8fb;color:#0f172a;font:16px/1.6 Inter,sans-serif}}
  header{{background:#fff;border-bottom:1px solid #e2e8f0}}
  .hwrap{{max-width:1120px;margin:0 auto;padding:14px 20px;display:flex;align-items:center;
          justify-content:space-between;gap:12px}}
  main{{max-width:1120px;margin:28px auto;padding:0 20px}}
  .card{{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:20px;
         box-shadow:0 2px 8px rgba(2,8,23,.04)}}
  .grid{{display:grid;grid-template-columns:repeat(12,1fr);gap:16px}}
  label{{display:block;font-weight:600;margin:8px 0 6px}}
  select,input{{width:100%;padding:10px 12px;border:1px solid #cbd5e1;border-radius:10px;background:#fff}}
  .btn{{background:#0ea5e9;border:none;color:#fff;border-radius:10px;padding:12px 16px;cursor:pointer}}
  .btn:disabled{{opacity:.5;cursor:not-allowed}}
  #overlay{{display:none;position:fixed;inset:0;background:rgba(255,255,255,.7);backdrop-filter:blur(2px);
            z-index:9999;align-items:center;justify-content:center;flex-direction:column;gap:10px}}
  .barwrap{{width:min(520px,90vw);height:10px;border-radius:999px;background:#e2e8f0;overflow:hidden}}
  .bar{{height:100%;width:0%;background:#0ea5e9;transition:width .2s linear}}
</style>
</head>
<body>
<header><div class="hwrap">
  <div><a href="/campaign" style="color:#0a66c2;text-decoration:none">← Kampagne wählen</a></div>
  <div><b>Neukontakte</b></div>
  <div>{authed_html}</div>
</div></header>

<main><section class="card">
  <div class="grid">
    <div class="col-4"><label>Fachbereich</label>
      <select id="fachbereich"><option value="">– bitte auswählen –</option></select></div>
    <div class="col-3"><label>Batch ID</label><input id="batch_id" placeholder="Bxxx"/></div>
    <div class="col-3"><label>Kampagne</label><input id="campaign" placeholder="z. B. Frühling 2025"/></div>
    <div class="col-2" style="display:flex;align-items:flex-end;justify-content:flex-end">
      <button class="btn" id="btnExport" disabled>Abgleich & Download</button>
    </div>
  </div>
</section></main>

<div id="overlay"><div id="phase"></div>
<div class="barwrap"><div class="bar" id="bar"></div></div></div>

<script>
const el = id => document.getElementById(id);
function showOverlay(msg){{el('phase').textContent=msg;el('overlay').style.display='flex';}}
function setProgress(p){{el('bar').style.width=Math.min(100,Math.max(0,p))+'%';}}
async function loadOptions(){{
  showOverlay('Lade Fachbereiche …');setProgress(15);
  const r=await fetch('/neukontakte/options');
  const data=await r.json();
  const sel=el('fachbereich');
  sel.innerHTML='<option value="">– bitte auswählen –</option>';
  data.options.forEach(o=>{{const opt=document.createElement('option');opt.value=o.value;
    opt.textContent=o.label+' ('+o.count+')';sel.appendChild(opt);}});
  el('overlay').style.display='none';
  sel.onchange=()=>el('btnExport').disabled=!sel.value;
}}
async function startExport(){{
  const fb=el('fachbereich').value;if(!fb)return alert('Bitte Fachbereich wählen');
  const bid=el('batch_id').value;const camp=el('campaign').value;
  showOverlay('Starte Export …');setProgress(10);
  const r=await fetch('/neukontakte/export_start',{{method:'POST',
    headers:{{'Content-Type':'application/json'}},
    body:JSON.stringify({{fachbereich:fb,batch_id:bid,campaign:camp}})}}); 
  if(!r.ok)return alert('Fehler beim Start');
  const{{job_id}}=await r.json();
  let done=false;while(!done){{await new Promise(r=>setTimeout(r,500));
    const p=await fetch('/neukontakte/export_progress?job_id='+job_id);
    const s=await p.json();el('phase').textContent=s.phase+' ('+s.percent+'%)';
    setProgress(s.percent);done=s.done;}}
  window.location.href='/neukontakte/export_download?job_id='+job_id;
}}
el('btnExport').onclick=startExport;
loadOptions();
</script>
</body></html>""")

# =============================================================================
# Frontend – Nachfass (stabil, modern, sauber)
# =============================================================================
@app.get("/nachfass", response_class=HTMLResponse)
async def nachfass_page(request: Request):
    authed = bool(user_tokens.get("default") or PD_API_TOKEN)
    auth_info = "<span class='muted'>angemeldet</span>" if authed else "<a href='/login'>Anmelden</a>"

    html = """<!doctype html><html lang="de">
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
  textarea,input{width:100%;padding:10px 12px;border:1px solid #cbd5e1;border-radius:10px}
  .btn{background:#0ea5e9;border:none;color:#fff;border-radius:10px;
        padding:12px 16px;cursor:pointer;font-weight:600}
  .btn:hover{background:#0284c7}
  #overlay{display:none;position:fixed;inset:0;background:rgba(255,255,255,.7);
            backdrop-filter:blur(2px);z-index:9999;align-items:center;justify-content:center;flex-direction:column;gap:10px}
  .barwrap{width:min(520px,90vw);height:10px;border-radius:999px;background:#e2e8f0;overflow:hidden}
  .bar{height:100%;width:0%;background:#0ea5e9;transition:width .25s linear}
  table{width:100%;border-collapse:collapse;margin-top:20px;
         border:1px solid #e2e8f0;border-radius:10px;box-shadow:0 2px 8px rgba(2,8,23,.04);background:#fff}
  th,td{padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:left}
  th{background:#f8fafc;font-weight:600}
  tr:hover{background:#f1f5f9}
</style>
</head>
<body>
<header>
  <div class="hwrap">
    <div><a href='/campaign' style='color:#0a66c2;text-decoration:none'>← Kampagne wählen</a></div>
    <div><b>Nachfass</b></div>
    <div>""" + auth_info + """</div>
  </div>
</header>

<main>
  <section class="card">
    <label>Batch IDs (1–2 Werte)</label>
    <textarea id="nf_batch_ids" rows="3" placeholder="z. B. B111, B222"></textarea>
    <small style="color:#64748b">Komma oder Zeilenumbruch. Max. 2 IDs werden berücksichtigt.</small>

    <label style="margin-top:12px">Batch ID (Export)</label>
    <input id="batch_id" placeholder="B999"/>

    <label style="margin-top:12px">Kampagnenname</label>
    <input id="campaign" placeholder="z. B. Nachfass KW45"/>

    <div style="margin-top:20px;text-align:right">
      <button class="btn" id="btnExportNf">Abgleich & Download</button>
    </div>
  </section>

  <section id="excludedSection" style="margin-top:30px;">
    <h3>Nicht berücksichtigte Datensätze</h3>

    <!-- Summary Box wird dynamisch eingefügt -->

    <div id="excludedTable">
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
          <tr><td colspan="5" style="text-align:center;color:#888">Noch keine Daten geladen</td></tr>
        </tbody>
      </table>
    </div>
  </section>
</main>

<div id="overlay">
  <div id="phase" style="color:#0f172a;font-weight:500"></div>
  <div class="barwrap"><div class="bar" id="bar"></div></div>
</div>

<script>
const el = id => document.getElementById(id);
function showOverlay(m){el('phase').textContent=m||'';el('overlay').style.display='flex';}
function hideOverlay(){el('overlay').style.display='none';}
function setProgress(p){el('bar').style.width=Math.max(0,Math.min(100,p))+'%';}

function _parseIDs(raw){
  return raw.split(/[\\n,;]/).map(s=>s.trim()).filter(Boolean).slice(0,2);
}

async function startExportNf(){
  const ids=_parseIDs(el('nf_batch_ids').value);
  if(ids.length===0)return alert('Bitte mindestens eine Batch ID angeben.');
  const bid=el('batch_id').value||'';
  const camp=el('campaign').value||'';

  showOverlay('Starte Abgleich …');
  setProgress(5);

  try{
    const r=await fetch('/nachfass/export_start',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({nf_batch_ids:ids,batch_id:bid,campaign:camp})
    });
    if(!r.ok)throw new Error('Start fehlgeschlagen.');
    const {job_id}=await r.json();
    await poll(job_id);
  }catch(err){
    alert(err.message||'Fehler beim Starten.');
    hideOverlay();
  }
}

async function loadExcludedTable(){
  try{
    const r = await fetch('/nachfass/excluded/json');
    const data = await r.json();

    const body = document.querySelector('#excluded-table-body');
    body.innerHTML = '';

    // ---------------------------
    // 1) Summary Box (Batch/Filter)
    // ---------------------------
    const summaryBoxId = "excluded-summary-box";
    let summaryBox = document.getElementById(summaryBoxId);

    if (!summaryBox) {
      summaryBox = document.createElement("div");
      summaryBox.id = summaryBoxId;
      summaryBox.style.margin = "15px 0";
      summaryBox.style.padding = "12px 16px";
      summaryBox.style.background = "#fff";
      summaryBox.style.border = "1px solid #e2e8f0";
      summaryBox.style.borderRadius = "10px";
      summaryBox.style.boxShadow = "0 2px 8px rgba(2,8,23,.04)";
      document.querySelector("#excludedSection").prepend(summaryBox);
    }

    if (data.summary && data.summary.length > 0) {
      let html = "<b>Batch-/Filter-Ausschlüsse:</b><ul style='margin-top:6px'>";
      for (const s of data.summary) {
        html += `<li>${s.Grund}: <b>${s.Anzahl}</b></li>`;
      }
      html += "</ul>";
      summaryBox.innerHTML = html;
    } else {
      summaryBox.innerHTML = "<b>Keine Batch-/Filter-Ausschlüsse</b>";
    }

    // ---------------------------
    // 2) Abgleich-Zeilen (Fuzzy/ID)
    // ---------------------------
    if (!data.rows || data.rows.length === 0){
      body.innerHTML = `
        <tr>
          <td colspan="5" style="text-align:center;color:#888">
            Keine Datensätze durch Abgleich entfernt
          </td>
        </tr>`;
      return;
    }

    for (const row of data.rows){
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

  } catch(err){
    const body = document.querySelector('#excluded-table-body');
    body.innerHTML = `
      <tr><td colspan="5" style="text-align:center;color:red">
        Fehler beim Laden (${err.message})
      </td></tr>`;
  }
}

async function poll(job_id){
  let done=false;
  while(!done){
    await new Promise(r=>setTimeout(r,600));
    const r=await fetch('/nachfass/export_progress?job_id='+encodeURIComponent(job_id));
    if(!r.ok)break;
    const s=await r.json();
    el('phase').textContent=s.phase+' ('+(s.percent||0)+'%)';
    setProgress(s.percent||0);
    if(s.error){alert(s.error);hideOverlay();return;}
    done=s.done;
  }
  el('phase').textContent='Download startet …';
  setProgress(100);
  window.location.href='/nachfass/export_download?job_id='+encodeURIComponent(job_id);
  await loadExcludedTable();
  hideOverlay();
}

el('btnExportNf').addEventListener('click', startExportNf);
</script>

</body></html>"""
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

    excluded_df = await load_df_text("nf_excluded")
    deleted_df  = await load_df_text("nf_delete_log")

    excluded_summary = []
    if not excluded_df.empty:
        for _, r in excluded_df.iterrows():
            excluded_summary.append({
                "Grund": r["Grund"],
                "Anzahl": int(r["Anzahl"])
            })

    rows = []
    if not deleted_df.empty:
        deleted_df = deleted_df.replace({None: "", np.nan: ""})

        for _, r in deleted_df.iterrows():
            rows.append({
                "Kontakt ID": r.get("Kontakt ID") or r.get("id") or "",
                "Name": r.get("Name") or "",
                "Organisation ID": r.get("Organisation ID") or "",
                "Organisationsname": r.get("Organisationsname") or "",
                "Grund": r.get("Grund") or r.get("reason") or ""
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
@app.post("/nachfass/export_start")
async def export_start_nf(request: Request):
    try:
        data = await request.json()
        nf_batch_ids = data.get("nf_batch_ids") or []
        batch_id     = data.get("batch_id") or ""
        campaign     = data.get("campaign") or ""

        job_id = str(uuid.uuid4())
        job = Job()
        JOBS[job_id] = job

        job.filename_base = slugify_filename(campaign or f"Nachfass_{batch_id}")
        job.phase = "Starte Nachfass-Export …"
        job.percent = 0

        async def run():
            try:
                job.phase = "Lade Nachfass-Daten …"
                job.percent = 10

                df = await _build_nf_master_final(nf_batch_ids, batch_id, campaign, job_obj=job)

                job.phase = "Führe Abgleich durch …"
                job.percent = 60
                await _reconcile("nf")

                job.phase = "Erzeuge Excel-Datei …"
                job.percent = 85

                ready = await load_df_text("nf_master_ready")
                export_df = build_nf_export(ready)
                excel_bytes = _df_to_excel_bytes(export_df)

                file_path = f"/tmp/{job.filename_base}.xlsx"
                with open(file_path, "wb") as f:
                    f.write(excel_bytes)

                job.path = file_path
                job.total_rows = len(export_df)
                job.phase = f"Fertig – {job.total_rows} Zeilen"
                job.percent = 100
                job.done = True

            except Exception as e:
                job.error = str(e)
                job.phase = "Fehler"
                job.percent = 100
                job.done = True

        asyncio.create_task(run())
        return {"job_id": job_id}

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)




# =============================================================================
# Fortschritt abfragen
# =============================================================================
@app.get("/nachfass/export_progress")
async def nachfass_export_progress(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        return JSONResponse({"error": "Job nicht gefunden"}, status_code=404)
    return {
        "phase": job.phase,
        "percent": job.percent,
        "done": job.done,
        "error": job.error
    }

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
        filename=f"{job.filename_base}.xlsx"
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
