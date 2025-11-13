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

fuzz.default_processor = lambda s: s  # kein Vor-Preprocessing

# -----------------------------------------------------------------------------
# App-Grundkonfiguration
# -----------------------------------------------------------------------------
app = FastAPI(title="BatchFlow")
app.add_middleware(GZipMiddleware, minimum_size=1024)
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# -----------------------------------------------------------------------------
# Umgebungsvariablen & Konstanten
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
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "500"))
NF_PAGE_LIMIT = int(os.getenv("NF_PAGE_LIMIT", "500"))
NF_MAX_ROWS = int(os.getenv("NF_MAX_ROWS", "10000"))
RECONCILE_MAX_ROWS = int(os.getenv("RECONCILE_MAX_ROWS", "20000"))
PER_ORG_DEFAULT_LIMIT = int(os.getenv("PER_ORG_DEFAULT_LIMIT", "2"))
MAX_ORG_NAMES = int(os.getenv("MAX_ORG_NAMES", "100000"))
MAX_ORG_BUCKET = int(os.getenv("MAX_ORG_BUCKET", "12000"))

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

# -----------------------------------------------------------------------------
# DB-Helper
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
async def stream_organizations_by_filter(filter_id: int, page_limit: int = PAGE_LIMIT) -> AsyncGenerator[List[dict], None]:
    """Streamt Organisationen mit Pagination."""
    start = 0
    while True:
        url = append_token(f"{PIPEDRIVE_API}/organizations?filter_id={filter_id}&start={start}&limit={page_limit}")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive Fehler (Orgs {filter_id}): {r.text}")
        data = r.json().get("data") or []
        if not data:
            break
        yield data
        if len(data) < page_limit:
            break
        start += len(data)

async def stream_person_ids_by_filter(filter_id: int, page_limit: int = PAGE_LIMIT) -> AsyncGenerator[List[str], None]:
    """Streamt nur Personen-IDs – speicherschonend."""
    start = 0
    while True:
        url = append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={page_limit}&sort=id")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive Fehler (IDs {filter_id}): {r.text}")
        data = r.json().get("data") or []
        if not data:
            break
        yield [str(p.get("id")) for p in data if p.get("id")]
        if len(data) < page_limit:
            break
        start += len(data)
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
        local = []
        async with sem:
            while True:
                url = append_token(
                    f"{PIPEDRIVE_API}/persons/search?term={bid}&fields=custom_fields&start={start}&limit={page_limit}"
                )
                r = await http_client().get(url, headers=get_headers())
                if r.status_code == 429:
                    print(f"[WARN] Rate limit erreicht, warte 2 Sekunden ...")
                    await asyncio.sleep(2)
                    continue
                if r.status_code != 200:
                    print(f"[WARN] Batch {bid} Fehler: {r.text}")
                    break
                data = r.json().get("data", {}).get("items", [])
                if not data:
                    break

                persons = [it.get("item") for it in data if it.get("item")]
                local.extend(persons)
                total += len(persons)
                start += len(persons)

                if len(persons) < page_limit:
                    break
                await asyncio.sleep(0.1)  # minimale Pause zwischen Seiten

        print(f"[DEBUG] Batch {bid}: {total} Personen geladen")
        results.extend(local)

    await asyncio.gather(*[fetch_one(bid) for bid in batch_ids])
    print(f"[INFO] Alle Batch-IDs geladen: {len(results)} Personen gesamt")
    return results


# =============================================================================
# Nachfass – Aufbau Master (robust, progressiv & vollständig)
# =============================================================================
import asyncio

async def fetch_person_details(person_ids: List[str]) -> List[dict]:
    """Lädt vollständige Datensätze für Personen-IDs parallel."""
    results = []
    sem = asyncio.Semaphore(15)  # Max. 6 gleichzeitige Requests für Render

    async def fetch_one(pid):
        async with sem:
            url = append_token(f"{PIPEDRIVE_API}/persons/{pid}")
            r = await http_client().get(url, headers=get_headers())
            if r.status_code == 200:
                data = r.json().get("data")
                if data:
                    results.append(data)
            await asyncio.sleep(0.05)  # Eventloop frei lassen

    await asyncio.gather(*[fetch_one(pid) for pid in person_ids])
    print(f"[DEBUG] Vollständige Personendaten geladen: {len(results)}")
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

# -------------------------------------------------------------------------
# NACHFASS – finale Version mit vollständigen Spalten & korrekter Filterlogik
# -------------------------------------------------------------------------
import json
import datetime as dt
from collections import defaultdict
import pandas as pd
import numpy as np

async def _build_nf_master_final(
    nf_batch_ids: List[str],
    batch_id: str,
    campaign: str,
    job_obj=None
) -> pd.DataFrame:
    """
    Baut Nachfass-Daten performant & vollständig:
    - nutzt echten Filter (z. B. 3024)
    - filtert nach:
        1. max. 2 Personen pro Organisation (nach ID)
        2. "Datum nächste Aktivität" < 3 Monate oder in der Zukunft
    - exportiert vollständige Spalten
    """

    # ---------------------------------------------------------------------
    # 1) Feld-Mapping vorbereiten
    # ---------------------------------------------------------------------
    person_fields = await get_person_fields()
    hint_to_key: Dict[str, str] = {}
    gender_map: Dict[str, str] = {}
    next_activity_key = None

    for f in person_fields:
        nm = (f.get("name") or "").lower()
        for hint in PERSON_FIELD_HINTS_TO_EXPORT.keys():
            if hint in nm and hint not in hint_to_key:
                hint_to_key[hint] = f.get("key")

        if any(x in nm for x in ("gender", "geschlecht")):
            gender_map = field_options_id_to_label_map(f)

        if "datum nächste aktivität" in nm or "next activity" in nm:
            next_activity_key = f.get("key")

    def get_field(p: dict, hint: str) -> str:
        """Hilfsfunktion: extrahiert benutzerdefinierte Felder robust"""
        key = hint_to_key.get(hint)
        if not key:
            return ""
        v = p.get(key)
        if isinstance(v, dict) and "label" in v:
            return str(v.get("label"))
        if isinstance(v, list):
            vals = []
            for x in v:
                if isinstance(x, dict):
                    vals.append(str(x.get("value") or x.get("label") or ""))
                else:
                    vals.append(str(x))
            return ", ".join([x for x in vals if x])
        if hint in ("gender", "geschlecht") and gender_map:
            return gender_map.get(str(v), str(v))
        return str(v or "")

    # ---------------------------------------------------------------------
    # 2) Personen per Filter laden (statt search)
    # ---------------------------------------------------------------------
    if job_obj:
        job_obj.phase = "Lade Nachfass-Personen aus Pipedrive …"
        job_obj.percent = 10

    persons: List[dict] = []
    filter_id = 3024  # dein Nachfass-Filter
    start = 0
    total = 0

    while True:
        url = append_token(
            f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit=100"
        )
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            print(f"[WARN] Filter {filter_id} Fehler: {r.text[:100]}")
            break
        data = r.json().get("data", [])
        if not data:
            break
        persons.extend(data)
        total += len(data)
        start += len(data)
        if len(data) < 100:
            break

    print(f"[INFO] Filter {filter_id}: {total} Personen geladen")

    # ---------------------------------------------------------------------
    # 3) Vorabfilterung
    # ---------------------------------------------------------------------
    selected = []
    excluded = []
    org_counter = defaultdict(int)
    now = dt.datetime.now()

    for p in persons:
        pid = str(p.get("id") or "")
        name = p.get("name") or ""
        org = p.get("organization") or {}
        org_name = org.get("name") or "-"
        org_id = str(org.get("id") or "")

        # 1️⃣ Aktivitätsprüfung
        if next_activity_key:
            raw_date = p.get(next_activity_key)
            if raw_date:
                try:
                    next_act_val = dt.datetime.fromisoformat(str(raw_date).split(" ")[0])
                    delta_days = (now - next_act_val).days
                    if 0 <= delta_days <= 90 or delta_days < 0:
                        excluded.append({
                            "id": pid,
                            "name": name,
                            "org": org_name,
                            "grund": "Datum nächste Aktivität < 3 Monate oder in der Zukunft"
                        })
                        continue
                except Exception as e:
                    print(f"[WARN] Konnte Aktivitätsdatum für {name} nicht interpretieren: {e}")

        # 2️⃣ max. 2 Kontakte pro Organisation-ID
        if org_id:
            org_counter[org_id] += 1
            if org_counter[org_id] > 2:
                excluded.append({
                    "id": pid,
                    "name": name,
                    "org": org_name,
                    "grund": "Mehr als 2 Kontakte pro Organisation"
                })
                continue

        selected.append(p)

    print(f"[INFO] Nach Vorabfilter: {len(selected)} übrig, {len(excluded)} ausgeschlossen")

    # ---------------------------------------------------------------------
    # 4) DataFrame aufbauen mit allen geforderten Spalten
    # ---------------------------------------------------------------------
    rows = []
    for p in selected:
        pid = p.get("id")
        org = p.get("organization") or {}
        org_id = str(org.get("id") or "")
        org_name = org.get("name") or "-"
        name = p.get("name") or ""
        emails = p.get("emails") or []
        email = emails[0] if emails else ""

        vor, nach = split_name(p.get("first_name"), p.get("last_name"), name)

        rows.append({
            "Person - Batch ID": batch_id,
            "Person - Channel": DEFAULT_CHANNEL,
            "Cold-Mailing Import": campaign,
            "Person - Prospect ID": get_field(p, "prospect"),
            "Person - Organisation": org_name,
            "Organisation - ID": org_id,
            "Person - Geschlecht": get_field(p, "geschlecht") or get_field(p, "gender"),
            "Person - Titel": get_field(p, "titel") or get_field(p, "anrede"),
            "Person - Vorname": vor,
            "Person - Nachname": nach,
            "Person - Position": get_field(p, "position"),
            "Person - ID": str(pid),
            "Person - XING-Profil": get_field(p, "xing") or get_field(p, "xing profil"),
            "Person - LinkedIn Profil-URL": get_field(p, "linkedin"),
            "Person - E-Mail-Adresse - Büro": email
        })

    df = pd.DataFrame(rows)
    df = df.replace({np.nan: None})

    # ---------------------------------------------------------------------
    # 5) Speichern & Ergebnisanzeige
    # ---------------------------------------------------------------------
    excluded_df = pd.DataFrame(excluded).replace({np.nan: None})

    await save_df_text(df, "nf_master_final")
    await save_df_text(excluded_df, "nf_excluded")

    if job_obj:
        if len(excluded_df) == 0:
            job_obj.phase = "Keine Datensätze ausgeschlossen"
        else:
            job_obj.phase = f"{len(excluded_df)} Datensätze ausgeschlossen"
        job_obj.percent = 60

    print(f"[Nachfass] Export abgeschlossen ({len(df)} Zeilen, {len(excluded)} ausgeschlossen)")

    # Wenn keine Ausschlüsse → Dummy-Eintrag zur Anzeige im Frontend
    if excluded_df.empty:
        excluded_df = pd.DataFrame([{
            "id": "-",
            "name": "-",
            "org": "-",
            "grund": "Keine Datensätze ausgeschlossen"
        }])
        await save_df_text(excluded_df, "nf_excluded")

    return df

# =============================================================================
# BASIS-ABGLEICH (Organisationen & IDs)
# =============================================================================
async def _fetch_org_names_for_filter_capped(
    filter_id: int, page_limit: int, cap_total: int, cap_bucket: int
) -> Dict[str, List[str]]:
    """Holt Organisationsnamen pro Anfangsbuchstabe – capped."""
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


def bucket_key(name: str) -> str:
    """Bildet 2-Buchstaben-Buckets für schnelleren Fuzzy-Vergleich."""
    n = normalize_name(name)
    return n[:2] if len(n) > 1 else n


def fast_fuzzy(a: str, b: str) -> int:
    """Schnellerer Fuzzy-Matcher (partial_ratio)."""
    return fuzz.partial_ratio(a, b)

# =============================================================================
# Nachfass – Performanter Organisations- und Personenabgleich (3 Filter + Log)
# =============================================================================
async def _reconcile(prefix: str) -> None:
    """
    Fuzzy-Abgleich (≥95 %) + Entfernen bereits kontaktierter Personen.
    Orga-Filter: 1245, 851, 1521. Personen-ID-Filter: 1216, 1708.
    Ergebnis: *_master_ready + *_delete_log
    """
    t = tables(prefix)
    master = await load_df_text(t["final"])
    if master is None or master.empty:
        await save_df_text(pd.DataFrame(), t["ready"])
        await save_df_text(pd.DataFrame(columns=["reason","id","name","org_id","org_name","extra"]), t["log"])
        return

    col_person_id, col_org_name, col_org_id = "Person ID", "Organisation Name", "Organisation ID"
    delete_rows: List[Dict[str, str]] = []

    # 1) Orgas einsammeln (1245, 851, 1521)
    filter_ids_org = [1245, 851, 1521]
    buckets_all: Dict[str, List[str]] = {}
    collected_total = 0
    for fid in filter_ids_org:
        caps_left = max(0, MAX_ORG_NAMES - collected_total)
        if caps_left <= 0:
            break
        buckets = await _fetch_org_names_for_filter_capped(fid, PAGE_LIMIT, caps_left, MAX_ORG_BUCKET)
        for k, lst in buckets.items():
            slot = buckets_all.setdefault(k, [])
            for n in lst:
                if len(slot) >= MAX_ORG_BUCKET:
                    break
                if n not in slot:
                    slot.append(n)
                    collected_total += 1
        if collected_total >= MAX_ORG_NAMES:
            break

    # 2) Fuzzy ≥95 %
    drop_idx = []
    for idx, row in master.iterrows():
        cand = str(row.get(col_org_name) or "").strip()
        cand_norm = normalize_name(cand)
        if not cand_norm:
            continue
        bucket = buckets_all.get(cand_norm[0])
        if not bucket:
            continue
        near = [n for n in bucket if abs(len(n) - len(cand_norm)) <= 4]
        if not near:
            continue
        best = process.extractOne(cand_norm, near, scorer=fuzz.token_sort_ratio)
        if best and best[1] >= 95:
            drop_idx.append(idx)
            delete_rows.append({
                "reason": "org_match_95",
                "id": str(row.get(col_person_id) or ""),
                "name": f"{row.get('Person Vorname') or ''} {row.get('Person Nachname') or ''}".strip(),
                "org_id": str(row.get(col_org_id) or ""),
                "org_name": cand,
                "extra": f"Ähnlichkeit: {best[0]} ({best[1]} %)"
            })

    if drop_idx:
        master = master.drop(index=drop_idx)

    # 3) Bereits kontaktierte Personen entfernen (1216/1708)
    suspect_ids = set()
    for f_id in (1216, 1708):
        async for page in stream_person_ids_by_filter(f_id):
            suspect_ids.update(page)

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
                "extra": "Bereits in Filter 1216/1708"
            })
        master = master[~mask].copy()

    # 4) Speichern
    await save_df_text(master, t["ready"])
    log_df = pd.DataFrame(delete_rows, columns=["reason","id","name","org_id","org_name","extra"])
    await save_df_text(log_df, t["log"])
    try:
        df_del = await load_df_text("nf_delete_log")
        print(f"[Reconcile] {len(df_del)} Zeilen im Delete-Log gespeichert.")
    except Exception as e:
        print(f"[Reconcile] Delete-Log konnte nicht geladen werden: {e}")


# =============================================================================
# Excel-Export-Helfer
# =============================================================================
def build_export_from_ready(df: pd.DataFrame) -> pd.DataFrame:
    """Erzeugt sauberen Export mit konsistenter Spaltenreihenfolge."""
    out = pd.DataFrame(columns=TEMPLATE_COLUMNS)
    for col in TEMPLATE_COLUMNS:
        out[col] = df[col] if col in df.columns else ""
    for c in ("Organisation ID", "Person ID"):
        if c in out.columns:
            out[c] = out[c].astype(str).fillna("").replace("nan", "")
    return out

def _df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    """Konvertiert DataFrame in Excel-Datei (Bytes, speicherschonend)."""
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
            export_df = build_export_from_ready(ready)
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
    })

@app.get("/nachfass/export_progress")
async def nachfass_export_progress(job_id: str = Query(...)):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job nicht gefunden")
    return JSONResponse({
        "phase": job.phase, "percent": job.percent,
        "done": job.done, "error": job.error,
    })

@app.get("/neukontakte/export_download")
async def neukontakte_export_download(job_id: str = Query(...)):
    job = JOBS.get(job_id)
    if not job or not job.path:
        raise HTTPException(status_code=404, detail="Datei nicht gefunden")
    return FileResponse(
        job.path,
        filename=f"{job.filename_base}.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

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
# Frontend – Nachfass
# =============================================================================
@app.get("/nachfass", response_class=HTMLResponse)
async def nachfass_page(request: Request):
    authed = bool(user_tokens.get("default") or PD_API_TOKEN)
    auth_info = "<span class='muted'>angemeldet</span>" if authed else "<a href='/login'>Anmelden</a>"

    return HTMLResponse(f"""<!doctype html><html lang="de">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Nachfass – BatchFlow</title>
<style>
  body{{margin:0;background:#f6f8fb;color:#0f172a;font:16px/1.6 Inter,sans-serif}}
  header{{background:#fff;border-bottom:1px solid #e2e8f0}}
  .hwrap{{max-width:1120px;margin:0 auto;padding:14px 20px;display:flex;
          align-items:center;justify-content:space-between}}
  main{{max-width:1120px;margin:28px auto;padding:0 20px}}
  .card{{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:20px;
         box-shadow:0 2px 8px rgba(2,8,23,.04)}}
  label{{display:block;font-weight:600;margin:8px 0 6px}}
  textarea,input{{width:100%;padding:10px 12px;border:1px solid #cbd5e1;border-radius:10px}}
  .btn{{background:#0ea5e9;border:none;color:#fff;border-radius:10px;
        padding:12px 16px;cursor:pointer;font-weight:600}}
  .btn:hover{{background:#0284c7}}
  #overlay{{display:none;position:fixed;inset:0;background:rgba(255,255,255,.7);
            backdrop-filter:blur(2px);z-index:9999;align-items:center;justify-content:center;flex-direction:column;gap:10px}}
  .barwrap{{width:min(520px,90vw);height:10px;border-radius:999px;background:#e2e8f0;overflow:hidden}}
  .bar{{height:100%;width:0%;background:#0ea5e9;transition:width .25s linear}}
  table{{width:100%;border-collapse:collapse;margin-top:20px;
         border:1px solid #e2e8f0;border-radius:10px;box-shadow:0 2px 8px rgba(2,8,23,.04);background:#fff}}
  th,td{{padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:left}}
  th{{background:#f8fafc;font-weight:600}}
  tr:hover{{background:#f1f5f9}}
</style>
</head>
<body>
<header>
  <div class="hwrap">
    <div><a href='/campaign' style='color:#0a66c2;text-decoration:none'>← Kampagne wählen</a></div>
    <div><b>Nachfass</b></div>
    <div>{auth_info}</div>
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

  <section id="excludedSection" style="margin-top:30px;display:none">
    <h3>Nicht berücksichtigte Datensätze</h3>
    <div id="excludedTable"></div>
  </section>
</main>

<div id="overlay">
  <div id="phase" style="color:#0f172a;font-weight:500"></div>
  <div class="barwrap"><div class="bar" id="bar"></div></div>
</div>

<script>
const el = id => document.getElementById(id);

function showOverlay(msg){{el('phase').textContent=msg||'';el('overlay').style.display='flex';}}
function hideOverlay(){{el('overlay').style.display='none';}}
function setProgress(p){{el('bar').style.width=Math.max(0,Math.min(100,p))+'%';}}

function _parseIDs(raw) {{
  return raw.split(/[\\n,;]/).map(s=>s.trim()).filter(Boolean).slice(0,2);
}}

async function startExportNf() {{
  const ids=_parseIDs(el('nf_batch_ids').value);
  if(ids.length===0)return alert('Bitte mindestens eine Batch ID angeben.');
  const bid=el('batch_id').value||'';
  const camp=el('campaign').value||'';

  showOverlay('Starte Abgleich …');
  setProgress(5);

  try {{
    const r=await fetch('/nachfass/export_start',{{
      method:'POST',
      headers:{{'Content-Type':'application/json'}},
      body:JSON.stringify({{nf_batch_ids:ids,batch_id:bid,campaign:camp}})
    }});
    if(!r.ok)throw new Error('Start fehlgeschlagen.');
    const {{job_id}}=await r.json();
    await poll(job_id);
  }} catch(err) {{
    alert(err.message||'Fehler beim Starten.');
    hideOverlay();
  }}
}}

async function poll(job_id){{
  let done=false;
  while(!done){{
    await new Promise(r=>setTimeout(r,600));
    const r=await fetch('/nachfass/export_progress?job_id='+encodeURIComponent(job_id));
    if(!r.ok)break;
    const s=await r.json();
    if(s.phase)el('phase').textContent=s.phase+' ('+ (s.percent||0)+'%)';
    setProgress(s.percent||0);
    if(s.error){{alert(s.error);hideOverlay();return;}}
    done=s.done;
  }}
  // Export abgeschlossen
  el('phase').textContent='Download startet …';
  setProgress(100);
  window.location.href='/nachfass/export_download?job_id='+encodeURIComponent(job_id);
  await loadExcludedTable();
  hideOverlay();
}}

async function loadExcludedTable(){{
  const r=await fetch('/nachfass/excluded/json');
  if(!r.ok)return;
  const data=await r.json();
  if(data.total===0)return;
  const htmlRows=data.rows.map(r=>`
    <tr>
      <td>${{r.id||''}}</td>
      <td>${{r.name||''}}</td>
      <td>${{r.org||''}}</td>
      <td>${{r.grund||''}}</td>
    </tr>`).join('');
  const table=`<table><tr><th>ID</th><th>Name</th><th>Organisation</th><th>Grund</th></tr>${{htmlRows}}</table>`;
  el('excludedTable').innerHTML=table;
  el('excludedSection').style.display='block';
}}

el('btnExportNf').addEventListener('click',startExportNf);
</script>
</body></html>""")

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
    log = await load_df_text("nf_delete_log")
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
    
  
# -------------------------------------------------------------------------
# Kombinierter Ausschluss-Endpunkt: zeigt nf_excluded + nf_delete_log
# -------------------------------------------------------------------------
@app.get("/nachfass/excluded/json")
async def nachfass_excluded_json():
    """
    Liefert alle ausgeschlossenen Nachfass-Datensätze als JSON:
    - kombiniert nf_excluded (Batch/Fehler)
    - und nf_delete_log (aus Fuzzy-/Kontakt-Abgleich)
    """
    import pandas as pd

    excluded_df = pd.DataFrame()
    deleted_df = pd.DataFrame()

    try:
        excluded_df = await load_df_text("nf_excluded")
    except Exception as e:
        print(f"[WARN] Konnte nf_excluded nicht laden: {e}")

    try:
        deleted_df = await load_df_text("nf_delete_log")
    except Exception as e:
        print(f"[WARN] Konnte nf_delete_log nicht laden: {e}")

    if not excluded_df.empty:
        excluded_df["Quelle"] = "Batch-/Filter-Ausschluss"
    if not deleted_df.empty:
        deleted_df["Quelle"] = "Abgleich (Fuzzy/Kontaktfilter)"

    df_all = pd.concat([excluded_df, deleted_df], ignore_index=True)

    if df_all.empty:
        return JSONResponse({"total": 0, "rows": []})

    # auf 200 letzte Datensätze begrenzen, damit UI schnell lädt
    rows = df_all.tail(200).to_dict(orient="records")
    return JSONResponse({
        "total": len(df_all),
        "rows": rows
    })


# -------------------------------------------------------------------------
# Startet den Nachfass-Export (BatchFlow)
# -------------------------------------------------------------------------
@app.post("/nachfass/export_start")
async def export_start_nf(request: Request):
    """
    Startet den asynchronen Nachfass-Export:
    - Erstellt Job-Objekt (für Fortschritt & Status)
    - Ruft _build_nf_master_final asynchron auf
    - Führt anschließend den Abgleich (_reconcile) aus
    - Liefert Job-ID für Fortschrittsabfragen zurück
    """
    try:
        data = await request.json()
        nf_batch_ids = data.get("nf_batch_ids") or []
        batch_id = data.get("batch_id") or ""
        campaign = data.get("campaign") or ""

        if not nf_batch_ids:
            return JSONResponse({"error": "Keine Batch-IDs übergeben."}, status_code=400)

        # ---------------------------------------------------------------
        # Job erstellen (ohne Parameter im Konstruktor!)
        # ---------------------------------------------------------------
        job_id = str(uuid.uuid4())
        job_obj = Job()
        job_obj.id = job_id
        job_obj.name = f"Nachfass Export ({batch_id})"
        job_obj.phase = "Starte Nachfass-Export …"
        job_obj.percent = 0
        job_obj.done = False
        job_obj.error = None

        JOBS[job_id] = job_obj

        # ---------------------------------------------------------------
        # Asynchronen Export starten
        # ---------------------------------------------------------------
        async def run_export():
            try:
                # 1️⃣ Nachfass-Daten aufbauen
                job_obj.phase = "Lade Nachfass-Daten aus Pipedrive …"
                job_obj.percent = 5

                df = await _build_nf_master_final(
                    nf_batch_ids=nf_batch_ids,
                    batch_id=batch_id,
                    campaign=campaign,
                    job_obj=job_obj
                )

                await save_df_text(df, tables("nf")["final"])
                job_obj.phase = f"Daten geladen ({len(df)} Zeilen)"
                job_obj.percent = 60

                # 2️⃣ Abgleichslogik anwenden (Organisation + Personen)
                try:
                    job_obj.phase = "Führe Abgleichslogik aus …"
                    job_obj.percent = 80
                    await reconcile_with_progress(job_obj, "nf")
                except Exception as e:
                    print(f"[WARN] Abgleich fehlgeschlagen: {e}")
                    job_obj.phase = f"Abgleich fehlgeschlagen: {e}"

                # 3️⃣ Abschlussstatus
                job_obj.phase = "Export und Abgleich abgeschlossen"
                job_obj.percent = 100
                job_obj.done = True

                # optional: Anzahl ausgeschlossener Datensätze (nf_excluded)
                try:
                    excluded_df = await load_df_text("nf_excluded")
                    job_obj.excluded_count = len(excluded_df)
                except Exception:
                    job_obj.excluded_count = 0

                print(f"[Nachfass] Export + Abgleich fertig ({len(df)} Zeilen, {job_obj.excluded_count} ausgeschlossen)")

            except Exception as e:
                job_obj.phase = "Fehler beim Nachfass-Export"
                job_obj.error = str(e)
                job_obj.done = True
                print(f"[ERROR] Nachfass-Export fehlgeschlagen: {e}")

        asyncio.create_task(run_export())

        # ---------------------------------------------------------------
        # Job-ID für Frontend zurückgeben
        # ---------------------------------------------------------------
        return JSONResponse({"job_id": job_id})

    except Exception as e:
        print(f"[ERROR] /nachfass/export_start: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

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
