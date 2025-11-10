# master_fixed_v2_part1.py — Teil 1/5
# Basis: BatchFlow (FastAPI + Pipedrive + Neon)
# Getestet für Render Free Tier (Python 3.12, 512 MB RAM)

import os, re, io, uuid, time, asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, AsyncGenerator
import numpy as np, pandas as pd, httpx, asyncpg
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
async def stream_persons_by_filter(filter_id: int, page_limit: int = PAGE_LIMIT) -> AsyncGenerator[List[dict], None]:
    """Streamt Personen aus einem Filter mit Pagination."""
    start = 0
    while True:
        url = append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={page_limit}")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive Fehler: {r.text}")
        data = r.json().get("data") or []
        if not data:
            break
        yield data
        if len(data) < page_limit:
            break
        start += len(data)

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
# Nachfass – Personen laden nach Batch-ID (mit Paging & Fortschritt)
# =============================================================================
async def stream_persons_by_batch_id(
    batch_key: str,
    batch_ids: List[str],
    page_limit: int = 100,   # Pipedrive erlaubt max. 100
    job_obj=None,
    update_progress=None
) -> AsyncGenerator[List[dict], None]:
    """
    Lädt Personen für jede Batch-ID über /persons/search mit Paging.
    Holt alle Treffer, nicht nur die erste Seite.
    """
    for bid in batch_ids:
        start = 0
        total = 0
        page = 0
        while True:
            url = append_token(
                f"{PIPEDRIVE_API}/persons/search?term={bid}&fields=custom_fields&start={start}&limit={page_limit}"
            )
            r = await http_client().get(url, headers=get_headers())
            if r.status_code != 200:
                print(f"[WARN] Batch {bid} Fehler: {r.text}")
                break

            data = r.json().get("data", {}).get("items", [])
            if not data:
                break

            persons = [it.get("item") for it in data if it.get("item")]
            yield persons

            total += len(persons)
            page += 1
            start += len(persons)

            # Fortschrittsanzeige (optional)
            if job_obj:
                job_obj.phase = f"Batch {bid}: Seite {page}, {total} Personen geladen"
                job_obj.percent = min(20 + (total // 100), 60)
            print(f"[DEBUG] Batch {bid}: Seite {page}, {len(persons)} Personen (Start={start})")

            # Abbruch, wenn weniger als page_limit
            if len(persons) < page_limit:
                break

        print(f"[INFO] Batch {bid}: {total} Personen gefunden")

# =============================================================================
# Nachfass – Aufbau Master (robust, progressiv & vollständig)
# =============================================================================
import asyncio

async def fetch_person_details(person_ids: List[str]) -> List[dict]:
    """Lädt vollständige Datensätze für Personen-IDs parallel."""
    results = []
    sem = asyncio.Semaphore(6)  # Max. 6 gleichzeitige Requests für Render

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


async def _build_nf_master_final(
    nf_batch_ids: List[str],
    batch_id: str,
    campaign: str,
    job_obj=None,
) -> pd.DataFrame:
    """Baut Nachfass-Daten mit Fortschritt & robuster Feldsuche."""
    fields = await get_person_fields()

    # Batch-Feld tolerant finden
    batch_key = None
    for f in fields:
        nm = (f.get("name") or "").lower()
        if any(x in nm for x in ("batch", "batchid", "batch id")):
            batch_key = f.get("key")
            break
    if not batch_key:
        raise RuntimeError("Personenfeld „Batch ID“ wurde nicht gefunden.")

    # Feldzuordnung für Export
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
            vals = []
            for x in v:
                if isinstance(x, dict):
                    x = x.get("value")
                if x:
                    vals.append(str(x))
            return ", ".join(vals)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        return str(v)

    persons: List[dict] = []
    total = 0
    page_count = 0

    if job_obj:
        job_obj.phase = "Starte Nachfass …"
        job_obj.percent = 5

    # Personen nach Batch-ID suchen
    for bid in nf_batch_ids:
        async for chunk in stream_persons_by_batch_id(batch_key, [bid]):
            ids = [str(p.get("id")) for p in chunk if p.get("id")]
            if not ids:
                continue

            # Details laden
            details = await fetch_person_details(ids)
            for p in details:
                if str(p.get(batch_key)) not in nf_batch_ids:
                    continue
                persons.append(p)

            total += len(details)
            page_count += 1
            if job_obj:
                job_obj.phase = f"Lade Batch {bid} – Seite {page_count} ({total} Personen)"
                job_obj.percent = min(10 + (total // 50), 40)
            await asyncio.sleep(0.05)

        print(f"[INFO] Batch {bid}: {total} Personen geladen.")

    if not persons:
        print("[WARN] Keine Personen gefunden.")
        return pd.DataFrame(columns=TEMPLATE_COLUMNS)

    # DataFrame aufbauen
    rows = []
    for p in persons:
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

    df = pd.DataFrame(rows, columns=TEMPLATE_COLUMNS)
    print(f"[DEBUG] DataFrame für Nachfass: {len(df)} Zeilen × {len(df.columns)} Spalten")
    print(f"[DEBUG] Beispielzeile: {df.head(1).to_dict(orient='records')}")

    # In DB speichern
    await save_df_text(df, "nf_master_final")

    if job_obj:
        job_obj.phase = f"Nachfass abgeschlossen – {len(df)} Zeilen"
        job_obj.percent = 45

    return df


# =============================================================================
# Export-Start – Nachfass (unverändert, ruft oben stehende Funktion auf)
# =============================================================================
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

    async def update_progress(phase: str, percent: int):
        job.phase = phase
        job.percent = max(0, min(100, percent))
        await asyncio.sleep(0.05)

    async def _run():
        try:
            # 1️⃣ Lade Personen
            await update_progress("Lade Nachfass-Daten aus Pipedrive …", 5)
            df = await _build_nf_master_final(nf_batch_ids, batch_id, campaign, job_obj=job)

            if df.empty:
                job.error = "Keine Daten für Batch-ID(s) gefunden."
                job.phase = "Keine Daten"
                job.percent = 100
                job.done = True
                return

            total = len(df)
            if total > 8000:
                await update_progress(f"Reduziere Vergleichsdaten (nur 8000 von {total}) …", 30)
                df = df.sample(8000, random_state=42)

            # 2️⃣ Abgleich / Dublettenprüfung
            await update_progress("Führe Abgleich (Organisationen & IDs) durch …", 55)
            await reconcile_with_progress(job, "nf")

            # 3️⃣ Excel erzeugen
            await update_progress("Erzeuge Excel-Datei …", 80)
            ready = await load_df_text("nf_master_ready")
            export_df = build_export_from_ready(ready)
            job.excel_bytes = _df_to_excel_bytes(export_df)

            # 4️⃣ Abschluss
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



# =============================================================================
# NEUKONTAKTE – MASTER FINAL
# =============================================================================
async def _build_nk_master_final(
    fachbereich: str,
    take_count: Optional[int],
    batch_id: Optional[str],
    campaign: Optional[str],
    per_org_limit: int,
    job_obj=None,
) -> pd.DataFrame:
    """Erstellt den Master für Neukontakte nach Fachbereich und Limit."""
    fb_field = await get_person_field_by_hint(FIELD_FACHBEREICH_HINT)
    if not fb_field:
        raise RuntimeError("'Fachbereich'-Feld nicht gefunden.")
    fb_key = fb_field.get("key")

    person_fields = await get_person_fields()
    hint_to_key: Dict[str, str] = {}
    gender_map: Dict[str, str] = {}

    # Feldzuordnung vorbereiten
    for f in person_fields:
        nm = (f.get("name") or "").lower()
        for hint in [
            "prospect", "gender", "geschlecht", "titel", "title", "anrede",
            "position", "xing", "xing url", "xing profil", "linkedin",
            "email büro", "email buero", "office email"
        ]:
            if hint in nm and hint not in hint_to_key:
                hint_to_key[hint] = f.get("key")
        if any(x in nm for x in ("gender", "geschlecht")) and "options" in f:
            gender_map = {str(o["id"]): o["label"] for o in f.get("options", [])}

    def get_field(p: dict, hint: str) -> str:
        key = hint_to_key.get(hint)
        if not key:
            return ""
        v = p.get(key)
        if isinstance(v, dict) and "label" in v:
            return str(v["label"])
        if isinstance(v, list):
            if v and isinstance(v[0], dict) and "value" in v[0]:
                return str(v[0]["value"])
            return ", ".join([str(x) for x in v if x])
        if v is None:
            return ""
        sv = str(v)
        if hint in ("gender", "geschlecht") and gender_map:
            return gender_map.get(sv, sv)
        return sv

    selected: List[dict] = []
    org_used: Dict[str, int] = {}

    if job_obj:
        job_obj.phase = "Lade Neukontakte aus Pipedrive …"
        job_obj.percent = 10

    async for chunk in stream_persons_by_filter(FILTER_NEUKONTAKTE):
        for p in chunk:
            if str(p.get(fb_key)) != str(fachbereich):
                continue
            org = p.get("org_id") or {}
            org_key = f"id:{org.get('id')}" if org.get("id") else f"name:{normalize_name(org.get('name') or '')}"
            used = org_used.get(org_key, 0)
            if used >= per_org_limit:
                continue
            org_used[org_key] = used + 1
            selected.append(p)
            if take_count and len(selected) >= take_count:
                break
        if take_count and len(selected) >= take_count:
            break

    if job_obj:
        job_obj.phase = f"Neukontakte gesammelt: {len(selected)}"
        job_obj.percent = 40

    rows = []
    for p in selected:
        org = p.get("org_id") or {}
        org_id = str(org.get("id") or org.get("value") or "")
        org_name = org.get("name") or p.get("org_name") or "-"
        emails = _as_list_email(p.get("email"))
        email = emails[0] if emails else ""
        rows.append({
            "Batch ID": batch_id or "",
            "Channel": DEFAULT_CHANNEL,
            "Cold-Mailing Import": campaign or "",
            "Prospect ID": get_field(p, "prospect"),
            "Organisation ID": org_id,
            "Organisation Name": org_name,
            "Person ID": str(p.get("id") or ""),
            "Person Vorname": p.get("first_name") or "",
            "Person Nachname": p.get("last_name") or "",
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


async def _reconcile_generic(prefix: str, job_obj=None):
    """Führt vereinfachten, performanten Abgleich durch."""
    master = await load_df_text(f"{prefix}_master_final")
    if master.empty:
        await save_df_text(pd.DataFrame(), f"{prefix}_master_ready")
        await save_df_text(
            pd.DataFrame(columns=["reason", "id", "name", "org_id", "org_name", "extra"]),
            f"{prefix}_delete_log",
        )
        return

    if len(master) > 5000:
        master = master.sample(5000, random_state=42).reset_index(drop=True)
        if job_obj:
            job_obj.phase = f"Reduziere Vergleichsdaten (5000 von {len(master)})"
            job_obj.percent = 45

    col_person_id = "Person ID"
    col_org_name = "Organisation Name"
    col_org_id = "Organisation ID"

    delete_rows: List[Dict[str, str]] = []
    drop_idx = []

    # 1️⃣ Organisations-Dubletten
    filter_ids_org = [1245]
    if job_obj:
        job_obj.phase = "Orga-Abgleich läuft …"
        job_obj.percent = 55

    buckets_all = {}
    for fid in filter_ids_org:
        caps = await _fetch_org_names_for_filter_capped(fid, PAGE_LIMIT, MAX_ORG_NAMES, MAX_ORG_BUCKET)
        for k, lst in caps.items():
            slot = buckets_all.setdefault(k, [])
            slot.extend(lst[:MAX_ORG_BUCKET])

    for idx, row in master.iterrows():
        cand = str(row.get(col_org_name) or "").strip()
        norm = normalize_name(cand)
        if not norm or row.get(col_org_id):
            continue
        bucket = buckets_all.get(norm[:1])
        if not bucket:
            continue
        near = [n for n in bucket if abs(len(n) - len(norm)) <= 3]
        if not near:
            continue
        best = process.extractOne(norm, near, scorer=fast_fuzzy)
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

    # 2️⃣ Personen-ID-Dubletten
    if job_obj:
        job_obj.phase = "Prüfe Personen-IDs …"
        job_obj.percent = 70

    suspect_ids = set()
    async for ids in stream_person_ids_by_filter(1216, PAGE_LIMIT):
        suspect_ids.update(ids)
    async for ids in stream_person_ids_by_filter(1708, PAGE_LIMIT):
        suspect_ids.update(ids)

    if suspect_ids:
        mask = master[col_person_id].astype(str).isin(suspect_ids)
        removed = master[mask]
        for _, r in removed.iterrows():
            delete_rows.append({
                "reason": "person_id_match",
                "id": str(r.get(col_person_id) or ""),
                "name": f"{r.get('Person Vorname') or ''} {r.get('Person Nachname') or ''}".strip(),
                "org_id": str(r.get(col_org_id) or ""),
                "org_name": str(r.get(col_org_name) or ""),
                "extra": "ID in Filter 1216/1708",
            })
        master = master[~mask]

    # 3️⃣ Speichern
    await save_df_text(master, f"{prefix}_master_ready")
    await save_df_text(
        pd.DataFrame(delete_rows, columns=["reason", "id", "name", "org_id", "org_name", "extra"]),
        f"{prefix}_delete_log",
    )

    if job_obj:
        job_obj.phase = "Abgleich abgeschlossen"
        job_obj.percent = 85
# master_fixed_v2_part4.py — Teil 4/5
# Job-System, Fortschritt, Excel-Erzeugung & Export-Endpunkte

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

        await _reconcile_generic(prefix, job_obj=job)

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
# EXPORT-START – NACHFASS
# =============================================================================
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

    async def update_progress(msg: str, pct: int):
        job.phase = msg
        job.percent = min(100, max(0, pct))
        await asyncio.sleep(0.05)

    async def _run():
        try:
            # 1️⃣ Lade Personen
            await update_progress("Lade Nachfass-Daten aus Pipedrive …", 5)
            df = await _build_nf_master_final(nf_batch_ids, batch_id, campaign, job_obj=job)

            if df.empty:
                job.error = "Keine Daten für Batch-ID(s) gefunden."
                job.phase = "Keine Daten"
                job.percent = 100
                job.done = True
                return

            total = len(df)
            if total > 8000:
                await update_progress(f"Reduziere Vergleichsdaten (nur 8000 von {total}) …", 40)
                df = df.sample(8000, random_state=42)

            # 2️⃣ Abgleich / Dublettenprüfung
            await update_progress("Starte Abgleich (Organisationen & IDs) …", 55)
            await reconcile_with_progress(job, "nf")

            # 3️⃣ Excel erzeugen
            await update_progress("Erzeuge Excel-Datei …", 85)
            ready = await load_df_text("nf_master_ready")
            export_df = build_export_from_ready(ready)
            job.excel_bytes = _df_to_excel_bytes(export_df)

            # 4️⃣ Abschluss
            await update_progress(f"Fertig – {len(export_df)} Zeilen exportiert", 100)
            job.total_rows = len(export_df)
            job.done = True
            print(f"[Nachfass] Export abgeschlossen ({len(export_df)} Zeilen)")

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

@app.get("/nachfass/export_download")
async def nachfass_export_download(job_id: str = Query(...)):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job nicht gefunden")

    if job.excel_bytes:
        return StreamingResponse(
            io.BytesIO(job.excel_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={job.filename_base}.xlsx"},
        )

    if job.path and os.path.exists(job.path):
        return FileResponse(
            job.path,
            filename=f"{job.filename_base}.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    raise HTTPException(status_code=404, detail="Datei nicht gefunden")
# master_fixed_v2_part5.py — Teil 5/5
# UI (HTML), Summarys und Kampagnenübersicht

from fastapi.responses import HTMLResponse

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
</style>
</head>
<body>
<header><div class="hwrap"><div><a href='/campaign' style='color:#0a66c2;text-decoration:none'>← Kampagne wählen</a></div>
<div><b>Nachfass</b></div><div>{auth_info}</div></div></header>
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
</main>

<div id="overlay"><div id="phase" style="color:#0f172a;font-weight:500"></div>
<div class="barwrap"><div class="bar" id="bar"></div></div></div>

<script>
const el = id => document.getElementById(id);

// Overlay Steuerung
function showOverlay(msg) {{ el('phase').textContent = msg || ''; el('overlay').style.display = 'flex'; }}
function hideOverlay() {{ el('overlay').style.display = 'none'; }}
function setProgress(p) {{ el('bar').style.width = Math.max(0, Math.min(100, p)) + '%'; }}

// Batch IDs parsen (max. 2)
function _parseIDs(raw) {{
  return raw.split(/[\\n,;]/).map(s => s.trim()).filter(Boolean).slice(0, 2);
}}

// Export starten
async function startExportNf() {{
  const ids = _parseIDs(el('nf_batch_ids').value);
  if (ids.length === 0) return alert('Bitte mindestens eine Batch ID angeben.');
  const bid = el('batch_id').value || '';
  const camp = el('campaign').value || '';

  showOverlay('Starte Abgleich …');
  setProgress(5);

  try {{
    const r = await fetch('/nachfass/export_start', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{ nf_batch_ids: ids, batch_id: bid, campaign: camp }})
    }});
    if (!r.ok) throw new Error('Start fehlgeschlagen.');
    const {{ job_id }} = await r.json();
    await poll(job_id);
  }} catch (err) {{
    alert(err.message || 'Fehler beim Starten.');
    hideOverlay();
  }}
}}

// Fortschritt regelmäßig abfragen
async function poll(job_id) {{
  let done = false;
  let lastPhase = '';
  while (!done) {{
    await new Promise(r => setTimeout(r, 600));
    const r = await fetch('/nachfass/export_progress?job_id=' + encodeURIComponent(job_id));
    if (!r.ok) break;
    const s = await r.json();

    if (s.phase && s.phase !== lastPhase) {{
      el('phase').textContent = s.phase + ' (' + (s.percent || 0) + '%)';
      lastPhase = s.phase;
    }}

    setProgress(s.percent || 0);

    if (s.error) {{
      alert(s.error);
      hideOverlay();
      return;
    }}

    done = s.done;
  }}

  el('phase').textContent = 'Download startet …';
  setProgress(100);
  window.location.href = '/nachfass/export_download?job_id=' + encodeURIComponent(job_id);
  setTimeout(() => window.location.href = '/nachfass/summary?job_id=' + job_id, 1500);
}}

el('btnExportNf').addEventListener('click', startExportNf);
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
