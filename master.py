# master20251117_v1_FIXED.py
# BatchFlow – Nachfass (komplett überarbeitet, stabil & Pipedrive-safe)

import os, re, io, uuid, time, asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, AsyncGenerator
import numpy as np
import pandas as pd
import httpx
import asyncpg
from fastapi import FastAPI, Request, Body, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware
from rapidfuzz import fuzz, process

fuzz.default_processor = lambda s: s   # kein Vor-Preprocessing

# ------------------------------------------------------------
# FastAPI Grundsetup
# ------------------------------------------------------------

app = FastAPI(title="BatchFlow")
app.add_middleware(GZipMiddleware, minimum_size=1024)

if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# ------------------------------------------------------------
# Umgebungsvariablen & Konstanten
# ------------------------------------------------------------

PD_API_TOKEN = os.getenv("PD_API_TOKEN", "")
PIPEDRIVE_API = "https://api.pipedrive.com/v1"
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL fehlt (Neon-DSN).")

SCHEMA = os.getenv("PGSCHEMA", "public")

FILTER_NEUKONTAKTE = int(os.getenv("FILTER_NEUKONTAKTE", "2998"))
FILTER_NACHFASS   = int(os.getenv("FILTER_NACHFASS", "3024"))

DEFAULT_CHANNEL = "Cold E-Mail"

PAGE_LIMIT = 100
NF_PAGE_LIMIT = 100   # Pipedrive erlaubt maximal 100

# ------------------------------------------------------------
# Caches
# ------------------------------------------------------------

user_tokens: Dict[str, str] = {}
_PERSON_FIELDS_CACHE: Optional[List[dict]] = None

# ------------------------------------------------------------
# Template Spalten (für Export)
# ------------------------------------------------------------

TEMPLATE_COLUMNS = [
    "Batch ID","Channel","Cold-Mailing Import","Prospect ID","Organisation ID","Organisation Name",
    "Person ID","Person Vorname","Person Nachname","Person Titel","Person Geschlecht","Person Position",
    "Person E-Mail","XING Profil","LinkedIn URL"
]

# ------------------------------------------------------------
# FastAPI Startup / Shutdown
# ------------------------------------------------------------

def http_client() -> httpx.AsyncClient:
    return app.state.http

def get_pool() -> asyncpg.Pool:
    return app.state.pool

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

# ------------------------------------------------------------
# Hilfsfunktionen
# ------------------------------------------------------------

def normalize_name(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"[^a-z0-9 ]", "", s.lower())
    return re.sub(r"\s+", " ", s).strip()

def slugify_filename(name: str, fallback="BatchFlow_Export") -> str:
    s = re.sub(r"[^\w\-. ]+", "", (name or "").strip())
    return re.sub(r"\s+", "_", s) or fallback

async def ensure_table_text(conn: asyncpg.Connection, table: str, cols: List[str]):
    defs = ", ".join([f'"{c}" TEXT' for c in cols])
    await conn.execute(f'CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{table}" ({defs})')

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
        ph = ", ".join(f'${i}' for i in range(1, len(cols)+1))
        sql = f'INSERT INTO "{SCHEMA}"."{table}" ({cols_sql}) VALUES ({ph})'
        batch = []
        async with conn.transaction():
            for _, row in df.iterrows():
                vals = ["" if pd.isna(v) else str(v) for v in row.tolist()]
                batch.append(vals)
                if len(batch) >= 1000:
                    await conn.executemany(sql, batch)
                    batch = []
            if batch:
                await conn.executemany(sql, batch)

async def load_df_text(table: str) -> pd.DataFrame:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(f'SELECT * FROM "{SCHEMA}"."{table}"')
    if not rows:
        return pd.DataFrame()
    cols = list(rows[0].keys())
    return pd.DataFrame([{c:r[c] for c in cols} for r in rows]).replace({"":np.nan})

# ------------------------------------------------------------
# Pipedrive Helper
# ------------------------------------------------------------

def get_headers() -> Dict[str, str]:
    token = user_tokens.get("default", "")
    return {"Authorization": f"Bearer {token}"} if token else {}

def append_token(url: str) -> str:
    if "api_token=" in url:
        return url
    if not user_tokens.get("default") and PD_API_TOKEN:
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}api_token={PD_API_TOKEN}"
    return url
################################## Ende Teil 1 ##################################
# ------------------------------------------------------------
# Personenfelder & Feld-Mapping
# ------------------------------------------------------------

async def get_person_fields() -> List[dict]:
    global _PERSON_FIELDS_CACHE
    if _PERSON_FIELDS_CACHE is not None:
        return _PERSON_FIELDS_CACHE

    url = append_token(f"{PIPEDRIVE_API}/personFields")
    r = await http_client().get(url, headers=get_headers())
    r.raise_for_status()
    _PERSON_FIELDS_CACHE = r.json().get("data") or []
    return _PERSON_FIELDS_CACHE


async def get_batch_field_key() -> Optional[str]:
    """
    Findet das Pipedrive-Feld „Batch ID“ korrekt.
    """
    fields = await get_person_fields()
    for f in fields:
        nm = (f.get("name") or "").lower()
        if nm == "batch id" or "batch" in nm:
            return f.get("key")
    return None


def field_options_id_to_label_map(field: dict) -> Dict[str, str]:
    opts = field.get("options") or []
    mp = {}
    for o in opts:
        oid = str(o.get("id"))
        lab = str(o.get("label") or o.get("name") or oid)
        mp[oid] = lab
    return mp


# ------------------------------------------------------------
# Personen-IDs aus beliebigen Filtern
# ------------------------------------------------------------

async def stream_person_ids_by_filter(filter_id: int) -> AsyncGenerator[List[str], None]:
    """
    Liefert Personen-IDs seitenweise aus einem Pipedrive-Filter (stabil).
    """
    start = 0
    limit = 100

    while True:
        url = append_token(
            f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={limit}&sort=id"
        )
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive Fehler (IDs {filter_id}): {r.text}")

        data = (r.json() or {}).get("data") or []
        if not data:
            break

        yield [str(p.get("id")) for p in data if p.get("id")]

        if len(data) < limit:
            break

        start += limit


# ------------------------------------------------------------
# NEU: EXAKTE Batch-Feld-Suche per dynamischem Pipedrive-Filter
# ------------------------------------------------------------

async def get_person_ids_by_batch_value(batch_field_key: str, batch_value: str) -> list[str]:
    """
    Baut einen temporären Pipedrive-Filter:
        Personen, deren Batch-Feld == batch_value
    Holt ALLE IDs, komplett & stabil paginiert.
    """

    # Filter anlegen
    payload = {
        "name": f"tmp_batch_{batch_value}",
        "conditions": [
            {
                "field_id": batch_field_key,
                "operator": "=",
                "value": batch_value
            }
        ],
        "type": "people"
    }

    create_url = append_token(f"{PIPEDRIVE_API}/filters")
    r = await http_client().post(create_url, headers=get_headers(), json=payload)
    r.raise_for_status()
    filter_id = r.json()["data"]["id"]

    # IDs sammeln
    all_ids = []
    async for chunk in stream_person_ids_by_filter(filter_id):
        all_ids.extend(chunk)

    # Filter wieder löschen (Clean-Up)
    try:
        del_url = append_token(f"{PIPEDRIVE_API}/filters/{filter_id}")
        await http_client().delete(del_url, headers=get_headers())
    except:
        pass

    return all_ids


# ------------------------------------------------------------
# Vollständige Personendetails holen
# ------------------------------------------------------------

async def fetch_person_details(person_ids: List[str]) -> List[dict]:
    """
    Holt vollständige Datensätze für jede Person.
    Sehr stabil, parallelisiert, Pipedrive-safe.
    """
    results = []
    sem = asyncio.Semaphore(8)

    async def load_one(pid: str):
        async with sem:
            url = append_token(f"{PIPEDRIVE_API}/persons/{pid}")
            r = await http_client().get(url, headers=get_headers())
            if r.status_code == 200:
                d = r.json().get("data")
                if d:
                    results.append(d)
            await asyncio.sleep(0.05)

    await asyncio.gather(*[load_one(pid) for pid in person_ids])
    print(f"[DEBUG] Vollständige Personendaten geladen: {len(results)}")
    return results


# ------------------------------------------------------------
# NEUE Hauptfunktion für Batch-Feld-basierte Personenladung
# ------------------------------------------------------------

async def load_persons_by_batch_ids(nf_batch_ids: list[str]) -> list[dict]:
    """
    Endgültige, stabile Lösung.
    Holt Personen exakt gemäß Batch-Feld.
    Vergleichbar mit ‚Batch ID = XYZ‘ im Pipedrive UI.
    """

    batch_field_key = await get_batch_field_key()
    if not batch_field_key:
        raise Exception("Batch-Feld nicht gefunden! Name muss ‚Batch ID‘ sein.")

    all_ids = set()

    # 1. IDs pro Batch-ID holen
    for bid in nf_batch_ids:
        ids = await get_person_ids_by_batch_value(batch_field_key, bid)
        all_ids.update(ids)

    # 2. Details laden
    persons = await fetch_person_details(list(all_ids))

    print(f"[Nachfass] Batch-Feld-Treffer geladen: {len(persons)} Personen")
    return persons


# ------------------------------------------------------------
# Helper für Activity- und Datumsfelder
# ------------------------------------------------------------

_NEXT_ACTIVITY_KEY = None
_LAST_ACTIVITY_KEY = None

async def get_next_activity_key() -> Optional[str]:
    global _NEXT_ACTIVITY_KEY
    if _NEXT_ACTIVITY_KEY is not None:
        return _NEXT_ACTIVITY_KEY
    fields = await get_person_fields()
    for f in fields:
        nm = (f.get("name") or "").lower()
        if "next activity" in nm or "nächste" in nm:
            _NEXT_ACTIVITY_KEY = f.get("key")
            return _NEXT_ACTIVITY_KEY
    return None


async def get_last_activity_key() -> Optional[str]:
    global _LAST_ACTIVITY_KEY
    if _LAST_ACTIVITY_KEY is not None:
        return _LAST_ACTIVITY_KEY
    fields = await get_person_fields()
    for f in fields:
        nm = (f.get("name") or "").lower()
        if "last activity" in nm or "letzte" in nm:
            _LAST_ACTIVITY_KEY = f.get("key")
            return _LAST_ACTIVITY_KEY
    return None

################################## Ende Teil 2 ##################################

# ============================================================
# Nachfass – Aufbau Master (NEU, stabil & 100% korrekt)
# ============================================================

import datetime as dt
from collections import defaultdict

# Feld-Mapping-Hints (für get_field)
PERSON_FIELD_HINTS_TO_EXPORT = {
    "prospect": "prospect",
    "titel": "titel",
    "title": "title",
    "anrede": "anrede",
    "gender": "gender",
    "geschlecht": "geschlecht",
    "position": "position",
    "xing": "xing",
    "xing url": "xing url",
    "xing profil": "xing profil",
    "linkedin": "linkedin",
    "linkedin url": "linkedin url",
}


async def _build_nf_master_final(
    nf_batch_ids: list[str],
    batch_id: str,
    campaign: str,
    job_obj=None
) -> pd.DataFrame:

    print(f"[Nachfass] Starte Master-Build für Batch-IDs: {nf_batch_ids}")

    # ------------------------------------------------------------
    # Personenfelder einlesen + Mapping vorbereiten
    # ------------------------------------------------------------

    person_fields = await get_person_fields()
    hint_to_key = {}
    gender_map = {}
    next_activity_key = None

    for f in person_fields:
        nm = (f.get("name") or "").lower()

        # Feldmapping für Export vorbereiten
        for hint in PERSON_FIELD_HINTS_TO_EXPORT:
            if hint in nm and hint not in hint_to_key:
                hint_to_key[hint] = f.get("key")

        # Gender Mapping
        if "gender" in nm or "geschlecht" in nm:
            gender_map = {str(o["id"]): o["label"] for o in (f.get("options") or [])}

        # Nächste Aktivität
        if "next activity" in nm or "datum nächste" in nm:
            next_activity_key = f.get("key")

    # Minihelper zum Feldauslesen
    def get_field(p: dict, hint: str) -> str:
        key = hint_to_key.get(hint)
        if not key:
            return ""
        v = p.get(key)

        if isinstance(v, dict):
            return v.get("label") or v.get("value") or ""

        if isinstance(v, list):
            values = []
            for item in v:
                if isinstance(item, dict):
                    values.append(item.get("value") or item.get("label") or "")
                else:
                    values.append(str(item))
            return ", ".join([x for x in values if x])

        if hint in ("gender", "geschlecht") and gender_map:
            return gender_map.get(str(v), str(v))

        return str(v or "")

    # ------------------------------------------------------------
    # 1) Personen laden (NEU: stabile Filtertechnik)
    # ------------------------------------------------------------

    persons = await load_persons_by_batch_ids(nf_batch_ids)
    print(f"[INFO] {len(persons)} Personen vor Filterlogik")

    selected = []
    excluded = []

    org_counter = defaultdict(int)
    now = dt.datetime.now()

    # ------------------------------------------------------------
    # 2) Vorabfilter-Regeln anwenden
    # ------------------------------------------------------------

    for p in persons:
        pid = str(p.get("id") or "")

        name = p.get("name") or ""

        # Organisation extrahieren
        org = p.get("organization") or {}
        org_id = str(org.get("id") or "")
        org_name = org.get("name") or "-"

        # Fallback für verschobene Orga-Struktur
        if not org_id or org_name == "-":
            meta = p.get("metadata", {})
            if "organization" in meta:
                org_meta = meta["organization"]
                org_id = org_meta.get("id") or org_id
                org_name = org_meta.get("name") or org_name

        # **Regel 1: nächste Aktivität (nicht nachfassen, wenn <= 90 Tage oder Zukunft)**
        if next_activity_key:
            raw_date = p.get(next_activity_key)
            if raw_date:
                try:
                    d = dt.datetime.fromisoformat(str(raw_date)[:10])
                    delta = (now - d).days

                    if delta < 0 or 0 <= delta <= 90:
                        excluded.append({
                            "Kontakt ID": pid,
                            "Name": name,
                            "Organisation ID": org_id,
                            "Organisationsname": org_name,
                            "Grund": "Datum nächste Aktivität < 3 Monate oder in Zukunft"
                        })
                        continue
                except:
                    pass

        # **Regel 2: Pro Organisation nur max. 2 Kontakte**
        if org_id:
            org_counter[org_id] += 1
            if org_counter[org_id] > 2:
                excluded.append({
                    "Kontakt ID": pid,
                    "Name": name,
                    "Organisation ID": org_id,
                    "Organisationsname": org_name,
                    "Grund": "Mehr als 2 Kontakte pro Organisation"
                })
                continue

        selected.append(p)

    print(f"[INFO] Nach Vorabfilter: {len(selected)} akzeptiert, {len(excluded)} ausgeschlossen")

    # ------------------------------------------------------------
    # 3) DataFrame aufbauen (Exportstruktur)
    # ------------------------------------------------------------

    rows = []
    for p in selected:
        pid = str(p.get("id"))
        org = p.get("organization") or {}
        org_name = org.get("name") or "-"
        org_id = str(org.get("id") or "")

        name = p.get("name") or ""
        vor, nach = "", ""

        parts = name.split()
        if len(parts) == 1:
            vor = parts[0]
        elif len(parts) >= 2:
            vor = " ".join(parts[:-1])
            nach = parts[-1]

        emails = p.get("emails") or []
        email = ""
        if emails:
            first = emails[0]
            if isinstance(first, dict):
                email = first.get("value") or ""
            else:
                email = str(first)

        # XING Feld extrahieren
        xing = ""
        for key, val in p.items():
            if isinstance(key, str) and "xing" in key.lower():
                if isinstance(val, str) and val.startswith("http"):
                    xing = val
                elif isinstance(val, list):
                    xing = ", ".join(
                        [x.get("value") for x in val if isinstance(x, dict) and x.get("value")]
                    )
                break

        rows.append({
            "Batch ID": batch_id,
            "Channel": DEFAULT_CHANNEL,
            "Cold-Mailing Import": campaign,
            "Prospect ID": get_field(p, "prospect"),
            "Organisation ID": org_id,
            "Organisation Name": org_name,
            "Person ID": pid,
            "Person Vorname": vor,
            "Person Nachname": nach,
            "Person Titel": get_field(p, "titel") or get_field(p, "title") or get_field(p, "anrede"),
            "Person Geschlecht": get_field(p, "gender") or get_field(p, "geschlecht"),
            "Person Position": get_field(p, "position"),
            "Person E-Mail": email,
            "XING Profil": xing,
            "LinkedIn URL": get_field(p, "linkedin") or get_field(p, "linkedin url"),
        })

    df = pd.DataFrame(rows)

    # ------------------------------------------------------------
    # 4) Excluded DataFrame separat speichern
    # ------------------------------------------------------------

    excluded_df = pd.DataFrame(excluded) if excluded else pd.DataFrame([{
        "Kontakt ID": "-",
        "Name": "-",
        "Organisation ID": "-",
        "Organisationsname": "-",
        "Grund": "Keine Datensätze ausgeschlossen"
    }])

    await save_df_text(excluded_df, "nf_excluded")

    # Save final
    await save_df_text(df, "nf_master_final")

    print(f"[Nachfass] Master Final erzeugt: {len(df)} Datensätze")

    return df

################################## Ende Teil 3 ##################################
# ============================================================
# Reconcile – Entfernt Orga-Dubletten (≥95 %) + Kontakt-Dubletten
# ============================================================

async def _fetch_org_names_for_filter_capped(
    filter_id: int, page_limit: int, cap_total: int, cap_bucket: int
) -> Dict[str, List[str]]:
    """
    Holt Organisationsnamen aus einem Pipedrive-Filter – capped für Performance.
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


def bucket_key(name: str) -> str:
    """2-Buchstaben-Bucket als Fuzzy-Optimierung."""
    n = normalize_name(name)
    return n[:2] if len(n) > 1 else n


def fast_fuzzy(a: str, b: str) -> int:
    """Schnellerer Fuzzy-Matcher."""
    return fuzz.partial_ratio(a, b)


# ============================================================
# Abschluss-Abgleich für Nachfass
# ============================================================

async def _reconcile(prefix: str) -> None:
    """
    1. Orga-Dubletten (1245, 851, 1521)
    2. Personen-ID-Dubletten (1216, 1708)
    Speichert *_master_ready + *_delete_log.
    """
    t = tables(prefix)
    master = await load_df_text(t["final"])

    if master is None or master.empty:
        await save_df_text(pd.DataFrame(), t["ready"])
        await save_df_text(pd.DataFrame(columns=["reason","id","name","org_id","org_name","extra"]), t["log"])
        return

    col_pid = "Person ID"
    col_oname = "Organisation Name"
    col_oid = "Organisation ID"

    delete_rows: List[Dict[str, str]] = []

    # ------------------------------------------------------------
    # 1) Organisationen einsammeln
    # ------------------------------------------------------------

    filter_ids_org = [1245, 851, 1521]
    buckets_all: Dict[str, List[str]] = {}
    total = 0

    for fid in filter_ids_org:
        overshoot = max(0, MAX_ORG_NAMES - total)
        if overshoot <= 0:
            break

        buckets = await _fetch_org_names_for_filter_capped(fid, PAGE_LIMIT, overshoot, MAX_ORG_BUCKET)
        for key, lst in buckets.items():
            slot = buckets_all.setdefault(key, [])
            for name in lst:
                if len(slot) >= MAX_ORG_BUCKET:
                    break
                if name not in slot:
                    slot.append(name)
                    total += 1

    # ------------------------------------------------------------
    # 2) Fuzzy-Orga-Abgleich ≥95 %
    # ------------------------------------------------------------

    drop_idx = []
    for idx, row in master.iterrows():
        org_name = str(row.get(col_oname) or "")
        org_norm = normalize_name(org_name)

        if not org_norm:
            continue

        b = org_norm[0]
        bucket = buckets_all.get(b)
        if not bucket:
            continue

        candidates = [x for x in bucket if abs(len(x) - len(org_norm)) <= 4]
        if not candidates:
            continue

        best = process.extractOne(org_norm, candidates, scorer=fuzz.token_sort_ratio)
        if best and best[1] >= 95:
            drop_idx.append(idx)
            delete_rows.append({
                "reason": "org_match_95",
                "id": str(row.get(col_pid)),
                "name": f"{row.get('Person Vorname')} {row.get('Person Nachname')}".strip(),
                "org_id": str(row.get(col_oid)),
                "org_name": org_name,
                "extra": f"Ähnlichkeit {best[1]} %"
            })

    if drop_idx:
        master = master.drop(index=drop_idx)

    # ------------------------------------------------------------
    # 3) Personen-ID-Dubletten (bereits kontaktiert)
    # ------------------------------------------------------------

    suspect_ids = set()
    for fid in (1216, 1708):
        async for page in stream_person_ids_by_filter(fid):
            suspect_ids.update(page)

    if suspect_ids:
        mask = master[col_pid].astype(str).isin(suspect_ids)
        removed = master[mask]

        for _, r in removed.iterrows():
            delete_rows.append({
                "reason": "person_id_match",
                "id": str(r.get(col_pid)),
                "name": f"{r.get('Person Vorname')} {r.get('Person Nachname')}".strip(),
                "org_id": str(r.get(col_oid)),
                "org_name": str(r.get(col_oname)),
                "extra": "Bereits in Kontakt-Filter 1216/1708"
            })

        master = master[~mask]

    # ------------------------------------------------------------
    # 4) Speichern
    # ------------------------------------------------------------

    await save_df_text(master, t["ready"])
    log_df = pd.DataFrame(delete_rows, columns=["reason","id","name","org_id","org_name","extra"])
    await save_df_text(log_df, t["log"])

    print(f"[Reconcile] {len(log_df)} Zeilen im Delete-Log gespeichert.")


# ============================================================
# Excel-Export-Helfer
# ============================================================

def build_export_from_ready(df: pd.DataFrame) -> pd.DataFrame:
    """Ordnet Spalten exakt wie TEMPLATE_COLUMNS."""
    out = pd.DataFrame(columns=TEMPLATE_COLUMNS)
    for col in TEMPLATE_COLUMNS:
        if col in df.columns:
            out[col] = df[col]
        else:
            out[col] = ""
    for c in ("Organisation ID", "Person ID"):
        if c in out.columns:
            out[c] = out[c].astype(str).fillna("").replace("nan", "")
    return out


def _df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    """Konvertiert DataFrame in Excel-Datei."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Export")
        ws = writer.sheets["Export"]

        # Spalten formatieren (Textformat)
        col_index = {col: i + 1 for i, col in enumerate(df.columns)}
        for name in ("Organisation ID", "Person ID"):
            if name in col_index:
                coln = col_index[name]
                for i in range(2, len(df) + 2):
                    ws.cell(i, coln).number_format = "@"

    buf.seek(0)
    return buf.getvalue()


################################## Ende Teil 4 ##################################
# ============================================================
# Job-Verwaltung
# ============================================================

class Job:
    def __init__(self):
        self.phase = "Warten…"
        self.percent = 0
        self.done = False
        self.error = None
        self.path = None
        self.total_rows = 0
        self.filename_base = "BatchFlow_Export"
        self.excel_bytes = None
        self.excluded_count = 0

JOBS: Dict[str, Job] = {}


# ============================================================
# Reconcile mit Fortschritt
# ============================================================

async def reconcile_with_progress(job: Job, prefix: str):
    try:
        job.phase = "Vorbereitung läuft…"; job.percent = 10
        await asyncio.sleep(0.2)

        job.phase = "Lade Vergleichsdaten…"; job.percent = 25
        await asyncio.sleep(0.2)

        await _reconcile(prefix)

        job.phase = "Abgleich abgeschlossen"; job.percent = 100
        job.done = True

    except Exception as e:
        job.error = f"Fehler beim Abgleich: {e}"
        job.phase = "Fehler"
        job.percent = 100
        job.done = True


# ============================================================
# /nachfass/export_start  — KORRIGIERTER ENDPOINT
# ============================================================

@app.post("/nachfass/export_start")
async def export_start_nf(request: Request):
    """
    Startet den Nachfass-Export:
    - Lädt Personen über stabile Batch-ID-Suche
    - Baut nf_master_final
    - Führt Abgleich durch
    - Bietet Download an
    """
    try:
        data = await request.json()
        nf_batch_ids = data.get("nf_batch_ids") or []
        batch_id = data.get("batch_id") or ""
        campaign = data.get("campaign") or ""

        if not nf_batch_ids:
            return JSONResponse({"error": "Keine Batch-IDs übergeben."}, status_code=400)

        # Job erstellen
        job_id = str(uuid.uuid4())
        job = Job()
        job.id = job_id
        job.phase = "Starte Nachfass-Export…"
        job.percent = 1
        JOBS[job_id] = job

        # Asynchron ausführen
        async def run():
            try:
                # STEP 1 – Personen laden
                job.phase = "Lade Nachfass-Daten…"
                job.percent = 5

                df = await _build_nf_master_final(
                    nf_batch_ids=nf_batch_ids,
                    batch_id=batch_id,
                    campaign=campaign,
                    job_obj=job
                )

                await save_df_text(df, "nf_master_final")

                job.phase = f"Daten geladen ({len(df)} Zeilen)"
                job.percent = 60

                # STEP 2 – Abgleich
                job.phase = "Führe Abgleichslogik aus…"
                job.percent = 80

                await reconcile_with_progress(job, "nf")

                # STEP 3 – Abschluss
                job.phase = "Export + Abgleich abgeschlossen"
                job.percent = 100
                job.done = True

                # Anzahl Excluded
                try:
                    exc = await load_df_text("nf_excluded")
                    job.excluded_count = len(exc)
                except:
                    job.excluded_count = 0

                print(f"[Nachfass] Export abgeschlossen – {len(df)} Zeilen, excluded={job.excluded_count}")

            except Exception as e:
                job.error = str(e)
                job.phase = "Fehler während Verarbeitung"
                job.percent = 100
                job.done = True
                print(f"[ERROR] Nachfass Fehler: {e}")

        asyncio.create_task(run())

        return JSONResponse({"job_id": job_id})

    except Exception as e:
        print(f"[ERROR] /nachfass/export_start: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)



# ============================================================
# Fortschritt-Endpunkte
# ============================================================

@app.get("/nachfass/export_progress")
async def nachfass_export_progress(job_id: str = Query(...)):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job nicht gefunden")

    return JSONResponse({
        "phase": job.phase,
        "percent": job.percent,
        "done": job.done,
        "error": job.error
    })


# ============================================================
# Download des erzeugten Nachfass-Exports
# ============================================================

@app.get("/nachfass/export_download")
async def nachfass_export_download(job_id: str = Query(...)):

    df = await load_df_text("nf_master_final")
    if df is None or df.empty:
        return JSONResponse({"detail": "Keine Exportdaten gefunden"}, status_code=404)

    file_path = f"/tmp/nachfass_{job_id}.xlsx"
    df.to_excel(file_path, index=False)

    return FileResponse(
        file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"nachfass_{job_id}.xlsx"
    )


# ============================================================
# Ausschlüsse anzeigen: /nachfass/excluded/json und /excluded
# ============================================================

@app.get("/nachfass/excluded/json")
async def nachfass_excluded_json():

    excluded_df = pd.DataFrame()
    deleted_df = pd.DataFrame()

    # Excluded (aus Master-Build)
    try:
        excluded_df = await load_df_text("nf_excluded")
        excluded_df["Quelle"] = "Batch-/Filter-Ausschluss"
    except:
        pass

    # Delete-Log (aus Abgleich)
    try:
        deleted_df = await load_df_text("nf_delete_log")
        deleted_df["Quelle"] = "Abgleich (Fuzzy/Kontaktfilter)"
    except:
        pass

    df = pd.concat([excluded_df, deleted_df], ignore_index=True)

    if df.empty:
        return JSONResponse({"total": 0, "rows": []})

    rows = df.tail(200).to_dict(orient="records")

    return JSONResponse({"total": len(df), "rows": rows})


@app.get("/nachfass/excluded", response_class=HTMLResponse)
async def nachfass_excluded_page():
    return HTMLResponse("""
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
        }
        table { width: 100%; border-collapse: collapse; }
        th, td {
          border-bottom: 1px solid #eee;
          padding: 8px;
          text-align: left;
        }
        th { background: #f8fafc; }
        tr:hover td { background: #f1f5f9; }
        .center { text-align: center; color: #999; }
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
      async function loadTable(){
        const r = await fetch('/nachfass/excluded/json');
        const data = await r.json();
        const tbody = document.getElementById('excluded-table-body');
        tbody.innerHTML = '';

        if(!data.rows || data.rows.length === 0){
          tbody.innerHTML = '<tr><td colspan="6" class="center">Keine Daten</td></tr>';
          return;
        }

        for(const row of data.rows){
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td>${row["Kontakt ID"] || row.id || ""}</td>
            <td>${row.Name || row.name || ""}</td>
            <td>${row["Organisation ID"] || ""}</td>
            <td>${row["Organisationsname"] || row.org || ""}</td>
            <td>${row.Grund || row.reason || ""}</td>
            <td>${row.Quelle || ""}</td>`;
          tbody.appendChild(tr);
        }
      }
      loadTable();
      </script>
    </body>
    </html>
    """)


# ============================================================
# Redirects & Fallbacks (für Pipedrive /overview usw.)
# ============================================================
@app.get("/{full_path:path}", include_in_schema=False)
async def catch_all(full_path: str, request: Request):

    # 1. Gültige Seiten explizit zulassen
    allowed_paths = {
        "", "/", "campaign", "neukontakte", "nachfass",
        "neukontakte/export_start", "neukontakte/export_progress",
        "neukontakte/export_download", "nachfass/export_start",
        "nachfass/export_progress", "nachfass/export_download",
        "nachfass/excluded", "nachfass/excluded/json"
    }

    if full_path in allowed_paths:
        raise HTTPException(status_code=404, detail="Not found")

    # 2. Alles andere auf die Kampagnenstartseite lenken
    return RedirectResponse("/campaign", status_code=302)



################################## Ende Teil 5 ##################################

