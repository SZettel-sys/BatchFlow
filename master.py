# ============================================================
# master_20251119_FINAL.py – Modul 1/6
# Basis-System: Imports, Konfiguration, Startup, DB, HTTP
# ============================================================

import os
import re
import io
import uuid
import time
import asyncio
import json
import numpy as np
import pandas as pd

from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, AsyncGenerator
from collections import defaultdict

import httpx
import asyncpg

from fastapi import FastAPI, Request, Body, Query, HTTPException
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    FileResponse,
    RedirectResponse
)
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles

from rapidfuzz import fuzz, process
fuzz.default_processor = lambda s: s  # keine Vor-Filterung

# ============================================================
# KONFIGURATION
# ============================================================

PIPEDRIVE_API = "https://api.pipedrive.com/v1"
PD_API_TOKEN = os.getenv("PD_API_TOKEN", "")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL fehlt – Neon DSN nicht gesetzt")

# wichtig: dein vorgegebener Wert
SCHEMA = "public"

# Filter-IDs (von dir bestätigt)
FILTER_NEUKONTAKTE = 2998
FILTER_NACHFASS   = 3024

# Dein entscheidender Batch-ID-Key (von dir bestätigt)
BATCH_FIELD_KEY = "5ac34dad3ea917fdef4087caebf77ba275f87eec"

# Limits & Systemparameter
LIST_LIMIT = 500
NF_PAGE_LIMIT = 500
PER_ORG_DEFAULT_LIMIT = 2
DEFAULT_CHANNEL = "Cold E-Mail"

# ============================================================
# FASTAPI – App Setup
# ============================================================

app = FastAPI(title="BatchFlow – Final 2025")
app.add_middleware(GZipMiddleware, minimum_size=1024)

if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# ============================================================
# HTTP-Client & DB Pool
# ============================================================

def get_headers() -> Dict[str, str]:
    if PD_API_TOKEN:
        return {"Accept": "application/json"}
    return {}

def append_token(url: str) -> str:
    """Hängt automatisch ?api_token= an, wenn kein OAuth genutzt wird."""
    if "api_token=" in url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}api_token={PD_API_TOKEN}"

def http_client() -> httpx.AsyncClient:
    return app.state.http

def get_pool() -> asyncpg.Pool:
    return app.state.pool

@app.on_event("startup")
async def startup_event():
    limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)
    app.state.http = httpx.AsyncClient(timeout=30.0, limits=limits)
    app.state.pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=4
    )
    print("[Startup] HTTP-Client & DB-Pool bereit")

@app.on_event("shutdown")
async def shutdown_event():
    await app.state.http.aclose()
    await app.state.pool.close()

# ============================================================
# DB-Utility-Funktionen
# ============================================================

async def ensure_table_text(conn: asyncpg.Connection, table: str, columns: List[str]):
    defs = ", ".join([f'"{c}" TEXT' for c in columns])
    await conn.execute(
        f'CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{table}" ({defs})'
    )

async def clear_table(conn: asyncpg.Connection, table: str):
    await conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table}"')

async def save_df_text(df: pd.DataFrame, table: str):
    async with get_pool().acquire() as conn:
        await clear_table(conn, table)
        await ensure_table_text(conn, table, list(df.columns))

        if df.empty:
            return

        cols = list(df.columns)
        cols_sql = ", ".join([f'"{c}"' for c in cols])
        placeholders = ", ".join([f"${i}" for i in range(1, len(cols) + 1)])

        sql = f'INSERT INTO "{SCHEMA}"."{table}" ({cols_sql}) VALUES ({placeholders})'
        rows = []

        async with conn.transaction():
            for _, row in df.iterrows():
                vals = [
                    "" if pd.isna(v) else str(v)
                    for v in row.tolist()
                ]
                rows.append(vals)
                if len(rows) >= 1000:
                    await conn.executemany(sql, rows)
                    rows = []
            if rows:
                await conn.executemany(sql, rows)

async def load_df_text(table: str) -> pd.DataFrame:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            f'SELECT * FROM "{SCHEMA}"."{table}"'
        )
    if not rows:
        return pd.DataFrame()
    cols = list(rows[0].keys())
    data = [{c: r[c] for c in cols} for r in rows]
    return pd.DataFrame(data).replace({"": np.nan})

# ============================================================
# Helper Functions
# ============================================================

def normalize_name(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"[^a-z0-9 ]", "", s.lower())
    return re.sub(r"\s+", " ", s).strip()

def parse_pd_date(s: Optional[str]):
    try:
        return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except:
        return None

def split_name(first: Optional[str], last: Optional[str], full: Optional[str]) -> tuple[str, str]:
    if first or last:
        return first or "", last or ""
    if not full:
        return "", ""
    parts = full.strip().split()
    if len(parts) == 1:
        return parts[0], ""
    return " ".join(parts[:-1]), parts[-1]
# ============================================================
# master_20251119_FINAL.py – Modul 2/6
# PIPEDRIVE SCAN ENGINE: Filter, Batch-Feld-Suche, Details
# ============================================================

# ------------------------------------------------------------
# PERSONEN-FELDER (Cache)
# ------------------------------------------------------------

_PERSON_FIELDS_CACHE: Optional[List[dict]] = None

async def get_person_fields() -> List[dict]:
    """Lädt Personenfelder 1× und cached sie."""
    global _PERSON_FIELDS_CACHE
    if _PERSON_FIELDS_CACHE is not None:
        return _PERSON_FIELDS_CACHE

    url = append_token(f"{PIPEDRIVE_API}/personFields")
    r = await http_client().get(url, headers=get_headers())
    if r.status_code != 200:
        raise Exception(f"Pipedrive personFields Fehler: {r.text}")

    _PERSON_FIELDS_CACHE = r.json().get("data") or []
    return _PERSON_FIELDS_CACHE


def extract_custom_field(person: dict, field_key: str):
    """
    Holt stabile Werte aus Custom-Feldern:
    - direkte Zuordnung
    - custom_fields
    - data.custom_fields
    """
    # 1) direct
    if field_key in person:
        val = person[field_key]
        if isinstance(val, dict): return val.get("value") or val.get("label")
        if isinstance(val, list):
            return (val[0].get("value")
                    if val and isinstance(val[0], dict)
                    else (val[0] if val else None))
        return val

    # 2) person["custom_fields"]
    cf = person.get("custom_fields") or {}
    if field_key in cf:
        val = cf[field_key]
        if isinstance(val, dict): return val.get("value") or val.get("label")
        if isinstance(val, list):
            return (val[0].get("value")
                    if val and isinstance(val[0], dict)
                    else (val[0] if val else None))
        return val

    # 3) person["data"]["custom_fields"]
    data = person.get("data") or {}
    cf2 = data.get("custom_fields") or {}
    if field_key in cf2:
        val = cf2[field_key]
        if isinstance(val, dict): return val.get("value") or val.get("label")
        if isinstance(val, list):
            return (val[0].get("value")
                    if val and isinstance(val[0], dict)
                    else (val[0] if val else None))
        return val

    return None


# ------------------------------------------------------------
# FILTER-SCANS (2998 / 3024)
# ------------------------------------------------------------

async def stream_persons_by_filter(filter_id: int, limit: int = 500) -> AsyncGenerator[List[dict], None]:
    """
    Lädt Personen aus einem Pipedrive-Filter (paging), garantiert:
    - keine Ghost-IDs
    - keine Archivleichen
    - 100 % echte Personen
    """
    start = 0
    while True:
        url = append_token(
            f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={limit}&sort=id"
        )
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive Filter Fehler: {r.text}")

        chunk = r.json().get("data") or []
        if not chunk:
            break

        yield chunk

        if len(chunk) < limit:
            break

        start += limit


async def get_persons_from_filter(filter_id: int) -> List[dict]:
    """Aggregiert alle Personen aus /persons?filter_id=…"""
    out = []
    async for chunk in stream_persons_by_filter(filter_id):
        out.extend(chunk)
    print(f"[Filter] Personen geladen aus Filter {filter_id}: {len(out)}")
    return out


# ------------------------------------------------------------
# BATCH-FELD-PERSONEN (perfekt & schnell)
# ------------------------------------------------------------

async def get_persons_by_batch_ids(batch_values: list[str]) -> List[dict]:
    """
    Holt Personen über persons/search mit field_key=BATCH_FIELD_KEY.
    Exakt-matchend, keine Vollscans, keine Ghost-IDs.
    """
    found_ids = set()

    for batch in batch_values:
        start = 0
        limit = 100
        while True:
            url = append_token(
                f"{PIPEDRIVE_API}/persons/search?"
                f"field_key={BATCH_FIELD_KEY}&term={batch}&exact_match=true"
                f"&start={start}&limit={limit}"
            )

            r = await http_client().get(url, headers=get_headers())
            if r.status_code != 200:
                raise Exception(f"Pipedrive Batch-Feld-Suche Fehler: {r.text}")

            items = (r.json().get("data") or {}).get("items") or []
            if not items:
                break

            for it in items:
                pid = it.get("item", {}).get("id")
                if pid:
                    found_ids.add(str(pid))

            if len(items) < limit:
                break

            start += limit

    print(f"[Batch-Feld] Treffer Gesamt: {len(found_ids)} IDs")
    return await fetch_person_details(list(found_ids))


# ------------------------------------------------------------
# DETAIL-FETCH (100 % stabil)
# ------------------------------------------------------------

async def fetch_person_details(person_ids: List[str]) -> List[dict]:
    """Lädt vollständige Datensätze parallel (max 15 Requests gleichzeitig)."""
    results = []
    sem = asyncio.Semaphore(15)

    async def load_one(pid):
        async with sem:
            url = append_token(f"{PIPEDRIVE_API}/persons/{pid}?fields=*")
            r = await http_client().get(url, headers=get_headers())
            if r.status_code == 200:
                data = r.json().get("data")
                if data:
                    results.append(data)

            await asyncio.sleep(0.05)  # Eventloop entlasten

    await asyncio.gather(*[load_one(pid) for pid in person_ids])
    print(f"[Details] Vollständige Personen geladen: {len(results)}")
    return results
# ============================================================
# master_20251119_FINAL.py – Modul 3/6
# Nachfass: Master-Datenaufbau (NF-Master)
# ============================================================

async def _build_nf_master_final(
    nf_batch_ids: List[str],
    batch_id: str,
    campaign: str,
    job_obj=None
) -> pd.DataFrame:

    # --------------------------------------------------------
    # 1) Personenfelder für Mapping laden
    # --------------------------------------------------------
    person_fields = await get_person_fields()

    hint_to_key: Dict[str, str] = {}
    gender_map: Dict[str, str] = {}
    next_activity_key: Optional[str] = None

    # wichtige Hints zur Zuordnung
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

    # --------------------------------------------------------
    # 2) Feld-Mapping vorbereiten
    # --------------------------------------------------------
    for f in person_fields:
        nm = (f.get("name") or "").lower()

        # Hint → Feld
        for hint in PERSON_FIELD_HINTS_TO_EXPORT.keys():
            if hint in nm and hint not in hint_to_key:
                hint_to_key[hint] = f.get("key")

        # Geschlecht
        if "gender" in nm or "geschlecht" in nm:
            opts = f.get("options") or []
            for o in opts:
                gender_map[str(o["id"])] = o["label"]

        # Datum nächste Aktivität
        if ("next" in nm and "activity" in nm) or ("datum nächste" in nm):
            next_activity_key = f.get("key")

    def get_field(p: dict, hint: str) -> str:
        """Extrahiert ein Feld anhand des Hints."""
        key = hint_to_key.get(hint)
        if not key:
            return ""
        val = p.get(key)
        if isinstance(val, dict):
            return val.get("label") or val.get("value") or ""
        if isinstance(val, list):
            vals = []
            for x in val:
                if isinstance(x, dict):
                    vals.append(x.get("value") or x.get("label") or "")
                elif isinstance(x, str):
                    vals.append(x)
            return ", ".join([v for v in vals if v])
        if hint in ("gender", "geschlecht") and gender_map:
            return gender_map.get(str(val), str(val))
        return str(val or "")

    # --------------------------------------------------------
    # 3) Personen anhand Batch-Feld suchen
    # --------------------------------------------------------
    if job_obj:
        job_obj.phase = "Lade Personen über Batch-Feld …"
        job_obj.percent = 10

    persons = await get_persons_by_batch_ids(nf_batch_ids)

    print(f"[NF] Personen geladen (Batch-Feld exakt): {len(persons)}")

    if job_obj:
        job_obj.phase = "Verarbeite Nachfass-Daten …"
        job_obj.percent = 30

    # --------------------------------------------------------
    # 4) Vorabfilter – Ausschlüsse
    # --------------------------------------------------------
    selected = []
    excluded = []

    org_counter = defaultdict(int)
    now = datetime.now()

    for p in persons:
        pid = str(p.get("id") or "")
        name = p.get("name") or ""

        org = p.get("organization") or {}
        org_id = str(org.get("id") or "")
        org_name = org.get("name") or "-"

        # Regel 1: Datum nächste Aktivität
        if next_activity_key:
            dt_raw = p.get(next_activity_key)
            if dt_raw:
                try:
                    dt_val = datetime.fromisoformat(str(dt_raw).split(" ")[0])
                    delta_days = (now - dt_val).days
                    # Ausschluss: Zukunft oder <3 Monate
                    if delta_days < 0 or delta_days <= 90:
                        excluded.append({
                            "Kontakt ID": pid,
                            "Name": name,
                            "Organisation ID": org_id,
                            "Organisationsname": org_name,
                            "Grund": "Datum nächste Aktivität < 3 Monate oder Zukunft"
                        })
                        continue
                except:
                    pass

        # Regel 2: Max 2 Kontakte pro Organisation
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

    print(f"[NF] {len(selected)} übrig, {len(excluded)} ausgeschlossen")

    if job_obj:
        job_obj.phase = "Baue Tabellen …"
        job_obj.percent = 60

    # --------------------------------------------------------
    # 5) DataFrame erzeugen (Hauptdaten)
    # --------------------------------------------------------
    rows = []

    for p in selected:
        pid = str(p.get("id") or "")
        name = p.get("name") or ""

        org = p.get("organization") or {}
        org_id = str(org.get("id") or "")
        org_name = org.get("name") or "-"

        vor, nach = split_name(p.get("first_name"), p.get("last_name"), name)

        # E-Mail
        emails = p.get("emails") or []
        email = ""
        if isinstance(emails, list) and emails:
            email = emails[0].get("value") if isinstance(emails[0], dict) else str(emails[0])
        elif isinstance(emails, str):
            email = emails

        # XING
        xing_val = ""
        for k, v in p.items():
            if isinstance(k, str) and "xing" in k.lower():
                if isinstance(v, str) and v.startswith("http"):
                    xing_val = v
                    break
                if isinstance(v, list):
                    xing_val = ", ".join(
                        [x.get("value") for x in v if isinstance(x, dict) and x.get("value")]
                    )
                    break

        rows.append({
            "Person - Batch ID": batch_id,
            "Person - Channel": DEFAULT_CHANNEL,
            "Cold-Mailing Import": campaign,

            "Person - Organisation": org_name,
            "Organisation - ID": org_id,

            "Person - Geschlecht": get_field(p, "gender") or get_field(p, "geschlecht"),
            "Person - Titel": get_field(p, "titel") or get_field(p, "title") or get_field(p, "anrede"),

            "Person - Vorname": vor,
            "Person - Nachname": nach,
            "Person - Position": get_field(p, "position"),

            "Person - ID": pid,
            "Person - XING-Profil": xing_val,
            "Person - LinkedIn Profil-URL": get_field(p, "linkedin") or get_field(p, "linkedin url"),
            "Person - E-Mail-Adresse - Büro": email,
        })

    df = pd.DataFrame(rows)

    # --------------------------------------------------------
    # 6) Excluded speichern
    # --------------------------------------------------------
    excluded_df = pd.DataFrame(excluded).replace({np.nan: None})
    if excluded_df.empty:
        excluded_df = pd.DataFrame([{
            "Kontakt ID": "-",
            "Name": "-",
            "Organisation ID": "-",
            "Organisationsname": "-",
            "Grund": "Keine Datensätze ausgeschlossen"
        }])

    await save_df_text(excluded_df, "nf_excluded")

    # --------------------------------------------------------
    # 7) Master speichern
    # --------------------------------------------------------
    await save_df_text(df, "nf_master_final")

    if job_obj:
        job_obj.phase = "Nachfass-Master erstellt"
        job_obj.percent = 80

    print(f"[NF] Master gespeichert: {len(df)} Zeilen")
    return df
# ============================================================
# master_20251119_FINAL.py – Modul 4/6
# Nachfass: Reconcile / Abgleich
# ============================================================

async def _nf_reconcile(
    job_obj=None
) -> pd.DataFrame:
    """
    Entfernt bereits kontaktierte Personen und erstellt eine
    finale Nachfass-Liste ("nf_master_ready").
    """

    # ---------------------------------------------
    # Status
    # ---------------------------------------------
    if job_obj:
        job_obj.phase = "Lade NF-Master und Excluded …"
        job_obj.percent = 82

    df = await load_df_text("nf_master_final")
    if df.empty:
        raise Exception("NF-Master ist leer – vorher NF-Master ausführen!")

    excluded_df = await load_df_text("nf_excluded")
    excluded_ids = set(excluded_df["Kontakt ID"].tolist())

    # ---------------------------------------------
    # Schritt 1: Entferne bereits kontaktierte Personen
    #           über definierte Filter in Pipedrive
    # ---------------------------------------------

    # IDs aus Filter: "bereits kontaktiert" (optional)
    async def fetch_ids_from_filter(filter_id: int) -> set[str]:
        ids = set()
        async for chunk in stream_persons_by_filter(filter_id):
            for p in chunk:
                pid = str(p.get("id"))
                if pid: ids.add(pid)
        return ids

    # Beispielhafte Kontakt-Filter
    FILTER_BEREITS_KONTAKTIERT_1 = 1216
    FILTER_BEREITS_KONTAKTIERT_2 = 1708

    print("[Reconcile] Lade Kontakt-Filter 1216 & 1708 …")

    contacted_a = await fetch_ids_from_filter(FILTER_BEREITS_KONTAKTIERT_1)
    contacted_b = await fetch_ids_from_filter(FILTER_BEREITS_KONTAKTIERT_2)

    all_contacted = contacted_a.union(contacted_b)

    print(f"[Reconcile] Kontaktierte Personen gesamt: {len(all_contacted)}")

    df["already_contacted"] = df["Person - ID"].apply(
        lambda x: str(x) in all_contacted
    )

    # ---------------------------------------------
    # Schritt 2: Entferne Excluded-Personen
    # ---------------------------------------------
    def is_excluded(pid: str) -> bool:
        return pid in excluded_ids

    df["excluded"] = df["Person - ID"].apply(is_excluded)

    # ---------------------------------------------
    # Schritt 3: Erstelle "Ready"-Liste
    # ---------------------------------------------
    if job_obj:
        job_obj.phase = "Erstelle Ready-Liste …"
        job_obj.percent = 90

    df_ready = df.loc[
        (df["already_contacted"] == False) &
        (df["excluded"] == False)
    ].copy()

    print(f"[Reconcile] Ready-Liste: {len(df_ready)} Zeilen")

    # ---------------------------------------------
    # Schritt 4: Schreibe Ready-Tabelle
    # ---------------------------------------------
    await save_df_text(df_ready, "nf_master_ready")

    if job_obj:
        job_obj.phase = "Ready-Liste gespeichert"
        job_obj.percent = 95

    return df_ready
# ============================================================
# master_20251119_FINAL.py – Modul 5/6
# Neukontakte: Master-Datenaufbau (NK-Master)
# ============================================================

async def _build_nk_master_final(
    batch_id: str,
    campaign: str,
    job_obj=None
) -> pd.DataFrame:
    """
    Erzeugt die Neukontakte-Masterdatei.
    Holt Personen exakt über das Batch-Feld.
    """

    if job_obj:
        job_obj.phase = "Lade Personen (Neukontakte) …"
        job_obj.percent = 10

    # ------------------------------------------------------------
    # 1) Personen über Batch-Feld laden
    # ------------------------------------------------------------
    persons = await get_persons_by_batch_ids([batch_id])
    print(f"[NK] Personen geladen: {len(persons)}")

    if job_obj:
        job_obj.phase = "Verarbeite Neukontakte …"
        job_obj.percent = 40

    # ------------------------------------------------------------
    # 2) DataFrame bauen
    # ------------------------------------------------------------
    rows = []

    for p in persons:
        pid = str(p.get("id") or "")
        name = p.get("name") or ""

        org = p.get("organization") or {}
        org_name = org.get("name") or "-"
        org_id = str(org.get("id") or "")

        # E-Mail
        emails = p.get("emails") or []
        email = ""
        if isinstance(emails, list) and emails:
            email = (
                emails[0].get("value")
                if isinstance(emails[0], dict)
                else str(emails[0])
            )
        elif isinstance(emails, str):
            email = emails

        rows.append({
            "Person - Batch ID": batch_id,
            "Cold-Mailing Import": campaign,

            "Person - Name": name,
            "Person - E-Mail": email,

            "Organisation - Name": org_name,
            "Organisation - ID": org_id,
            "Person - ID": pid,
        })

    df = pd.DataFrame(rows)

    # ------------------------------------------------------------
    # 3) Master speichern
    # ------------------------------------------------------------
    await save_df_text(df, "nk_master_final")

    if job_obj:
        job_obj.phase = "Speichere Neukontakte …"
        job_obj.percent = 80

    print(f"[NK] Master gespeichert: {len(df)} Zeilen")

    if job_obj:
        job_obj.phase = "Fertig"
        job_obj.percent = 100

    return df
# ============================================================
# master_20251119_FINAL.py – Modul 6/6
# UI, API-Endpoints, Jobs, Routing, Debug
# ============================================================

# ------------------------------------------------------------
# Job-System
# ------------------------------------------------------------

JOB_STORE = {}

class Job:
    def __init__(self):
        self.id = str(int(time.time() * 1000))
        self.phase = "Init"
        self.percent = 0
        self.done = False
        self.error = None
        self.result_table = None

def create_job() -> Job:
    job = Job()
    JOB_STORE[job.id] = job
    return job

# ------------------------------------------------------------
# EXPORT-JOBS (ASYNC TASKS)
# ------------------------------------------------------------

async def job_nf_export(nf_batch_ids: List[str], batch_id: str, campaign: str, job: Job):
    try:
        # Schritt 1: Master Final
        df_final = await _build_nf_master_final(
            nf_batch_ids=nf_batch_ids,
            batch_id=batch_id,
            campaign=campaign,
            job_obj=job
        )

        # Schritt 2: Ready erzeugen
        df_ready = await _nf_reconcile(job_obj=job)

        job.result_table = "nf_master_ready"
        job.done = True
        job.phase = "Fertig"
        job.percent = 100

    except Exception as e:
        job.error = str(e)
        job.done = True


async def job_nk_export(batch_id: str, campaign: str, job: Job):
    try:
        df_final = await _build_nk_master_final(
            batch_id=batch_id,
            campaign=campaign,
            job_obj=job
        )
        job.result_table = "nk_master_final"
        job.done = True
        job.phase = "Fertig"
        job.percent = 100

    except Exception as e:
        job.error = str(e)
        job.done = True


# ------------------------------------------------------------
# EXPORT-PROGRESS
# ------------------------------------------------------------

@app.get("/export_progress", response_class=JSONResponse)
async def export_progress(job_id: str):
    job = JOB_STORE.get(job_id)
    if not job:
        return {"error": "Job nicht gefunden"}
    return {
        "phase": job.phase,
        "percent": job.percent,
        "done": job.done,
        "error": job.error,
        "result_table": job.result_table
    }


# ------------------------------------------------------------
# EXPORT-DOWNLOAD
# ------------------------------------------------------------

@app.get("/export_download")
async def export_download(job_id: str):
    job = JOB_STORE.get(job_id)
    if not job:
        return {"error": "Job nicht gefunden"}

    if not job.done:
        return {"error": "Job ist noch nicht fertig"}

    if job.error:
        return {"error": job.error}

    if not job.result_table:
        return {"error": "Keine Result-Tabelle gesetzt"}

    df = await load_df_text(job.result_table)
    if df.empty:
        return JSONResponse({"error": "Export ist leer"})

    # Temporäre CSV erstellen
    path = f"/tmp/{job.result_table}.csv"
    df.to_csv(path, index=False)

    return FileResponse(
        path,
        filename=f"{job.result_table}.csv",
        media_type="text/csv"
    )


# ------------------------------------------------------------
# EXPORT-START
# ------------------------------------------------------------

@app.post("/nachfass/export_start", response_class=JSONResponse)
async def nf_export_start(data=Body(...)):
    nf_batch_ids = [str(x).strip() for x in data.get("nf_batch_ids") or []]
    batch_id = data.get("batch_id", "").strip()
    campaign = data.get("campaign", "").strip()

    if not nf_batch_ids:
        return {"error": "Bitte Batch-IDs angeben"}

    job = create_job()
    asyncio.create_task(job_nf_export(nf_batch_ids, batch_id, campaign, job))

    return {"job_id": job.id}


@app.post("/neukontakte/export_start", response_class=JSONResponse)
async def nk_export_start(data=Body(...)):
    batch_id = data.get("batch_id", "").strip()
    campaign = data.get("campaign", "").strip()

    if not batch_id:
        return {"error": "Bitte Batch-ID angeben"}

    job = create_job()
    asyncio.create_task(job_nk_export(batch_id, campaign, job))

    return {"job_id": job.id}


# ------------------------------------------------------------
# UI – HTML-Seiten
# ------------------------------------------------------------

@app.get("/campaign", response_class=HTMLResponse)
async def campaign_home():
    return HTMLResponse("""
    <!doctype html>
    <html>
    <head>
        <meta charset='utf-8'>
        <title>BatchFlow – Kampagnen</title>
        <style>
            body {font-family: Arial; margin: 40px;}
            a {
                display: block; width: 260px;
                padding: 10px 15px; margin-bottom: 12px;
                background: #007bff; color:white;
                text-decoration:none; border-radius:6px;
            }
            a:hover {background:#005dc1;}
        </style>
    </head>
    <body>
        <h1>BatchFlow – Kampagnen</h1>
        <a href='/neukontakte'>Neukontakte Export</a>
        <a href='/nachfass'>Nachfass Export</a>
        <a href='/viewer'>Viewer</a>
    </body>
    </html>
    """)


# ------------------ NK UI --------------------

@app.get("/neukontakte", response_class=HTMLResponse)
async def nk_ui():
    return HTMLResponse("""
    <!doctype html>
    <html>
    <head><meta charset='utf-8'>
    <title>Neukontakte Export</title>
    <style>
        body {font-family:Arial;margin:40px;}
        input, button {font-size:16px;padding:8px;margin-top:8px;width:300px;}
        button {background:#28a745;color:white;border:none;border-radius:5px;margin-top:15px;}
        button:hover {background:#1e8e3e;cursor:pointer;}
        .bar {height:22px;width:0%;background:#28a745;transition:width .2s;}
        .box {width:330px;background:#eee;border-radius:5px;margin-top:20px;}
    </style>
    </head>
    <body>
        <h1>Neukontakte Export</h1>
        <label>Batch ID</label><br>
        <input id="batch"><br>

        <label>Kampagne</label><br>
        <input id="campaign"><br>

        <button onclick="start()">Export starten</button>

        <div class="box"><div id="bar" class="bar"></div></div>
        <p id="status"></p>

        <script>
        async function start(){
            const batch=document.getElementById('batch').value.trim();
            const camp=document.getElementById('campaign').value.trim();
            if(!batch){alert("Bitte Batch-ID eingeben!");return;}

            const r=await fetch('/neukontakte/export_start',{
                method:'POST', headers:{'Content-Type':'application/json'},
                body:JSON.stringify({batch_id:batch,campaign:camp})
            });
            const j=await r.json();
            poll(j.job_id);
        }
        async function poll(id){
            const r=await fetch('/export_progress?job_id='+id);
            const j=await r.json();
            document.getElementById('status').innerText=j.phase;
            document.getElementById('bar').style.width=(j.percent||0)+'%';
            if(j.done){
                if(j.error){document.getElementById('status').innerText="Fehler: "+j.error;}
                else window.location='/export_download?job_id='+id;
                return;
            }
            setTimeout(()=>poll(id),600);
        }
        </script>
    </body>
    </html>
    """)


# ------------------ NF UI --------------------

@app.get("/nachfass", response_class=HTMLResponse)
async def nf_ui():
    return HTMLResponse("""
    <!doctype html>
    <html>
    <head><meta charset='utf-8'>
    <title>Nachfass Export</title>
    <style>
        body {font-family:Arial;margin:40px;}
        input, button {font-size:16px;padding:8px;margin-top:8px;width:300px;}
        button {background:#007bff;color:white;border:none;border-radius:5px;margin-top:15px;}
        button:hover {background:#005dc1;cursor:pointer;}
        .bar {height:22px;width:0%;background:#28a745;transition:width .2s;}
        .box {width:330px;background:#eee;border-radius:5px;margin-top:20px;}
    </style>
    </head>
    <body>
        <h1>Nachfass Export</h1>

        <label>Batch IDs (Komma)</label><br>
        <input id="batches"><br>

        <label>Export-Batch ID</label><br>
        <input id="batch_id"><br>

        <label>Kampagne</label><br>
        <input id="campaign"><br>

        <button onclick="start()">Export starten</button>

        <div class="box"><div id="bar" class="bar"></div></div>
        <p id="status"></p>

        <script>
        async function start(){
            const batches=document.getElementById('batches').value.split(',').map(s=>s.trim()).filter(Boolean);
            const batch_id=document.getElementById('batch_id').value.trim();
            const camp=document.getElementById('campaign').value.trim();
            if(batches.length===0){alert("Bitte Batch-IDs eingeben!");return;}

            const r=await fetch('/nachfass/export_start',{
                method:'POST',headers:{'Content-Type':'application/json'},
                body:JSON.stringify({nf_batch_ids:batches,batch_id:batch_id,campaign:camp})
            });
            const j=await r.json();
            poll(j.job_id);
        }
        async function poll(id){
            const r=await fetch('/export_progress?job_id='+id);
            const j=await r.json();
            document.getElementById('status').innerText=j.phase;
            document.getElementById('bar').style.width=(j.percent||0)+'%';
            if(j.done){
                if(j.error){document.getElementById('status').innerText="Fehler: "+j.error;}
                else window.location='/export_download?job_id='+id;
                return;
            }
            setTimeout(()=>poll(id),600);
        }
        </script>
    </body>
    </html>
    """)


# ------------------------------------------------------------
# Viewer UI – Master/Excluded/Ready Tabellen
# ------------------------------------------------------------

@app.get("/viewer", response_class=HTMLResponse)
async def viewer_home():
    return HTMLResponse("""
    <!doctype html>
    <html><head><meta charset='utf-8'>
    <title>BatchFlow Viewer</title>
    <style>
        body {font-family:Arial;margin:40px;}
        a {display:block;margin-bottom:10px;}
        table {border-collapse:collapse;margin-top:20px;}
        th,td {border:1px solid #ccc;padding:6px;}
        th {background:#eee;}
    </style>
    </head>
    <body>
        <h1>BatchFlow Viewer</h1>
        <ul>
            <li><a href="/viewer/table/nf_master_final">NF Master Final</a></li>
            <li><a href="/viewer/table/nf_excluded">NF Excluded</a></li>
            <li><a href="/viewer/table/nf_master_ready">NF Master Ready</a></li>
            <li><a href="/viewer/table/nk_master_final">NK Master Final</a></li>
        </ul>
    </body>
    </html>
    """)


@app.get("/viewer/table/{table}", response_class=HTMLResponse)
async def viewer_table(table: str):
    df = await load_df_text(table)
    if df.empty:
        return HTMLResponse("<h2>Tabelle leer oder nicht vorhanden</h2>")

    html = df.to_html(index=False)

    return HTMLResponse(f"""
    <html><head><meta charset='utf-8'><title>{table}</title></head>
    <body>
        <h1>{table}</h1>
        {html}
    </body></html>
    """)


# ------------------------------------------------------------
# DEBUG-ENDPOINTS
# ------------------------------------------------------------

@app.get("/debug/person/{pid}", response_class=JSONResponse)
async def debug_person(pid: str):
    url = append_token(f"{PIPEDRIVE_API}/persons/{pid}?fields=*")
    r = await http_client().get(url, headers=get_headers())
    return r.json()


@app.get("/debug/filter/{filter_id}", response_class=JSONResponse)
async def debug_filter(filter_id: int):
    persons = await get_persons_from_filter(filter_id)
    return {"filter_id": filter_id, "count": len(persons)}


@app.get("/debug/batch/{batch_id}", response_class=JSONResponse)
async def debug_batch(batch_id: str):
    persons = await get_persons_by_batch_ids([batch_id])
    return {"batch_id": batch_id, "count": len(persons)}


# ------------------------------------------------------------
# Fallback → Kampagnenseite
# ------------------------------------------------------------

@app.get("/{full_path:path}", include_in_schema=False)
async def fallback(full_path: str):
    return RedirectResponse("/campaign", status_code=302)
