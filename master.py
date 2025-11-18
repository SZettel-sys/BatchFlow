# ============================================================
# master_20251119_FINAL.py – Modul 1/6
# System & Setup (FastAPI, DB, HTTP Client, Utilities)
# ============================================================

import os
import re
import io
import uuid
import time
import asyncio
import json
import httpx
import asyncpg
import numpy as np
import pandas as pd

from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, AsyncGenerator
from collections import defaultdict

from fastapi import FastAPI, Request, Body, Query
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    FileResponse,
    RedirectResponse,
    StreamingResponse
)
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles

# ------------------------------------------------------------
# App Initialisierung
# ------------------------------------------------------------

app = FastAPI(title="BatchFlow 2025")
app.add_middleware(GZipMiddleware, minimum_size=1024)

if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# ------------------------------------------------------------
# Environment Variablen
# ------------------------------------------------------------

PD_API_TOKEN = os.getenv("PD_API_TOKEN", "")
PIPEDRIVE_API = "https://api.pipedrive.com/v1"

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL fehlt (Neon DSN).")

SCHEMA = "public"

FILTER_NEUKONTAKTE = int(os.getenv("FILTER_NEUKONTAKTE", "2998"))
FILTER_NACHFASS = int(os.getenv("FILTER_NACHFASS", "3024"))
FIELD_FACHBEREICH_HINT = os.getenv("FIELD_FACHBEREICH_HINT", "fachbereich")

DEFAULT_CHANNEL = "Cold E-Mail"

PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "500"))
NF_PAGE_LIMIT = int(os.getenv("NF_PAGE_LIMIT", "500"))
NF_MAX_ROWS = int(os.getenv("NF_MAX_ROWS", "10000"))
RECONCILE_MAX_ROWS = int(os.getenv("RECONCILE_MAX_ROWS", "20000"))

PER_ORG_DEFAULT_LIMIT = int(os.getenv("PER_ORG_DEFAULT_LIMIT", "2"))

MAX_ORG_NAMES = int(os.getenv("MAX_ORG_NAMES", "100000"))
MAX_ORG_BUCKET = int(os.getenv("MAX_ORG_BUCKET", "12000"))

# Dein Batch-ID Custom-Feld
BATCH_FIELD_KEY = "5ac34dad3ea917fdef4087caebf77ba275f87eec"

# ------------------------------------------------------------
# Hilfsfunktionen
# ------------------------------------------------------------

def get_headers():
    return {"Accept": "application/json"}

def append_token(url: str) -> str:
    return f"{url}&api_token={PD_API_TOKEN}" if "?" in url else f"{url}?api_token={PD_API_TOKEN}"

_http_client = None

def http_client():
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(timeout=30.0)
    return _http_client

# ------------------------------------------------------------
# DB Pool
# ------------------------------------------------------------

_pool = None

async def get_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL)
    return _pool

# ------------------------------------------------------------
# Text-Tabellen speichern / laden (Pandas)
# ------------------------------------------------------------

async def ensure_table_text(conn, table: str, columns: List[str]):
    cols = ', '.join([f'"{c}" TEXT' for c in columns])
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{table} (
            {cols}
        );
    """)

async def save_df_text(df: pd.DataFrame, table: str):
    df = df.replace({np.nan: None})
    async with (await get_pool()).acquire() as conn:
        await ensure_table_text(conn, table, list(df.columns))
        await conn.execute(f"TRUNCATE {SCHEMA}.{table};")
        rows = [tuple(str(x) if x is not None else None for x in row) for row in df.values]
        if rows:
            await conn.copy_records_to_table(
                table,
                schema_name=SCHEMA,
                records=rows,
                columns=list(df.columns)
            )

async def load_df_text(table: str) -> pd.DataFrame:
    async with (await get_pool()).acquire() as conn:
        rows = await conn.fetch(f"SELECT * FROM {SCHEMA}.{table};")
    if not rows:
        return pd.DataFrame()
    cols = rows[0].keys()
    data = [{col: val for col,val in zip(cols, row)} for row in rows]
    return pd.DataFrame(data).replace({"": np.nan})

# ------------------------------------------------------------
# Namens-Splitter
# ------------------------------------------------------------

def split_name(first_name: Optional[str], last_name: Optional[str], full_name: str):
    if first_name and last_name:
        return first_name, last_name
    parts = full_name.split()
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    return full_name, ""
# ============================================================
# master_20251119_FINAL.py – Modul 2/6
# Filter-First Engine (NF), NK Engine & Parallel Detail Loader
# ============================================================
# ------------------------------------------------------------
# GLOBALER RETRY WRAPPER (für 429 + Netzwerkfehler)
# ------------------------------------------------------------

async def safe_request(method, url, headers=None, max_retries=10):
    delay = 1.5  # Start bei 1.5s (sanft für Pipedrive)

    for attempt in range(max_retries):
        try:
            r = await http_client().request(method, url, headers=headers)

            # Erfolgreich
            if r.status_code != 429:
                return r

            # 429 - Rate Limit → warten
            print(f"[Retry] 429 erhalten. Warte {delay:.1f}s …")
            await asyncio.sleep(delay)
            delay = min(delay * 1.6, 15.0)  # Exponentiell, max 15s

        except Exception as e:
            print(f"[Retry] Netzwerkfehler: {e}, Warte {delay:.1f}s …")
            await asyncio.sleep(delay)
            delay = min(delay * 1.6, 15.0)

    raise Exception(f"Request fehlgeschlagen nach {max_retries} Retries: {url}")

# ------------------------------------------------------------
# PERSONEN-FELDER CACHE
# ------------------------------------------------------------

_PERSON_FIELDS_CACHE: Optional[List[dict]] = None

async def get_person_fields() -> List[dict]:
    """Lädt Personenfelder einmalig und cached sie."""
    global _PERSON_FIELDS_CACHE
    if _PERSON_FIELDS_CACHE is not None:
        return _PERSON_FIELDS_CACHE

    url = append_token(f"{PIPEDRIVE_API}/personFields")
    r = await http_client().get(url, headers=get_headers())

    if r.status_code != 200:
        raise Exception(f"personFields Fehler: {r.text}")

    _PERSON_FIELDS_CACHE = r.json().get("data") or []
    return _PERSON_FIELDS_CACHE


# ------------------------------------------------------------
# Custom-Felder robust auslesen
# ------------------------------------------------------------

def extract_custom_field(person: dict, field_key: str):
    """Liest ein beliebiges Custom-Feld sicher aus."""
    # 1) Direkt
    if field_key in person:
        val = person[field_key]
        if isinstance(val, dict):  return val.get("value") or val.get("label")
        if isinstance(val, list):  return val[0].get("value") if (val and isinstance(val[0], dict)) else (val[0] if val else None)
        return val

    # 2) custom_fields root
    cf = person.get("custom_fields") or {}
    if field_key in cf:
        val = cf[field_key]
        if isinstance(val, dict):  return val.get("value") or val.get("label")
        if isinstance(val, list):  return val[0].get("value") if (val and isinstance(val[0], dict)) else (val[0] if val else None)
        return val

    # 3) data.custom_fields
    data = person.get("data") or {}
    cf2 = data.get("custom_fields") or {}
    if field_key in cf2:
        val = cf2[field_key]
        if isinstance(val, dict):  return val.get("value") or val.get("label")
        if isinstance(val, list):  return val[0].get("value") if (val and isinstance(val[0], dict)) else (val[0] if val else None)
        return val

    return None

# ------------------------------------------------------------
# SMART BATCH DETAIL FETCH (Chunk Size = 50)
# ------------------------------------------------------------

async def fetch_person_details(person_ids: List[str]) -> List[dict]:
    """
    Holt Personendetails in stabilen, parallelen 50er-Chunks.
    Optimiert für 300–1500 Personen.
    Pipedrive-Limit-sicher.
    """

    results = []
    chunk_size = 50

    # Personen in 50er-Chunks aufteilen
    chunks = [person_ids[i:i + chunk_size] for i in range(0, len(person_ids), chunk_size)]

    # Semaphore → bis zu 20 gleichzeitige Requests pro Chunk
    sem = asyncio.Semaphore(40)

    async def load_one(pid):
        async with sem:
            url = append_token(f"{PIPEDRIVE_API}/persons/{pid}?fields=*")
            r = await http_client().get(url, headers=get_headers())

            if r.status_code == 200:
                data = r.json().get("data")
                if data:
                    results.append(data)

            # Kurz warten → verhindert 429, aber schnell!
            await asyncio.sleep(0.004)

    # Chunks nacheinander, aber CHUNK-INTERN parallel
    for chunk in chunks:
        await asyncio.gather(*(load_one(pid) for pid in chunk))

    return results

# ============================================================
# HIGH-END STREAM PRODUCER – Parallel Filter Loader
# ============================================================

async def stream_filter_ids(filter_id: int, max_workers: int = 6):
    """
    Lädt Personen-IDs aus Filter 3024 extrem schnell:
    - mehrere Pages gleichzeitig
    - adaptive Retry
    - Streaming Queue
    """

    limit = 500
    queue = asyncio.Queue()
    done_flag = False

    async def page_worker(start_index):
        nonlocal done_flag
        while not done_flag:

            url = append_token(
                f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start_index}&limit={limit}&fields=id"
            )

            r = await safe_request("GET", url, headers=get_headers())
            data = r.json().get("data") or []

            # In Queue legen
            await queue.put(data)

            if len(data) < limit:
                done_flag = True
                return

            start_index += limit

    # Worker starten (6 gleichzeitig)
    tasks = [asyncio.create_task(page_worker(i * limit)) for i in range(max_workers)]

    async def generator():
        # So lange Worker laufen: streamen
        while True:
            if all(t.done() for t in tasks) and queue.empty():
                break

            try:
                items = await asyncio.wait_for(queue.get(), timeout=1)
                yield items
            except asyncio.TimeoutError:
                continue

    return generator()
# ============================================================
# HIGH-END STREAM CONSUMER – Smart Batch Detail Loader
# ============================================================

async def consume_filter_stream_and_load_details(generator, batch_size=50):
    """
    Nimmt einen ID-Stream (vom Producer) entgegen
    und lädt Details sofort in Smart-Batches.

    Vorteile:
    - Detail-Fetch beginnt, während IDs noch laden
    - Pipeline ohne Wartezeiten
    - sehr hohe Geschwindigkeit
    """

    results = []
    pending_ids = []
    sem = asyncio.Semaphore(20)  # 20 Parallel-Requests pro Batch

    async def load_one(pid):
        async with sem:
            url = append_token(f"{PIPEDRIVE_API}/persons/{pid}?fields=*")
            r = await safe_request("GET", url, headers=get_headers())
            if r.status_code == 200:
                data = r.json().get("data")
                if data:
                    results.append(data)
        await asyncio.sleep(0.004)

    async def flush_batch():
        """Ein Batch mit 50 IDs parallel laden."""
        if not pending_ids:
            return
        batch = pending_ids.copy()
        pending_ids.clear()
        await asyncio.gather(*(load_one(pid) for pid in batch))

    # IDs aus dem Stream holen → sofort Details laden
    async for page in generator:
        for p in page:
            pid = str(p.get("id"))
            if not pid:
                continue

            pending_ids.append(pid)

            # Wenn Batch voll → sofort laden
            if len(pending_ids) >= batch_size:
                await flush_batch()

    # Restliche IDs laden, wenn Stream abgeschlossen ist
    await flush_batch()

    return results



# ============================================================
# HIGH-END STREAM ORCHESTRATOR
# Kombiniert Producer + Consumer → 10× schneller als normal
# ============================================================

async def get_persons_from_filter_detailed(filter_id: int) -> List[dict]:
    """
    High-End Pipeline:
    - Lädt Filter 3024 über parallele Page-Producer
    - Streamt IDs live zum Consumer
    - Lädt Details sofort in Smart-Batches
    - Kein Warten mehr auf "alle IDs"
    """

    print(f"[Stream] Starte High-End Stream Pipeline für Filter {filter_id} …")

    # 1) Producer vorbereiten
    generator = await stream_filter_ids(filter_id)

    # 2) Consumer starten (Chunk 50)
    persons = await consume_filter_stream_and_load_details(generator, batch_size=50)

    print(f"[Stream] Pipeline abgeschlossen. Geladene Personen: {len(persons)}")

    return persons




# ------------------------------------------------------------
# NK ENGINE – Batch-Feld exakte Suche
# ------------------------------------------------------------

async def get_nk_persons(batch_value: str) -> List[dict]:
    """Neukontakte: Batch-Feld ist Hauptkriterium."""
    ids = set()
    start = 0
    batch_value_clean = batch_value.strip()

    while True:
        url = append_token(
            f"{PIPEDRIVE_API}/persons/search?"
            f"field_key={BATCH_FIELD_KEY}&term={batch_value_clean}"
            f"&exact_match=true&start={start}&limit=100"
        )
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Batch-Suche Fehler: {r.text}")

        items = (r.json().get("data") or {}).get("items") or []
        if not items:
            break

        for it in items:
            pid = it.get("item", {}).get("id")
            if pid:
                ids.add(str(pid))

        if len(items) < 100:
            break
        start += 100

    print(f"[NK] Batch-Feld IDs: {len(ids)}")
    return await fetch_person_details(list(ids))


# ------------------------------------------------------------
# NF ENGINE – FILTER-FIRST + BATCH-Feldprüfung
# ------------------------------------------------------------

async def get_nf_persons_filter_first(batch_values: list[str]) -> List[dict]:
    """
    NF:
    1) Lade ALLE Personen aus Filter 3024
    2) Prüfe Batch-Feld in Python
    """
    persons = await get_persons_from_filter_detailed(FILTER_NACHFASS)

    batchnorm = {b.strip().lower() for b in batch_values}
    valid = []

    for p in persons:
        val = extract_custom_field(p, BATCH_FIELD_KEY)
        if not val:
            continue

        sval = str(val).strip().lower()
        if sval in batchnorm:
            valid.append(p)

    print(f"[NF] Nach Batch-Check übrig: {len(valid)} Personen")
    return valid
# ------------------------------------------------------------
# SIMPLE ID FETCHER FÜR FILTER (wird in Reconcile benötigt)
# ------------------------------------------------------------
async def get_ids_from_filter(filter_id: int, limit: int = 500) -> set[str]:
    ids = set()
    start = 0

    while True:
        url = append_token(
            f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={limit}&fields=id"
        )
        r = await safe_request("GET", url, headers=get_headers())

        data = (r.json().get("data") or [])
        if not data:
            break

        for item in data:
            pid = item.get("id")
            if pid:
                ids.add(str(pid))

        if len(data) < limit:
            break

        start += limit

    return ids

# ============================================================
# master_20251119_FINAL.py – Modul 3/6
# Nachfass: Master-Datenaufbau (Filter-First + Batch-Feld)
# ============================================================

async def _build_nf_master_final(
    nf_batch_ids: List[str],
    batch_id: str,
    campaign: str,
    job_obj=None
) -> pd.DataFrame:

    # --------------------------------------------------------
    # 1) Personen laden (Filter 3024 → Batch-Feld)
    # --------------------------------------------------------
    if job_obj:
        job_obj.phase = "Lade Personen aus Filter 3024 …"
        job_obj.percent = 10

    persons = await get_nf_persons_filter_first(nf_batch_ids)

    print(f"[NF] Personen nach Filter + Batch-Prüfung: {len(persons)}")

    if job_obj:
        job_obj.phase = "Verarbeite Nachfass-Daten …"
        job_obj.percent = 30

    # --------------------------------------------------------
    # 2) Personenfelder laden (für Mapping)
    # --------------------------------------------------------
    person_fields = await get_person_fields()

    hint_to_key: Dict[str, str] = {}
    gender_map: Dict[str, str] = {}
    next_activity_key: Optional[str] = None

    PERSON_FIELD_HINTS_TO_EXPORT = {
        "prospect": "prospect",
        "titel": "titel",
        "title": "title",
        "anrede": "anrede",
        "gender": "gender",
        "geschlecht": "geschlecht",
        "position": "position",
        "xing": "xing",
        "linkedin": "linkedin",
    }

    # Feldzuordnung
    for f in person_fields:
        nm = (f.get("name") or "").lower()

        # Mapping via Hint
        for hint in PERSON_FIELD_HINTS_TO_EXPORT.keys():
            if hint in nm and hint not in hint_to_key:
                hint_to_key[hint] = f.get("key")

        # Geschlecht
        if "gender" in nm or "geschlecht" in nm:
            for o in (f.get("options") or []):
                gender_map[str(o["id"])] = o["label"]

        # nächste Aktivität
        if "next" in nm and "activity" in nm:
            next_activity_key = f.get("key")

    # Hilfsfunktion
    def get_field(p: dict, hint: str) -> str:
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
            return ", ".join(v for v in vals if v)
        if hint in ("gender", "geschlecht") and gender_map:
            return gender_map.get(str(val), str(val))
        return str(val or "")

    # --------------------------------------------------------
    # 3) Vorab-Prüfungen / Ausschlüsse
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

        # Regel 1: Datum nächste Aktivität < 3 Monate
        if next_activity_key:
            dt_raw = p.get(next_activity_key)
            if dt_raw:
                try:
                    dt_val = datetime.fromisoformat(str(dt_raw).split(" ")[0])
                    delta_days = (now - dt_val).days
                    if delta_days < 0 or delta_days <= 90:
                        excluded.append({
                            "Kontakt ID": pid,
                            "Name": name,
                            "Organisation ID": org_id,
                            "Organisationsname": org_name,
                            "Grund": "Datum nächste Aktivität < 3 Monate"
                        })
                        continue
                except:
                    pass

        # Regel 2: max 2 Personen pro Organisation
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

    print(f"[NF] Ausgewählt: {len(selected)}, Excluded: {len(excluded)}")

    if job_obj:
        job_obj.phase = "Baue NF-Master Tabelle …"
        job_obj.percent = 60

    # --------------------------------------------------------
    # 4) DataFrame für Master erzeugen
    # --------------------------------------------------------
    rows = []

    for p in selected:

        pid = str(p.get("id") or "")
        name = p.get("name") or ""

        org = p.get("organization") or {}
        org_id = str(org.get("id") or "")
        org_name = org.get("name") or "-"

        # Namensaufteilung
        first, last = split_name(
            p.get("first_name"),
            p.get("last_name"),
            name
        )

        # E-Mail
        emails = p.get("emails") or []
        email = ""
        if isinstance(emails, list) and emails:
            email = emails[0].get("value") if isinstance(emails[0], dict) else str(emails[0])
        elif isinstance(emails, str):
            email = emails

        # XING-Feld
        xing_url = ""
        for k, v in p.items():
            if isinstance(k, str) and "xing" in k.lower():
                if isinstance(v, str) and v.startswith("http"):
                    xing_url = v
                elif isinstance(v, list):
                    xing_url = ", ".join(
                        x.get("value")
                        for x in v
                        if isinstance(x, dict) and x.get("value")
                    )
                break

        rows.append({
            "Person - Batch ID": batch_id,
            "Person - Channel": DEFAULT_CHANNEL,
            "Cold-Mailing Import": campaign,

            "Person - Organisation": org_name,
            "Organisation - ID": org_id,

            "Person - Geschlecht": get_field(p, "gender") or get_field(p, "geschlecht"),
            "Person - Titel": get_field(p, "titel") or get_field(p, "title"),

            "Person - Vorname": first,
            "Person - Nachname": last,
            "Person - Position": get_field(p, "position"),

            "Person - ID": pid,
            "Person - XING-Profil": xing_url,
            "Person - LinkedIn Profil-URL": get_field(p, "linkedin"),

            "Person - E-Mail-Adresse - Büro": email,
        })

    df = pd.DataFrame(rows)

    # --------------------------------------------------------
    # 5) Excluded speichern
    # --------------------------------------------------------
    excluded_df = pd.DataFrame(excluded).replace({np.nan: None})
    if excluded_df.empty:
        excluded_df = pd.DataFrame([{
            "Kontakt ID": "-",
            "Name": "-",
            "Organisation ID": "-",
            "Organisationsname": "-",
            "Grund": "Keine Ausschlüsse"
        }])

    await save_df_text(excluded_df, "nf_excluded")

    # --------------------------------------------------------
    # 6) Master Final speichern
    # --------------------------------------------------------
    await save_df_text(df, "nf_master_final")

    if job_obj:
        job_obj.phase = "Nachfass Master erstellt"
        job_obj.percent = 80

    print(f"[NF] Master gespeichert: {len(df)} Zeilen")

    return df
# ============================================================
# master_20251119_FINAL.py – Modul 4/6
# Reconcile: Ausschlüsse + Kontakt-Status → Ready-Tabelle
# ============================================================

async def _nf_reconcile(job_obj=None):
    """
    Erzeugt die Nachfass-Ready-Liste:
    - Entfernt bereits kontaktierte (Filter 1216 + 1708)
    - Entfernt NF-excluded
    - Speichert nf_master_ready
    """

    # --------------------------------------------------------
    # 1) NF-Master laden
    # --------------------------------------------------------
    if job_obj:
        job_obj.phase = "Lade NF-Master …"
        job_obj.percent = 82

    df = await load_df_text("nf_master_final")
    if df.empty:
        raise Exception("NF-Master ist leer – zuerst NF-Master erstellen!")

    # NF-Excluded laden
    excluded_df = await load_df_text("nf_excluded")
    excluded_ids = set(excluded_df.get("Kontakt ID", pd.Series()).astype(str).tolist())

    # --------------------------------------------------------
    # 2) Kontaktfilter (already contacted) parallel laden
    # --------------------------------------------------------
    FILTER_A = 1216
    FILTER_B = 1708

    if job_obj:
        job_obj.phase = "Lade Kontakt-Filter …"
        job_obj.percent = 85

    # paralleles Laden
    contacted_a, contacted_b = await asyncio.gather(
        get_ids_from_filter(FILTER_A),
        get_ids_from_filter(FILTER_B)
    )

    all_contacted = contacted_a.union(contacted_b)

    print(f"[Reconcile] Kontaktierte Personen: {len(all_contacted)}")

    # --------------------------------------------------------
    # 3) Markierungen setzen
    # --------------------------------------------------------
    df["already_contacted"] = df["Person - ID"].astype(str).isin(all_contacted)
    df["excluded"] = df["Person - ID"].astype(str).isin(excluded_ids)

    # --------------------------------------------------------
    # 4) Ready-Liste erstellen
    # --------------------------------------------------------
    if job_obj:
        job_obj.phase = "Erstelle Ready-Liste …"
        job_obj.percent = 90

    df_ready = df.loc[
        (df["already_contacted"] == False) &
        (df["excluded"] == False)
    ].copy()

    print(f"[Reconcile] Ready: {len(df_ready)} Zeilen")

    # --------------------------------------------------------
    # 5) Ready-Liste speichern
    # --------------------------------------------------------
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

    # --------------------------------------------------------
    # 1) Personen via NK-Engine (Batch-Feld exakte Suche)
    # --------------------------------------------------------
    if job_obj:
        job_obj.phase = "Lade Neukontakte …"
        job_obj.percent = 10

    # 🔥 Der richtige Call (neue NK Engine aus Modul 2)
    persons = await get_nk_persons(batch_id)

    print(f"[NK] Personen aus Batch-Feld '{batch_id}': {len(persons)}")

    if job_obj:
        job_obj.phase = "Verarbeite Neukontakte …"
        job_obj.percent = 40

    # --------------------------------------------------------
    # 2) DataFrame für NK-Master
    # --------------------------------------------------------
    rows = []

    for p in persons:

        pid = str(p.get("id") or "")
        name = p.get("name") or ""

        org = p.get("organization") or {}
        org_id = str(org.get("id") or "")
        org_name = org.get("name") or "-"

        # E-Mail
        emails = p.get("emails") or []
        email = ""
        if isinstance(emails, list) and emails:
            email = emails[0].get("value") if isinstance(emails[0], dict) else str(emails[0])
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

    # --------------------------------------------------------
    # 3) NK Master speichern
    # --------------------------------------------------------
    if job_obj:
        job_obj.phase = "Speichere NK Master …"
        job_obj.percent = 80

    await save_df_text(df, "nk_master_final")

    if job_obj:
        job_obj.phase = "Neukontakte abgeschlossen"
        job_obj.percent = 100

    print(f"[NK] Master gespeichert: {len(df)} Zeilen")

    return df
# ============================================================
# master_20251119_FINAL.py – Modul 6/6
# UI, Export-Jobs, Routing, Debug & Diagnose-Tools
# ============================================================

# ------------------------------------------------------------
# Job System
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
# EXPORT-JOBS (ASYNC)
# ------------------------------------------------------------

async def job_nf_export(nf_batch_ids: List[str], batch_id: str, campaign: str, job: Job):
    try:
        df_final = await _build_nf_master_final(
            nf_batch_ids=nf_batch_ids,
            batch_id=batch_id,
            campaign=campaign,
            job_obj=job
        )

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
        return JSONResponse({"error": "Job nicht gefunden"})

    if not job.done:
        return JSONResponse({"error": "Job ist noch nicht fertig"})

    if job.error:
        return JSONResponse({"error": job.error})

    df = await load_df_text(job.result_table)
    if df.empty:
        return JSONResponse({"error": "Tabelle ist leer"})

    path = f"/tmp/{job.result_table}.csv"
    df.to_csv(path, index=False)

    return FileResponse(
        path,
        filename=f"{job.result_table}.csv",
        media_type="text/csv"
    )


# ------------------------------------------------------------
# EXPORT-START (NF + NK)
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
# UI – HOME
# ------------------------------------------------------------

@app.get("/campaign", response_class=HTMLResponse)
async def campaign_home():
    return HTMLResponse("""
    <html>
    <head>
        <meta charset='utf-8'>
        <title>BatchFlow Kampagnen</title>
        <style>
            body {font-family:Arial;margin:40px;}
            a {
                display:block;width:260px;padding:10px;margin-bottom:12px;
                background:#0066ee;color:white;text-decoration:none;border-radius:6px;
            }
            a:hover {background:#004ec2;}
        </style>
    </head>
    <body>
        <h1>BatchFlow 2025</h1>
        <a href='/neukontakte'>Neukontakte Export</a>
        <a href='/nachfass'>Nachfass Export</a>
        <a href='/viewer'>Tabellen Viewer</a>
    </body>
    </html>
    """)


# ------------------------------------------------------------
# UI – NEUKONTAKTE
# ------------------------------------------------------------

@app.get("/neukontakte", response_class=HTMLResponse)
async def nk_ui():
    return HTMLResponse("""<html>
    <head><meta charset='utf-8'><title>NK Export</title>
    <style>
        body {font-family:Arial;margin:40px;}
        input,button {font-size:16px;padding:8px;margin-top:8px;width:300px;}
        button {background:#28a745;color:white;border:none;border-radius:5px;margin-top:15px;}
        button:hover {background:#1e8e3e;}
        .bar{height:22px;width:0%;background:#28a745;transition:width .2s;}
        .box{width:330px;background:#eee;border-radius:5px;margin-top:20px;}
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
                method:'POST',headers:{'Content-Type':'application/json'},
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
    </html>""")


# ------------------------------------------------------------
# UI – NACHFASS
# ------------------------------------------------------------

@app.get("/nachfass", response_class=HTMLResponse)
async def nf_ui():
    return HTMLResponse("""<html>
    <head><meta charset='utf-8'><title>Nachfass Export</title>
    <style>
        body {font-family:Arial;margin:40px;}
        input,button {font-size:16px;padding:8px;margin-top:8px;width:300px;}
        button{
            background:#0066ee;color:white;border:none;border-radius:5px;margin-top:15px;
        }
        button:hover{background:#004ec2;}
        .bar{height:22px;width:0%;background:#28a745;transition:width .2s;}
        .box{width:330px;background:#eee;border-radius:5px;margin-top:20px;}
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
    </html>""")


# ------------------------------------------------------------
# Tab VIEWER
# ------------------------------------------------------------

@app.get("/viewer", response_class=HTMLResponse)
async def viewer_home():
    return HTMLResponse("""
    <html><head><meta charset='utf-8'>
    <title>BatchFlow Tabellen</title>
    <style>body{font-family:Arial;margin:40px;}a{display:block;margin-bottom:10px;}</style>
    </head>
    <body>
        <h1>Tabellen Viewer</h1>
        <a href="/viewer/table/nf_master_final">NF Master Final</a>
        <a href="/viewer/table/nf_excluded">NF Excluded</a>
        <a href="/viewer/table/nf_master_ready">NF Master Ready</a>
        <a href="/viewer/table/nk_master_final">NK Master Final</a>
    </body>
    </html>
    """)


@app.get("/viewer/table/{table}", response_class=HTMLResponse)
async def viewer_table(table: str):
    df = await load_df_text(table)
    if df.empty:
        return HTMLResponse("<h2>Tabelle leer oder nicht vorhanden</h2>")
    return HTMLResponse(df.to_html(index=False))


# ------------------------------------------------------------
# DEBUG-TOOLS (repariert)
# ------------------------------------------------------------

@app.get("/debug/batch/{batch_id}", response_class=JSONResponse)
async def debug_batch(batch_id: str):
    persons = await get_nk_persons(batch_id)
    return {"batch_id": batch_id, "count": len(persons)}


@app.get("/debug/filter/{filter_id}", response_class=JSONResponse)
async def debug_filter(filter_id: int):
    persons = await get_persons_from_filter_detailed(filter_id)
    return {"filter_id": filter_id, "count": len(persons)}


# ------------------------------------------------------------
# NF DIAGNOSE TOOL
# ------------------------------------------------------------

@app.get("/debug/nf_inspect", response_class=JSONResponse)
async def debug_nf_inspect(batch: str):
    batch = batch.strip().lower()
    if not batch:
        return {"error":"Bitte ?batch=B443 angeben"}

    persons = await get_persons_from_filter_detailed(FILTER_NACHFASS)

    matched = []
    no_batch = []
    wrong_batch = []

    for p in persons:
        val = extract_custom_field(p, BATCH_FIELD_KEY)
        pid = str(p.get("id"))
        name = p.get("name")
        org = (p.get("organization") or {}).get("name","-")

        if not val:
            no_batch.append({"id":pid,"name":name,"org":org})
            continue

        sval = str(val).strip().lower()

        if sval == batch:
            matched.append({"id":pid,"name":name,"org":org})
        else:
            wrong_batch.append({"id":pid,"name":name,"org":org,"value":sval})

    return {
        "filter_total": len(persons),
        "matched_batch": len(matched),
        "without_batch": len(no_batch),
        "wrong_batch": len(wrong_batch),
        "details":{
            "matched": matched,
            "no_batch": no_batch,
            "wrong_batch": wrong_batch
        }
    }


# ------------------------------------------------------------
# NK DIAGNOSE TOOL
# ------------------------------------------------------------

@app.get("/debug/nk_inspect", response_class=JSONResponse)
async def debug_nk_inspect(batch: str):
    persons = await get_nk_persons(batch)
    return {
        "batch": batch,
        "count": len(persons),
        "ids": [p.get("id") for p in persons]
    }


# ------------------------------------------------------------
# Fallback → Kampagnenseite
# ------------------------------------------------------------

@app.get("/{full_path:path}", include_in_schema=False)
async def fallback(full_path: str):
    return RedirectResponse("/campaign", status_code=302)
