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

# ============================================================
# Hilfsfunktionen (API Handling): Token, Header, safe_request
# ============================================================

import httpx
import asyncio

# API-Basis
PIPEDRIVE_API = "https://api.pipedrive.com/v1"
PD_API_TOKEN = os.getenv("PD_API_TOKEN", "").strip()


def append_token(url: str) -> str:
    """Fügt den API-Token korrekt an jede Pipedrive-URL an."""
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}api_token={PD_API_TOKEN}"


def get_headers() -> dict:
    """Standard-Header für Pipedrive API."""
    return {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }


async def safe_request(
    method: str,
    url: str,
    headers: dict,
    max_retries: int = 10,
    initial_delay: float = 1.5
):
    """
    Sichere API-Abfrage mit:
        - 429 Retry (Rate Limit)
        - Fehlerlogging
        - Exponentiellem Backoff
        - Stabil für 300.000+ Kontakte
    """

    delay = initial_delay

    for attempt in range(1, max_retries + 1):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.request(method, url, headers=headers)

            # 200 OK → zurückgeben
            if r.status_code == 200:
                return r

            # 429 → Warte & Retry
            if r.status_code == 429:
                print(f"[Retry] 429 erhalten. Warte {delay:.1f}s …")
                await asyncio.sleep(delay)
                delay *= 1.7
                continue

            # andere Fehler → loggen, aber nicht sofort crashen
            print(f"[WARN] API Fehler {r.status_code}: {url}")
            print(r.text)
            return r

        except Exception as e:
            print(f"[ERR] safe_request Exception: {e}")

        await asyncio.sleep(delay)
        delay *= 1.5

    # Wenn alle Versuche gagalgen
    raise Exception(f"Request fehlgeschlagen nach {max_retries} Retries: {url}")


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
# MODUL 2 – SEARCH-Driven NF Engine (FINAL 2025.5)
# Repliziert Filter 3024 1:1 in Python
# Funktioniert mit 300.000+ Kontakten
# ============================================================

# ------------------------------------------------------------
# Utility: Custom Field Helper
# ------------------------------------------------------------

def cf(p: dict, key: str):
    """Extract custom field from person or organization."""
    try:
        return (p.get("custom_fields") or {}).get(key)
    except:
        return None


# ------------------------------------------------------------
# Pipedrive SEARCH (für Custom Fields)
# ------------------------------------------------------------

async def search_persons_by_field(field_key: str, term: str = "*") -> List[str]:
    """
    Liefert IDs aller Personen, bei denen ein bestimmtes Custom Field gesetzt ist.
    term="*" bedeutet: optional, aber nützlich um "alle Werte" zu finden.
    Funktioniert auch bei 300.000+ Kontakten.
    """
    ids = []
    start = 0
    limit = 100

    while True:
        url = append_token(
            f"{PIPEDRIVE_API}/persons/search"
            f"?term={term}&field_key={field_key}&start={start}&limit={limit}"
        )

        r = await safe_request("GET", url, headers=get_headers())
        data = (r.json().get("data") or {})
        items = data.get("items") or []

        for item in items:
            pid = item["item"]["id"]
            ids.append(str(pid))

        if len(items) < limit:
            break

        start += limit
        await asyncio.sleep(0.002)

    return ids


# ------------------------------------------------------------
# Smart Batch Detail Loader – PERSONEN
# ------------------------------------------------------------

async def load_person_details(ids: List[str], batch_size: int = 50) -> List[dict]:
    """
    Holt vollständige Personendetails.
    40 parallele Requests = optimal für Render Free Tier.
    """
    results = []
    sem = asyncio.Semaphore(40)

    async def load_one(pid):
        async with sem:
            url = append_token(f"{PIPEDRIVE_API}/persons/{pid}?fields=*")
            r = await safe_request("GET", url, headers=get_headers())
            if r.status_code == 200:
                data = r.json().get("data")
                if data:
                    results.append(data)
        await asyncio.sleep(0.003)

    # Batches à 50 Personen
    for i in range(0, len(ids), batch_size):
        batch = ids[i:i + batch_size]
        await asyncio.gather(*(load_one(pid) for pid in batch))

    return results


# ------------------------------------------------------------
# Smart Batch Loader – ORGANISATIONEN
# ------------------------------------------------------------

async def load_org_details(org_ids: List[str], batch_size: int = 50) -> Dict[str, dict]:
    """
    Holt Organisationsdetails, inklusive:
    - Labels
    - Deal-Zähler
    - Vertriebsstopp
    - Name
    """
    results = {}
    sem = asyncio.Semaphore(40)

    async def load_one(oid):
        async with sem:
            url = append_token(f"{PIPEDRIVE_API}/organizations/{oid}?fields=*")
            r = await safe_request("GET", url, headers=get_headers())
            if r.status_code == 200:
                data = r.json().get("data")
                if data:
                    results[str(oid)] = data
        await asyncio.sleep(0.003)

    for i in range(0, len(org_ids), batch_size):
        batch = org_ids[i:i + batch_size]
        await asyncio.gather(*(load_one(oid) for oid in batch))

    return results


# ------------------------------------------------------------
# FILTER 3024 – Python Replikation
# ------------------------------------------------------------

ORG_VERTRIEBSSTOP_KEY = "61d238b86784db69f7300fe8f12f54c601caeff8"
PERSON_BATCH_KEY = "5ac34dad3ea917fdef4087caebf77ba275f87eec"
PERSON_PROSPECT_KEY = "f9138f9040c44622808a4b8afda2b1b75ee5acd0"

def nf_filter_3024(person: dict, org: dict) -> bool:
    """Exakte Kopie des Filter 3024 in Python."""

    # --------------------------------------------------------
    # PERSON-EBENE
    # --------------------------------------------------------

    # Label: nicht "BIZFORWARD SPERRE"
    person_labels = person.get("label_ids") or []
    if any("BIZFORWARD SPERRE" in str(l) for l in person_labels):
        return False

    # Batch ID muss gesetzt sein
    if not cf(person, PERSON_BATCH_KEY):
        return False

    # E-Mail muss vorhanden sein
    emails = person.get("email") or []
    if not emails or not emails[0].get("value"):
        return False


    # --------------------------------------------------------
    # ORGANISATIONS-EBENE
    # --------------------------------------------------------

    if not org:
        return False

    # Organisation → Name != "Freelancer"
    if org.get("name", "").strip().lower() == "freelancer":
        return False

    # Org-Labels
    org_labels = org.get("label_ids") or []

    # Verkaufsstop: dauerhafter
    if any("VERTRIEBSSTOPP DAUERHAFT" in str(l) for l in org_labels):
        return False

    # Verkaufsstop: vorübergehend
    if any("VERTRIEBSSTOPP VORÜBERGEHEND" in str(l) for l in org_labels):
        return False

    # Organisation-Level muss leer sein
    org_level = cf(org, "0ab03885d6792086a0bb007d6302d14b13b0c7d1")
    if org_level not in (None, "", 0):
        return False

    # Deals = 0
    if org.get("open_deals_count", 0) != 0:
        return False
    if org.get("closed_deals_count", 0) != 0:
        return False
    if org.get("won_deals_count", 0) != 0:
        return False
    if org.get("lost_deals_count", 0) != 0:
        return False

    # Vertriebsstopp darf NICHT gesetzt sein
    vertriebsstop = cf(org, ORG_VERTRIEBSSTOP_KEY)
    if vertriebsstop and vertriebsstop != "keine Freelancer-Anstellung":
        return False

    return True


# ------------------------------------------------------------
# NF PIPELINE – SEARCH Driven (FINAL)
# ------------------------------------------------------------

async def load_nf_candidates() -> List[dict]:
    """
    Diese Pipeline ist skalierbar bis 1 Mio. Kontakte.
    Sucht nur relevante Personen und lädt nur relevante Organisationen.
    """

    print("[NF] Starte SEARCH-Driven NF Pipeline…")

    # 1) Personen mit Batch ID
    batch_ids = await search_persons_by_field(PERSON_BATCH_KEY)
    print(f"[NF] Personen mit Batch ID: {len(batch_ids)}")

    # 2) Personen mit Prospect ID
    prospect_ids = await search_persons_by_field(PERSON_PROSPECT_KEY)
    print(f"[NF] Personen mit Prospect ID: {len(prospect_ids)}")

    # 3) Vereinigung (keine Dubletten)
    person_ids = list(set(batch_ids) | set(prospect_ids))
    print(f"[NF] Gesamte NF-Kandidaten (Personen): {len(person_ids)}")

    if not person_ids:
        return []

    # 4) Details für Personen laden
    persons = await load_person_details(person_ids)
    print(f"[NF] Geladene Personendetails: {len(persons)}")

    # 5) relevante Organisations-IDs extrahieren
    org_ids = list({str(p.get("org_id")) for p in persons if p.get("org_id")})
    print(f"[NF] Betroffene Organisationen: {len(org_ids)}")

    # 6) Organisationdetails laden
    orgs = await load_org_details(org_ids)

    # 7) Python-Filter anwenden
    final_nf = []
    for p in persons:
        oid = str(p.get("org_id"))
        org = orgs.get(oid)
        if nf_filter_3024(p, org):
            final_nf.append(p)

    print(f"[NF] Finale NF-Personen (Filter 3024 Replikat): {len(final_nf)}")
    return final_nf

# ============================================================
# MODUL 3 – NF MASTER-DATENAUFBAU (FINAL 2025.6)
# Kompatibel mit Search-Driven NF Engine (Modul 2)
# ============================================================

# Custom Field Keys für Komfort
PERSON_BATCH_KEY = "5ac34dad3ea917fdef4087caebf77ba275f87eec"
PERSON_PROSPECT_KEY = "f9138f9040c44622808a4b8afda2b1b75ee5acd0"
PERSON_GENDER_KEY = "c4f5f434cdb0cfce3f6d62ec7291188fe968ac72"
PERSON_TITLE_KEY  = "0343bc43a91159aaf33a463ca603dc5662422ea5"
PERSON_POSITION_KEY = "4585e5de11068a3bccf02d8b93c126bcf5c257ff"
PERSON_XING_KEY = "44ebb6feae2a670059bc5261001443a2878a2b43"
PERSON_LINKEDIN_KEY = "25563b12f847a280346bba40deaf527af82038cc"


async def _build_nf_master_final(
    nf_batch_ids: List[str],
    batch_id: str,        # EXPORT-Batch
    campaign: str,
    job_obj=None
) -> pd.DataFrame:

    # --------------------------------------------------------
    # 1) NF-Personen laden (SEARCH Engine 2025.5)
    # --------------------------------------------------------
    if job_obj:
        job_obj.phase = "Lade NF-Kandidaten (Search Engine)…"
        job_obj.percent = 10

    persons = await load_nf_candidates()

    print(f"[NF] Kandidaten nach Filter 3024-Replikation: {len(persons)}")

    if job_obj:
        job_obj.phase = "Verarbeite Nachfass-Daten …"
        job_obj.percent = 30


    # --------------------------------------------------------
    # 2) Ausschlussregeln (max 2 pro Org / next activity)
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

        # RULE 1: next_activity_date >= 90 Tage
        dt_raw = p.get("next_activity_date")
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

        # RULE 2: max. 2 Personen pro Organisation
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
    # 3) MASTER-TABELLE ERSTELLEN
    # --------------------------------------------------------

    rows = []

    for p in selected:

        pid = str(p.get("id") or "")
        name = p.get("name") or ""

        org = p.get("organization") or {}
        org_id = str(org.get("id") or "")
        org_name = org.get("name") or "-"

        # Vor / Nachname
        first, last = split_name(
            p.get("first_name"),
            p.get("last_name"),
            name
        )

        # E-Mail
        email = ""
        emails = p.get("email") or []
        if isinstance(emails, list) and emails:
            email = emails[0].get("value") or ""

        # XING
        xing_val = cf(p, PERSON_XING_KEY)
        if isinstance(xing_val, list):
            xing_val = ", ".join(
                x.get("value") for x in xing_val
                if isinstance(x, dict) and x.get("value")
            )

        # LinkedIn
        linkedin_val = cf(p, PERSON_LINKEDIN_KEY)
        if isinstance(linkedin_val, list):
            linkedin_val = ", ".join(
                x.get("value") for x in linkedin_val
                if isinstance(x, dict) and x.get("value")
            )

        rows.append({
            # EXPORT Batch ID (wie du wolltest)
            "Person - Batch ID": batch_id,

            "Person - Channel": DEFAULT_CHANNEL,
            "Cold-Mailing Import": campaign,

            "Person - Organisation": org_name,
            "Organisation - ID": org_id,

            "Person - Geschlecht": cf(p, PERSON_GENDER_KEY),
            "Person - Titel": cf(p, PERSON_TITLE_KEY),

            "Person - Vorname": first,
            "Person - Nachname": last,
            "Person - Position": cf(p, PERSON_POSITION_KEY),

            "Person - ID": pid,
            "Person - XING-Profil": xing_val or "",
            "Person - LinkedIn Profil-URL": linkedin_val or "",

            "Person - E-Mail-Adresse - Büro": email,
        })


    df = pd.DataFrame(rows)


    # --------------------------------------------------------
    # 4) Excluded speichern
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
    # 5) Master speichern
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
