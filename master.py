# ============================================================
# =============== master20251118_CLEAN.py ====================
# ============================================================
# Vollständig überarbeitete, extrem schnelle & stabile Version
# Mit BatchEngine V4 (Hybrid Scan)
# ============================================================

import os
import asyncio
import json
import time
from datetime import datetime
from typing import List, Dict
from collections import defaultdict

import httpx
import pandas as pd
import numpy as np

from fastapi import FastAPI, Request, Body
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    FileResponse,
    RedirectResponse,
)
from fastapi.middleware.gzip import GZipMiddleware


# ============================================================
# =================== KONFIGURATION ===========================
# ============================================================

PIPEDRIVE_API = "https://api.pipedrive.com/v1"
# PIPEDRIVE_TOKEN = os.getenv("PIPEDRIVE_TOKEN", "")
PIPEDRIVE_TOKEN = "9a4b406edd6fa501a48e4832906866643950edbe"
BATCH_FIELD_KEY = "5ac34dad3ea917fdef4087caebf77ba275f87eec"   # korrektes Batch-ID Feld
DEFAULT_CHANNEL = "E-Mail"

SCHEMA = "public"   # verwendetes DB-Schema


# ============================================================
# ================== FASTAPI SETUP ============================
# ============================================================

app = FastAPI(title="BatchFlow 2025 – CLEAN Version")
app.add_middleware(GZipMiddleware, minimum_size=1024)


# ============================================================
# ==================== HTTP CLIENT ============================
# ============================================================

_client = None

def http_client():
    global _client
    if _client:
        return _client
    _client = httpx.AsyncClient(timeout=30)
    return _client


def append_token(url: str) -> str:
    if "api_token=" in url:
        return url
    if "?" in url:
        return url + f"&api_token={PIPEDRIVE_TOKEN}"
    return url + f"?api_token={PIPEDRIVE_TOKEN}"


def get_headers():
    return {"Accept": "application/json"}


# ============================================================
# ================= CUSTOM FIELD EXTRACTOR ====================
# ============================================================

def extract_custom_field(person: dict, field_key: str):
    """
    Robuster Extraktor für Pipedrive-Custom-Felder.
    Funktioniert für:
    - Direkt im Root-Objekt
    - person["custom_fields"]
    - person["data"]["custom_fields"]
    - dict, list, value
    """

    # 1. Direktes Feld
    if field_key in person:
        val = person[field_key]
        if isinstance(val, dict):
            return val.get("value") or val.get("label")
        if isinstance(val, list):
            return (
                val[0].get("value")
                if val and isinstance(val[0], dict)
                else (val[0] if val else None)
            )
        return val

    # 2. custom_fields
    cf = person.get("custom_fields") or {}
    if field_key in cf:
        val = cf[field_key]
        if isinstance(val, dict):
            return val.get("value") or val.get("label")
        if isinstance(val, list):
            return (
                val[0].get("value")
                if val and isinstance(val[0], dict)
                else (val[0] if val else None)
            )
        return val

    # 3. data.custom_fields
    data = person.get("data") or {}
    cf2 = data.get("custom_fields") or {}
    if field_key in cf2:
        val = cf2[field_key]
        if isinstance(val, dict):
            return val.get("value") or val.get("label")
        if isinstance(val, list):
            return (
                val[0].get("value")
                if val and isinstance(val[0], dict)
                else (val[0] if val else None)
            )
        return val

    return None


# ============================================================
# ===================== JOB SYSTEM ===========================
# ============================================================

JOB_STORE = {}

def create_job():
    job_id = str(int(time.time() * 1000))
    JOB_STORE[job_id] = {
        "phase": "Init",
        "percent": 0,
        "done": False,
        "error": None,
        "result_file": None,
    }
    return job_id


def update_job(job_id, phase=None, percent=None, done=None, error=None):
    if job_id not in JOB_STORE:
        return
    if phase is not None:
        JOB_STORE[job_id]["phase"] = phase
    if percent is not None:
        JOB_STORE[job_id]["percent"] = percent
    if done is not None:
        JOB_STORE[job_id]["done"] = done
    if error is not None:
        JOB_STORE[job_id]["error"] = error


def finalize_job(job_id, filename: str):
    JOB_STORE[job_id]["done"] = True
    JOB_STORE[job_id]["percent"] = 100
    JOB_STORE[job_id]["result_file"] = filename


# ============================================================
# =============== SPEICHERN & DOWNLOADS ======================
# ============================================================

async def save_df_text(df: pd.DataFrame, name: str) -> str:
    """Speichert DataFrame als Text in der DB."""
    csv_text = df.to_csv(index=False)
    # → Falls DB nicht aktiv: einfach ignorieren. (Render Free fallback)
    try:
        from utils_db import get_pool
        pool = get_pool()
        async with pool.acquire() as con:
            await con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA}.exports (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    content TEXT
                );
                """
            )
            await con.execute(
                f"INSERT INTO {SCHEMA}.exports (name, content) VALUES ($1, $2)",
                name,
                csv_text,
            )
    except:
        pass

    return csv_text


def df_to_file_response(df: pd.DataFrame, filename: str) -> FileResponse:
    path = f"/tmp/{filename}"
    df.to_csv(path, index=False)
    return FileResponse(path, filename=filename, media_type="text/csv")

# ============================================================
# =============  BatchEngine V5 – DETAIL-MATCH  ===============
# ============================================================

BLOCK_SIZE = 500
PARALLEL_DETAILS = 10      # Anzahl paralleler Detailrequests
DETAIL_DELAY = 0.025       # 25ms – schützt vor Rate-Limit


async def fetch_id_block(start: int) -> list[str]:
    """
    Holt NUR die IDs der Personen.
    Pipedrive liefert hierbei schnell & zuverlässig.
    """
    url = append_token(
        f"{PIPEDRIVE_API}/persons?"
        f"start={start}&limit={BLOCK_SIZE}&"
        f"fields=id"
    )
    r = await http_client().get(url, headers=get_headers())
    if r.status_code != 200:
        return []

    data = r.json().get("data") or []
    return [str(p["id"]) for p in data]


async def get_all_person_ids() -> list[str]:
    """
    Holt ALLE Personen-IDs, blockweise.
    Schnell, RAM-sicher.
    """

    # 1) Gesamtzahl bestimmen
    meta_url = append_token(f"{PIPEDRIVE_API}/persons?start=0&limit=1&fields=id")
    meta = await http_client().get(meta_url, headers=get_headers())
    total = meta.json().get("additional_data", {}).get("pagination", {}).get("total_count", 0)

    print(f"[BatchEngine V5] Gesamtpersonen in Pipedrive: {total}")

    starts = list(range(0, total + 1, BLOCK_SIZE))

    ids = []
    sem = asyncio.Semaphore(6)

    async def load_block(start):
        async with sem:
            block_ids = await fetch_id_block(start)
            ids.extend(block_ids)

    await asyncio.gather(*[load_block(s) for s in starts])

    print(f"[BatchEngine V5] IDs geladen: {len(ids)}")

    return ids



async def get_person_details_if_batch_match(pid: str, batch_values: list[str]):
    """
    Holt Detaildaten einer Person und prüft,
    ob das Batch-Feld zu unseren Batch-IDs passt.
    """

    url = append_token(f"{PIPEDRIVE_API}/persons/{pid}?fields=*")
    r = await http_client().get(url, headers=get_headers())
    if r.status_code != 200:
        return None

    d = r.json().get("data") or {}

    val = extract_custom_field(d, BATCH_FIELD_KEY)

    # Passt die Batch-ID?
    if val in batch_values:
        return d

    return None



async def get_persons_by_batch_ids(batch_field_key: str, batch_values: list[str]) -> list[dict]:
    """
    BatchEngine V5:

    1. Holt alle Personen-IDs (blockweise)
    2. Lädt die Detaildaten NUR für IDs, bei denen die Batch-ID passt
    3. Maximal parallelisiert, aber sicher (wegen Pipedrive-Limits)

    → Funktioniert für ALLE Feldtypen
    → Auch wenn /persons Textfelder NICHT liefert
    """

    # 1: IDs holen
    ids = await get_all_person_ids()

    print(f"[BatchEngine V5] Starte Detailprüfung...")

    # 2: Detailprüfung
    details = []
    sem = asyncio.Semaphore(PARALLEL_DETAILS)

    async def process_id(pid):
        async with sem:
            d = await get_person_details_if_batch_match(pid, batch_values)
            if d:
                details.append(d)
            await asyncio.sleep(DETAIL_DELAY)

    await asyncio.gather(*[process_id(pid) for pid in ids])

    print(f"[BatchEngine V5] Batch-Treffer: {len(details)}")

    return details

# ============================================================
# =============  NEUKONTAKTE – EXPORT LOGIK  ================
# ============================================================

async def build_nk_export(batch_id: str, campaign: str, job_id: str) -> pd.DataFrame:
    """
    Erzeugt den Neukontakte-Export.
    Verwendet BatchEngine V5 für maximale Zuverlässigkeit.
    """

    update_job(job_id, phase="Lade Personen (Batch V5)", percent=10)

    # Personen anhand EINER Batch-ID holen
    persons = await get_persons_by_batch_ids(BATCH_FIELD_KEY, [batch_id])

    update_job(job_id, phase="Verarbeite Datensätze", percent=40)

    rows = []

    for p in persons:
        pid = str(p.get("id") or "")
        name = p.get("name") or ""

        org = p.get("organization") or {}
        org_name = org.get("name") or "-"
        org_id = org.get("id") or ""

        # E-Mail extrahieren
        emails = p.get("emails") or []
        email = ""
        if isinstance(emails, list) and emails:
            email = (
                emails[0].get("value")
                if isinstance(emails[0], dict)
                else str(emails[0])
            )

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

    update_job(job_id, phase="Speichere Datei", percent=80)

    # Dateiname konsistent
    filename = f"neukontakte_{batch_id}.csv"

    # CSV speichern
    await save_df_text(df, filename)

    update_job(job_id, phase="Fertig", percent=100, done=True)

    return df


# ============================================================
# ============= NK – API ENDPOINTS ===========================
# ============================================================

@app.post("/neukontakte/export_start")
async def nk_export_start(data=Body(...)):
    """
    Startet den NK-Export als Hintergrundjob.
    """

    batch_id = data.get("batch_id", "").strip()
    campaign = data.get("campaign", "").strip()

    job_id = create_job()
    update_job(job_id, phase="Init")

    asyncio.create_task(run_nk_export(batch_id, campaign, job_id))

    return {"job_id": job_id}



async def run_nk_export(batch_id: str, campaign: str, job_id: str):
    try:
        df = await build_nk_export(batch_id, campaign, job_id)
        filename = f"neukontakte_{batch_id}.csv"
        df_to_file_response(df, filename)
        finalize_job(job_id, filename)
    except Exception as e:
        update_job(job_id, error=str(e), done=True)



@app.get("/neukontakte/export_progress")
async def nk_export_progress(job_id: str):
    return JOB_STORE.get(job_id, {"error": "Job nicht gefunden"})



@app.get("/neukontakte/export_download")
async def nk_export_download(job_id: str):

    job = JOB_STORE.get(job_id)
    if not job:
        return {"error": "Job nicht gefunden"}

    filename = job.get("result_file")
    if not filename:
        return {"error": "Keine Datei erzeugt"}

    path = f"/tmp/{filename}"

    if not os.path.exists(path):
        return {"error": "Datei nicht gefunden"}

    df = pd.read_csv(path)
    return df_to_file_response(df, filename)

# ============================================================
# =============  NACHFASS – EXPORT LOGIK  ====================
# ============================================================

# Feld-Hints für Matching
PERSON_FIELD_HINTS = {
    "prospect": "prospect",
    "titel": "titel",
    "title": "title",
    "anrede": "anrede",
    "gender": "gender",
    "geschlecht": "geschlecht",
    "position": "position",
    "xing": "xing",
    "xing profil": "xing profil",
    "linkedin": "linkedin",
    "linkedin url": "linkedin url",
}


def split_name(first, last, raw_name):
    """Intelligentes Aufteilen von Namen."""
    if first or last:
        return first or "", last or ""
    if raw_name:
        parts = raw_name.split()
        if len(parts) == 1:
            return parts[0], ""
        return parts[0], " ".join(parts[1:])
    return "", ""


async def get_person_fields():
    """Lädt alle Personenfelder aus Pipedrive (für Hint-Mapping)."""
    url = append_token(f"{PIPEDRIVE_API}/personFields")
    r = await http_client().get(url, headers=get_headers())
    if r.status_code != 200:
        return []
    return r.json().get("data") or []


def field_options_id_to_label_map(field):
    """Mapping für enum-Felder wie Geschlecht."""
    opts = field.get("options") or []
    return {str(o["id"]): o["label"] for o in opts}


async def build_nf_export(nf_batch_ids: list[str], batch_id: str, campaign: str, job_id: str):

    update_job(job_id, phase="Lade Personen (Batch V5)", percent=10)

    # 1) Alle relevanten Personen laden
    persons = await get_persons_by_batch_ids(BATCH_FIELD_KEY, nf_batch_ids)

    update_job(job_id, phase="Feld-Mapping vorbereiten", percent=25)

    # 2) Feld-Mapping
    person_fields = await get_person_fields()
    hint_to_key = {}
    gender_map = {}
    next_activity_key = None

    for f in person_fields:
        nm = (f.get("name") or "").lower()

        # Hint-Mapping
        for hint in PERSON_FIELD_HINTS.keys():
            if hint in nm and hint not in hint_to_key:
                hint_to_key[hint] = f["key"]

        # Geschlecht
        if "gender" in nm or "geschlecht" in nm:
            gender_map = field_options_id_to_label_map(f)

        # Datum nächste Aktivität
        if ("next" in nm and "activity" in nm) or ("datum nächste aktivität" in nm):
            next_activity_key = f["key"]

    def get_field(p: dict, hint: str) -> str:
        """Generische Feld-Abfrage per Hint."""
        key = hint_to_key.get(hint)
        if not key:
            return ""
        val = p.get(key)
        if isinstance(val, dict):
            return val.get("value") or val.get("label") or ""
        if isinstance(val, list):
            vals = []
            for x in val:
                if isinstance(x, dict):
                    vals.append(x.get("value") or x.get("label"))
                else:
                    vals.append(str(x))
            return ", ".join([v for v in vals if v])
        if hint in ("gender", "geschlecht") and gender_map:
            return gender_map.get(str(val), str(val))
        return str(val or "")

    # 3) FILTERLOGIK
    update_job(job_id, phase="Filter Personen", percent=45)

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

        # Regel 1: Datum nächste Aktivität < 3 Monate ODER Zukunft
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
                            "Grund": "Nächste Aktivität <3 Monate oder Zukunft"
                        })
                        continue
                except:
                    pass

        # Regel 2: max. 2 Kontakte pro Organisation
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

    print(f"[NF] Ausgewählt: {len(selected)}, ausgeschlossen: {len(excluded)}")

    update_job(job_id, phase="Baue Tabellen", percent=70)

    # 4) DataFrame erzeugen
    rows = []

    for p in selected:

        pid = str(p.get("id") or "")
        name = p.get("name") or ""

        org = p.get("organization") or {}
        org_name = org.get("name") or "-"
        org_id = str(org.get("id") or "")

        vor, nach = split_name(p.get("first_name"), p.get("last_name"), name)

        # E-Mail
        emails = p.get("emails") or []
        email = ""
        if isinstance(emails, list) and emails:
            email = (
                emails[0].get("value")
                if isinstance(emails[0], dict)
                else str(emails[0])
            )

        # Xing-Profil
        xing_val = ""
        for k, v in p.items():
            if "xing" in str(k).lower():
                if isinstance(v, str) and "http" in v:
                    xing_val = v
                elif isinstance(v, list):
                    xing_val = ", ".join(
                        [
                            x.get("value")
                            for x in v
                            if isinstance(x, dict) and x.get("value")
                        ]
                    )
                break

        rows.append({
            "Person - Batch ID": batch_id,
            "Person - Channel": DEFAULT_CHANNEL,
            "Cold-Mailing Import": campaign,
            "Person - Organisation": org_name,
            "Organisation - ID": org_id,
            "Person - Geschlecht": get_field(p, "gender"),
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

    # 5) Excluded-Daten vorbereiten
    excluded_df = pd.DataFrame(excluded).replace({np.nan: None})

    if excluded_df.empty:
        excluded_df = pd.DataFrame([{
            "Kontakt ID": "-",
            "Name": "-",
            "Organisation ID": "-",
            "Organisationsname": "-",
            "Grund": "Keine Datensätze ausgeschlossen"
        }])

    update_job(job_id, phase="Speichere Dateien", percent=90)

    main_file = f"nachfass_{batch_id}.csv"
    excluded_file = f"nachfass_excluded_{batch_id}.csv"

    await save_df_text(df, main_file)
    await save_df_text(excluded_df, excluded_file)

    update_job(job_id, phase="Fertig", percent=100, done=True)

    return df



# ============================================================
# ============= NF – API ENDPOINTS ===========================
# ============================================================

@app.post("/nachfass/export_start")
async def nf_export_start(data=Body(...)):
   
    nf_batch_ids = [str(x).strip() for x in (data.get("nf_batch_ids") or [])]
    batch_id = (data.get("batch_id") or "").strip()
    campaign = (data.get("campaign") or "").strip()

    job_id = create_job()
    update_job(job_id, phase="Init")

    asyncio.create_task(run_nf_export(nf_batch_ids, batch_id, campaign, job_id))

    return {"job_id": job_id}



async def run_nf_export(nf_batch_ids, batch_id, campaign, job_id):
    try:
        df = await build_nf_export(nf_batch_ids, batch_id, campaign, job_id)
        filename = f"nachfass_{batch_id}.csv"
        df_to_file_response(df, filename)
        finalize_job(job_id, filename)
    except Exception as e:
        update_job(job_id, error=str(e), done=True)



@app.get("/nachfass/export_progress")
async def nf_export_progress(job_id: str):
    return JOB_STORE.get(job_id, {"error": "Job nicht gefunden"})



@app.get("/nachfass/export_download")
async def nf_export_download(job_id: str):

    job = JOB_STORE.get(job_id)
    if not job:
        return {"error": "Job nicht gefunden"}

    filename = job.get("result_file")
    if not filename:
        return {"error": "Keine Datei erzeugt"}

    path = f"/tmp/{filename}"

    if not os.path.exists(path):
        return {"error": "Datei nicht gefunden"}

    df = pd.read_csv(path)
    return df_to_file_response(df, filename)

# ============================================================
# =======================   UI – HOME   =======================
# ============================================================

@app.get("/campaign", response_class=HTMLResponse)
async def campaign_home():
    return HTMLResponse("""
    <!doctype html>
    <html lang='de'>
    <head>
        <meta charset='utf-8'>
        <title>BatchFlow – Kampagnen</title>
        <style>
            body { font-family: Arial; margin: 40px; }
            h1 { margin-bottom: 20px; }
            a {
                display: block;
                padding: 12px 18px;
                margin-bottom: 15px;
                background: #007bff;
                color: white;
                text-decoration: none;
                width: 280px;
                border-radius: 6px;
                font-size: 17px;
            }
            a:hover { background: #005dc1; }
        </style>
    </head>
    <body>
        <h1>BatchFlow – Kampagnenübersicht</h1>

        <a href='/neukontakte'>Neukontakte Export</a>
        <a href='/nachfass'>Nachfass Export</a>
    </body>
    </html>
    """)



# ============================================================
# ====================   UI – NEUKONTAKTE   ==================
# ============================================================

@app.get("/neukontakte", response_class=HTMLResponse)
async def nk_home():
    return HTMLResponse("""
    <!doctype html>
    <html lang='de'>
    <head>
        <meta charset='utf-8'>
        <title>Neukontakte Export</title>
        <style>
            body { font-family: Arial; margin: 40px; }
            input, button { padding: 8px; margin: 10px 0; font-size: 16px; width: 300px; }
            button {
                background: #007bff; color: white; border: none;
                padding: 10px 20px; border-radius: 4px;
            }
            button:hover { background: #005dc1; cursor: pointer; }

            .progress { margin-top: 20px; width: 330px; background: #eee; border-radius: 6px; }
            .bar { height: 22px; width: 0%; background: #28a745; transition: width 0.25s; border-radius: 6px; }
        </style>
    </head>
    <body>

        <h1>Neukontakte – Export</h1>

        <label>Batch ID</label><br>
        <input id='batch' placeholder='z. B. B443'><br>

        <label>Kampagne</label><br>
        <input id='campaign' placeholder='Kampagnenname'><br>

        <button onclick='startExport()'>Export starten</button>

        <div class='progress'><div id='bar' class='bar'></div></div>
        <p id='status'></p>

        <script>
        async function startExport(){
            const batch = document.getElementById('batch').value.trim();
            const campaign = document.getElementById('campaign').value.trim();

            if(!batch){ alert("Bitte Batch ID eingeben!"); return; }

            const r = await fetch('/neukontakte/export_start', {
                method: 'POST',
                headers: {'Content-Type':'application/json'},
                body: JSON.stringify({ batch_id: batch, campaign: campaign })
            });
            const j = await r.json();
            poll(j.job_id);
        }

        async function poll(id){
            const r = await fetch('/neukontakte/export_progress?job_id='+id);
            const j = await r.json();

            document.getElementById('status').innerText = j.phase || "";
            document.getElementById('bar').style.width = (j.percent||0) + '%';

            if(j.done){
                if(j.error){
                    document.getElementById('status').innerText = "Fehler: " + j.error;
                } else {
                    window.location = '/neukontakte/export_download?job_id='+id;
                }
                return;
            }
            setTimeout(() => poll(id), 600);
        }
        </script>

    </body>
    </html>
    """)



# ============================================================
# =====================   UI – NACHFASS   ====================
# ============================================================

@app.get("/nachfass", response_class=HTMLResponse)
async def nf_home():
    return HTMLResponse("""
    <!doctype html>
    <html lang='de'>
    <head>
        <meta charset='utf-8'>
        <title>Nachfass Export</title>
        <style>
            body { font-family: Arial; margin: 40px; }
            input, button { padding: 8px; margin: 10px 0; font-size: 16px; width: 300px; }
            button {
                background: #007bff; color: white; border: none;
                padding: 10px 20px; border-radius: 4px;
            }
            button:hover { background: #005dc1; cursor: pointer; }

            .progress { margin-top: 20px; width: 330px; background: #eee; border-radius: 6px; }
            .bar { height: 22px; width: 0%; background: #28a745; transition: width 0.25s; border-radius: 6px; }
        </style>
    </head>
    <body>

        <h1>Nachfass – Export</h1>

        <label>Batch-ID(s) für Nachfass</label><br>
        <input id='nf_batches' placeholder='z. B. B443,B442,B441'><br>

        <label>Export-Batch ID</label><br>
        <input id='batch_id' placeholder='Haupt-Batch (B443)'><br>

        <label>Kampagne</label><br>
        <input id='campaign' placeholder='Kampagnenname'><br>

        <button onclick='startNF()'>Nachfass Export starten</button>

        <div class='progress'><div id='bar' class='bar'></div></div>
        <p id='status'></p>

        <script>
        async function startNF(){
            const nf = document.getElementById('nf_batches').value
                        .split(',')
                        .map(x => x.trim())
                        .filter(Boolean);

            const batch_id = document.getElementById('batch_id').value.trim();
            const campaign = document.getElementById('campaign').value.trim();

            if(nf.length === 0){ alert("Bitte Batch-IDs eingeben!"); return; }

            const r = await fetch('/nachfass/export_start', {
                method:'POST',
                headers:{'Content-Type':'application/json'},
                body: JSON.stringify({ nf_batch_ids: nf, batch_id: batch_id, campaign: campaign })
            });
            const j = await r.json();
            poll(j.job_id);
        }

        async function poll(id){
            const r = await fetch('/nachfass/export_progress?job_id='+id);
            const j = await r.json();

            document.getElementById('status').innerText = j.phase || "";
            document.getElementById('bar').style.width = (j.percent||0) + '%';

            if(j.done){
                if(j.error){
                    document.getElementById('status').innerHTML = "Fehler: " + j.error;
                } else {
                    window.location = '/nachfass/export_download?job_id='+id;
                }
                return;
            }
            setTimeout(() => poll(id), 600);
        }
        </script>

    </body>
    </html>
    """)
# ============================================================
# ======================= DEBUG MODUS =========================
# ============================================================

@app.get("/debug/person/{pid}", response_class=JSONResponse)
async def debug_person(pid: str):
    """
    ROHDATEN einer Person aus Pipedrive.
    Prüft:
    - Ist das Batch-Feld im Detail-Endpoint vorhanden?
    - Sind Felder korrekt gefüllt?
    - Wie sieht das Datenmodell aus?
    """
    url = append_token(f"{PIPEDRIVE_API}/persons/{pid}?fields=*")
    r = await http_client().get(url, headers=get_headers())
    return r.json()



@app.get("/debug/block/{start}", response_class=JSONResponse)
async def debug_block(start: int):
    """
    Prüft, ob Pipedrive die Listen-Daten korrekt liefert.
    Wichtig für Block-Scans.
    """
    url = append_token(
        f"{PIPEDRIVE_API}/persons?"
        f"start={start}&limit={BLOCK_SIZE}&fields=id,name"
    )
    r = await http_client().get(url, headers=get_headers())
    return r.json()



@app.get("/debug/batch/{batch_id}", response_class=JSONResponse)
async def debug_batch(batch_id: str):
    """
    Zeigt, wie viele Personen für eine Batch-ID gefunden werden.
    Nutzt die BatchEngine V5 (Detail-Match).
    """

    ids = await get_all_person_ids()
    matched = []

    sem = asyncio.Semaphore(6)

    async def check(pid):
        async with sem:
            url = append_token(f"{PIPEDRIVE_API}/persons/{pid}?fields=*")
            r = await http_client().get(url, headers=get_headers())
            if r.status_code == 200:
                d = r.json().get("data") or {}
                val = extract_custom_field(d, BATCH_FIELD_KEY)
                if val == batch_id:
                    matched.append(pid)

    await asyncio.gather(*[check(pid) for pid in ids])

    return {
        "batch_id": batch_id,
        "treffer": len(matched),
        "erste_ids": matched[:50]
    }



# ============================================================
# ====================  CATCH-ALL ROUTE ======================
# ============================================================

@app.get("/{full_path:path}", include_in_schema=False)
async def fallback(full_path: str):
    """
    Saubere Umleitung aller unbekannten URLs:
    → Immer zur Kampagnenseite
    """
    return RedirectResponse("/campaign", status_code=302)




