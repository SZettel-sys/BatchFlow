# ============================================================
# master20251117_FINAL.py (Teil 1/4)
# Vollständig überarbeitete, robuste BatchFlow-Version:
# - 108k-optimierte Batch-ID Engine
# - Stabil für Render Free
# - Keine Search API mehr
# - Keine Filter-API notwendig
# ============================================================

import os, re, json, asyncio, io, time
import datetime as dt
from typing import List, Dict, Optional
from collections import defaultdict

import httpx
import asyncpg
import pandas as pd
import numpy as np

from fastapi import FastAPI, Request, Body, Query
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware


# ============================================================
# App Setup
# ============================================================

app = FastAPI(title="BatchFlow")
app.add_middleware(GZipMiddleware, minimum_size=1024)

if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


# ============================================================
# Konstante: Batch ID Feld-Key (fest eingebaut)
# ============================================================

BATCH_FIELD_KEY = "5ac34dad3ea917fdef4087caebf77ba275f87eec"


# ============================================================
# Globale Variablen / ENV Variablen
# ============================================================

PIPEDRIVE_API = "https://api.pipedrive.com/v1"
PD_API_TOKEN = os.getenv("PD_API_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
SCHEMA = os.getenv("PGSCHEMA", "public")

DEFAULT_CHANNEL = "Cold E-Mail"

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL fehlt")


# ============================================================
# HTTP + DB Setup
# ============================================================

def get_headers():
    return {"Content-Type": "application/json"}

def append_token(url: str) -> str:
    if "api_token=" in url:
        return url
    return f"{url}{'&' if '?' in url else '?'}api_token={PD_API_TOKEN}"


@app.on_event("startup")
async def _startup():
    limits = httpx.Limits(max_keepalive_connections=8, max_connections=16)
    app.state.http = httpx.AsyncClient(timeout=60, limits=limits)
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)


@app.on_event("shutdown")
async def _shutdown():
    await app.state.http.aclose()
    await app.state.pool.close()


def http_client() -> httpx.AsyncClient:
    return app.state.http

def get_pool():
    return app.state.pool

def extract_custom_field(person: dict, field_key: str):
    """
    Holt ein Custom-Feld zuverlässig aus einem Pipedrive-Personenobjekt.
    Unterstützt alle Rückgabevarianten (root, custom_fields, nested, dict, list).
    """

    # 1. Direktes Feld
    if field_key in person:
        val = person[field_key]
        if isinstance(val, dict):
            return val.get("value") or val.get("label")
        if isinstance(val, list):
            if val and isinstance(val[0], dict):
                return val[0].get("value") or val[0].get("label")
            return val[0] if val else None
        return val

    # 2. custom_fields
    cf = person.get("custom_fields") or {}
    if field_key in cf:
        val = cf[field_key]
        if isinstance(val, dict):
            return val.get("value") or val.get("label")
        if isinstance(val, list):
            if val and isinstance(val[0], dict):
                return val[0].get("value") or val[0].get("label")
            return val[0] if val else None
        return val

    # 3. nested: data.custom_fields
    data = person.get("data") or {}
    cf2 = data.get("custom_fields") or {}
    if field_key in cf2:
        val = cf2[field_key]
        if isinstance(val, dict):
            return val.get("value") or val.get("label")
        if isinstance(val, list):
            if val and isinstance(val[0], dict):
                return val[0].get("value") or val[0].get("label")
            return val[0] if val else None
        return val

    return None

# ============================================================
# =========  NEUE 108K Batch-ID Engine (ultrastabil) =========
# ============================================================

async def get_all_person_ids_with_batch(batch_field_key: str) -> list[dict]:
    """
    Lädt ALLE Personen, aber nur:
    - id
    - batch_field_key (z. B. "5ac34dad3ea917...")
    
    Vorteil:
    - extrem geringer Speicherverbrauch
    - perfekt für >100.000 Personen
    """
    out = []
    start = 0
    limit = 500

    while True:
        url = append_token(
            f"{PIPEDRIVE_API}/persons?start={start}&limit={limit}"
            f"&fields=id,{batch_field_key}"
        )
        r = await http_client().get(url, headers=get_headers())
        r.raise_for_status()

        data = r.json().get("data") or []
        if not data:
            break

        out.extend(data)

        if len(data) < limit:
            break

        start += limit

    return out


async def get_person_ids_for_batch(batch_field_key: str, batch_values: list[str]) -> list[str]:
    """
    Schritt 2:
    Filtert durch ALLE Personen anhand des Batch-Feldwertes.
    100 % exakt, unabhängig von Pipedrive-Indexierung.
    """
    all_people = await get_all_person_ids_with_batch(batch_field_key)
    batch_values = [v.strip() for v in batch_values if v.strip()]
    ids = []

    for p in all_people:
        val = extract_custom_field(p, batch_field_key)
       
        # Pipedrive liefert Custom-Felder in verschiedenen Formaten
        if isinstance(val, dict):
            val = val.get("value")
        if isinstance(val, list) and val:
            if isinstance(val[0], dict):
                val = val[0].get("value")
            else:
                val = val[0]

        if str(val) in batch_values:
            ids.append(str(p["id"]))

    return ids


async def fetch_person_details(person_ids: List[str]) -> List[dict]:
    """
    Schritt 3:
    Vollständige Personendetails nachladen.
    Parallelisiert und gedrosselt (Render-sicher).
    """
    out = []
    sem = asyncio.Semaphore(10)

    async def fetch_one(pid):
        async with sem:
            url = append_token(f"{PIPEDRIVE_API}/persons/{pid}")
            r = await http_client().get(url, headers=get_headers())
            if r.status_code == 200:
                d = r.json().get("data")
                if d:
                    out.append(d)
            await asyncio.sleep(0.03)

    await asyncio.gather(*[fetch_one(pid) for pid in person_ids])
    return out


async def get_persons_by_batch_ids(batch_field_key: str, batch_values: list[str]) -> list[dict]:
    """
    Hauptfunktion:
    - ID-Liste anhand Batch-Feld (100 % exakt)
    - danach vollständige Personendetails
    """
    ids = await get_person_ids_for_batch(batch_field_key, batch_values)
    print(f"[INFO] Batch-Feld Treffer IDs: {len(ids)}")

    details = await fetch_person_details(ids)
    print(f"[INFO] Vollständige Personen geladen: {len(details)}")

    return details
# ============================================================
# =============   DATABASE + EXPORT UTILITIES   ==============
# ============================================================

async def save_df_text(df: pd.DataFrame, name: str) -> str:
    """
    Speichert DataFrame als CSV-Text in der DB (für Debug & Backup)
    und liefert den CSV-Text zurück.
    """
    csv_text = df.to_csv(index=False)
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
            f"""
            INSERT INTO {SCHEMA}.exports (name, content)
            VALUES ($1, $2)
            """,
            name,
            csv_text,
        )

    return csv_text



def df_to_file_response(df: pd.DataFrame, filename: str) -> FileResponse:
    """
    Schreibt DataFrame temporär als CSV in /tmp und liefert FileResponse.
    """
    path = f"/tmp/{filename}"
    df.to_csv(path, index=False)
    return FileResponse(path, filename=filename, media_type="text/csv")



# ============================================================
# ===================== JOB SYSTEM ===========================
# ============================================================

JOB_STORE = {}

def create_job():
    job_id = str(int(time.time() * 1000))
    JOB_STORE[job_id] = {
        "phase": "init",
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
    JOB_STORE[job_id]["result_file"] = filename
    JOB_STORE[job_id]["done"] = True
    JOB_STORE[job_id]["percent"] = 100



# ============================================================
# =============   NEUKONTAKTE – EXPORT LOGIK   ==============
# ============================================================

async def _build_nk_master(batch_id: str, campaign: str, job_obj=None) -> pd.DataFrame:

    update_job(job_obj, phase="Pipedrive Scan", percent=5)

    # Personen anhand Batch-Feld (NEUE Engine)
    persons = await get_persons_by_batch_ids(BATCH_FIELD_KEY, [batch_id])

    update_job(job_obj, phase="Mapping", percent=40)

    rows = []
    for p in persons:
        pid = str(p.get("id") or "")
        name = p.get("name") or ""

        org = p.get("organization") or {}
        org_id = org.get("id") or ""
        org_name = org.get("name") or ""

        emails = p.get("emails") or []
        email = ""
        if isinstance(emails, list) and emails:
            email = emails[0].get("value") if isinstance(emails[0], dict) else str(emails[0])

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
    await save_df_text(df, f"nk_master_{batch_id}")

    update_job(job_obj, phase="Fertig", percent=100, done=True)

    return df



# ============================================================
# ============= NK – API ENDPOINTS ===========================
# ============================================================

@app.post("/neukontakte/export_start")
async def nk_export_start(data=Body(...)):
    batch_id = data.get("batch_id", "").strip()
    campaign = data.get("campaign", "").strip()

    job_id = create_job()
    update_job(job_id, phase="Start")

    asyncio.create_task(_run_nk_export(batch_id, campaign, job_id))
    return {"job_id": job_id}



async def _run_nk_export(batch_id: str, campaign: str, job_id: str):
    try:
        update_job(job_id, phase="Starte Export", percent=5)
        df = await _build_nk_master(batch_id, campaign, job_id)
        filename = f"nk_export_{batch_id}.csv"
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
        return {"error": "Keine Datei."}
    df = pd.read_csv(f"/tmp/{filename}")
    return df_to_file_response(df, filename)
# ============================================================
# =============  NACHFASS – FILTER & EXPORT LOGIK  ============
# ============================================================

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

def split_name(first, last, fallback):
    """
    Hilfsfunktion zur Namensaufteilung.
    """
    if first or last:
        return first or "", last or ""
    parts = (fallback or "").split(" ")
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


# -------------------------------------------------------------------------
# FINALE VERSION: _build_nf_master_final
# -------------------------------------------------------------------------

async def _build_nf_master_final(
    nf_batch_ids: list[str],
    batch_id: str,
    campaign: str,
    job_obj=None
) -> pd.DataFrame:

    update_job(job_obj, phase="Lade Personen", percent=5)

    # ============================================================
    # Personen anhand Batch-Feld (neue Engine)
    # ============================================================
    persons = await get_persons_by_batch_ids(BATCH_FIELD_KEY, nf_batch_ids)
    print(f"[INFO] Personen gefunden: {len(persons)}")

    # ============================================================
    # Feldmapping (wie bisher)
    # ============================================================
    person_fields = await get_person_fields()
    hint_to_key, gender_map = {}, {}
    next_activity_key = None

    for f in person_fields:
        nm = (f.get("name") or "").lower()

        # Hints finden
        for hint in PERSON_FIELD_HINTS_TO_EXPORT.keys():
            if hint in nm and hint not in hint_to_key:
                hint_to_key[hint] = f.get("key")

        # Gender
        if any(x in nm for x in ("gender", "geschlecht")):
            gender_map = field_options_id_to_label_map(f)

        # Datum nächste Aktivität
        if "datum nächste aktivität" in nm or "next activity" in nm:
            next_activity_key = f.get("key")

    def get_field(p: dict, hint: str) -> str:
        key = hint_to_key.get(hint)
        if not key:
            return ""
        v = p.get(key)
        if isinstance(v, dict):
            return v.get("label") or v.get("value") or ""
        if isinstance(v, list):
            vals = []
            for x in v:
                if isinstance(x, dict):
                    vals.append(x.get("value") or x.get("label") or "")
                elif isinstance(x, str):
                    vals.append(x)
            return ", ".join([v for v in vals if v])
        if hint in ("gender", "geschlecht") and gender_map:
            return gender_map.get(str(v), str(v))
        return str(v or "")

    update_job(job_obj, phase="Filtervorbereitung", percent=25)


    # ============================================================
    # FEHLTE: wichtige Initialisierung (fix aus deiner Version)
    # ============================================================
    selected = []
    excluded = []
    org_counter = defaultdict(int)
    now = dt.datetime.now()


    # ============================================================
    # FILTERLOGIK
    # ============================================================
    for p in persons:
        pid = str(p.get("id") or "")
        name = p.get("name") or ""

        org = p.get("organization") or {}
        org_id = str(org.get("id") or "")
        org_name = org.get("name") or "-"

        # Fallback via metadata
        if not org_id or org_name == "-":
            if "organization" in p.get("metadata", {}):
                org_meta = p["metadata"]["organization"]
                org_id = org_meta.get("id") or org_id
                org_name = org_meta.get("name") or org_name

        # --------------------------------------------------------
        # Regel 1: Datum nächste Aktivität
        # --------------------------------------------------------
        if next_activity_key:
            raw_date = p.get(next_activity_key)
            if raw_date:
                try:
                    dt_val = dt.datetime.fromisoformat(str(raw_date).split(" ")[0])
                    delta_days = (now - dt_val).days
                    if 0 <= delta_days <= 90 or delta_days < 0:
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

        # --------------------------------------------------------
        # Regel 2: Max. 2 Personen pro Organisation
        # --------------------------------------------------------
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

    print(f"[INFO] Nach Filter: {len(selected)} übrig, {len(excluded)} ausgeschlossen")

    update_job(job_obj, phase="Tabellenbau", percent=60)


    # ============================================================
    # DataFrame Bau
    # ============================================================
    rows = []
    for p in selected:
        pid = str(p.get("id") or "")
        org = p.get("organization") or {}
        org_name = org.get("name") or "-"
        org_id = str(org.get("id") or "")
        if not org_name or org_name == "-":
            org_name = p.get("org_name") or "-"

        name = p.get("name") or ""
        vor, nach = split_name(p.get("first_name"), p.get("last_name"), name)

        emails = p.get("emails") or []
        email = ""
        if isinstance(emails, list) and emails:
            email = emails[0].get("value") if isinstance(emails[0], dict) else str(emails[0])
        elif isinstance(emails, str):
            email = emails

        # XING Feld robust
        xing_value = ""
        for k, v in p.items():
            if isinstance(k, str) and "xing" in k.lower():
                if isinstance(v, str) and v.startswith("http"):
                    xing_value = v
                    break
                if isinstance(v, list):
                    xing_value = ", ".join(
                        [x.get("value") for x in v if isinstance(x, dict) and x.get("value")]
                    )
                    break

        rows.append({
            "Person - Batch ID": batch_id,
            "Person - Channel": DEFAULT_CHANNEL,
            "Cold-Mailing Import": campaign,
            "Person - Prospect ID": get_field(p, "prospect"),
            "Person - Organisation": org_name,
            "Organisation - ID": org_id,
            "Person - Geschlecht": get_field(p, "gender") or get_field(p, "geschlecht"),
            "Person - Titel": get_field(p, "titel") or get_field(p, "title") or get_field(p, "anrede"),
            "Person - Vorname": vor,
            "Person - Nachname": nach,
            "Person - Position": get_field(p, "position"),
            "Person - ID": pid,
            "Person ID": pid,
            "Person - XING-Profil": xing_value,
            "Person - LinkedIn Profil-URL": get_field(p, "linkedin") or get_field(p, "linkedin url"),
            "Person - E-Mail-Adresse - Büro": email,
        })

    df = pd.DataFrame(rows)

    # ============================
    # Ausschlüsse DataFrame
    # ============================
    excluded_df = pd.DataFrame(excluded).replace({np.nan: None})

    if excluded_df.empty:
        excluded_df = pd.DataFrame([{
            "Kontakt ID": "-",
            "Name": "-",
            "Organisation ID": "-",
            "Organisationsname": "-",
            "Grund": "Keine Datensätze ausgeschlossen"
        }])

    excluded_df = excluded_df.replace({np.nan: None, np.inf: None, -np.inf: None})

    await save_df_text(df, "nf_master_final")
    await save_df_text(excluded_df, "nf_excluded")

    update_job(job_obj, phase="Fertig", percent=100, done=True)

    print(f"[Nachfass] Export abgeschlossen: {len(df)} Zeilen, {len(excluded)} ausgeschlossen")

    return df



# ============================================================
# ================= NF – API ENDPOINTS ========================
# ============================================================

@app.post("/nachfass/export_start")
async def nf_export_start(data=Body(...)):
    nf_batch_ids = data.get("nf_batch_ids") or []
    batch_id = data.get("batch_id", "").strip()
    campaign = data.get("campaign", "").strip()

    job_id = create_job()
    update_job(job_id, phase="Start")

    asyncio.create_task(_run_nf_export(nf_batch_ids, batch_id, campaign, job_id))
    return {"job_id": job_id}


async def _run_nf_export(nf_batch_ids, batch_id, campaign, job_id):
    try:
        update_job(job_id, phase="Starte Export", percent=5)
        df = await _build_nf_master_final(nf_batch_ids, batch_id, campaign, job_id)
        filename = f"nachfass_export_{batch_id}.csv"
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
        return {"error": "Keine Datei."}
    df = pd.read_csv(f"/tmp/{filename}")
    return df_to_file_response(df, filename)
    
# ============================================================
# ======================   CAMPAIGN UI   ======================
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
                padding: 12px 16px;
                margin-bottom: 12px;
                background: #007bff;
                color: white;
                text-decoration: none;
                width: 260px;
                border-radius: 6px;
            }
            a:hover { background: #005dc1; }
        </style>
    </head>
    <body>
        <h1>BatchFlow – Kampagnenübersicht</h1>
        <a href='/neukontakte'>Neukontakte</a>
        <a href='/nachfass'>Nachfass</a>
    </body>
    </html>
    """)



# ============================================================
# =======================   NK UI   ===========================
# ============================================================

@app.get("/neukontakte", response_class=HTMLResponse)
async def neukontakte_home():
    return HTMLResponse("""
    <!doctype html>
    <html lang='de'>
    <head>
        <meta charset='utf-8'>
        <title>Neukontakte</title>
        <style>
            body { font-family: Arial; margin: 40px; }
            input, button { padding: 8px; margin: 8px 0; font-size: 16px; }
            button {
                background: #007bff; color: white; border: none;
                padding: 10px 20px; border-radius: 4px;
            }
            button:hover { background: #005dc1; cursor: pointer; }
            .progress { margin-top: 20px; width: 350px; background: #eee; border-radius: 6px; }
            .bar { height: 20px; width: 0%; background: #28a745; transition: width 0.3s; border-radius: 6px; }
        </style>
    </head>
    <body>

        <h1>Neukontakte – Export</h1>

        Batch ID:<br>
        <input id='batch' placeholder='z. B. B443'><br>

        Kampagne:<br>
        <input id='campaign' placeholder='Name der Kampagne'><br>

        <button onclick='startExport()'>Export starten</button>

        <div class='progress'><div id='bar' class='bar'></div></div>
        <p id='status'></p>

        <script>
        async function startExport(){
            const batch = document.getElementById('batch').value.trim();
            const campaign = document.getElementById('campaign').value.trim();
            if(!batch){ alert("Bitte Batch ID eingeben!"); return; }

            const r = await fetch('/neukontakte/export_start', {
                method:'POST',
                headers:{'Content-Type':'application/json'},
                body: JSON.stringify({batch_id: batch, campaign: campaign})
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
            setTimeout(() => poll(id), 700);
        }
        </script>
    </body>
    </html>
    """)



# ============================================================
# =======================   NF UI   ===========================
# ============================================================

@app.get("/nachfass", response_class=HTMLResponse)
async def nachfass_home():
    return HTMLResponse("""
    <!doctype html>
    <html lang='de'>
    <head>
        <meta charset='utf-8'>
        <title>Nachfass</title>
        <style>
            body { font-family: Arial; margin: 40px; }
            input, button { padding: 8px; margin: 8px 0; font-size: 16px; }
            button {
                background: #007bff; color: white; border: none;
                padding: 10px 20px; border-radius: 4px;
            }
            button:hover { background: #005dc1; cursor: pointer; }
            .progress { margin-top: 20px; width: 350px; background: #eee; border-radius: 6px; }
            .bar { height: 20px; width: 0%; background: #28a745; transition: width 0.3s; border-radius: 6px; }
        </style>
    </head>

    <body>

        <h1>Nachfass – Export</h1>

        Batch-ID(s):<br>
        <input id='nf_batches' placeholder='B443,B442,B441'><br>

        Export-Batch ID:<br>
        <input id='batch_id' placeholder='B443'><br>

        Kampagne:<br>
        <input id='campaign' placeholder='Kampagnenname'><br>

        <button onclick='startNF()'>Nachfass starten</button>

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
                body: JSON.stringify({
                    nf_batch_ids: nf,
                    batch_id: batch_id,
                    campaign: campaign
                })
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
                    document.getElementById('status').innerText = "Fehler: " + j.error;
                } else {
                    window.location = '/nachfass/export_download?job_id='+id;
                }
                return;
            }
            setTimeout(() => poll(id), 700);
        }
        </script>

    </body>
    </html>
    """)


# ============================================================
# ===============   CATCH-ALL (Keine Loops!)   ================
# ============================================================

@app.get("/{full_path:path}", include_in_schema=False)
async def catch_all(full_path: str):
    return RedirectResponse("/campaign", status_code=302)


