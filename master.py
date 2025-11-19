# =============================================================
# MASTER 2025 – MODUL 1/8
# BOOTSTRAP • API CLIENT • UTILS • EXCEL EXPORT
# =============================================================

import os
import httpx
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse


# -------------------------------------------------------------
# PIPEDRIVE API BASIS
# -------------------------------------------------------------
PD_API_TOKEN = os.getenv("PD_API_TOKEN", "").strip()
PD_API_BASE = "https://api.pipedrive.com/v1"


def append_token(url: str) -> str:
    """Fügt api_token hinzu."""
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}api_token={PD_API_TOKEN}"


def get_headers() -> dict:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }


# -------------------------------------------------------------
# EXTREM STABILER REQUEST WRAPPER
# (429 / Timeout / Netzwerkfehler geschützt)
# -------------------------------------------------------------
async def safe_request(
    method: str,
    url: str,
    headers=None,
    client: Optional[httpx.AsyncClient] = None,
    max_retries: int = 10,
    initial_delay: float = 1.0
):
    delay = initial_delay

    for attempt in range(max_retries):
        try:
            if client:
                r = await client.request(method, url, headers=headers)
            else:
                async with httpx.AsyncClient(timeout=60.0) as c:
                    r = await c.request(method, url, headers=headers)

            if r.status_code == 200:
                return r.json()

            if r.status_code == 429:
                print(f"[429] Rate limit – warte {delay:.1f}s → {url}")
                await asyncio.sleep(delay)
                delay *= 1.6
                continue

            print(f"[WARN] API Fehler {r.status_code} → {url}")
            print(r.text)
            return r.json()

        except Exception as e:
            print(f"[ERR] safe_request: {e}")

        await asyncio.sleep(delay)
        delay *= 1.5

    raise Exception(f"API dauerhaft fehlgeschlagen → {url}")


# -------------------------------------------------------------
# CUSTOM FIELD HELPER
# -------------------------------------------------------------
def cf(obj: dict, key: str):
    if not obj:
        return None
    return (obj.get("custom_fields") or {}).get(key)


# -------------------------------------------------------------
# NAME SPLIT HELFER
# -------------------------------------------------------------
def split_name(first: str, last: str, full_name: str):
    if first or last:
        return first or "", last or ""

    if not full_name:
        return "", ""

    parts = full_name.strip().split(" ")
    if len(parts) == 1:
        return parts[0], ""

    return parts[0], " ".join(parts[1:])


# -------------------------------------------------------------
# EXCEL EXPORT: 1 Datei – 2 Tabellen ("Master", "Excluded")
# -------------------------------------------------------------
async def nf_export_to_excel(master_df, excluded_df, job_id: str) -> str:
    export_dir = "/tmp"
    filename = f"nachfass_export_{job_id}.xlsx"
    file_path = os.path.join(export_dir, filename)

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        master_df.to_excel(writer, sheet_name="Master", index=False)
        excluded_df.to_excel(writer, sheet_name="Excluded", index=False)

    print(f"[NF] Export gespeichert → {file_path}")
    return file_path
# =============================================================
# MASTER 2025 – MODUL 2/8
# FILTER-FIRST PERSONS ENGINE (3024 IDs → Persons Light)
# =============================================================

# Der Filter 3024 in Pipedrive
FILTER_3024_ID = 3024

# Custom Field Key der Batch-ID
BATCH_FIELD_KEY = "5ac34dad3ea917fdef4087caebf77ba275f87eec"


# -------------------------------------------------------------
# IDs aus FILTER 3024 holen
# -------------------------------------------------------------
async def nf_get_ids_from_filter(filter_id: int) -> List[int]:
    """Holt ALLE Personen-IDs aus Filter 3024 via Pipedrive Pagination."""
    persons = []
    start = 0
    limit = 500

    while True:
        url = append_token(
            f"{PD_API_BASE}/persons?filter_id={filter_id}&start={start}&limit={limit}"
        )

        resp = await safe_request("GET", url)
        data = resp.get("data") or []
        persons.extend([p["id"] for p in data])

        pag = (resp.get("additional_data") or {}).get("pagination") or {}
        if not pag.get("more_items_in_collection"):
            break

        start = pag.get("next_start") or (start + limit)

    print(f"[NF] IDs aus Filter {filter_id}: {len(persons)}")
    return persons


# -------------------------------------------------------------
# Personen LIGHT laden (nur Personen aus Filter 3024)
# -------------------------------------------------------------
async def nf_load_persons_light_from_filter(ids: List[int]) -> List[dict]:
    """Lädt Personen-Light-Daten für die IDs aus Filter 3024."""
    persons = []

    async def load_one(pid):
        try:
            url = append_token(f"{PD_API_BASE}/persons/{pid}")
            r = await safe_request("GET", url)
            if r and r.get("data"):
                persons.append(r["data"])
        except:
            pass

    await asyncio.gather(*(load_one(pid) for pid in ids))

    print(f"[NF] Personen (Light) geladen: {len(persons)}")
    return persons


# -------------------------------------------------------------
# Personen behalten, die eine Batch-ID haben
# -------------------------------------------------------------
def nf_filter_persons_with_batch(persons: List[dict]) -> List[dict]:
    """Filtert Personen mit gültiger Batch-ID."""
    result = []
    for p in persons:
        batch = p.get(BATCH_FIELD_KEY)
        if batch and str(batch).strip() != "":
            result.append(p)

    print(f"[NF] Personen mit Batch-ID: {len(result)}")
    return result


# -------------------------------------------------------------
# KOMPLETTE PERSONEN-PIPELINE
# (wird vom Runner aufgerufen)
# -------------------------------------------------------------
async def nf_filter_first_pipeline():
    """Nutzen wir, wenn man alles in einem Schritt holen will."""
    ids = await nf_get_ids_from_filter(FILTER_3024_ID)
    persons = await nf_load_persons_light_from_filter(ids)
    persons_batch = nf_filter_persons_with_batch(persons)
    return persons_batch
# =============================================================
# MASTER 2025 – MODUL 3/8
# MASTER BUILDER (Organisationen laden, Ausschlüsse, Exportdaten)
# =============================================================

# Organisations-Custom-Fields
ORG_FIELD_LEVEL = "0ab03885d6792086a0bb007d6302d14b13b0c7d1"
ORG_FIELD_STOP  = "61d238b86784db69f7300fe8f12f54c601caeff8"

# Personen-Felder
PROSPECT_FIELD_KEY = "f9138f9040c44622808a4b8afda2b1b75ee5acd0"
TITLE_FIELD_KEY     = "0343bc43a91159aaf33a463ca603dc5662422ea5"
POSITION_FIELD_KEY  = "4585e5de11068a3bccf02d8b93c126bcf5c257ff"
XING_FIELD_KEY      = "44ebb6feae2a670059bc5261001443a2878a2b43"
LINKEDIN_FIELD_KEY  = "25563b12f847a280346bba40deaf527af82038cc"
GENDER_FIELD_KEY    = "c4f5f434cdb0cfce3f6d62ec7291188fe968ac72"
NEXT_ACTIVITY_KEY   = "next_activity_date"


# -------------------------------------------------------------
# Organisationen LIGHT laden (für relevante Personen)
# -------------------------------------------------------------
async def nf_load_orgs_light_for_persons(persons: List[dict]) -> Dict[str, dict]:
    org_ids = {str(p.get("org_id")) for p in persons if p.get("org_id")}
    orgs = {}

    async def load_one(oid):
        try:
            url = append_token(f"{PD_API_BASE}/organizations/{oid}")
            r = await safe_request("GET", url)
            if r and r.get("data"):
                orgs[str(oid)] = r["data"]
        except Exception as e:
            print(f"[ORG-ERR] {oid}: {e}")

    await asyncio.gather(*(load_one(oid) for oid in org_ids))

    print(f"[NF] Organisationen geladen: {len(orgs)}")
    return orgs


# -------------------------------------------------------------
# Personen + Organisationen kombinieren
# -------------------------------------------------------------
def nf_merge_persons_orgs(persons: List[dict], orgs: Dict[str, dict]) -> List[dict]:
    merged = []
    for p in persons:
        oid = str(p.get("org_id") or "")
        merged.append({
            "person": p,
            "org": orgs.get(oid, {})
        })

    print(f"[NF] Kandidaten kombiniert: {len(merged)}")
    return merged


# -------------------------------------------------------------
# Ausschlussregeln (max. 2 Kontakte + next_activity)
# -------------------------------------------------------------
def nf_apply_exclusions(merged: List[dict]):
    selected = []
    excluded = []
    counter_org = defaultdict(int)
    now = datetime.now()

    for item in merged:
        p = item["person"]
        org = item["org"]

        pid = str(p.get("id"))
        oid = str(org.get("id") or "")
        oname = org.get("name") or "-"

        # === Regel 1: Aktivität < 3 Monate → EXCLUDE
        dt_raw = p.get(NEXT_ACTIVITY_KEY)
        if dt_raw:
            try:
                dt = datetime.fromisoformat(str(dt_raw).split(" ")[0])
                if (now - dt).days <= 90:
                    excluded.append({
                        "Kontakt ID": pid,
                        "Name": p.get("name"),
                        "Organisation": oname,
                        "Grund": "Nächste Aktivität < 3 Monate"
                    })
                    continue
            except:
                pass

        # === Regel 2: nur 2 Personen pro Organisation
        if oid:
            counter_org[oid] += 1
            if counter_org[oid] > 2:
                excluded.append({
                    "Kontakt ID": pid,
                    "Name": p.get("name"),
                    "Organisation": oname,
                    "Grund": "Mehr als 2 Kontakte pro Organisation"
                })
                continue

        selected.append(item)

    print(f"[NF] Ausgewählt: {len(selected)}, Excluded: {len(excluded)}")
    return selected, excluded


# -------------------------------------------------------------
# MASTER DATENFRAME BAUEN (Exportfertige Daten)
# -------------------------------------------------------------
def nf_build_master(selected: List[dict], batch_id: str, campaign: str) -> pd.DataFrame:
    rows = []

    for item in selected:
        p = item["person"]
        org = item["org"]

        pid = str(p.get("id"))
        oname = org.get("name") or "-"
        oid = str(org.get("id") or "")

        first, last = split_name(
            p.get("first_name"),
            p.get("last_name"),
            p.get("name") or ""
        )

        # E-Mail
        email = ""
        emails = p.get("email") or p.get("emails") or []
        if isinstance(emails, list) and emails:
            if isinstance(emails[0], dict):
                email = emails[0].get("value") or ""
            elif isinstance(emails[0], str):
                email = emails[0]

        rows.append({
            "Person - Batch ID": batch_id,
            "Person - Prospect ID": p.get(PROSPECT_FIELD_KEY) or "",
            "Person - Organisation": oname,
            "Organisation - ID": oid,
            "Person - Geschlecht": p.get(GENDER_FIELD_KEY) or "",
            "Person - Titel": p.get(TITLE_FIELD_KEY) or "",
            "Person - Vorname": first,
            "Person - Nachname": last,
            "Person - Position": p.get(POSITION_FIELD_KEY) or "",
            "Person - ID": pid,
            "Person - XING-Profil": p.get(XING_FIELD_KEY) or "",
            "Person - LinkedIn Profil": p.get(LINKEDIN_FIELD_KEY) or "",
            "Person - E-Mail": email,
            "Cold-Mailing Import": campaign
        })

    print(f"[NF] Master-Zeilen: {len(rows)}")
    return pd.DataFrame(rows)
# =============================================================
# MASTER 2025 – MODUL 4/8
# DETAIL LOADER (Persons & Organisations)
# =============================================================

DETAIL_SEMAPHORE = 40
ORG_DETAIL_SEMAPHORE = 30


# -------------------------------------------------------------
# Personen-Details laden
# -------------------------------------------------------------
async def nf_load_person_details(person_ids: List[str]) -> Dict[str, dict]:
    results = {}
    sem = asyncio.Semaphore(DETAIL_SEMAPHORE)

    async with httpx.AsyncClient(timeout=60.0) as client:

        async def load_one(pid):
            async with sem:
                url = append_token(f"{PD_API_BASE}/persons/{pid}?fields=*")
                r = await safe_request("GET", url, headers=get_headers(), client=client)
                data = r.get("data")
                if data:
                    results[str(pid)] = data

                await asyncio.sleep(0.001)

        await asyncio.gather(*(load_one(pid) for pid in person_ids))

    print(f"[NF] Personendetails geladen: {len(results)}")
    return results


# -------------------------------------------------------------
# Organisations-Details laden
# -------------------------------------------------------------
async def nf_load_org_details(org_ids: List[str]) -> Dict[str, dict]:
    results = {}
    sem = asyncio.Semaphore(ORG_DETAIL_SEMAPHORE)

    async with httpx.AsyncClient(timeout=60.0) as client:

        async def load_one(oid):
            async with sem:
                url = append_token(f"{PD_API_BASE}/organizations/{oid}?fields=*")
                r = await safe_request("GET", url, headers=get_headers(), client=client)
                data = r.get("data")
                if data:
                    results[str(oid)] = data

                await asyncio.sleep(0.001)

        await asyncio.gather(*(load_one(oid) for oid in org_ids))

    print(f"[NF] Organisationsdetails geladen: {len(results)}")
    return results


# -------------------------------------------------------------
# KOMBI: Details für NF-Kandidaten laden
# -------------------------------------------------------------
async def nf_load_details_for_candidates(candidates: List[dict]):
    # Person IDs
    person_ids = [
        str(item["person"].get("id"))
        for item in candidates
        if item["person"].get("id")
    ]

    print(f"[NF] Lade Personendetails für {len(person_ids)} Kandidaten …")
    persons_full = await nf_load_person_details(person_ids)

    # Organisation IDs
    org_ids = list({
        str(item["org"].get("id"))
        for item in candidates
        if item["org"].get("id")
    })

    print(f"[NF] Lade Organisationsdetails für {len(org_ids)} Organisationen …")
    orgs_full = await nf_load_org_details(org_ids)

    return persons_full, orgs_full
# =============================================================
# MASTER 2025 – MODUL 5/8
# FILTER 3024 – PYTHON-REPLIKATION (Organisation + Personen)
# =============================================================

# Personenbezogene Ausschluss-Labels
PERSON_LABEL_BLACKLIST = [
    "BIZFORWARD SPERRE", 
    "DO NOT CONTACT", 
    "SPERRE"
]

# Organisationsbezogene Blacklist-Begriffe
ORG_NAME_BLACKLIST = ["freelancer", "freelancers", "freelance"]

# Org Custom Fields werden bereits in Modul 3 definiert
# ORG_FIELD_LEVEL
# ORG_FIELD_STOP


# -------------------------------------------------------------
# ORGANISATIONS-FILTER (3024 Logik)
# -------------------------------------------------------------
def nf_filter_organization_3024(org: dict) -> bool:
    if not org:
        return False

    name = (org.get("name") or "").lower().strip()

    # 1) Name enthält "freelancer" → raus
    if any(bad in name for bad in ORG_NAME_BLACKLIST):
        return False

    # 2) Level-Feld muss leer/None sein
    level = cf(org, ORG_FIELD_LEVEL)
    if level not in (None, "", 0):
        return False

    # 3) Vertriebsstopp prüfen
    vst = cf(org, ORG_FIELD_STOP)
    if vst and vst != "keine Freelancer-Anstellung":
        return False

    # 4) Labels (Blacklisting auf Label-Ebene)
    labels = org.get("label_ids") or []
    if labels:
        for l in labels:
            ls = str(l).lower()
            if "stopp" in ls or "verbot" in ls:
                return False

    # 5) Deals müssen 0 sein
    if org.get("open_deals_count", 0) != 0:
        return False
    if org.get("closed_deals_count", 0) != 0:
        return False
    if org.get("won_deals_count", 0) != 0:
        return False
    if org.get("lost_deals_count", 0) != 0:
        return False

    return True


# -------------------------------------------------------------
# PERSONEN-FILTER (erste Stufe)
# -------------------------------------------------------------
def nf_filter_person_basic(p: dict) -> bool:
    """Filtert Personen, die offensichtliche Ausschlusskriterien treffen."""
    if not p:
        return False

    # Batch-ID muss vorhanden sein (Modul 2 filtert zwar schon,
    # aber hier doppelte Absicherung)
    if not p.get(BATCH_FIELD_KEY):
        return False

    # Email muss vorhanden sein
    emails = p.get("email") or p.get("emails") or []
    if isinstance(emails, list):
        if not emails or not (emails[0].get("value") if isinstance(emails[0], dict) else emails[0]):
            return False
    elif not emails:
        return False

    # Person muss eine Organisation besitzen
    if not p.get("org_id"):
        return False

    # Label-Blacklist
    labels = p.get("label_ids") or []
    for bad in PERSON_LABEL_BLACKLIST:
        if any(bad.lower() in str(l).lower() for l in labels):
            return False

    return True


# -------------------------------------------------------------
# FILTER-PROZESS: Personen + Orgas (3024)
# -------------------------------------------------------------
def nf_filter_3024(persons_with_batch: List[dict], orgs: Dict[str, dict]) -> List[dict]:
    """Anwendung der vollständigen Filter 3024 Logik."""
    
    # 1) gültige Organisationen bestimmen
    valid_orgs = {
        oid for oid, odata in orgs.items()
        if nf_filter_organization_3024(odata)
    }

    print(f"[NF] Gültige Organisationen (3024): {len(valid_orgs)}")

    # 2) Personen filtern, die gültige Orgas besitzen
    result = []
    for p in persons_with_batch:
        if not nf_filter_person_basic(p):
            continue

        oid = str(p.get("org_id"))
        if oid in valid_orgs:
            result.append(p)

    print(f"[NF] Personen nach Filter 3024: {len(result)}")
    return result
# =============================================================
# MASTER 2025 – MODUL 6/8
# FILTER-FIRST PIPELINE RUNNER (UI → Backend → Export)
# =============================================================

async def run_nf_pipeline_background(job_id: str, batch_ids: str, export_batch: str, campaign: str):
    job = get_job(job_id)
    if not job:
        return

    try:
        # ---------------------------------------------------------
        # PHASE 1 – IDs aus Filter 3024 holen
        # ---------------------------------------------------------
        job.phase = "Lade IDs aus Filter 3024 …"
        job.percent = 10

        ids = await nf_get_ids_from_filter(FILTER_3024_ID)
        if not ids:
            job.error = "Filter 3024 enthält keine Personen."
            job.phase = "Fehler"
            job.percent = 100
            return

        # ---------------------------------------------------------
        # PHASE 2 – Personen (Light)
        # ---------------------------------------------------------
        job.phase = "Lade Personen (Light) …"
        job.percent = 25

        persons_light = await nf_load_persons_light_from_filter(ids)
        if not persons_light:
            job.error = "Keine Personen geladen."
            job.phase = "Fehler"
            job.percent = 100
            return

        # ---------------------------------------------------------
        # PHASE 3 – Batch-ID Filter
        # ---------------------------------------------------------
        job.phase = "Filtere Personen mit Batch-ID …"
        job.percent = 35

        persons_batch = nf_filter_persons_with_batch(persons_light)

        # Falls UI eine Batchliste angibt (B443,B448,…)
        if batch_ids.strip():
            batch_set = {b.strip() for b in batch_ids.split(",")}
            persons_batch = [
                p for p in persons_batch
                if str(p.get(BATCH_FIELD_KEY)) in batch_set
            ]

        if not persons_batch:
            job.error = "Keine passenden Personen nach Batch-Auswahl."
            job.phase = "Fehler"
            job.percent = 100
            return

        # ---------------------------------------------------------
        # PHASE 4 – Organisationen laden
        # ---------------------------------------------------------
        job.phase = "Lade Organisationen …"
        job.percent = 50

        orgs = await nf_load_orgs_light_for_persons(persons_batch)

        # ---------------------------------------------------------
        # PHASE 5 – Filter 3024 anwenden
        # ---------------------------------------------------------
        job.phase = "Filtere Organisationen + Personen (3024) …"
        job.percent = 60

        persons_3024 = nf_filter_3024(persons_batch, orgs)

        if not persons_3024:
            job.error = "Filter 3024 ergibt 0 Personen."
            job.phase = "Fehler"
            job.percent = 100
            return

        # Kandidaten kombinieren
        merged = nf_merge_persons_orgs(persons_3024, orgs)

        # ---------------------------------------------------------
        # PHASE 6 – Ausschlussregeln
        # ---------------------------------------------------------
        job.phase = "Wende Ausschlussregeln an …"
        job.percent = 70

        selected, excluded = nf_apply_exclusions(merged)

        # ---------------------------------------------------------
        # PHASE 7 – Details laden
        # ---------------------------------------------------------
        job.phase = "Lade Details (Personen + Organisationen) …"
        job.percent = 80

        persons_full, orgs_full = await nf_load_details_for_candidates(selected)

        # ---------------------------------------------------------
        # PHASE 8 – Master bauen
        # ---------------------------------------------------------
        job.phase = "Erstelle Master-Tabelle …"
        job.percent = 87

        # Verwendung der UI-Werte!
        master_df = nf_build_master(
            selected,
            batch_id=export_batch,
            campaign=campaign
        )

        excluded_df = pd.DataFrame(excluded).replace({np.nan: None})
        if excluded_df.empty:
            excluded_df = pd.DataFrame([{
                "Kontakt ID": "-",
                "Name": "-",
                "Organisation": "-",
                "Grund": "Keine Ausschlüsse"
            }])

        # ---------------------------------------------------------
        # PHASE 9 – Excel speichern
        # ---------------------------------------------------------
        job.phase = "Schreibe Excel-Datei …"
        job.percent = 95

        file_path = await nf_export_to_excel(master_df, excluded_df, job_id)

        # ---------------------------------------------------------
        # FERTIG
        # ---------------------------------------------------------
        job.file_path = file_path
        job.phase = "Fertig"
        job.percent = 100
        return

    except Exception as e:
        job.error = str(e)
        job.phase = "Fehler"
        job.percent = 100
        print("[NF ERROR]", e)
# =============================================================
# MASTER 2025 – MODUL 7/8
# UI JOB MANAGER (Speichert Status, Fortschritt, Fehler, Datei)
# =============================================================

class NFJob:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.phase = "Initialisiere Nachfass …"
        self.percent = 0
        self.file_path = None
        self.error = None
        self.start_ts = datetime.now()

    def to_dict(self):
        return {
            "job_id": self.job_id,
            "phase": self.phase,
            "percent": self.percent,
            "file_path": self.file_path,
            "error": self.error
        }


# Alle laufenden Jobs werden hier gespeichert:
NF_JOBS: Dict[str, NFJob] = {}


def create_job() -> NFJob:
    """Erzeugt einen neuen Nachfass-Job."""
    job_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    job = NFJob(job_id)
    NF_JOBS[job_id] = job
    return job


def get_job(job_id: str) -> Optional[NFJob]:
    """Job anhand der ID abrufen."""
    return NF_JOBS.get(job_id)
# =============================================================
# MASTER 2025 – MODUL 8/8
# UI ROUTER + PREMIUM HTML TEMPLATE
# =============================================================

ui_router = APIRouter()


# -------------------------------------------------------------
# STARTSEITE (Premium UI)
# -------------------------------------------------------------
@ui_router.get("/", response_class=HTMLResponse)
async def ui_home(request: Request):
    return await render_premium_ui()


# -------------------------------------------------------------
# UI: Nachfass starten (POST)
# -------------------------------------------------------------
@ui_router.post("/ui/nachfass/start")
async def ui_nf_start(request: Request, background: BackgroundTasks):
    data = await request.json()

    batch_ids = data.get("batch_ids", "")
    export_batch = data.get("export_batch", "")
    campaign = data.get("campaign", "")

    job = create_job()
    job.phase = "Starte Nachfass …"
    job.percent = 1

    background.add_task(
        run_nf_pipeline_background,
        job.job_id,
        batch_ids,
        export_batch,
        campaign
    )

    return {"job_id": job.job_id}


# -------------------------------------------------------------
# UI: Job Status
# -------------------------------------------------------------
@ui_router.get("/ui/nachfass/status")
async def ui_nf_status(job_id: str):
    job = get_job(job_id)
    if not job:
        return JSONResponse({"error": "Job nicht gefunden"}, status_code=404)
    return job.to_dict()


# -------------------------------------------------------------
# UI: Datei herunterladen
# -------------------------------------------------------------
@ui_router.get("/ui/nachfass/download")
async def ui_nf_download(job_id: str):
    job = get_job(job_id)
    if not job or not job.file_path:
        raise HTTPException(400, "Datei nicht verfügbar")

    filename = os.path.basename(job.file_path)
    return FileResponse(
        job.file_path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
# -------------------------------------------------------------
# PREMIUM HTML (mit Eingabefeldern & AJAX)
# -------------------------------------------------------------
async def render_premium_ui():
    return HTMLResponse(
        content="""
<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8" />
<title>Nachfass Export – Premium UI</title>

<style>
    body {
        margin: 0;
        padding: 0;
        font-family: Inter, Arial, sans-serif;
        background: #f1f3f6;
        color: #222;
    }
    .container {
        max-width: 780px;
        margin: 80px auto;
        background: #fff;
        padding: 40px 50px;
        border-radius: 16px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.10);
    }
    h1 {
        margin-top: 0;
        font-size: 28px;
    }
    p {
        font-size: 15px;
        color: #555;
        margin-bottom: 25px;
    }
    label {
        display: block;
        margin-top: 20px;
        margin-bottom: 6px;
        font-weight: 600;
    }
    input {
        width: 100%;
        padding: 12px 14px;
        font-size: 15px;
        border-radius: 10px;
        border: 1px solid #ddd;
        margin-bottom: 10px;
    }
    .btn {
        background: #0078ff;
        color: #fff;
        padding: 14px 24px;
        border-radius: 10px;
        font-size: 16px;
        cursor: pointer;
        border: none;
        outline: none;
        transition: 0.2s ease;
        margin-top: 20px;
    }
    .btn:hover {
        background: #005fcc;
    }
    .hidden { display: none; }
    .status-box {
        background: #f9fafb;
        border-left: 4px solid #0078ff;
        padding: 15px 20px;
        border-radius: 8px;
        margin-top: 25px;
        font-size: 15px;
    }
    .progress {
        margin-top: 20px;
        width: 100%;
        background: #eaecef;
        border-radius: 50px;
        height: 14px;
        overflow: hidden;
    }
    .progress-inner {
        height: 100%;
        width: 0%;
        background: #0078ff;
        transition: width 0.3s ease;
    }
    .download-btn {
        margin-top: 25px;
        padding: 14px 26px;
        background: #28a745;
        border-radius: 10px;
        color: white;
        font-size: 16px;
        text-decoration: none;
        display: inline-block;
    }
    .download-btn:hover { background: #1e8f39; }
    .error {
        background: #ffe5e5;
        border-left: 4px solid #ff4444;
        padding: 15px;
        margin-top: 25px;
        border-radius: 8px;
        color: #a40000;
    }
</style>

</head>
<body>

<div class="container">
    <h1>Nachfass Export</h1>
    <p>Bitte Batch-IDs, Export-Batch und Kampagne eingeben und dann den Export starten.</p>

    <!-- FORMULARFELDER -->
    <label for="batchList">Batch-IDs (Liste):</label>
    <input id="batchList" type="text" placeholder="z.B. B443,B448,B449" />

    <label for="exportBatch">Export-Batch-ID:</label>
    <input id="exportBatch" type="text" placeholder="z.B. B000" />

    <label for="campaign">Kampagne:</label>
    <input id="campaign" type="text" placeholder="z.B. Test-Kampagne Q1/25" />

    <button class="btn" id="startBtn" onclick="startJob()">🚀 Nachfass starten</button>

    <div id="statusBox" class="status-box hidden">
        <strong>Status:</strong> <span id="phaseText">–</span>
        <div class="progress">
            <div class="progress-inner" id="progBar"></div>
        </div>
    </div>

    <div id="downloadBox" class="hidden">
        <a id="downloadLink" class="download-btn" href="#">⬇️ Export herunterladen</a>
    </div>

    <div id="errorBox" class="error hidden"></div>
</div>


<script>
let jobId = null;
let pollInterval = null;

function startJob() {
    document.getElementById("startBtn").disabled = true;

    const payload = {
        batch_ids: document.getElementById("batchList").value,
        export_batch: document.getElementById("exportBatch").value,
        campaign: document.getElementById("campaign").value
    };

    fetch("/ui/nachfass/start", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(payload)
    })
    .then(r => r.json())
    .then(data => {
        jobId = data.job_id;

        document.getElementById("statusBox").classList.remove("hidden");
        pollInterval = setInterval(checkStatus, 1200);
    })
    .catch(err => {
        showError("Konnte Job nicht starten: " + err);
    });
}

function checkStatus() {
    fetch(`/ui/nachfass/status?job_id=${jobId}`)
    .then(r => r.json())
    .then(data => {
        if (data.error) {
            showError(data.error);
            clearInterval(pollInterval);
            return;
        }

        document.getElementById("phaseText").innerText = data.phase;
        document.getElementById("progBar").style.width = data.percent + "%";

        if (data.percent >= 100) {
            clearInterval(pollInterval);

            if (data.file_path) {
                document.getElementById("downloadBox").classList.remove("hidden");
                document.getElementById("downloadLink").href = `/ui/nachfass/download?job_id=${jobId}`;
            } else {
                showError("Fertig, aber keine Datei gefunden!");
            }
        }
    })
    .catch(err => {
        showError("Status-Abfrage fehlgeschlagen: " + err);
    });
}

function showError(msg) {
    const box = document.getElementById("errorBox");
    box.classList.remove("hidden");
    box.innerText = msg;
}
</script>

</body>
</html>
        """
    )
# ============================================================
# UI ENDPOINT (HTML)
# ============================================================

@ui_router.get("/", response_class=HTMLResponse)
async def ui_page():
    return """
    <html>
       <head>...</head>
       <body> ... </body>
    </html>
    """

# ============================================================
# FASTAPI APP BOOTSTRAP
# ============================================================

from fastapi import FastAPI

app = FastAPI()

# Router registrieren
app.include_router(ui_router)

