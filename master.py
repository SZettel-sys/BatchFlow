# master.py – Abschnitt 1 "Neukontakte" (Filter 2998)
# Stack: FastAPI + httpx + asyncpg + pandas

import os
import re
import asyncio
import httpx
import asyncpg
import pandas as pd
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

load_dotenv()

# ====== ENV ======
PD_API_TOKEN = os.getenv("PD_API_TOKEN")
DATABASE_URL  = os.getenv("DATABASE_URL")  # postgresql://... ?sslmode=require

if not PD_API_TOKEN:
    raise ValueError("PD_API_TOKEN fehlt")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL fehlt")

# ====== Konstanten ======
PIPEDRIVE_API = "https://api.pipedrive.com/v1"
FILTER_NEUKONTAKTE = 2998

FIELD_NAME_FACHBEREICH = "Fachbereich_Kampagne"
FIELD_NAME_BATCH       = "Batch ID"
FIELD_NAME_CHANNEL     = "Channel"
FIELD_NAME_ORGART      = "Organisationsart"
CHANNEL_VALUE          = "Cold-Mail"

# ====== FastAPI / Assets ======
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# ====== Utils ======
def _norm(s: str) -> str:
    """Robuste Feld-Namens-Normalisierung (bindestriche/unterstriche/Leerzeichen egal)."""
    return re.sub(r'[^a-z0-9]+', '', (s or '').lower())

async def get_conn():
    return await asyncpg.connect(DATABASE_URL)

async def save_df(df: pd.DataFrame, table: str):
    """Speichert DataFrame als einfache TEXT-Tabelle in Neon."""
    conn = await get_conn()
    try:
        await conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        cols = ", ".join([f'"{c}" TEXT' for c in df.columns])
        await conn.execute(f'CREATE TABLE "{table}" ({cols})')
        if df.empty:
            return
        cols_list = list(df.columns)
        placeholders = ", ".join(f"${i+1}" for i in range(len(cols_list)))
        stmt = f'INSERT INTO "{table}" ({", ".join([f"""\"{c}\"""" for c in cols_list])}) VALUES ({placeholders})'
        records = [tuple("" if pd.isna(row[c]) else str(row[c]) for c in cols_list) for _, row in df.iterrows()]
        await conn.executemany(stmt, records)
    finally:
        await conn.close()


# ====== Pipedrive Helpers ======
async def get_person_fields():
    """Liefert Mapping für Original- und normalisierte Feldnamen -> {key, options, name}."""
    url = f"{PIPEDRIVE_API}/personFields?api_token={PD_API_TOKEN}"
    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json().get("data") or []

    mapping = {}
    for f in data:
        name = f.get("name")
        key = f.get("key")
        options = f.get("options") or []
        mapping[name] = {"key": key, "options": options, "name": name}
        mapping[_norm(name)] = {"key": key, "options": options, "name": name}
    return mapping

async def fetch_persons_by_filter(filter_id: int):
    persons, start, limit = [], 0, 500
    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            url = f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={limit}&api_token={PD_API_TOKEN}"
            r = await client.get(url)
            if r.status_code != 200:
                raise Exception(f"Pipedrive API Fehler: {r.text}")
            chunk = r.json().get("data") or []
            if not chunk:
                break
            persons.extend(chunk)
            if len(chunk) < limit:
                break
            start += limit
    return persons

def extract_email(value):
    if isinstance(value, list) and value:
        return value[0].get("value")
    return None

def extract_unique_options_from_persons(persons, field_key):
    """Wenn Feld kein Enum ist: distinct Werte aus Personen (String) als Dropdown erzeugen."""
    uniq, seen = [], set()
    for p in persons:
        val = p.get(field_key)
        if isinstance(val, list):
            for v in val:
                s = str(v).strip()
                if s and s not in seen:
                    seen.add(s); uniq.append({"id": s, "label": s})
        else:
            s = str(val).strip() if val is not None else ""
            if s and s not in seen:
                seen.add(s); uniq.append({"id": s, "label": s})
    uniq.sort(key=lambda x: x["label"].lower())
    return uniq

async def update_person(person_id: int, payload: dict):
    url = f"{PIPEDRIVE_API}/persons/{person_id}?api_token={PD_API_TOKEN}"
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.put(url, json=payload)
    return r.status_code, r.text


# ====== Bereinigung: Orgart leer + max 2 Kontakte pro Org ======
def apply_basic_cleanup(df: pd.DataFrame, key_orgart: str) -> pd.DataFrame:
    # 1) Organisationsart != leer -> verwerfen
    if key_orgart in df.columns:
        df = df[df[key_orgart].isna() | (df[key_orgart] == "")]
    # 2) max. 2 Kontakte pro Organisation
    org_col = "org_id" if "org_id" in df.columns else ("org_name" if "org_name" in df.columns else None)
    if org_col and org_col in df.columns:
        df["_rank"] = df.groupby(org_col).cumcount() + 1
        df = df[df["_rank"] <= 2].drop(columns=["_rank"], errors="ignore")
    return df


# ====== Routes ======
@app.get("/", response_class=HTMLResponse)
async def root_redirect():
    return HTMLResponse('<meta http-equiv="refresh" content="0;url=/neukontakte">')

@app.get("/overview", response_class=HTMLResponse)
async def old_redirect():
    return HTMLResponse('<meta http-equiv="refresh" content="0;url=/neukontakte">')


@app.get("/neukontakte", response_class=HTMLResponse)
async def neukontakte_form(request: Request):
    # Feldoptionen für Fachbereich laden (per /personFields)
    fields = await get_person_fields()

    # Feld robust finden (egal ob "_" / " – " / Leerzeichen)
    wanted_norm = _norm(FIELD_NAME_FACHBEREICH)
    fach = fields.get(wanted_norm) or fields.get(FIELD_NAME_FACHBEREICH) or fields.get("Fachbereich – Kampagne")

    if not fach:
        return HTMLResponse(
            "<h3 style='color:red'>❌ Feld „Fachbereich_Kampagne“ nicht gefunden. Bitte Feldbezeichnung in Pipedrive prüfen.</h3>",
            status_code=500
        )

    options = fach.get("options") or []
    if not options:
        # dynamisch aus Filter 2998 extrahieren
        persons = await fetch_persons_by_filter(FILTER_NEUKONTAKTE)
        options = extract_unique_options_from_persons(persons, fach["key"])

    return templates.TemplateResponse(
        "neukontakte_form.html",
        {"request": request, "options": options}
    )


@app.post("/neukontakte/preview", response_class=HTMLResponse)
async def neukontakte_preview(
    request: Request,
    fachbereich_value: str = Form(...),
    batch_id: str = Form(...)
):
    try:
        fields = await get_person_fields()

        # Feld-Keys ermitteln (robust)
        wanted_norm = _norm(FIELD_NAME_FACHBEREICH)
        fach_meta = fields.get(wanted_norm) or fields.get(FIELD_NAME_FACHBEREICH) or fields.get("Fachbereich – Kampagne")
        if not fach_meta:
            return HTMLResponse("<h3 style='color:red'>❌ Feld „Fachbereich_Kampagne“ nicht gefunden.</h3>", status_code=500)

        key_fach   = fach_meta["key"]
        key_batch  = (fields.get(_norm(FIELD_NAME_BATCH)) or fields.get(FIELD_NAME_BATCH) or {}).get("key")
        key_chan   = (fields.get(_norm(FIELD_NAME_CHANNEL)) or fields.get(FIELD_NAME_CHANNEL) or {}).get("key")
        key_orgart = (fields.get(_norm(FIELD_NAME_ORGART))  or fields.get(FIELD_NAME_ORGART)  or {}).get("key")

        # Personen aus Filter 2998 laden und clientseitig filtern
        persons = await fetch_persons_by_filter(FILTER_NEUKONTAKTE)
        filtered = [p for p in persons if key_fach in p and (str(p[key_fach]) == str(fachbereich_value))]

        df = pd.DataFrame(filtered)
        if df.empty:
            return HTMLResponse("<h3>Keine Datensätze für diesen Fachbereich.</h3>")

        # Vorschau-Felder
        if "email" in df.columns:
            df["E-Mail"] = df["email"].apply(extract_email)
        if "org_name" in df.columns:
            df.rename(columns={"org_name": "Organisation"}, inplace=True)
        df["Batch ID (Vorschau)"] = batch_id
        if key_chan:
            df["Channel (Vorschau)"] = CHANNEL_VALUE

        # Rohdaten sichern
        await save_df(df, "nk_master")

        cols = [c for c in ["id", "name", "E-Mail", "Organisation", "Batch ID (Vorschau)"] if c in df.columns]
        preview_df = df[cols] if cols else df.head(50)
        html_table = preview_df.head(50).to_html(classes="table table-striped", index=False)

        return templates.TemplateResponse(
            "neukontakte_preview.html",
            {
                "request": request,
                "table": html_table,
                "count": len(df),
                "fachbereich_value": fachbereich_value,
                "batch_id": batch_id,
            },
        )
    except Exception as e:
        return HTMLResponse(f"<h3 style='color:red;'>❌ Fehler beim Abruf/Speichern:</h3><pre>{e}</pre>", status_code=500)


@app.post("/neukontakte/run", response_class=HTMLResponse)
async def neukontakte_run(
    request: Request,
    fachbereich_value: str = Form(...),
    batch_id: str = Form(...)
):
    try:
        fields = await get_person_fields()
        wanted_norm = _norm(FIELD_NAME_FACHBEREICH)
        fach_meta = fields.get(wanted_norm) or fields.get(FIELD_NAME_FACHBEREICH) or fields.get("Fachbereich – Kampagne")
        if not fach_meta:
            return HTMLResponse("<h3 style='color:red'>❌ Feld „Fachbereich_Kampagne“ nicht gefunden.</h3>", status_code=500)

        key_fach   = fach_meta["key"]
        key_batch  = (fields.get(_norm(FIELD_NAME_BATCH)) or fields.get(FIELD_NAME_BATCH) or {}).get("key")
        key_chan   = (fields.get(_norm(FIELD_NAME_CHANNEL)) or fields.get(FIELD_NAME_CHANNEL) or {}).get("key")
        key_orgart = (fields.get(_norm(FIELD_NAME_ORGART))  or fields.get(FIELD_NAME_ORGART)  or {}).get("key")

        persons = await fetch_persons_by_filter(FILTER_NEUKONTAKTE)
        data = [p for p in persons if key_fach in p and (str(p[key_fach]) == str(fachbereich_value))]
        if not data:
            return HTMLResponse("<h3>Keine Datensätze für diesen Fachbereich.</h3>")

        # Pipedrive-Updates (Batch, Channel)
        async def _upd(p):
            payload = {}
            if key_batch:
                payload[key_batch] = batch_id
            if key_chan:
                payload[key_chan] = CHANNEL_VALUE
            status, _txt = await update_person(p["id"], payload)
            return status

        sem = asyncio.Semaphore(8)  # moderates parallelisieren
        async def guarded(p):
            async with sem:
                return await _upd(p)

        results = await asyncio.gather(*[guarded(p) for p in data])
        updated = sum(1 for s in results if s == 200)

        # DataFrame + Bereinigung
        df = pd.DataFrame(data)
        if "email" in df.columns:
            df["E-Mail"] = df["email"].apply(extract_email)
        if "org_name" in df.columns:
            df.rename(columns={"org_name": "Organisation"}, inplace=True)
        # key_orgart in Klartextspalte für Cleanup
        if key_orgart and key_orgart in df.columns:
            df.rename(columns={key_orgart: "Organisationsart"}, inplace=True)

        df = apply_basic_cleanup(df, "Organisationsart")

        await save_df(df, "nk_master_final")

        info_html = f"""
        <h2>✅ Abgleich abgeschlossen</h2>
        <ul>
          <li>Fachbereich: <b>{fachbereich_value}</b></li>
          <li>Batch ID gesetzt: <b>{batch_id}</b></li>
          <li>Personen geladen: <b>{len(data)}</b></li>
          <li>In Pipedrive aktualisiert: <b>{updated}</b></li>
          <li>Nach Regeln behalten: <b>{len(df)}</b> (Organisationsart leer, max. 2 Kontakte/Organisation)</li>
          <li>Neon Tabellen: <code>nk_master</code> (Roh) & <code>nk_master_final</code> (final)</li>
        </ul>
        <a href="/neukontakte">↩️ Zurück</a>
        """
        return HTMLResponse(info_html)

    except Exception as e:
        return HTMLResponse(f"<h3 style='color:red;'>❌ Fehler beim Abgleich:</h3><pre>{e}</pre>", status_code=500)
