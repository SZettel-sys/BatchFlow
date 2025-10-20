# master.py – Abschnitt 1 "Neukontakte" (Filter 2998)
# Stack: FastAPI + httpx + asyncpg + Pandas
# Funktionen:
#  - /neukontakte  (Form: Fachbereich + Batch-ID)
#  - /neukontakte/preview  (Vorschau)
#  - /neukontakte/run      (Abgleich + Updates in Pipedrive + Speichern nach Neon)

import os
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

PD_API_TOKEN = os.getenv("PD_API_TOKEN")
DATABASE_URL  = os.getenv("DATABASE_URL")  # ACHTUNG: postgresql://..., NICHT postgresql+asyncpg://

if not PD_API_TOKEN:
    raise ValueError("PD_API_TOKEN fehlt")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL fehlt")

PIPEDRIVE_API = "https://api.pipedrive.com/v1"
FILTER_NEUKONTAKTE = 2998
FIELD_NAME_FACHBEREICH = "Fachbereich_Kampagne"
FIELD_NAME_BATCH       = "Batch ID"
FIELD_NAME_CHANNEL     = "Channel"
FIELD_NAME_ORGART      = "Organisationsart"   # für Bereinigung (wenn Wert gesetzt -> verwerfen)
CHANNEL_VALUE          = "Cold-Mail"

# ------------------- FastAPI / Templates / Static -------------------
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ------------------- DB Helper -------------------
async def get_conn():
    return await asyncpg.connect(DATABASE_URL)

async def save_df(conn, df: pd.DataFrame, table: str):
    await conn.execute(f'DROP TABLE IF EXISTS "{table}"')
    # simple TEXT Schema
    cols = ", ".join([f'"{c}" TEXT' for c in df.columns])
    await conn.execute(f'CREATE TABLE "{table}" ({cols})')
    if df.empty:
        return
    # insert in batches
    cols_list = list(df.columns)
    placeholders = ", ".join(f"${i+1}" for i in range(len(cols_list)))
    stmt = f'INSERT INTO "{table}" ({", ".join([f"""\"{c}\"""" for c in cols_list])}) VALUES ({placeholders})'
    records = [tuple("" if pd.isna(v) else str(v)) for _, row in df.iterrows() for v in [row]]
    # records muss Liste von Tupeln sein:
    records = [tuple("" if pd.isna(row[c]) else str(row[c]) for c in cols_list) for _, row in df.iterrows()]
    await conn.executemany(stmt, records)

# ------------------- Pipedrive Helpers -------------------
async def get_person_fields():
    """Mappe Feld-NAME -> (key, options)"""
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
        mapping[name] = {"key": key, "options": options}
    return mapping

async def fetch_persons_by_filter(filter_id: int):
    persons = []
    start, limit = 0, 500
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

async def update_person(person_id: int, payload: dict):
    url = f"{PIPEDRIVE_API}/persons/{person_id}?api_token={PD_API_TOKEN}"
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.put(url, json=payload)
    return r.status_code, r.text

# ------------------- Business Logik Abschnitt 1 -------------------
def extract_email(value):
    if isinstance(value, list) and value:
        return value[0].get("value")
    return None

def apply_basic_cleanup(df: pd.DataFrame, key_orgart: str) -> pd.DataFrame:
    # 1) Organisationsart != leer -> verwerfen
    if key_orgart in df.columns:
        df = df[df[key_orgart].isna() | (df[key_orgart]=="")]

    # 2) max. 2 Kontakte pro Organisation (org_id oder org_name als Fallback)
    org_col = "org_id" if "org_id" in df.columns else "org_name"
    if org_col in df.columns:
        df["_rank"] = df.groupby(org_col).cumcount()+1
        df = df[df["_rank"] <= 2]
        df = df.drop(columns=["_rank"], errors="ignore")
    return df

# ------------------- Routes -------------------
@app.get("/neukontakte", response_class=HTMLResponse)
async def neukontakte_form(request: Request):
    # Feldoptionen für Fachbereich laden (per /personFields)
    fields = await get_person_fields()
    fach = fields.get(FIELD_NAME_FACHBEREICH, {})
    options = fach.get("options") or []  # [{id, label}, ...] wenn Auswahlfeld
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
        # Felder bestimmen
        fields = await get_person_fields()
        key_fach   = fields[FIELD_NAME_FACHBEREICH]["key"]
        key_batch  = fields[FIELD_NAME_BATCH]["key"]
        key_chan   = fields[FIELD_NAME_CHANNEL]["key"] if FIELD_NAME_CHANNEL in fields else None
        key_orgart = fields[FIELD_NAME_ORGART]["key"]  if FIELD_NAME_ORGART in fields else None

        # Personen aus Filter 2998 laden
        persons = await fetch_persons_by_filter(FILTER_NEUKONTAKTE)
        if not persons:
            return HTMLResponse("<h3>Keine Datensätze im Filter gefunden.</h3>")

        # clientseitig nach Fachbereich filtern
        filtered = []
        for p in persons:
            if key_fach in p and (p[key_fach] == fachbereich_value or str(p[key_fach]) == str(fachbereich_value)):
                filtered.append(p)

        df = pd.DataFrame(filtered)
        if df.empty:
            return HTMLResponse("<h3>Keine Datensätze für diesen Fachbereich.</h3>")

        # hübsche Vorschau-Felder vorbereiten
        df["E-Mail"] = df["email"].apply(extract_email) if "email" in df.columns else None
        if "org_name" in df.columns:
            df.rename(columns={"org_name": "Organisation"}, inplace=True)
        df["Batch ID (Vorschau)"] = batch_id
        if key_chan:
            df["Channel (Vorschau)"] = CHANNEL_VALUE

        # temporär speichern (Rohdaten)
        async with get_conn() as conn:
            await save_df(conn, df, "nk_master")

        # Vorschau rendern (erste 50)
        show_cols = [c for c in ["id", "name", "E-Mail", "Organisation", "Batch ID (Vorschau)"] if c in df.columns]
        preview_df = df[show_cols] if show_cols else df.head(50)
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
    """
    Abgleich starten:
      - alle Datensätze aus Filter 2998 & Fachbereich laden
      - Channel="Cold-Mail" (falls vorhanden) + Batch ID auf Personen schreiben
      - Bereinigung: Organisationsart != leer -> verwerfen; max. 2 Kontakte/Org
      - finale Tabelle nk_master_final speichern
    """
    try:
        fields = await get_person_fields()
        key_fach   = fields[FIELD_NAME_FACHBEREICH]["key"]
        key_batch  = fields[FIELD_NAME_BATCH]["key"]
        key_chan   = fields[FIELD_NAME_CHANNEL]["key"] if FIELD_NAME_CHANNEL in fields else None
        key_orgart = fields[FIELD_NAME_ORGART]["key"]  if FIELD_NAME_ORGART in fields else None

        persons = await fetch_persons_by_filter(FILTER_NEUKONTAKTE)

        # clientseitig filtern
        data = [p for p in persons if key_fach in p and (p[key_fach] == fachbereich_value or str(p[key_fach]) == str(fachbereich_value))]

        if not data:
            return HTMLResponse("<h3>Keine Datensätze für diesen Fachbereich.</h3>")

        # Updates zu Pipedrive vorbereiten
        async def _upd(p):
            payload = { key_batch: batch_id }
            if key_chan:
                payload[key_chan] = CHANNEL_VALUE
            status, text = await update_person(p["id"], payload)
            return status

        # parallel aber moderat (Rate-Limit)
        sem = asyncio.Semaphore(8)
        async def guarded_upd(p):
            async with sem:
                return await _upd(p)

        results = await asyncio.gather(*[guarded_upd(p) for p in data])
        updated = sum(1 for s in results if s == 200)

        # in DataFrame + Bereinigung
        df = pd.DataFrame(data)
        df["E-Mail"] = df["email"].apply(extract_email) if "email" in df.columns else None
        if "org_name" in df.columns:
            df.rename(columns={"org_name": "Organisation"}, inplace=True)
        if key_orgart:
            df.rename(columns={key_orgart: "Organisationsart"}, inplace=True)

        df = apply_basic_cleanup(df, "Organisationsart")

        # final speichern
        async with get_conn() as conn:
            await save_df(conn, df, "nk_master_final")

        info_html = f"""
        <h2>✅ Abgleich abgeschlossen</h2>
        <ul>
          <li>Ausgewählter Fachbereich: <b>{fachbereich_value}</b></li>
          <li>Batch ID geschrieben: <b>{batch_id}</b></li>
          <li>Personen geladen: <b>{len(data)}</b></li>
          <li>Personen in Pipedrive aktualisiert (Batch/Channel): <b>{updated}</b></li>
          <li>Final nach Regeln behalten: <b>{len(df)}</b> (max. 2 Kontakte/Organisation, Organisationsart leer)</li>
          <li>Gespeichert in Neon: <b>nk_master_final</b></li>
        </ul>
        <a href="/neukontakte">↩️ Zurück</a>
        """
        return HTMLResponse(info_html)

    except Exception as e:
        return HTMLResponse(f"<h3 style='color:red;'>❌ Fehler beim Abgleich:</h3><pre>{e}</pre>", status_code=500)
