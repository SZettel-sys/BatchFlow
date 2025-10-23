import os
import re
import asyncio
from typing import Optional

import httpx
import asyncpg
import pandas as pd
import numpy as np
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ========= Konfiguration =========
PD_API_TOKEN = os.getenv("PD_API_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")  # Format: postgresql://... ?sslmode=require

if not PD_API_TOKEN:
    raise ValueError("PD_API_TOKEN fehlt")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL fehlt")

PIPEDRIVE_API = "https://api.pipedrive.com/v1"
FILTER_NEUKONTAKTE = 2998

FIELD_NAME_FACHBEREICH = "Fachbereich_Kampagne"
FIELD_NAME_BATCH       = "Batch ID"
FIELD_NAME_CHANNEL     = "Channel"
FIELD_NAME_ORGART      = "Organisationsart"
CHANNEL_VALUE          = "Cold-Mail"

# ========= FastAPI & Assets =========
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ========= Utils =========
def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

async def get_conn():
    return await asyncpg.connect(DATABASE_URL)

async def save_df(df: pd.DataFrame, table: str):
    """Speichert DataFrame als TEXT-Spalten in Neon (einfach & robust)."""
    conn = await get_conn()
    try:
        await conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        if df.empty:
            await conn.execute(f'CREATE TABLE "{table}" ("_empty" TEXT)')
            return
        cols = ", ".join([f'"{c}" TEXT' for c in df.columns])
        await conn.execute(f'CREATE TABLE "{table}" ({cols})')
        cols_list = list(df.columns)
        placeholders = ", ".join(f"${i+1}" for i in range(len(cols_list)))
        stmt = f'INSERT INTO "{table}" ({", ".join([f"""\"{c}\"""" for c in cols_list])}) VALUES ({placeholders})'
        records = [tuple("" if pd.isna(row[c]) else str(row[c]) for c in cols_list) for _, row in df.iterrows()]
        if records:
            await conn.executemany(stmt, records)
    finally:
        await conn.close()

def extract_email(value):
    """Robust: akzeptiert list/tuple/dict/np.array/NaN/str."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, dict):
        return value.get("value") or None
    if isinstance(value, (list, tuple)):
        if len(value) == 0:
            return None
        first = value[0]
        if isinstance(first, dict):
            return first.get("value") or None
        return str(first) if first is not None else None
    return str(value) if value is not None else None

def to_int(v, default=0) -> int:
    try:
        if v is None: 
            return default
        s = str(v).strip()
        return int(s) if s != "" else default
    except:
        return default

# ========= Pipedrive =========
async def get_person_fields():
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

def extract_unique_options_from_persons(persons, field_key):
    uniq, seen = [], set()
    for p in persons:
        val = p.get(field_key)
        s = str(val).strip() if val is not None else ""
        if s and s not in seen:
            seen.add(s)
            uniq.append({"id": s, "label": s})
    uniq.sort(key=lambda x: x["label"].lower())
    return uniq

async def update_person(person_id: int, payload: dict):
    url = f"{PIPEDRIVE_API}/persons/{person_id}?api_token={PD_API_TOKEN}"
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.put(url, json=payload)
    return r.status_code

# ========= Cleanup Regeln =========
def apply_basic_cleanup(df: pd.DataFrame, key_orgart: str) -> pd.DataFrame:
    # 1) Organisationsart != leer -> verwerfen
    if key_orgart in df.columns:
        df = df[df[key_orgart].isna() | (df[key_orgart] == "")]
    # 2) max. 2 Kontakte je Organisation
    org_col = "org_id" if "org_id" in df.columns else ("org_name" if "org_name" in df.columns else None)
    if org_col and org_col in df.columns:
        df["_rank"] = df.groupby(org_col).cumcount() + 1
        df = df[df["_rank"] <= 2].drop(columns=["_rank"], errors="ignore")
    return df

# ========= Redirects =========
@app.get("/", response_class=HTMLResponse)
async def root_redirect():
    return HTMLResponse('<meta http-equiv="refresh" content="0;url=/neukontakte">')

@app.get("/overview", response_class=HTMLResponse)
async def old_redirect():
    return HTMLResponse('<meta http-equiv="refresh" content="0;url=/neukontakte">')

# ========= Form (Design) =========
@app.get("/neukontakte", response_class=HTMLResponse)
async def neukontakte_form(request: Request):
    fields = await get_person_fields()

    wanted_norm = _norm(FIELD_NAME_FACHBEREICH)
    fach = fields.get(wanted_norm) or fields.get(FIELD_NAME_FACHBEREICH) or fields.get("Fachbereich – Kampagne")
    if not fach:
        return HTMLResponse(
            "<div class='error'>❌ Feld „Fachbereich_Kampagne“ nicht gefunden. Bitte prüfen.</div>",
            status_code=500
        )

    persons = await fetch_persons_by_filter(FILTER_NEUKONTAKTE)
    options = fach.get("options") or extract_unique_options_from_persons(persons, fach["key"])

    counts = {}
    for p in persons:
        val = p.get(fach["key"])
        s = str(val).strip() if val is not None else ""
        if s:
            counts[s] = counts.get(s, 0) + 1

    enriched = []
    for opt in options:
        oid, label = str(opt["id"]), opt["label"]
        c = counts.get(oid, counts.get(label, 0))
        enriched.append({"id": opt["id"], "label": label, "count": c or 0})

    total_count = sum(counts.values())

    return templates.TemplateResponse(
        "neukontakte_form.html",
        {"request": request, "options": enriched, "total": total_count}
    )

# ========= Vorschau =========
@app.post("/neukontakte/preview", response_class=HTMLResponse)
async def neukontakte_preview(
    request: Request,
    fachbereich_value: str = Form(...),
    batch_id: str = Form(...),
    take_count: Optional[str] = Form(None)   # <-- robust gegen leer/fehlend
):
    try:
        take_n = to_int(take_count, 0)

        fields = await get_person_fields()
        wanted_norm = _norm(FIELD_NAME_FACHBEREICH)
        fach = fields.get(wanted_norm) or fields.get(FIELD_NAME_FACHBEREICH) or fields.get("Fachbereich – Kampagne")
        if not fach:
            return HTMLResponse("<div class='error'>❌ Feld „Fachbereich_Kampagne“ nicht gefunden.</div>", status_code=500)

        key_fach   = fach["key"]
        key_chan   = (fields.get(_norm(FIELD_NAME_CHANNEL)) or fields.get(FIELD_NAME_CHANNEL) or {}).get("key")

        persons = await fetch_persons_by_filter(FILTER_NEUKONTAKTE)
        filtered = [p for p in persons if key_fach in p and (str(p[key_fach]) == str(fachbereich_value))]
        if take_n and int(take_n) > 0:
            filtered = filtered[: int(take_n)]

        df = pd.DataFrame(filtered)
        if df.empty:
            return HTMLResponse("<div class='card'><h3>Keine Datensätze für diese Auswahl.</h3></div>")

        if "email" in df.columns:
            df["E-Mail"] = df["email"].apply(extract_email)
        if "org_name" in df.columns:
            df.rename(columns={"org_name": "Organisation"}, inplace=True)
        df["Batch ID (Vorschau)"] = batch_id
        if key_chan:
            df["Channel (Vorschau)"] = CHANNEL_VALUE

        await save_df(df, "nk_master")

        cols = [c for c in ["id", "name", "E-Mail", "Organisation", "Batch ID (Vorschau)"] if c in df.columns]
        preview_df = df[cols] if cols else df.head(50)
        html_table = preview_df.head(50).to_html(classes="grid", index=False, border=0)

        return templates.TemplateResponse(
            "neukontakte_preview.html",
            {
                "request": request,
                "table": html_table,
                "count": len(df),
                "fachbereich_value": fachbereich_value,
                "batch_id": batch_id,
                "take_count": take_n,
            },
        )
    except Exception as e:
        return HTMLResponse(f"<div class='error'><h3>❌ Fehler beim Abruf/Speichern:</h3><pre>{e}</pre></div>", status_code=500)

# ========= Run / Schreiben & Cleanup =========
@app.post("/neukontakte/run", response_class=HTMLResponse)
async def neukontakte_run(
    request: Request,
    fachbereich_value: str = Form(...),
    batch_id: str = Form(...),
    take_count: Optional[str] = Form(None)  # <-- robust gegen leer/fehlend
):
    try:
        take_n = to_int(take_count, 0)

        fields = await get_person_fields()
        wanted_norm = _norm(FIELD_NAME_FACHBEREICH)
        fach = fields.get(wanted_norm) or fields.get(FIELD_NAME_FACHBEREICH) or fields.get("Fachbereich – Kampagne")
        if not fach:
            return HTMLResponse("<div class='error'>❌ Feld „Fachbereich_Kampagne“ nicht gefunden.</div>", status_code=500)

        key_fach   = fach["key"]
        key_batch  = (fields.get(_norm(FIELD_NAME_BATCH))   or fields.get(FIELD_NAME_BATCH)   or {}).get("key")
        key_chan   = (fields.get(_norm(FIELD_NAME_CHANNEL)) or fields.get(FIELD_NAME_CHANNEL) or {}).get("key")
        key_orgart = (fields.get(_norm(FIELD_NAME_ORGART))  or fields.get(FIELD_NAME_ORGART)  or {}).get("key")

        persons = await fetch_persons_by_filter(FILTER_NEUKONTAKTE)
        data = [p for p in persons if key_fach in p and (str(p[key_fach]) == str(fachbereich_value))]
        if take_n and int(take_n) > 0:
            data = data[: int(take_n)]

        if not data:
            return HTMLResponse("<div class='card'><h3>Keine Datensätze für diese Auswahl.</h3></div>")

        async def _upd(p):
            payload = {}
            if key_batch: payload[key_batch] = batch_id
            if key_chan:  payload[key_chan]  = CHANNEL_VALUE
            return await update_person(p["id"], payload)

        sem = asyncio.Semaphore(8)
        async def guarded(p):
            async with sem:
                return await _upd(p)

        results = await asyncio.gather(*[guarded(p) for p in data])
        updated = sum(1 for s in results if s == 200)

        df = pd.DataFrame(data)
        if "email" in df.columns:
            df["E-Mail"] = df["email"].apply(extract_email)
        if "org_name" in df.columns:
            df.rename(columns={"org_name": "Organisation"}, inplace=True)
        if key_orgart and key_orgart in df.columns:
            df.rename(columns={key_orgart: "Organisationsart"}, inplace=True)

        df = apply_basic_cleanup(df, "Organisationsart")
        await save_df(df, "nk_master_final")

        html = f"""
        <div class="card">
          <div class="result-bar">
            <div><b>✅ Abgleich abgeschlossen</b></div>
            <div class="stats">
              <span>Fachbereich: <b>{fachbereich_value}</b></span>
              <span>Batch ID: <b>{batch_id}</b></span>
              <span>Ausgewählt: <b>{len(data)}</b></span>
              <span>Aktualisiert: <b>{updated}</b></span>
              <span>Nach Cleanup: <b>{len(df)}</b></span>
            </div>
          </div>
          <a class="btn" href="/neukontakte">↩︎ Zurück</a>
        </div>
        """
        return HTMLResponse(html)

    except Exception as e:
        return HTMLResponse(f"<div class='error'><h3>❌ Fehler beim Abgleich:</h3><pre>{e}</pre></div>", status_code=500)
