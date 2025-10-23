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
        # fix: Spaltenliste sauber ohne verschachtelte f-Strings bauen
        cols_sql = ", ".join(f'"{c}"' for c in cols_list)
        stmt = f'INSERT INTO "{table}" ({cols_sql}) VALUES ({placeholders})'
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
        # Pipedrive liefert oft [{"label":"work","value":"...","primary":true}]
        first = value[0]
        if isinstance(first, dict):
            return first.get("value") or None
        return str(first)
    return str(value)

async def get_persons_by_filter(filter_id: int, limit: int = 500):
    start = 0
    all_items = []
    async with httpx.AsyncClient(timeout=45.0) as client:
        while True:
            url = f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={limit}&api_token={PD_API_TOKEN}"
            r = await client.get(url)
            if r.status_code != 200:
                raise RuntimeError(f"Pipedrive Fehler: {r.text}")
            data = r.json().get("data") or []
            if not data:
                break
            all_items.extend(data)
            if len(data) < limit:
                break
            start += limit
    return all_items

async def get_person_fields():
    async with httpx.AsyncClient(timeout=30.0) as client:
        url = f"{PIPEDRIVE_API}/personFields?api_token={PD_API_TOKEN}"
        r = await client.get(url)
        r.raise_for_status()
        return r.json().get("data") or []

def detect_keys(fields, *labels):
    """Findet Pipedrive-Feldschlüssel anhand von Labels (Fallback: Name)."""
    res = {}
    for label in labels:
        norm = _norm(label)
        for f in fields:
            # bevorzugt exakte Label-Übereinstimmung
            if _norm(f.get("name") or "") == norm:
                res[label] = f.get("key")
                break
        else:
            # notfalls 'name' enthält ...
            for f in fields:
                if norm in _norm(f.get("name") or ""):
                    res[label] = f.get("key")
                    break
    return res

def build_fachbereich_options(persons, key_fachbereich: str):
    raw = []
    for p in persons:
        v = p.get(key_fachbereich)
        if v is None:
            continue
        if isinstance(v, list):
            for x in v:
                if x:
                    raw.append(x)
        else:
            raw.append(v)
    # aufbereiten & zählen
    counts = {}
    for x in raw:
        s = str(x).strip()
        if not s:
            continue
        counts[s] = counts.get(s, 0) + 1
    # Option-Liste
    opts = [{"value": k, "label": k, "count": v} for k, v in sorted(counts.items(), key=lambda x: x[1], reverse=True)]
    return opts

def to_select(data: list[str]):
    """Hilfsformat für <select> (Template)."""
    seen = set()
    uniq = []
    for s in data:
        s = str(s).strip()
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
async def overview_redirect():
    return HTMLResponse('<meta http-equiv="refresh" content="0;url=/neukontakte">')

# ========= UI: Form =========
@app.get("/neukontakte", response_class=HTMLResponse)
async def neukontakte(request: Request):
    persons = await get_persons_by_filter(FILTER_NEUKONTAKTE)
    fields = await get_person_fields()
    keys = detect_keys(fields, FIELD_NAME_FACHBEREICH, FIELD_NAME_BATCH, FIELD_NAME_CHANNEL, FIELD_NAME_ORGART)
    key_fb   = keys.get(FIELD_NAME_FACHBEREICH)
    opts = build_fachbereich_options(persons, key_fb) if key_fb else []

    return templates.TemplateResponse(
        "neukontakte.html",
        {
            "request": request,
            "total": len(persons),
            "options": opts,
            "filter_id": FILTER_NEUKONTAKTE,
        },
    )

# ========= Preview =========
@app.post("/neukontakte/preview", response_class=HTMLResponse)
async def neukontakte_preview(
    request: Request,
    fachbereich_value: str = Form(...),
    batch_id: str = Form(...),
    take_count: Optional[str] = Form(None)   # <-- robust gegen leer/fehlen
):
    try:
        persons = await get_persons_by_filter(FILTER_NEUKONTAKTE)
        fields = await get_person_fields()
        keys = detect_keys(fields, FIELD_NAME_FACHBEREICH, FIELD_NAME_BATCH, FIELD_NAME_CHANNEL, FIELD_NAME_ORGART)
        key_fb   = keys.get(FIELD_NAME_FACHBEREICH)
        key_batch= keys.get(FIELD_NAME_BATCH)
        key_chan = keys.get(FIELD_NAME_CHANNEL)

        # filtern auf Fachbereich
        def in_fb(p):
            v = p.get(key_fb)
            if isinstance(v, list):
                return any(str(x) == fachbereich_value for x in v if x is not None)
            return str(v) == fachbereich_value

        data = [p for p in persons if in_fb(p)] if key_fb else persons
        # begrenzen
        take_n = None
        if take_count:
            try:
                take_n = max(1, int(take_count))
            except Exception:
                take_n = None
        if take_n:
            data = data[:take_n]

        # DataFrame vorbereiten
        df = pd.json_normalize(data)
        # Feldmapping (Batch/Channel fürs UI nur als Vorschau)
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
    take_count: Optional[str] = Form(None)  # <-- robust gegen leer/fehlen
):
    try:
        # neu ziehen (keine DB-Abhängigkeit)
        persons = await get_persons_by_filter(FILTER_NEUKONTAKTE)
        fields = await get_person_fields()
        keys = detect_keys(fields, FIELD_NAME_FACHBEREICH, FIELD_NAME_BATCH, FIELD_NAME_CHANNEL, FIELD_NAME_ORGART)
        key_fb   = keys.get(FIELD_NAME_FACHBEREICH)
        key_batch= keys.get(FIELD_NAME_BATCH)
        key_chan = keys.get(FIELD_NAME_CHANNEL)
        key_org  = keys.get(FIELD_NAME_ORGART)

        # filtern & begrenzen (wie Preview)
        def in_fb(p):
            v = p.get(key_fb)
            if isinstance(v, list):
                return any(str(x) == fachbereich_value for x in v if x is not None)
            return str(v) == fachbereich_value
        data = [p for p in persons if in_fb(p)] if key_fb else persons

        # limit
        take_n = None
        if take_count:
            try:
                take_n = max(1, int(take_count))
            except Exception:
                take_n = None
        if take_n:
            data = data[:take_n]

        # Batch & Channel ins Live-System schreiben
        updated = 0
        if key_batch or key_chan:
            # Parallelisierung in moderater Breite
            sem = asyncio.Semaphore(8)
            async def do_update(p):
                nonlocal updated
                pid = p.get("id")
                if not pid:
                    return
                payload = {}
                if key_batch:
                    payload[key_batch] = batch_id
                if key_chan:
                    payload[key_chan] = CHANNEL_VALUE
                async with sem:
                    status = await update_person(pid, payload)
                if status in (200, 201, 204):
                    updated += 1

            await asyncio.gather(*(do_update(p) for p in data))

        # df bauen und Cleanup anwenden (Orga-Art leer, max.2/Orga)
        df = pd.json_normalize(data)
        if "email" in df.columns:
            df["E-Mail"] = df["email"].apply(extract_email)
        if "org_name" in df.columns:
            df.rename(columns={"org_name": "Organisation"}, inplace=True)
        df["Batch ID"] = batch_id
        df["Channel"]  = CHANNEL_VALUE
        if key_org:
            df[key_org] = df.get(key_org)
        df = apply_basic_cleanup(df, key_org or FIELD_NAME_ORGART)

        # final speichern (für Export/Weiterverarbeitung)
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

# ========= Catch-all Fallback =========
@app.get("/{full_path:path}", include_in_schema=False)
async def catch_all(full_path: str):
    return HTMLResponse('<meta http-equiv="refresh" content="0;url=/neukontakte">')
