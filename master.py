import os
import re
import time
import asyncio
from typing import Optional, List, Dict, Any

import httpx
import asyncpg
import pandas as pd
import numpy as np
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware

# ========= Konfiguration =========
PD_API_TOKEN = os.getenv("PD_API_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")  # postgresql://... ?sslmode=require

if not PD_API_TOKEN:
    raise ValueError("PD_API_TOKEN fehlt.")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL fehlt.")

PIPEDRIVE_API = "https://api.pipedrive.com/v1"
FILTER_NEUKONTAKTE = 2998  # erster Abschnitt

FIELD_NAME_FACHBEREICH = "Fachbereich_Kampagne"
FIELD_NAME_BATCH       = "Batch ID"
FIELD_NAME_CHANNEL     = "Channel"
FIELD_NAME_ORGART      = "Organisationsart"
CHANNEL_VALUE          = "Cold-Mail"

# ========= App / Middleware / Static =========
app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=500)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ========= HTTP-Client (Keep-Alive) & kleiner Cache =========
HTTP_TIMEOUT = 60.0
http_client = httpx.AsyncClient(timeout=HTTP_TIMEOUT)

_person_fields_cache: Dict[str, Any] = {"data": None, "ts": 0.0}
PERSON_FIELDS_TTL = 900  # 15 Minuten

_filter_cache: Dict[int, Dict[str, Any]] = {}
FILTER_TTL = 120  # 2 Minuten

# ========= Hilfsfunktionen =========
def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

def to_int(v, default=0) -> int:
    try:
        if v is None:
            return default
        s = str(v).strip()
        return int(s) if s != "" else default
    except:
        return default

def extract_email(value):
    """Robust: akzeptiert list/tuple/dict/np.array/NaN/str."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, dict):
        return value.get("value") or None
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        first = value[0]
        if isinstance(first, dict):
            return first.get("value") or None
        return str(first) if first is not None else None
    return str(value) if value is not None else None

async def get_conn():
    return await asyncpg.connect(DATABASE_URL)

async def save_df_text(df: pd.DataFrame, table: str):
    """Einfacher TEXT-Export nach Neon (robust & schnell genug)."""
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
        records = [
            tuple("" if pd.isna(row[c]) else str(row[c]) for c in cols_list)
            for _, row in df.iterrows()
        ]
        if records:
            await conn.executemany(stmt, records)
    finally:
        await conn.close()

# ========= Pipedrive: Felder & Personen =========
async def get_person_fields() -> Dict[str, Dict[str, Any]]:
    now = time.time()
    c = _person_fields_cache
    if c["data"] and now - c["ts"] < PERSON_FIELDS_TTL:
        return c["data"]

    url = f"{PIPEDRIVE_API}/personFields?api_token={PD_API_TOKEN}"
    r = await http_client.get(url)
    r.raise_for_status()
    data = r.json().get("data") or []

    mapping = {}
    for f in data:
        name = f.get("name")
        key = f.get("key")
        options = f.get("options") or []
        mapping[name] = {"key": key, "options": options, "name": name}
        mapping[_norm(name)] = {"key": key, "options": options, "name": name}

    _person_fields_cache.update(data=mapping, ts=now)
    return mapping

async def fetch_persons_by_filter(filter_id: int) -> List[Dict[str, Any]]:
    now = time.time()
    c = _filter_cache.get(filter_id)
    if c and now - c["ts"] < FILTER_TTL:
        return c["data"]

    persons, start, limit = [], 0, 500
    while True:
        url = f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={limit}&api_token={PD_API_TOKEN}"
        r = await http_client.get(url)
        if r.status_code != 200:
            raise Exception(f"Pipedrive API Fehler: {r.text}")
        chunk = r.json().get("data") or []
        if not chunk:
            break
        persons.extend(chunk)
        if len(chunk) < limit:
            break
        start += limit

    _filter_cache[filter_id] = {"data": persons, "ts": now}
    return persons

async def update_person(person_id: int, payload: dict) -> int:
    url = f"{PIPEDRIVE_API}/persons/{person_id}?api_token={PD_API_TOKEN}"
    r = await http_client.put(url, json=payload)
    return r.status_code

# ========= Cleanups =========
def apply_basic_cleanup(df: pd.DataFrame, key_orgart: str) -> pd.DataFrame:
    # 1) Organisationsart != leer -> Drop
    if key_orgart in df.columns:
        df = df[df[key_orgart].isna() | (df[key_orgart] == "")]
    # 2) Max. 2 Kontakte je Organisation
    org_col = "org_id" if "org_id" in df.columns else ("org_name" if "org_name" in df.columns else None)
    if org_col and org_col in df.columns:
        df["_rank"] = df.groupby(org_col).cumcount() + 1
        df = df[df["_rank"] <= 2].drop(columns=["_rank"], errors="ignore")
    return df

# ========= Redirects =========
@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse('<meta http-equiv="refresh" content="0;url=/neukontakte">')

@app.get("/overview", response_class=HTMLResponse)
async def overview_redirect():
    return HTMLResponse('<meta http-equiv="refresh" content="0;url=/neukontakte">')

# ========= UI-HTML-Snippets =========
def base_css() -> str:
    return """
    <style>
      :root{--blue:#0ba6ff;--bg:#f5f7fa;--card:#fff;--text:#222;--muted:#62738a;--border:#e5e9f0;}
      *{box-sizing:border-box}
      body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Inter,Arial;}
      header{display:flex;align-items:center;gap:10px;justify-content:space-between;padding:18px 24px;border-bottom:1px solid var(--border);background:#fff}
      header .left{display:flex;align-items:center;gap:12px}
      .badge{background:#e8f6ff;color:#0a7dc0;border:1px solid #cbe9ff;padding:6px 12px;border-radius:10px;font-weight:600}
      .container{max-width:1100px;margin:18px auto;padding:0 18px}
      .card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:18px}
      label{display:block;margin:8px 0 6px;font-weight:600}
      select,input{width:100%;padding:10px 12px;border:1px solid var(--border);border-radius:8px;outline:none;background:#fff}
      .row{display:grid;grid-template-columns:1fr 1fr;gap:14px}
      .muted{color:var(--muted);font-size:12px;margin-top:6px}
      .actions{display:flex;justify-content:flex-end;margin-top:16px}
      .btn{display:inline-flex;align-items:center;gap:8px;background:var(--blue);color:#fff;border:none;padding:10px 16px;border-radius:8px;text-decoration:none;cursor:pointer}
      .btn:hover{filter:brightness(.95)}
      table{border-collapse:collapse;width:100%}
      th,td{border:1px solid var(--border);padding:8px 10px;text-align:left}
      th{background:#f4f7fb}
      .grid{border:1px solid var(--border);border-radius:10px;overflow:hidden}
      .error{background:#fff3f3;border:1px solid #ffdede;color:#c00;border-radius:10px;padding:12px}
      .head-kpis{color:#64748b;font-size:14px}
      .head-kpis b{color:#0f172a}
      .pill{background:#eef6ff;border:1px solid #d7ecff;color:#0a6ba8;padding:6px 10px;border-radius:999px;font-weight:600}
    </style>
    """

def header_html(total: Optional[int]) -> str:
    total_html = f'<div class="head-kpis">Gesamt im Filter: <b>{total:,}</b></div>' if total else ""
    return f"""
    <header>
      <div class="left">
        <a class="pill" href="/neukontakte">Neukontakte (Filter {FILTER_NEUKONTAKTE})</a>
      </div>
      {total_html}
    </header>
    """

# ========= /neukontakte (Form) =========
@app.get("/neukontakte", response_class=HTMLResponse)
async def neukontakte_form(request: Request):
    fields = await get_person_fields()
    fach = fields.get(_norm(FIELD_NAME_FACHBEREICH)) or fields.get(FIELD_NAME_FACHBEREICH)
    if not fach:
        return HTMLResponse(base_css() + header_html(None) + "<div class='container'><div class='error'>❌ Feld „Fachbereich_Kampagne“ nicht gefunden.</div></div>", status_code=500)

    # Optionen nur aus personFields (ohne Full-Fetch); Total ermitteln wir via fetch (gecached)
    options = fach.get("options") or []
    # Für die Totalzahl müssen wir einmal alle Personen holen (Cache 120s)
    persons = await fetch_persons_by_filter(FILTER_NEUKONTAKTE)
    total_count = len(persons)

    # Counts pro Option (einmalig, aber schnell, da ohnehin persons im Cache sind)
    counts = {}
    key_fach = fach["key"]
    for p in persons:
        val = p.get(key_fach)
        s = str(val).strip() if val is not None else ""
        if s:
            counts[s] = counts.get(s, 0) + 1

    opts_html = ""
    for o in options:
        label = o.get("label") or ""
        oid = o.get("id")
        # Zahl in Klammern anzeigen
        count = counts.get(str(oid), counts.get(label, 0)) or 0
        opts_html += f'<option value="{oid}">{label} ({count})</option>'

    html = f"""
    {base_css()}
    {header_html(total_count)}
    <div class="container">
      <div class="card">
        <form method="post" action="/neukontakte/preview">
          <label>Fachbereich</label>
          <select name="fachbereich_value" required>
            {opts_html}
          </select>
          <div class="muted">Die Zahl in Klammern zeigt die vorhandenen Datensätze im Filter.</div>

          <div class="row" style="margin-top:14px">
            <div>
              <label>Wie viele Datensätze nehmen?</label>
              <input type="text" name="take_count" placeholder="z. B. 900">
              <div class="muted">Leer lassen = alle Datensätze des gewählten Fachbereichs.</div>
            </div>
            <div>
              <label>Batch ID</label>
              <input type="text" name="batch_id" placeholder="Bxxx" required>
              <div class="muted">Beispiel: B477</div>
            </div>
          </div>

          <div class="actions">
            <button class="btn" type="submit">Vorschau laden</button>
          </div>
        </form>
      </div>
    </div>
    """
    return HTMLResponse(html)

# ========= /neukontakte/preview =========
@app.post("/neukontakte/preview", response_class=HTMLResponse)
async def neukontakte_preview(
    request: Request,
    fachbereich_value: str = Form(...),
    batch_id: str = Form(...),
    take_count: Optional[str] = Form(None)
):
    try:
        take_n = to_int(take_count, 0)

        fields = await get_person_fields()
        fach = fields.get(_norm(FIELD_NAME_FACHBEREICH)) or fields.get(FIELD_NAME_FACHBEREICH)
        if not fach:
            return HTMLResponse(base_css() + header_html(None) + "<div class='container'><div class='error'>❌ Feld „Fachbereich_Kampagne“ nicht gefunden.</div></div>", status_code=500)

        key_fach   = fach["key"]
        key_chan   = (fields.get(_norm(FIELD_NAME_CHANNEL)) or fields.get(FIELD_NAME_CHANNEL) or {}).get("key")

        persons = await fetch_persons_by_filter(FILTER_NEUKONTAKTE)
        filtered = [p for p in persons if key_fach in p and (str(p[key_fach]) == str(fachbereich_value))]
        if take_n and take_n > 0:
            filtered = filtered[:take_n]

        df = pd.DataFrame(filtered)
        total = len(filtered)
        if df.empty:
            html = f"""{base_css()}{header_html(None)}<div class="container"><div class='card'><h3>Keine Datensätze für diese Auswahl.</h3><a class='btn' href="/neukontakte" style="margin-top:10px">Zurück</a></div></div>"""
            return HTMLResponse(html)

        if "email" in df.columns:
            df["E-Mail"] = df["email"].apply(extract_email)
        if "org_name" in df.columns:
            df.rename(columns={"org_name": "Organisation"}, inplace=True)
        df["Batch ID (Vorschau)"] = batch_id
        if key_chan:
            df["Channel (Vorschau)"] = CHANNEL_VALUE

        # kleine Vorschau
        cols = [c for c in ["id", "name", "E-Mail", "Organisation", "Batch ID (Vorschau)"] if c in df.columns]
        preview_df = df[cols] if cols else df.head(50)
        table_html = preview_df.head(50).to_html(classes="grid", index=False, border=0)

        html = f"""
        {base_css()}
        {header_html(None)}
        <div class="container">
          <div class="card">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
              <div><b>Vorschau</b></div>
              <div class="muted">Auswahl: <b>{total}</b> Datensätze — Batch ID: <b>{batch_id}</b></div>
            </div>
            {table_html}
            <form method="post" action="/neukontakte/run" style="margin-top:12px">
              <input type="hidden" name="fachbereich_value" value="{fachbereich_value}">
              <input type="hidden" name="batch_id" value="{batch_id}">
              <input type="hidden" name="take_count" value="{take_n}">
              <div class="actions">
                <a class="btn" href="/neukontakte" style="background:#6b7280">Zurück</a>
                <button class="btn" type="submit">Abgleich starten</button>
              </div>
            </form>
          </div>
        </div>
        """
        return HTMLResponse(html)

    except Exception as e:
        return HTMLResponse(f"{base_css()}{header_html(None)}<div class='container'><div class='error'><h3>❌ Fehler beim Abruf:</h3><pre>{e}</pre></div></div>", status_code=500)

# ========= /neukontakte/run =========
@app.post("/neukontakte/run", response_class=HTMLResponse)
async def neukontakte_run(
    request: Request,
    fachbereich_value: str = Form(...),
    batch_id: str = Form(...),
    take_count: Optional[str] = Form(None)
):
    try:
        take_n = to_int(take_count, 0)

        fields = await get_person_fields()
        fach = fields.get(_norm(FIELD_NAME_FACHBEREICH)) or fields.get(FIELD_NAME_FACHBEREICH)
        if not fach:
            return HTMLResponse(base_css() + header_html(None) + "<div class='container'><div class='error'>❌ Feld „Fachbereich_Kampagne“ nicht gefunden.</div></div>", status_code=500)

        key_fach   = fach["key"]
        key_batch  = (fields.get(_norm(FIELD_NAME_BATCH))   or fields.get(FIELD_NAME_BATCH)   or {}).get("key")
        key_chan   = (fields.get(_norm(FIELD_NAME_CHANNEL)) or fields.get(FIELD_NAME_CHANNEL) or {}).get("key")
        key_orgart = (fields.get(_norm(FIELD_NAME_ORGART))  or fields.get(FIELD_NAME_ORGART)  or {}).get("key")

        persons = await fetch_persons_by_filter(FILTER_NEUKONTAKTE)
        data = [p for p in persons if key_fach in p and (str(p[key_fach]) == str(fachbereich_value))]
        if take_n and take_n > 0:
            data = data[:take_n]
        if not data:
            return HTMLResponse(f"{base_css()}{header_html(None)}<div class='container'><div class='card'><h3>Keine Datensätze für diese Auswahl.</h3><a class='btn' href='/neukontakte' style='margin-top:10px'>Zurück</a></div></div>")

        # Pipedrive-Updates (concurrency begrenzen)
        payload = {}
        if key_batch: payload[key_batch] = batch_id
        if key_chan:  payload[key_chan]  = CHANNEL_VALUE

        sem = asyncio.Semaphore(6)
        async def _upd(p):
            async with sem:
                return await update_person(p["id"], payload)

        results = await asyncio.gather(*[_upd(p) for p in data])
        updated = sum(1 for s in results if s == 200)

        # Endergebnis aufbereiten + Cleanups + in Neon speichern
        df = pd.DataFrame(data)
        if "email" in df.columns:
            df["E-Mail"] = df["email"].apply(extract_email)
        if "org_name" in df.columns:
            df.rename(columns={"org_name": "Organisation"}, inplace=True)
        if key_orgart and key_orgart in df.columns:
            df.rename(columns={key_orgart: "Organisationsart"}, inplace=True)

        df = apply_basic_cleanup(df, "Organisationsart")
        await save_df_text(df, "nk_master_final")

        html = f"""
        {base_css()}
        {header_html(None)}
        <div class="container">
          <div class="card">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
              <b>✅ Abgleich abgeschlossen</b>
              <div class="muted">Batch ID: <b>{batch_id}</b></div>
            </div>
            <div class="muted" style="margin-bottom:12px">
              Ausgewählt: <b>{len(data)}</b> &nbsp;|&nbsp; Aktualisiert: <b>{updated}</b> &nbsp;|&nbsp; Nach Cleanup (Neon): <b>{len(df)}</b>
            </div>
            <a class="btn" href="/neukontakte">Zurück</a>
          </div>
        </div>
        """
        return HTMLResponse(html)

    except Exception as e:
        return HTMLResponse(f"{base_css()}{header_html(None)}<div class='container'><div class='error'><h3>❌ Fehler beim Abgleich:</h3><pre>{e}</pre></div></div>", status_code=500)

# ========= Shutdown: HTTP-Client sauber schließen =========
@app.on_event("shutdown")
async def _shutdown_event():
    try:
        await http_client.aclose()
    except:
        pass
