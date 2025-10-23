# master.py
import os
import re
import json
import math
from typing import Optional, Dict, Any, List

import numpy as np
import pandas as pd
import httpx
import asyncpg

from fastapi import FastAPI, Request, Body
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from rapidfuzz import fuzz, process

# -----------------------------------------------------------------------------
# Konfiguration
# -----------------------------------------------------------------------------
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
if not BASE_URL:
    raise ValueError("❌ BASE_URL fehlt (z. B. https://deine-app.onrender.com)")

# OAuth (optional)
PD_CLIENT_ID = os.getenv("PD_CLIENT_ID", "")
PD_CLIENT_SECRET = os.getenv("PD_CLIENT_SECRET", "")
OAUTH_AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
OAUTH_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"

# Fallback API Token (optional)
PD_API_TOKEN = os.getenv("PD_API_TOKEN", "")

# Pipedrive API Root
PIPEDRIVE_API = "https://api.pipedrive.com/v1"

# Neon / Postgres
DATABASE_URL = os.getenv("DATABASE_URL", "")

# Felder
FILTER_NEUKONTAKTE = 2998
FIELD_FACHBEREICH_HINT = "fachbereich"
FIELD_ORGART_HINT = "organisationsart"

# Visual / Vorgaben
DEFAULT_CHANNEL = "Cold-Mail"
COLD_MAILING_IMPORT_LABEL = "Cold-Mailing Import"

user_tokens: Dict[str, str] = {}

# -----------------------------------------------------------------------------
# HTTP Client
# -----------------------------------------------------------------------------
http_client = httpx.AsyncClient(timeout=60.0)

# -----------------------------------------------------------------------------
# Hilfsfunktionen
# -----------------------------------------------------------------------------
def _as_list_email(value) -> List[str]:
    """Robust gegen None / str / dict / list / tuple / numpy.ndarray."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, np.ndarray):
        value = value.tolist()

    if isinstance(value, dict):
        v = value.get("value")
        return [v] if v else []

    if isinstance(value, (list, tuple)):
        out: List[str] = []
        for x in value:
            if isinstance(x, dict):
                v = x.get("value")
                if v:
                    out.append(str(v))
            elif x is not None and not (isinstance(x, float) and pd.isna(x)):
                out.append(str(x))
        return out

    return [str(value)]

# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------
async def get_conn():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL fehlt")
    return await asyncpg.connect(DATABASE_URL)

async def ensure_table_text(conn: asyncpg.Connection, table: str, cols: List[str]):
    col_defs = ", ".join([f'"{c}" TEXT' for c in cols])
    await conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')

async def clear_table(conn: asyncpg.Connection, table: str):
    await conn.execute(f'DROP TABLE IF EXISTS "{table}"')

async def save_df_text(df: pd.DataFrame, table: str):
    """Speichert DataFrame schnell und stabil."""
    conn = await get_conn()
    try:
        await clear_table(conn, table)
        await ensure_table_text(conn, table, list(df.columns))
        if df.empty:
            return

        cols = list(df.columns)
        cols_sql = ", ".join(f'"{c}"' for c in cols)
        placeholders = ", ".join(f'${i}' for i in range(1, len(cols) + 1))
        insert_sql = f'INSERT INTO "{table}" ({cols_sql}) VALUES ({placeholders})'

        records = []
        for _, row in df.iterrows():
            vals = ["" if pd.isna(v) else str(v) for v in row.tolist()]
            records.append(vals)

        async with conn.transaction():
            await conn.executemany(insert_sql, records)
    finally:
        await conn.close()

async def load_df_text(table: str) -> pd.DataFrame:
    conn = await get_conn()
    try:
        rows = await conn.fetch(f'SELECT * FROM "{table}"')
        if not rows:
            return pd.DataFrame()
        cols = list(rows[0].keys())
        data = [tuple(r[c] for c in cols) for r in rows]
        df = pd.DataFrame(data, columns=cols).replace({"": np.nan})
        return df
    finally:
        await conn.close()

# -----------------------------------------------------------------------------
# Pipedrive Helper
# -----------------------------------------------------------------------------
def get_headers() -> Dict[str, str]:
    token = user_tokens.get("default", "")
    if token:
        return {"Authorization": f"Bearer {token}"}
    return {}

def append_token(url: str) -> str:
    if "api_token=" in url:
        return url
    if user_tokens.get("default"):
        return url
    if PD_API_TOKEN:
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}api_token={PD_API_TOKEN}"
    return url

async def get_field_key_by_hint(label_hint: str) -> Optional[str]:
    url = append_token(f"{PIPEDRIVE_API}/personFields")
    r = await http_client.get(url, headers=get_headers())
    fields = r.json().get("data") or []
    hint = label_hint.lower()
    for f in fields:
        if hint in (f.get("name") or "").lower():
            return f.get("key")
    return None

async def fetch_persons_by_filter(filter_id: int) -> List[dict]:
    persons: List[dict] = []
    start, limit = 0, 500
    while True:
        url = append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={limit}")
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
    return persons

async def fetch_organizations_by_filter(filter_id: int) -> List[dict]:
    orgs: List[dict] = []
    start, limit = 0, 500
    while True:
        url = append_token(f"{PIPEDRIVE_API}/organizations?filter_id={filter_id}&start={start}&limit={limit}")
        r = await http_client.get(url)
        if r.status_code != 200:
            raise Exception(f"Pipedrive API Fehler (Orgs {filter_id}): {r.text}")
        chunk = r.json().get("data") or []
        if not chunk:
            break
        orgs.extend(chunk)
        if len(chunk) < limit:
            break
        start += limit
    return orgs

# -----------------------------------------------------------------------------
# Startseite
# -----------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def root():
    return RedirectResponse("/neukontakte")

@app.get("/neukontakte", response_class=HTMLResponse)
async def neukontakte(request: Request):
    total = len(await fetch_persons_by_filter(FILTER_NEUKONTAKTE))
    return HTMLResponse(f"""
        <h1>BatchFlow</h1>
        <p>Gesamt im Filter {FILTER_NEUKONTAKTE}: {total}</p>
        <a href="/neukontakte/options">Optionen laden</a>
    """)

# -----------------------------------------------------------------------------
# Optionen
# -----------------------------------------------------------------------------
@app.get("/neukontakte/options")
async def neukontakte_options():
    fb_key = await get_field_key_by_hint(FIELD_FACHBEREICH_HINT)
    persons = await fetch_persons_by_filter(FILTER_NEUKONTAKTE)
    total = len(persons)
    counts: Dict[str, int] = {}

    if fb_key:
        for p in persons:
            val = p.get(fb_key)
            if isinstance(val, np.ndarray):
                val = val.tolist()
            if isinstance(val, list):
                for v in val:
                    if v:
                        counts[str(v)] = counts.get(str(v), 0) + 1
            elif val:
                counts[str(val)] = counts.get(str(val), 0) + 1

    options = [{"value": k, "count": v} for k, v in counts.items()]
    return JSONResponse({"total": total, "options": options})

# -----------------------------------------------------------------------------
# Vorschau
# -----------------------------------------------------------------------------
@app.post("/neukontakte/preview", response_class=HTMLResponse)
async def neukontakte_preview(
    fachbereich: str = Body(...),
    take_count: Optional[int] = Body(None),
    batch_id: Optional[str] = Body(None)
):
    try:
        fb_key = await get_field_key_by_hint(FIELD_FACHBEREICH_HINT)
        persons = await fetch_persons_by_filter(FILTER_NEUKONTAKTE)

        sel = []
        for p in persons:
            val = p.get(fb_key)
            if isinstance(val, np.ndarray):
                val = val.tolist()
            match = False
            if isinstance(val, list):
                match = fachbereich in [str(x) for x in val if x]
            else:
                match = str(val) == str(fachbereich)
            if match:
                sel.append(p)
        if take_count and take_count > 0:
            sel = sel[: min(take_count, len(sel))]

        rows = []
        for p in sel:
            pid = p.get("id")
            name = p.get("name") or ""
            emails_raw = p.get("email")
            email_str = ", ".join(_as_list_email(emails_raw))
            org_name = "-"
            org = p.get("org_id")
            if isinstance(org, dict):
                org_name = org.get("name") or "-"

            rows.append({
                "Batch ID": batch_id or "",
                "Channel": DEFAULT_CHANNEL,
                COLD_MAILING_IMPORT_LABEL: DEFAULT_CHANNEL,
                "id": pid,
                "name": name,
                "E-Mail": email_str,
                "Organisation": org_name,
            })

        df = pd.DataFrame(rows)
        await save_df_text(df, "nk_master_final")

        preview_table = df.head(50).to_html(classes="grid", index=False, border=0)
        return HTMLResponse(f"""
        <h2>Vorschau: {fachbereich} ({len(df)} Datensätze)</h2>
        {preview_table}
        <br><a href="/neukontakte">Zurück</a>
        """)
    except Exception as e:
        return HTMLResponse(f"<pre style='color:red'>❌ Fehler beim Abruf/Speichern:\n{e}</pre>", status_code=500)

# -----------------------------------------------------------------------------
# Redirects für Pipedrive-Aufruf
# -----------------------------------------------------------------------------
from fastapi import Request

@app.get("/overview", include_in_schema=False)
async def overview_redirect(request: Request):
    """Leitet /overview?... Anfragen (z. B. aus Pipedrive) zur Startseite."""
    return RedirectResponse("/neukontakte", status_code=307)

@app.get("/{full_path:path}", include_in_schema=False)
async def catch_all(full_path: str, request: Request):
    """Alle unbekannten Pfade → /neukontakte."""
    return RedirectResponse("/neukontakte", status_code=307)

# -----------------------------------------------------------------------------
# Lokaler Start
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("master:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=False)
