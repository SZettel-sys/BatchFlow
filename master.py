# master.py
import os
import re
import time
import sys
from typing import Optional, Dict, List, Tuple, AsyncGenerator

import numpy as np
import pandas as pd
import httpx
import asyncpg

from fastapi import FastAPI, Request, Body
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware
from rapidfuzz import fuzz, process

# =============================================================================
# Konfiguration
# =============================================================================
app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1024)
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
if not DATABASE_URL:
    raise ValueError("DATABASE_URL fehlt")

SCHEMA = os.getenv("PGSCHEMA", "public")

# Filter/Felder
FILTER_NEUKONTAKTE = 2998
FIELD_FACHBEREICH_HINT = "fachbereich"
FIELD_ORGART_HINT = "organisationsart"

# UI/Defaults
DEFAULT_CHANNEL = "Cold-Mail"
COLD_MAILING_IMPORT_LABEL = "Cold-Mailing Import"

# Performance
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "200"))
RECONCILE_MAX_ROWS = int(os.getenv("RECONCILE_MAX_ROWS", "800"))

# OAuth Tokens (einfach)
user_tokens: Dict[str, str] = {}

# =============================================================================
# Startup: HTTP-Client + DB-Pool
# =============================================================================
def http_client() -> httpx.AsyncClient:
    return app.state.http  # type: ignore[attr-defined]

def get_pool() -> asyncpg.Pool:
    return app.state.pool  # type: ignore[attr-defined]

@app.on_event("startup")
async def _startup():
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    app.state.http = httpx.AsyncClient(timeout=60.0, limits=limits)
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)

@app.on_event("shutdown")
async def _shutdown():
    try:
        await app.state.http.aclose()
    finally:
        await app.state.pool.close()

# =============================================================================
# Health
# =============================================================================
@app.get("/healthz")
async def healthz():
    try:
        async with get_pool().acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

# =============================================================================
# Helpers
# =============================================================================
def _as_list_email(value) -> List[str]:
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

def normalize_name(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"\b(gmbh|ug|ag|kg|ohg|inc|ltd|co)\b", "", s)
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

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

async def get_person_fields() -> List[dict]:
    url = append_token(f"{PIPEDRIVE_API}/personFields")
    r = await http_client().get(url, headers=get_headers())
    r.raise_for_status()
    return r.json().get("data") or []

async def get_person_field_by_hint(label_hint: str) -> Optional[dict]:
    fields = await get_person_fields()
    hint = (label_hint or "").lower()
    for f in fields:
        if hint in (f.get("name") or "").lower():
            return f
    return None

def field_options_id_to_label_map(field: dict) -> Dict[str, str]:
    opts = field.get("options") or []
    mp: Dict[str, str] = {}
    for o in opts:
        oid = str(o.get("id"))
        lab = str(o.get("label") or o.get("name") or oid)
        mp[oid] = lab
    return mp

async def stream_count_person_field_values(
    filter_id: int,
    field_key: str,
    page_limit: int = PAGE_LIMIT,
) -> Tuple[int, Dict[str, int]]:
    total = 0
    counts: Dict[str, int] = {}
    start = 0
    while True:
        url = append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={page_limit}")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive API Fehler: {r.text}")
        chunk = r.json().get("data") or []
        if not chunk:
            break
        for p in chunk:
            total += 1
            val = p.get(field_key)
            if isinstance(val, np.ndarray):
                val = val.tolist()
            if isinstance(val, list):
                for v in val:
                    if v is not None and str(v).strip() != "":
                        key = str(v)
                        counts[key] = counts.get(key, 0) + 1
            elif val is not None and str(val).strip() != "":
                key = str(val)
                counts[key] = counts.get(key, 0) + 1
        if len(chunk) < page_limit:
            break
        start += page_limit
    return total, counts

async def fetch_persons_until_match(
    filter_id: int,
    match_fn,
    max_collect: Optional[int] = None,
    page_limit: int = PAGE_LIMIT,
) -> List[dict]:
    collected: List[dict] = []
    start = 0
    while True:
        url = append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={page_limit}")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive API Fehler: {r.text}")
        chunk = r.json().get("data") or []
        if not chunk:
            break
        for p in chunk:
            if match_fn(p):
                collected.append(p)
                if max_collect and len(collected) >= max_collect:
                    return collected
        if len(chunk) < page_limit:
            break
        start += page_limit
    return collected

async def fetch_organizations_by_filter(filter_id: int, page_limit: int = PAGE_LIMIT) -> AsyncGenerator[dict, None]:
    start = 0
    while True:
        url = append_token(f"{PIPEDRIVE_API}/organizations?filter_id={filter_id}&start={start}&limit={page_limit}")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive API Fehler (Orgs {filter_id}): {r.text}")
        chunk = r.json().get("data") or []
        if not chunk:
            break
        for o in chunk:
            yield o
        if len(chunk) < page_limit:
            break
        start += page_limit

async def stream_person_ids_by_filter(filter_id: int, page_limit: int = PAGE_LIMIT) -> AsyncGenerator[str, None]:
    start = 0
    while True:
        url = append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={page_limit}")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive API Fehler (Persons {filter_id}): {r.text}")
        data = r.json().get("data") or []
        if not data:
            break
        for p in data:
            pid = p.get("id")
            if pid is not None:
                yield str(pid)
        if len(data) < page_limit:
            break
        start += page_limit

# =============================================================================
# DB helpers (Schema-fest)
# =============================================================================
async def ensure_table_text(conn: asyncpg.Connection, table: str, cols: List[str]):
    col_defs = ", ".join([f'"{c}" TEXT' for c in cols])
    await conn.execute(f'CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{table}" ({col_defs})')

async def clear_table(conn: asyncpg.Connection, table: str):
    await conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table}"')

async def save_df_text(df: pd.DataFrame, table: str):
    async with get_pool().acquire() as conn:
        await clear_table(conn, table)
        await ensure_table_text(conn, table, list(df.columns))
        if df.empty:
            return
        cols = list(df.columns)
        cols_sql = ", ".join(f'"{c}"' for c in cols)
        placeholders = ", ".join(f'${i}' for i in range(1, len(cols) + 1))
        insert_sql = f'INSERT INTO "{SCHEMA}"."{table}" ({cols_sql}) VALUES ({placeholders})'
        records: List[List[str]] = []
        for _, row in df.iterrows():
            vals = ["" if pd.isna(v) else str(v) for v in row.tolist()]
            records.append(vals)
        async with conn.transaction():
            await conn.executemany(insert_sql, records)

async def load_df_text(table: str) -> pd.DataFrame:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(f'SELECT * FROM "{SCHEMA}"."{table}"')
    if not rows:
        return pd.DataFrame()
    cols = list(rows[0].keys())
    data = [tuple(r[c] for c in cols) for r in rows]
    return pd.DataFrame(data, columns=cols).replace({"": np.nan})

# =============================================================================
# Optionen-Cache (10 Minuten)
# =============================================================================
_OPTIONS_CACHE: dict = {"ts": 0.0, "total": 0, "options": []}
OPTIONS_TTL_SEC = 600

# =============================================================================
# Routing
# =============================================================================
@app.get("/")
def root():
    return RedirectResponse("/neukontakte")

@app.get("/login")
def login():
    if not PD_CLIENT_ID:
        return HTMLResponse("<h3>CLIENT_ID fehlt. Bitte OAuth-Daten setzen oder PD_API_TOKEN nutzen.</h3>", 400)
    redirect_uri = f"{BASE_URL}/oauth/callback"
    url = f"{OAUTH_AUTHORIZE_URL}?client_id={PD_CLIENT_ID}&redirect_uri={redirect_uri}"
    return RedirectResponse(url)

@app.get("/oauth/callback")
async def oauth_callback(code: str):
    redirect_uri = f"{BASE_URL}/oauth/callback"
    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": PD_CLIENT_ID,
        "client_secret": PD_CLIENT_SECRET
    }
    r = await http_client().post(OAUTH_TOKEN_URL, data=payload)
    tok = r.json()
    if "access_token" not in tok:
        return HTMLResponse(f"<h3>❌ OAuth Fehler: {tok}</h3>", 400)
    user_tokens["default"] = tok["access_token"]
    return RedirectResponse("/neukontakte")

# -----------------------------------------------------------------------------
@app.get("/neukontakte", response_class=HTMLResponse)
async def neukontakte(request: Request):
    total = await count_persons_in_filter(FILTER_NEUKONTAKTE)
    authed = bool(user_tokens.get("default") or PD_API_TOKEN)

    html = f"""
<!doctype html><html lang="de">
<head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Neukontakte (Filter {FILTER_NEUKONTAKTE})</title>
<style>
  body{{font-family: Inter, -apple-system, Segoe UI, Roboto, Arial, sans-serif;background:#f5f7fa;color:#1f2937;margin:0}}
  header{{display:flex;justify-content:space-between;align-items:center;padding:18px 22px;background:#fff;border-bottom:1px solid #e5e7eb}}
  .wrap{{max-width:1180px;margin:28px auto;padding:0 16px}}
  .card{{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:22px;box-shadow:0 1px 3px rgba(0,0,0,.05)}}
  label{{display:block;margin:12px 0 6px;font-weight:600}}
  select,input{{width:100%;padding:10px 12px;border:1px solid #cfd6df;border-radius:8px}}
  .row{{display:grid;grid-template-columns:1fr 220px 220px auto;gap:16px;align-items:end}}
  .muted{{color:#6b7280}}
  .btn{{background:#0ea5e9;border:none;color:#fff;border-radius:8px;padding:10px 16px;cursor:pointer}}
  .btn:hover{{background:#0284c7}}
  #overlay{{display:none;position:fixed;inset:0;background:rgba(255,255,255,.7);z-index:9999;align-items:center;justify-content:center}}
  .spinner{{width:48px;height:48px;border:4px solid #93c5fd;border-top-color:#1d4ed8;border-radius:50%;animation:spin 1s linear infinite}}
  @keyframes spin{{to{{transform:rotate(360deg)}}}}
</style>
</head>
<body>
<header>
  <div><b>Neukontakte (Filter {FILTER_NEUKONTAKTE})</b> · Gesamt: <b>{total}</b></div>
  <div>{"<span class='muted'>angemeldet</span>" if authed else "<a href='/login'>Anmelden</a>"}</div>
</header>

<div class="wrap">
  <div class="card">
    <label>Fachbereich</label>
    <select id="fachbereich"><option value="">– bitte auswählen –</option></select>
    <div class="muted" id="fbinfo" style="margin-top:6px;">Die Zahl in Klammern zeigt die vorhandenen Datensätze im Filter.</div>
    <div class="row" style="margin-top:14px;">
      <div>
        <label>Wie viele Datensätze nehmen?</label>
        <input type="number" id="take_count" placeholder="z. B. 900" min="1"/>
        <div class="muted">Leer lassen = alle Datensätze des gewählten Fachbereichs.</div>
      </div>
      <div>
        <label>Batch ID</label>
        <input id="batch_id" placeholder="Bxxx"/>
        <div class="muted">Beispiel: B111</div>
      </div>
      <div style="align-self:end;">
        <button class="btn" id="btnPreview">Vorschau laden</button>
      </div>
    </div>
  </div>
</div>

<div id="overlay"><div class="spinner"></div></div>

<script>
function showOverlay(){{document.getElementById("overlay").style.display="flex";}}
function hideOverlay(){{document.getElementById("overlay").style.display="none";}}

async function loadOptions(){{
  showOverlay();
  try {{
    const r = await fetch('/neukontakte/options', {{cache:'no-store'}});
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const data = await r.json();
    const sel = document.getElementById('fachbereich');
    sel.innerHTML = '<option value="">– bitte auswählen –</option>';
    data.options.forEach(o => {{
      const opt = document.createElement('option');
      opt.value = o.value;
      opt.textContent = o.label + ' (' + o.count + ')';
      sel.appendChild(opt);
    }});
    document.getElementById('fbinfo').textContent =
      "Gesamt im Filter: " + data.total + " | Fachbereiche: " + data.options.length;
  }} catch(e) {{
    alert('Fehler beim Laden der Fachbereiche: ' + e);
  }} finally {{
    hideOverlay();
  }}
}}

document.getElementById('btnPreview').addEventListener('click', async () => {{
  const fb = document.getElementById('fachbereich').value;
  const tc = document.getElementById('take_count').value || null;
  const bid = document.getElementById('batch_id').value || null;
  if(!fb) {{ alert('Bitte zuerst einen Fachbereich wählen.'); return; }}
  showOverlay();
  try {{
    const r = await fetch('/neukontakte/preview', {{
      method:'POST',
      headers:{{'Content-Type':'application/json'}},
      cache:'no-store',
      body: JSON.stringify({{ fachbereich: fb, take_count: tc ? parseInt(tc) : null, batch_id: bid }})
    }});
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const html = await r.text();
    document.open(); document.write(html); document.close();
  }} catch(e) {{
    alert('Fehler: ' + e);
  }} finally {{
    hideOverlay();
  }}
}});

loadOptions();
</script>
</body></html>
"""
    return HTMLResponse(html)

async def count_persons_in_filter(filter_id: int) -> int:
    start, total = 0, 0
    while True:
        url = append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={PAGE_LIMIT}")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            return total
        items = r.json().get("data") or []
        if not items:
            break
        total += len(items)
        if len(items) < PAGE_LIMIT:
            break
        start += PAGE_LIMIT
    return total

# -----------------------------------------------------------------------------
@app.get("/neukontakte/options")
async def neukontakte_options():
    now = time.time()
    if _OPTIONS_CACHE["options"] and (now - _OPTIONS_CACHE["ts"] < OPTIONS_TTL_SEC):
        return JSONResponse({"total": _OPTIONS_CACHE["total"], "options": _OPTIONS_CACHE["options"]})

    fb_field = await get_person_field_by_hint(FIELD_FACHBEREICH_HINT)
    if not fbassistant_RGCTXanalysis
