# master.py
import os
import re
import sys
import time
from typing import Optional, Dict, List, Tuple, AsyncGenerator

import numpy as np
import pandas as pd
import httpx
import asyncpg

from fastapi import FastAPI, Request, Body, Query
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

# Filter/Felder (hier ggf. je Kampagnentyp unterscheiden)
FILTER_NEUKONTAKTE = 2998
FIELD_FACHBEREICH_HINT = "fachbereich"
FIELD_ORGART_HINT = "organisationsart"

# UI/Defaults
DEFAULT_CHANNEL = "Cold E-Mail"             # fix gesetzt
COLD_MAILING_IMPORT_LABEL = "Cold-Mailing Import"

# Performance
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "500"))
RECONCILE_MAX_ROWS = int(os.getenv("RECONCILE_MAX_ROWS", "1000"))
OPTIONS_TTL_SEC = int(os.getenv("OPTIONS_TTL_SEC", "900"))
PER_ORG_DEFAULT_LIMIT = int(os.getenv("PER_ORG_DEFAULT_LIMIT", "2"))  # 1 oder 2

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

def split_name(full: str) -> Tuple[str, str]:
    if not full:
        return "", ""
    parts = str(full).strip().split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])

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

# ---- Pipedrive Streaming-Helper ---------------------------------------------
async def fetch_persons_until_match(
    filter_id: int,
    predicate,
    max_collect: Optional[int] = None,
    page_limit: int = PAGE_LIMIT,
) -> List[dict]:
    start = 0
    out: List[dict] = []
    while True:
        url = append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={page_limit}")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive API Fehler: {r.text}")
        items = r.json().get("data") or []
        if not items:
            break
        for p in items:
            if predicate(p):
                out.append(p)
                if max_collect and len(out) >= max_collect:
                    return out
        if len(items) < page_limit:
            break
        start += page_limit
    return out

async def fetch_organizations_by_filter(
    filter_id: int,
    page_limit: int = PAGE_LIMIT,
) -> AsyncGenerator[dict, None]:
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

async def stream_person_ids_by_filter(
    filter_id: int,
    page_limit: int = PAGE_LIMIT,
) -> AsyncGenerator[str, None]:
    start = 0
    while True:
        url = append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={page_limit}")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive API Fehler: {r.text}")
        items = r.json().get("data") or []
        if not items:
            break
        for p in items:
            pid = p.get("id")
            if pid is not None:
                yield str(pid)
        if len(items) < page_limit:
            break
        start += page_limit

# ---- Zählen nach „max N Kontakte pro Organisation“ --------------------------
async def stream_counts_with_org_cap(
    filter_id: int,
    fachbereich_key: str,
    per_org_limit: int,
    page_limit: int = PAGE_LIMIT,
) -> Tuple[int, Dict[str, int]]:
    total = 0
    counts: Dict[str, int] = {}
    org_used: Dict[str, int] = {}

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
            org_key = None
            org = p.get("org_id")
            if isinstance(org, dict):
                if org.get("id") is not None:
                    org_key = f"id:{org.get('id')}"
                elif org.get("name"):
                    org_key = f"name:{normalize_name(org.get('name'))}"
            if not org_key:
                pid = p.get("id")
                org_key = f"noorg:{pid}"

            used = org_used.get(org_key, 0)
            if used >= per_org_limit:
                continue

            org_used[org_key] = used + 1
            total += 1

            val = p.get(fachbereich_key)
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

# =============================================================================
# DB helpers
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
# Optionen-Cache
# =============================================================================
_OPTIONS_CACHE: Dict[int, dict] = {}

# =============================================================================
# Routing – Landing mit 3 Kampagnen
# =============================================================================
@app.get("/")
def root():
    return RedirectResponse("/campaign")

@app.get("/campaign", response_class=HTMLResponse)
async def campaign_home():
    html = """<!doctype html><html lang="de">
<head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>BatchFlow – Kampagne wählen</title>
<style>
  :root{--bg:#f5f7fa;--card:#fff;--muted:#6b7280;--border:#e5e7eb;--primary:#0ea5e9;--primary-h:#0284c7;}
  body{font-family:Inter,-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:var(--bg);color:#1f2937;margin:0}
  .wrap{max-width:980px;margin:48px auto;padding:0 16px}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:18px}
  .card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:24px;box-shadow:0 2px 6px rgba(0,0,0,.05);text-align:center}
  .title{font-size:22px;font-weight:700;margin-bottom:10px}
  .desc{color:var(--muted);margin-bottom:18px;min-height:42px}
  .btn{display:inline-block;background:var(--primary);color:#fff;text-decoration:none;border-radius:10px;padding:10px 14px}
  .btn:hover{background:var(--primary-h)}
  header{padding:18px 22px;background:#fff;border-bottom:1px solid var(--border)}
  header b{font-size:18px}
</style></head>
<body>
<header><b>BatchFlow</b> <span style="color:#9ca3af">– Kampagne auswählen</span></header>
<div class="wrap">
  <div class="grid">
    <div class="card">
      <div class="title">Neukontakte</div>
      <div class="desc">Neue Personen aus dem Pipedrive-Filter mit Orga-Limit, Bereinigung & Export.</div>
      <a class="btn" href="/neukontakte?mode=new">Starten</a>
    </div>
    <div class="card">
      <div class="title">Nachfass</div>
      <div class="desc">Folgekampagne für bereits kontaktierte Leads (Logik vorbereitet).</div>
      <a class="btn" href="/neukontakte?mode=nachfass">Starten</a>
    </div>
    <div class="card">
      <div class="title">Refresh</div>
      <div class="desc">Kontakte auffrischen/ergänzen (Logik vorbereitet).</div>
      <a class="btn" href="/neukontakte?mode=refresh">Starten</a>
    </div>
  </div>
</div>
</body></html>"""
    return HTMLResponse(html)

# =============================================================================
# Hauptformular – Harmonisiert; „Pro Organisation“ über Fachbereich
# =============================================================================
@app.get("/neukontakte", response_class=HTMLResponse)
async def neukontakte(request: Request, mode: str = Query("new")):
    authed = bool(user_tokens.get("default") or PD_API_TOKEN)
    title_map = {
        "new": "Neukontakte",
        "nachfass": "Nachfass",
        "refresh": "Refresh",
    }
    page_title = title_map.get(mode, "Neukontakte")

    html = f"""<!doctype html><html lang="de">
<head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{page_title} – BatchFlow</title>
<style>
  :root{{--bg:#f5f7fa;--card:#fff;--muted:#6b7280;--border:#e5e7eb;--primary:#0ea5e9;--primary-h:#0284c7}}
  body{{font-family: Inter, -apple-system, Segoe UI, Roboto, Arial, sans-serif;background:var(--bg);color:#1f2937;margin:0}}
  header{{display:flex;justify-content:space-between;align-items:center;padding:18px 22px;background:#fff;border-bottom:1px solid var(--border)}}
  .wrap{{max-width:1180px;margin:28px auto;padding:0 16px}}
  .card{{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:22px;box-shadow:0 2px 6px rgba(0,0,0,.05)}}
  label{{display:block;margin:10px 0 6px;font-weight:600}}
  select,input{{width:100%;padding:10px 12px;border:1px solid #cfd6df;border-radius:8px}}
  .row{{display:grid;grid-template-columns:1fr 1fr 1fr 1fr auto;gap:16px;align-items:end}}
  .rowTop{{display:grid;grid-template-columns:240px 1fr;gap:16px}}
  .muted{{color:var(--muted)}}
  .btn{{background:var(--primary);border:none;color:#fff;border-radius:8px;padding:10px 16px;cursor:pointer}}
  .btn:hover{{background:var(--primary-h)}}
  #overlay{{display:none;position:fixed;inset:0;background:rgba(255,255,255,.7);z-index:9999;align-items:center;justify-content:center}}
  .spinner{{width:48px;height:48px;border:4px solid #93c5fd;border-top-color:#1d4ed8;border-radius:50%;animation:spin 1s linear infinite}}
  @keyframes spin{{to{{transform:rotate(360deg)}}}}
  .breadcrumbs a{{color:#0a66c2;text-decoration:none}}
  .breadcrumbs a:hover{{text-decoration:underline}}
</style>
</head>
<body>
<header>
  <div class="breadcrumbs">
    <a href="/campaign">← Kampagne wählen</a>
  </div>
  <div><b>{page_title}</b> · Gesamt: <b id="total-count">lädt…</b></div>
  <div>{"<span class='muted'>angemeldet</span>" if authed else "<a href='/login'>Anmelden</a>"}</div>
</header>

<div class="wrap">
  <div class="card">
    <div class="rowTop">
      <div>
        <label>Pro Organisation</label>
        <select id="per_org_limit">
          <option value="1">1</option>
          <option value="2" selected>2</option>
        </select>
      </div>
      <div>
        <label>Fachbereich</label>
        <select id="fachbereich"><option value="">– bitte auswählen –</option></select>
        <div class="muted" id="fbinfo" style="margin-top:6px;">Die Zahl in Klammern berücksichtigt bereits „Pro Organisation“.</div>
      </div>
    </div>

    <div class="row" style="margin-top:14px;">
      <div>
        <label>Batch ID</label>
        <input id="batch_id" placeholder="Bxxx"/>
        <div class="muted">Beispiel: B111</div>
      </div>
      <div>
        <label>Kampagnenname</label>
        <input id="campaign" placeholder="z. B. Herbstkampagne"/>
      </div>
      <div>
        <label>Wie viele Datensätze nehmen?</label>
        <input type="number" id="take_count" placeholder="z. B. 900" min="1"/>
        <div class="muted">Leer lassen = alle Datensätze des gewählten Fachbereichs.</div>
      </div>
      <div style="align-self:end;">
        <button class="btn" id="btnPreview">Vorschau laden</button>
      </div>
    </div>
  </div>
</div>

<div id="overlay"><div class="spinner"></div></div>

<script>
const MODE = new URLSearchParams(location.search).get('mode') || 'new';

function showOverlay(){{document.getElementById("overlay").style.display="flex";}}
function hideOverlay(){{document.getElementById("overlay").style.display="none";}}

async function loadOptions(){{
  showOverlay();
  try {{
    const pol = document.getElementById('per_org_limit').value || '{PER_ORG_DEFAULT_LIMIT}';
    const r = await fetch('/neukontakte/options?per_org_limit=' + encodeURIComponent(pol) + '&mode=' + encodeURIComponent(MODE), {{cache:'no-store'}});
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
      "Gesamt (nach Orga-Limit): " + data.total + " | Fachbereiche: " + data.options.length;
    document.getElementById('total-count').textContent = String(data.total);
  }} catch(e) {{
    alert('Fehler beim Laden der Fachbereiche: ' + e);
  }} finally {{
    hideOverlay();
  }}
}}

document.getElementById('per_org_limit').addEventListener('change', loadOptions);

document.getElementById('btnPreview').addEventListener('click', async () => {{
  const fb = document.getElementById('fachbereich').value;
  const tc = document.getElementById('take_count').value || null;
  const bid = document.getElementById('batch_id').value || null;
  const camp = document.getElementById('campaign').value || null;
  const pol = document.getElementById('per_org_limit').value || '{PER_ORG_DEFAULT_LIMIT}';

  if(!fb) {{ alert('Bitte zuerst einen Fachbereich wählen.'); return; }}
  showOverlay();
  try {{
    const r = await fetch('/neukontakte/preview?mode=' + encodeURIComponent(MODE), {{
      method:'POST',
      headers:{{'Content-Type':'application/json'}},
      cache:'no-store',
      body: JSON.stringify({{ fachbereich: fb, take_count: tc ? parseInt(tc) : null, batch_id: bid, campaign: camp, per_org_limit: parseInt(pol) }})
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
</body></html>"""
    return HTMLResponse(html)

# =============================================================================
# Optionen – je nach Modus später unterschiedliche Filter/Regeln möglich
# =============================================================================
@app.get("/neukontakte/options")
async def neukontakte_options(
    per_org_limit: int = Query(PER_ORG_DEFAULT_LIMIT, ge=1, le=2),
    mode: str = Query("new"),
):
    now = time.time()
    cache_key = (per_org_limit, mode)
    cache = _OPTIONS_CACHE.get(hash(cache_key)) or {}
    if cache.get("options") and (now - cache.get("ts", 0.0) < OPTIONS_TTL_SEC):
        return JSONResponse({"total": cache["total"], "options": cache["options"]})

    # Modus-spezifische Auswahl (hier noch gleich – später anpassen)
    filter_id = FILTER_NEUKONTAKTE  # if mode == 'nachfass': ...; if mode == 'refresh': ...

    fb_field = await get_person_field_by_hint(FIELD_FACHBEREICH_HINT)
    if not fb_field:
        return JSONResponse({"total": 0, "options": []})
    fb_key = fb_field.get("key")
    id2label = field_options_id_to_label_map(fb_field)

    total, counts = await stream_counts_with_org_cap(filter_id, fb_key, per_org_limit)

    options = []
    for opt_id, cnt in counts.items():
        label = id2label.get(str(opt_id), str(opt_id))
        options.append({"value": opt_id, "label": label, "count": cnt})
    options.sort(key=lambda x: x["count"], reverse=True)

    _OPTIONS_CACHE[hash(cache_key)] = {"ts": now, "total": total, "options": options}
    return JSONResponse({"total": total, "options": options})

# =============================================================================
# Vorschau – Name split + Kampagne + Channel, Modus vorbereitbar
# =============================================================================
@app.post("/neukontakte/preview", response_class=HTMLResponse)
async def neukontakte_preview(
    fachbereich: str = Body(...),
    take_count: Optional[int] = Body(None),
    batch_id: Optional[str] = Body(None),
    campaign: Optional[str] = Body(None),
    per_org_limit: int = Body(PER_ORG_DEFAULT_LIMIT),
    mode: str = Query("new"),
):
    try:
        # Modus-spezifische Quellen (aktuell gleich)
        filter_id = FILTER_NEUKONTAKTE

        fb_field = await get_person_field_by_hint(FIELD_FACHBEREICH_HINT)
        if not fb_field:
            return HTMLResponse("<div style='padding:24px;color:#b00'>❌ 'Fachbereich'-Feld nicht gefunden.</div>", 500)
        fb_key = fb_field.get("key")
        id2label = field_options_id_to_label_map(fb_field)
        fb_label = id2label.get(str(fachbereich), str(fachbereich))

        def _match(p: dict) -> bool:
            val = p.get(fb_key)
            if isinstance(val, np.ndarray):
                val = val.tolist()
            if isinstance(val, list):
                return str(fachbereich) in [str(x) for x in val if x is not None]
            return str(val) == str(fachbereich)

        raw = await fetch_persons_until_match(filter_id, _match, max_collect=None)

        org_used: Dict[str, int] = {}
        sel: List[dict] = []
        for p in raw:
            org_key = None
            org = p.get("org_id")
            if isinstance(org, dict):
                if org.get("id") is not None:
                    org_key = f"id:{org.get('id')}"
                elif org.get("name"):
                    org_key = f"name:{normalize_name(org.get('name'))}"
            if not org_key:
                org_key = f"noorg:{p.get('id')}"

            used = org_used.get(org_key, 0)
            if used >= int(per_org_limit):
                continue
            org_used[org_key] = used + 1
            sel.append(p)

        if take_count and take_count > 0:
            sel = sel[: min(take_count, len(sel))]

        rows = []
        for p in sel:
            pid = p.get("id")
            full_name = p.get("name") or ""
            first, last = split_name(full_name)
            email_str = ", ".join(_as_list_email(p.get("email")))
            org_name = "-"
            org = p.get("org_id")
            if isinstance(org, dict):
                org_name = org.get("name") or "-"
            rows.append({
                "Batch ID": batch_id or "",
                "Channel": DEFAULT_CHANNEL,
                COLD_MAILING_IMPORT_LABEL: campaign or "",
                "id": pid,
                "Person - Vorname": first,
                "Person - Nachname": last,
                "E-Mail": email_str,
                "Organisation": org_name,
                "Fachbereich": fb_label,
                "Kampagnen-Typ": mode,
            })

        df = pd.DataFrame(rows)
        await save_df_text(df, "nk_master_final")

        preview_table = (df.head(50).to_html(classes="grid", index=False, border=0)
                         if not df.empty else "<i>Keine Daten</i>")

        html = f"""<!doctype html><html lang="de"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Vorschau – {mode.capitalize()}</title>
<style>
  body{{font-family: Inter, -apple-system, Segoe UI, Roboto, Arial, sans-serif;background:#f5f7fa;color:#1f2937;margin:0}}
  .grid{{width:100%;border-collapse:collapse}}
  .grid th,.grid td{{border:1px solid #e5e7eb;padding:8px 10px;text-align:left}}
  .grid th{{background:#f3f4f6}}
  .btn{{background:#0ea5e9;border:none;color:#fff;border-radius:8px;padding:10px 16px;cursor:pointer;text-decoration:none}}
  .btn:hover{{background:#0284c7}}
</style></head>
<body>
  <div style="padding:16px 20px;">
    <h3>Vorschau – Typ: <b>{mode.capitalize()}</b> | Fachbereich: <b>{fb_label}</b> | Batch: <b>{batch_id or "-"}</b> | Datensätze: <b>{len(df)}</b></h3>
    {preview_table}
    <p><a class="btn" href="/neukontakte?mode={mode}">Zurück</a></p>
    <p><button class="btn" id="btnReconcile">Abgleich durchführen</button></p>
  </div>
<script>
document.getElementById('btnReconcile').addEventListener('click', () => {{
  window.location.href = '/neukontakte/reconcile';
}});
</script>
</body></html>"""
        return HTMLResponse(html)
    except Exception as e:
        return HTMLResponse(f"<pre style='padding:24px;color:#b00'>❌ Fehler beim Abruf/Speichern:\n{e}</pre>", status_code=500)

# =============================================================================
# Abgleich – nk_master_ready & nk_delete_log (unverändert)
# =============================================================================
async def _reconcile_impl() -> HTMLResponse:
    print("==> reconcile START", file=sys.stderr, flush=True)

    master = await load_df_text("nk_master_final")
    print(f"==> reconcile loaded rows={len(master)}", file=sys.stderr, flush=True)
    if master.empty:
        return HTMLResponse("<div style='padding:24px'>Keine Daten in nk_master_final.</div>")

    if len(master) > RECONCILE_MAX_ROWS:
        return HTMLResponse(
            f"<div style='padding:24px;color:#b00'>❌ Zu viele Datensätze ({len(master)}). "
            f"Bitte auf ≤ {RECONCILE_MAX_ROWS} begrenzen.</div>",
            status_code=400
        )

    col_person_id = "id" if "id" in master.columns else None
    col_org_name = "Organisation" if "Organisation" in master.columns else None
    col_orgart = "Organisationsart" if "Organisationsart" in master.columns else None

    delete_log = []

    if col_orgart and col_orgart in master.columns:
        mask_orgtype = master[col_orgart].notna() & (master[col_orgart].astype(str).str.strip() != "")
        removed = master[mask_orgtype].copy()
        for _, r in removed.iterrows():
            pname = (str(r.get("Person - Vorname") or "") + " " + str(r.get("Person - Nachname") or "")).strip() or str(r.get("name") or "")
            delete_log.append({"reason":"organisationsart_set","id":r.get(col_person_id),"name":pname,
                               "org_name":r.get(col_org_name),"extra":str(r.get(col_orgart))})
        master = master[~mask_orgtype].copy()

    suspect_org_filters = [1245, 851, 1521]
    if col_org_name and col_org_name in master.columns:
        ext_buckets: Dict[str, List[str]] = {}
        for fid in suspect_org_filters:
            async for o in fetch_organizations_by_filter(fid, page_limit=PAGE_LIMIT):
                n = normalize_name(o.get("name") or "")
                if not n:
                    continue
                b = n[0]
                ext_buckets.setdefault(b, []).append(n)
        for b in list(ext_buckets.keys()):
            ext_buckets[b] = list(dict.fromkeys(ext_buckets[b]))

        drop_idx = []
        for idx, row in master.iterrows():
            cand = str(row.get(col_org_name) or "").strip()
            cand_norm = normalize_name(cand)
            if not cand_norm:
                continue
            bucket = ext_buckets.get(cand_norm[0])
            if not bucket:
                continue
            near = [n for n in bucket if abs(len(n) - len(cand_norm)) <= 4]
            if not near:
                continue
            best = process.extractOne(cand_norm, near, scorer=fuzz.token_sort_ratio)
            if best and best[1] >= 95:
                pname = (str(row.get("Person - Vorname") or "") + " " + str(row.get("Person - Nachname") or "")).strip() or str(row.get("name") or "")
                drop_idx.append(idx)
                delete_log.append({"reason":"org_match_95","id":row.get(col_person_id),"name":pname,
                                   "org_name":cand,"extra":f"Best Match: {best[0]} ({best[1]}%)"})
        if drop_idx:
            master = master.drop(index=drop_idx)

    suspect_person_filters = [1216, 1708]
    if col_person_id:
        suspect_ids = set()
        for fid in suspect_person_filters:
            async for pid in stream_person_ids_by_filter(fid, page_limit=PAGE_LIMIT):
                suspect_ids.add(pid)
        if suspect_ids:
            mask_pid = master[col_person_id].astype(str).isin(suspect_ids)
            removed = master[mask_pid].copy()
            for _, r in removed.iterrows():
                pname = (str(r.get("Person - Vorname") or "") + " " + str(r.get("Person - Nachname") or "")).strip() or str(r.get("name") or "")
                delete_log.append({"reason":"person_id_match","id":r.get(col_person_id),"name":pname,
                                   "org_name":r.get(col_org_name),"extra":"ID in Filter 1216/1708"})
            master = master[~mask_pid].copy()

    await save_df_text(master, "nk_master_ready")
    log_df = pd.DataFrame(delete_log, columns=["reason","id","name","org_name","extra"])
    await save_df_text(log_df, "nk_delete_log")

    stats = {
        "after": len(master),
        "del_orgtype": log_df[log_df["reason"]=="organisationsart_set"].shape[0],
        "del_orgmatch": log_df[log_df["reason"]=="org_match_95"].shape[0],
        "del_pid": log_df[log_df["reason"]=="person_id_match"].shape[0],
        "deleted_total": log_df.shape[0],
    }
    table_html = log_df.head(50).to_html(classes="grid", index=False, border=0) if not log_df.empty else "<i>keine</i>"

    html = f"""<!doctype html><html lang="de"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Abgleich – Ergebnis</title>
<style>
  body{{font-family: Inter, -apple-system, Segoe UI, Roboto, Arial, sans-serif;background:#f5f7fa;color:#1f2937;margin:0}}
  .wrap{{max-width:1180px;margin:28px auto;padding:0 16px}}
  .card{{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:22px;margin-top:16px;box-shadow:0 1px 3px rgba(0,0,0,.05)}}
  .grid{{width:100%;border-collapse:collapse}}
  .grid th,.grid td{{border:1px solid #e5e7eb;padding:8px 10px;text-align:left}}
  .grid th{{background:#f3f4f6}}
  .btn{{background:#0ea5e9;border:none;color:#fff;border-radius:8px;padding:10px 16px;cursor:pointer;text-decoration:none}}
  .btn:hover{{background:#0284c7}}
  .muted{{color:#6b7280}}
  .row{{display:flex;gap:10px;flex-wrap:wrap;align-items:center}}
</style></head>
<body>
<div class="wrap">
  <div class="card">
    <div class="row">
      <span>Ergebnis: <b>{stats["after"]}</b> Zeilen in <b>nk_master_ready</b></span>
    </div>
    <p class="muted">
      Entfernt (Organisationsart gesetzt): <b>{stats["del_orgtype"]}</b><br/>
      Entfernt (Orga-Match ≥95% – Filter 1245/851/1521): <b>{stats["del_orgmatch"]}</b><br/>
      Entfernt (Person-ID in Filtern 1216/1708): <b>{stats["del_pid"]}</b><br/>
      <b>Summe entfernt:</b> {stats["deleted_total"]}
    </p>
    <div class="row">
      <a class="btn" href="/campaign">Zur Kampagnenwahl</a>
      <a class="btn" href="/neukontakte?mode=new">Neukontakte</a>
    </div>
  </div>
  <div class="card">
    <h3>Entfernte Datensätze (Top 50)</h3>
    {table_html}
    <p class="muted">Vollständiges Log in Neon: <b>nk_delete_log</b></p>
  </div>
</div>
</body></html>"""
    print("==> reconcile DONE", file=sys.stderr, flush=True)
    return HTMLResponse(html)

@app.post("/neukontakte/reconcile", response_class=HTMLResponse)
async def neukontakte_reconcile_post():
    try:
        return await _reconcile_impl()
    except Exception as e:
        import traceback
        print("==> reconcile ERROR\n", traceback.format_exc(), file=sys.stderr, flush=True)
        return HTMLResponse(f"<pre style='padding:24px;color:#b00'>❌ Fehler beim Abgleich: {e}</pre>", status_code=500)

@app.get("/neukontakte/reconcile", response_class=HTMLResponse)
async def neukontakte_reconcile_get():
    return await neukontakte_reconcile_post()

# =============================================================================
# Redirects / Fallback
# =============================================================================
@app.get("/overview", include_in_schema=False)
async def overview_redirect(request: Request):
    return RedirectResponse("/campaign", status_code=307)

@app.get("/{full_path:path}", include_in_schema=False)
async def catch_all(full_path: str, request: Request):
    return RedirectResponse("/campaign", status_code=307)

# =============================================================================
# Start lokal
# =============================================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("master:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=False)
