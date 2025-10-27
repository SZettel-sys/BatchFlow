# master.py
import os
import re
import io
import sys
import time
from typing import Optional, Dict, List, Tuple, AsyncGenerator

import numpy as np
import pandas as pd
import httpx
import asyncpg

from fastapi import FastAPI, Request, Body, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse
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
FIELD_FACHBEREICH_HINT = "fachbereich"     # Feld in Pipedrive (Person)
# FIELD_ORGART_HINT bleibt ungenutzt (Regel entfernt)

# UI/Defaults
DEFAULT_CHANNEL = "Cold E-Mail"              # fix laut Wunsch
COLD_MAILING_IMPORT_LABEL = "Cold-Mailing Import"

# Performance
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "500"))
RECONCILE_MAX_ROWS = int(os.getenv("RECONCILE_MAX_ROWS", "10000"))
OPTIONS_TTL_SEC = int(os.getenv("OPTIONS_TTL_SEC", "900"))
PER_ORG_DEFAULT_LIMIT = int(os.getenv("PER_ORG_DEFAULT_LIMIT", "2"))

# OAuth Tokens (einfach)
user_tokens: Dict[str, str] = {}

# =============================================================================
# Template-Spalten (Excel-Export)
# =============================================================================
# >>> Passe diese Liste auf deine ZIEL-Datei an (Reihenfolge & Namen) <<<
TEMPLATE_COLUMNS = [
    # Kern-Header aus deiner Beschreibung + gängige Felder
    "Batch ID",
    "Channel",
    COLD_MAILING_IMPORT_LABEL,
    "Person - Prospect ID",
    "Person - Organisation",
    "Organisation - ID",
    "Person - Geschlecht",
    "Person - Titel",
    "Person - Vorname",
    "Person - Nachname",
    "Person - Position",
    "Person - ID",
    "Person - XING-Profil",
    "Person - LinkedIn Profil-URL",
    "Person - E-Mail-Adresse - Büro",
    # zusätzliche, oft gewünschte:
    "E-Mail",           # wir liefern hierhin die primäre E-Mail, wenn -Büro nicht vorhanden
    "Fachbereich",
]

# Mapping „Feld-Hinweis in Pipedrive“ → Zielspalte
# (wir suchen nach Person-Feldern, deren Name den Hint enthält; case-insensitive)
PERSON_FIELD_HINTS_TO_EXPORT = {
    "prospect": "Person - Prospect ID",
    "organisation": "Person - Organisation",        # selten als Personen-Custom-Feld gepflegt
    "org id": "Organisation - ID",                  # falls es ein Custom-Feld auf Person gibt
    "gender": "Person - Geschlecht",
    "geschlecht": "Person - Geschlecht",
    "titel": "Person - Titel",
    "position": "Person - Position",
    "xing": "Person - XING-Profil",
    "linkedin": "Person - LinkedIn Profil-URL",
    "email büro": "Person - E-Mail-Adresse - Büro",
    "email buero": "Person - E-Mail-Adresse - Büro",
    "e-mail büro": "Person - E-Mail-Adresse - Büro",
    "e-mail buero": "Person - E-Mail-Adresse - Büro",
}

# =============================================================================
# Startup: HTTP-Client + DB-Pool
# =============================================================================
def http_client() -> httpx.AsyncClient:
    return app.state.http  # type: ignore[attr-defined]

def get_pool() -> asyncpg.Pool:
    return app.state.pool  # type: ignore[attr-defined]

@app.on_event("startup")
async def _startup():
    limits = httpx.Limits(max_keepalive_connections=16, max_connections=24)
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

def split_name(first_name: Optional[str], last_name: Optional[str], full_name: Optional[str]) -> Tuple[str, str]:
    fn = first_name or ""
    ln = last_name or ""
    if fn or ln:
        return fn, ln
    n = (full_name or "").strip()
    if not n:
        return "", ""
    parts = n.split()
    if len(parts) == 1:
        return parts[0], ""
    return " ".join(parts[:-1]), parts[-1]

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

# =============================================================================
# Pipedrive fetcher (streamend) + Performance: early stop
# =============================================================================
async def stream_persons_by_filter(
    filter_id: int,
    page_limit: int = PAGE_LIMIT,
) -> AsyncGenerator[List[dict], None]:
    start = 0
    while True:
        url = append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={page_limit}&sort=name")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive API Fehler: {r.text}")
        data = r.json().get("data") or []
        if not data:
            break
        yield data
        if len(data) < page_limit:
            break
        start += page_limit

async def fetch_persons_until_match(
    filter_id: int,
    predicate,                      # (person) -> bool
    per_org_limit: int,
    max_collect: Optional[int] = None,
) -> List[dict]:
    org_used: Dict[str, int] = {}
    out: List[dict] = []
    async for chunk in stream_persons_by_filter(filter_id):
        for p in chunk:
            if not predicate(p):
                continue
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
            if used >= per_org_limit:
                continue
            org_used[org_key] = used + 1
            out.append(p)
            if max_collect and len(out) >= max_collect:
                return out
    return out

async def stream_counts_with_org_cap(
    filter_id: int,
    fachbereich_key: str,
    per_org_limit: int,
    page_limit: int = PAGE_LIMIT,
) -> Tuple[int, Dict[str, int]]:
    total = 0
    counts: Dict[str, int] = {}
    used_by_fb: Dict[str, Dict[str, int]] = {}
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
            fb_val = p.get(fachbereich_key)
            if isinstance(fb_val, np.ndarray):
                fb_val = fb_val.tolist()
            if isinstance(fb_val, list):
                fb_vals = [str(fb_val[0])] if fb_val else []
            elif fb_val is not None and str(fb_val).strip() != "":
                fb_vals = [str(fb_val)]
            else:
                fb_vals = []
            if not fb_vals:
                continue

            org_key = None
            org = p.get("org_id")
            if isinstance(org, dict):
                if org.get("id") is not None:
                    org_key = f"id:{org.get('id')}"
                elif org.get("name"):
                    org_key = f"name:{normalize_name(org.get('name'))}"
            if not org_key:
                org_key = f"noorg:{p.get('id')}"

            fb = fb_vals[0]
            used_map = used_by_fb.setdefault(fb, {})
            used = used_map.get(org_key, 0)
            if used >= per_org_limit:
                continue
            used_map[org_key] = used + 1
            counts[fb] = counts.get(fb, 0) + 1
            total += 1

        if len(chunk) < page_limit:
            break
        start += page_limit

    return total, counts

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
# Optionen-Cache
# =============================================================================
_OPTIONS_CACHE: Dict[int, dict] = {}

# =============================================================================
# UI – Kampagnenwahl
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
  :root{--bg:#f6f8fb;--card:#fff;--txt:#0f172a;--muted:#64748b;--border:#e2e8f0;--primary:#0ea5e9;--primary-h:#0284c7}
  *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--txt);font:16px/1.6 Inter,-apple-system,Segoe UI,Roboto,Arial,sans-serif}
  header{background:#fff;border-bottom:1px solid var(--border)}
  .hwrap{max-width:1120px;margin:0 auto;padding:16px 20px;display:flex;align-items:center;gap:12px}
  .brand{font-weight:700;font-size:18px}
  .sub{color:var(--muted)}
  main{max-width:1120px;margin:32px auto;padding:0 20px}
  .grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:20px}
  @media (max-width:1120px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
  @media (max-width:800px){.grid{grid-template-columns:1fr}}
  .card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:22px;box-shadow:0 2px 8px rgba(2,8,23,.04)}
  .title{font-weight:700;font-size:18px;margin:2px 0 6px}
  .desc{color:var(--muted);min-height:48px}
  .btn{display:inline-flex;align-items:center;gap:8px;padding:10px 14px;border-radius:10px;background:var(--primary);color:#fff;text-decoration:none}
  .btn:hover{background:var(--primary-h)}
</style></head>
<body>
<header><div class="hwrap"><div class="brand">BatchFlow</div><div class="sub">Kampagne auswählen</div></div></header>
<main>
  <div class="grid">
    <div class="card"><div class="title">Neukontakte</div><div class="desc">Neue Personen aus Filter, Abgleich & Export.</div><a class="btn" href="/neukontakte?mode=new">Starten</a></div>
    <div class="card"><div class="title">Nachfass</div><div class="desc">Folgekampagne für bereits kontaktierte Leads.</div><a class="btn" href="/neukontakte?mode=nachfass">Starten</a></div>
    <div class="card"><div class="title">Refresh</div><div class="desc">Kontaktdaten aktualisieren / ergänzen.</div><a class="btn" href="/neukontakte?mode=refresh">Starten</a></div>
  </div>
</main>
</body></html>"""
    return HTMLResponse(html)

# =============================================================================
# UI – Auswahl
# =============================================================================
@app.get("/neukontakte", response_class=HTMLResponse)
async def neukontakte(request: Request, mode: str = Query("new")):
    authed = bool(user_tokens.get("default") or PD_API_TOKEN)
    title_map = {"new":"Neukontakte","nachfass":"Nachfass","refresh":"Refresh"}
    page_title = title_map.get(mode, "Neukontakte")

    html = f"""<!doctype html><html lang="de">
<head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{page_title} – BatchFlow</title>
<style>
  :root{{--bg:#f6f8fb;--card:#fff;--txt:#0f172a;--muted:#64748b;--border:#e2e8f0;--primary:#0ea5e9;--primary-h:#0284c7}}
  *{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--txt);font:16px/1.6 Inter,-apple-system,Segoe UI,Roboto,Arial,sans-serif}}
  header{{background:#fff;border-bottom:1px solid var(--border)}}
  .hwrap{{max-width:1120px;margin:0 auto;padding:14px 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
  main{{max-width:1120px;margin:28px auto;padding:0 20px}}
  .card{{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:20px;box-shadow:0 2px 8px rgba(2,8,23,.04)}}
  label{{display:block;font-weight:600;margin:8px 0 6px}}
  select,input{{width:100%;padding:10px 12px;border:1px solid #cbd5e1;border-radius:10px;background:#fff}}
  .hint{{color:var(--muted);font-size:13px;margin-top:6px}}
  .rowTop{{display:grid;grid-template-columns:260px 1fr;gap:16px;margin-bottom:12px}}
  .row{{display:grid;grid-template-columns:repeat(4,1fr) 200px 200px;gap:16px;align-items:end}}
  @media (max-width:1120px){{.row{{grid-template-columns:repeat(2,1fr)}} .row>div:nth-last-child(-n+2){{grid-column:1/-1;display:flex;gap:10px;justify-content:flex-end}}}}
  .btn{{background:var(--primary);border:none;color:#fff;border-radius:10px;padding:12px 16px;cursor:pointer}}
  .btn.secondary{{background:#334155}} .btn.secondary:hover{{opacity:.95}}
  .btn:disabled{{opacity:.5;cursor:not-allowed}}
  .btn:hover:not(:disabled){{background:var(--primary-h)}}
  #overlay{{display:none;position:fixed;inset:0;background:rgba(255,255,255,.6);backdrop-filter:blur(2px);z-index:9999;align-items:center;justify-content:center}}
  .spinner{{width:44px;height:44px;border:4px solid #93c5fd;border-top-color:#1d4ed8;border-radius:50%;animation:spin 1s linear infinite}}
  @keyframes spin{{to{{transform:rotate(360deg)}}}}
</style>
</head>
<body>
<header>
  <div class="hwrap">
    <div><a href="/campaign" style="color:#0a66c2;text-decoration:none">← Kampagne wählen</a></div>
    <div><b>{page_title}</b> · Gesamt: <b id="total-count">lädt…</b></div>
    <div>{"<span style='color:#64748b'>angemeldet</span>" if authed else "<a href='/login'>Anmelden</a>"}</div>
  </div>
</header>

<main>
  <section class="card">
    <div class="rowTop">
      <div>
        <label>Kontakte pro Organisation</label>
        <select id="per_org_limit">
          <option value="1">1</option>
          <option value="2" selected>2</option>
        </select>
      </div>
      <div>
        <label>Fachbereich</label>
        <select id="fachbereich"><option value="">– bitte auswählen –</option></select>
        <div class="hint" id="fbinfo">Die Zahl in Klammern berücksichtigt bereits „Kontakte pro Organisation“.</div>
      </div>
    </div>

    <div class="row">
      <div>
        <label>Batch ID</label>
        <input id="batch_id" placeholder="Bxxx"/>
        <div class="hint">Beispiel: B111</div>
      </div>
      <div>
        <label>Kampagnenname</label>
        <input id="campaign" placeholder="z. B. Herbstkampagne"/>
      </div>
      <div>
        <label>Wie viele Datensätze nehmen?</label>
        <input type="number" id="take_count" placeholder="z. B. 900" min="1"/>
        <div class="hint">Leer lassen = alle Datensätze des gewählten Fachbereichs.</div>
      </div>
      <div></div>
      <div style="justify-self:end">
        <button class="btn" id="btnPreview" disabled>Vorschau laden</button>
      </div>
      <div style="justify-self:end">
        <button class="btn secondary" id="btnExport" disabled>Abgleich & Download</button>
      </div>
    </div>
  </section>
</main>

<div id="overlay"><div class="spinner"></div></div>

<script>
const MODE = new URLSearchParams(location.search).get('mode') || 'new';
const fbSel = document.getElementById('fachbereich');
const btnPrev = document.getElementById('btnPreview');
const btnExp  = document.getElementById('btnExport');

function toggleCTAs(){ const ok = !!fbSel.value; btnPrev.disabled = !ok; btnExp.disabled = !ok; }
fbSel.addEventListener('change', toggleCTAs);

function showOverlay(){ document.getElementById("overlay").style.display="flex"; }
function hideOverlay(){ document.getElementById("overlay").style.display="none"; }

async function loadOptions(){
  showOverlay();
  try{
    const pol = document.getElementById('per_org_limit').value || '{PER_ORG_DEFAULT_LIMIT}';
    const r = await fetch('/neukontakte/options?per_org_limit=' + encodeURIComponent(pol) + '&mode=' + encodeURIComponent(MODE), {cache:'no-store'});
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const data = await r.json();
    const sel = document.getElementById('fachbereich');
    sel.innerHTML = '<option value="">– bitte auswählen –</option>';
    data.options.forEach(o => {
      const opt = document.createElement('option');
      opt.value = o.value;
      opt.textContent = o.label + ' (' + o.count + ')';
      sel.appendChild(opt);
    });
    document.getElementById('fbinfo').textContent = "Gesamt (nach Orga-Limit): " + data.total + " · Fachbereiche: " + data.options.length;
    document.getElementById('total-count').textContent = String(data.total);
    toggleCTAs();
  }catch(e){ alert('Fehler beim Laden der Fachbereiche: ' + e); }
  finally{ hideOverlay(); }
}

document.getElementById('per_org_limit').addEventListener('change', loadOptions);

btnPrev.addEventListener('click', async () => {
  const fb  = document.getElementById('fachbereich').value;
  const tc  = document.getElementById('take_count').value || null;
  const bid = document.getElementById('batch_id').value || null;
  const camp= document.getElementById('campaign').value || null;
  const pol = document.getElementById('per_org_limit').value || '{PER_ORG_DEFAULT_LIMIT}';
  if(!fb){ alert('Bitte zuerst einen Fachbereich wählen.'); return; }
  showOverlay();
  try{
    const r = await fetch('/neukontakte/preview?mode=' + encodeURIComponent(MODE), {
      method:'POST', headers:{'Content-Type':'application/json'}, cache:'no-store',
      body: JSON.stringify({ fachbereich: fb, take_count: tc ? parseInt(tc) : null, batch_id: bid, campaign: camp, per_org_limit: parseInt(pol) })
    });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const html = await r.text(); document.open(); document.write(html); document.close();
  }catch(e){ alert('Fehler: ' + e); } finally{ hideOverlay(); }
});

btnExp.addEventListener('click', async () => {
  const fb  = document.getElementById('fachbereich').value;
  const tc  = document.getElementById('take_count').value || null;
  const bid = document.getElementById('batch_id').value || null;
  const camp= document.getElementById('campaign').value || null;
  const pol = document.getElementById('per_org_limit').value || '{PER_ORG_DEFAULT_LIMIT}';
  if(!fb){ alert('Bitte zuerst einen Fachbereich wählen.'); return; }
  showOverlay();
  try{
    const r = await fetch('/neukontakte/export?mode=' + encodeURIComponent(MODE), {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ fachbereich: fb, take_count: tc ? parseInt(tc) : null, batch_id: bid, campaign: camp, per_org_limit: parseInt(pol) })
    });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const blob = await r.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = 'BatchFlow_Export.xlsx'; a.click();
    URL.revokeObjectURL(url);
  }catch(e){ alert('Fehler: ' + e); } finally{ hideOverlay(); }
});

loadOptions();
</script>
</body></html>"""
    return HTMLResponse(html)

# =============================================================================
# Optionen (Fachbereichswerte)
# =============================================================================
@app.get("/neukontakte/options")
async def neukontakte_options(per_org_limit: int = Query(PER_ORG_DEFAULT_LIMIT, ge=1, le=2), mode: str = Query("new")):
    now = time.time()
    cache = _OPTIONS_CACHE.get(per_org_limit) or {}
    if cache.get("options") and (now - cache.get("ts", 0.0) < OPTIONS_TTL_SEC):
        return JSONResponse({"total": cache["total"], "options": cache["options"]})

    fb_field = await get_person_field_by_hint(FIELD_FACHBEREICH_HINT)
    if not fb_field:
        return JSONResponse({"total": 0, "options": []})
    fb_key = fb_field.get("key")
    id2label = field_options_id_to_label_map(fb_field)

    total, counts = await stream_counts_with_org_cap(FILTER_NEUKONTAKTE, fb_key, per_org_limit)

    options = []
    for opt_id, cnt in counts.items():
        label = id2label.get(str(opt_id), str(opt_id))
        options.append({"value": opt_id, "label": label, "count": cnt})
    options.sort(key=lambda x: x["count"], reverse=True)

    _OPTIONS_CACHE[per_org_limit] = {"ts": now, "total": total, "options": options}
    return JSONResponse({"total": total, "options": options})

# =============================================================================
# Vorschau – nk_master_final
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
        fb_field = await get_person_field_by_hint(FIELD_FACHBEREICH_HINT)
        if not fb_field:
            return HTMLResponse("<div style='padding:24px;color:#b00'>❌ 'Fachbereich'-Feld nicht gefunden.</div>", 500)
        fb_key = fb_field.get("key")
        id2label = field_options_id_to_label_map(fb_field)
        fb_label = id2label.get(str(fachbereich), str(fachbereich))

        # Personen-Feld-Mapping (HINTS → Pipedrive-Key)
        person_fields = await get_person_fields()
        hint_to_key: Dict[str, str] = {}
        for f in person_fields:
            nm = (f.get("name") or "").lower()
            for hint, col in PERSON_FIELD_HINTS_TO_EXPORT.items():
                if hint in nm and hint not in hint_to_key:
                    hint_to_key[hint] = f.get("key")

        def _match(p: dict) -> bool:
            val = p.get(fb_key)
            if isinstance(val, np.ndarray):
                val = val.tolist()
            if isinstance(val, list):
                return str(fachbereich) in [str(x) for x in val if x is not None]
            return str(val) == str(fachbereich)

        raw = await fetch_persons_until_match(
            FILTER_NEUKONTAKTE, _match, per_org_limit,
            max_collect=take_count if take_count and take_count > 0 else None
        )

        def get_field(p: dict, hint: str) -> str:
            key = hint_to_key.get(hint)
            if not key:
                return ""
            v = p.get(key)
            if isinstance(v, dict) and "label" in v:
                return str(v.get("label") or "")
            if isinstance(v, list):
                # Mehrfachfeld: erste sinnvolle Repräsentation
                if v and isinstance(v[0], dict) and "value" in v[0]:
                    return str(v[0].get("value") or "")
                return ", ".join([str(x) for x in v if x])
            return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)

        rows = []
        for p in raw[: (take_count or len(raw))]:
            pid = p.get("id")
            first = p.get("first_name")
            last = p.get("last_name")
            full = p.get("name")
            vor, nach = split_name(first, last, full)
            emails = _as_list_email(p.get("email"))
            email_primary = emails[0] if emails else ""
            org_name, org_id = "-", ""
            org = p.get("org_id")
            if isinstance(org, dict):
                org_name = org.get("name") or "-"
                if org.get("id") is not None:
                    org_id = str(org.get("id"))

            row = {
                "Batch ID": batch_id or "",
                "Channel": DEFAULT_CHANNEL,
                COLD_MAILING_IMPORT_LABEL: campaign or "",
                "Person - Prospect ID": get_field(p, "prospect"),
                "Person - Organisation": org_name,
                "Organisation - ID": org_id,
                "Person - Geschlecht": get_field(p, "gender") or get_field(p, "geschlecht"),
                "Person - Titel": get_field(p, "titel"),
                "Person - Vorname": vor,
                "Person - Nachname": nach,
                "Person - Position": get_field(p, "position"),
                "Person - ID": str(pid or ""),
                "Person - XING-Profil": get_field(p, "xing"),
                "Person - LinkedIn Profil-URL": get_field(p, "linkedin"),
                "Person - E-Mail-Adresse - Büro": get_field(p, "email büro") or get_field(p, "email buero"),
                "E-Mail": email_primary,
                "Fachbereich": fb_label,
            }

            rows.append(row)

        df = pd.DataFrame(rows)
        await save_df_text(df, "nk_master_final")

        preview_table = (df.head(50).to_html(classes="grid", index=False, border=0)
                         if not df.empty else "<i>Keine Daten</i>")

        html = f"""<!doctype html><html lang="de"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Vorschau – {fb_label}</title>
<style>
  :root{{--bg:#f6f8fb;--card:#fff;--txt:#0f172a;--muted:#64748b;--border:#e2e8f0;--primary:#0ea5e9;--primary-h:#0284c7}}
  *{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--txt);font:16px/1.6 Inter,-apple-system,Segoe UI,Roboto,Arial,sans-serif}}
  header{{background:#fff;border-bottom:1px solid var(--border)}}
  .hwrap{{max-width:1120px;margin:0 auto;padding:14px 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
  main{{max-width:1120px;margin:24px auto;padding:0 20px}}
  .card{{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:18px;box-shadow:0 2px 8px rgba(2,8,23,.04)}}
  .btn{{background:var(--primary);border:none;color:#fff;border-radius:10px;padding:10px 14px;cursor:pointer;text-decoration:none;display:inline-flex;gap:8px;align-items:center}}
  .btn:hover{{background:var(--primary-h)}}
  .btn.secondary{{background:#334155}} .btn.secondary:hover{{opacity:.95}}
  table{{width:100%;border-collapse:separate;border-spacing:0;border:1px solid var(--border);border-radius:12px;overflow:hidden}}
  th,td{{padding:10px 12px;border-bottom:1px solid var(--border);text-align:left;vertical-align:top}}
  th{{background:#f8fafc;font-weight:700}}
  tr:last-child td{{border-bottom:0}}
  .toolbar{{display:flex;gap:10px;justify-content:flex-end;margin-top:14px}}
</style></head>
<body>
<header>
  <div class="hwrap">
    <div><a href="/neukontakte" style="color:#0a66c2;text-decoration:none">← zurück zur Auswahl</a></div>
    <div><b>Vorschau</b> – Fachbereich: <b>{fb_label}</b> · Batch: <b>{batch_id or "-"}</b> · Zeilen: <b>{len(df)}</b></div>
    <div style="color:#64748b">Datensätze nach Orga-Limit</div>
  </div>
</header>

<main>
  <section class="card">
    {preview_table}
    <div class="toolbar">
      <a class="btn secondary" href="/neukontakte">Zurück</a>
      <button class="btn" id="btnReconcile">Abgleich durchführen</button>
    </div>
  </section>
</main>

<script>
  document.getElementById('btnReconcile').addEventListener('click', async () => {{
    const r = await fetch('/neukontakte/reconcile', {{method:'POST'}});
    const html = await r.text();
    document.open(); document.write(html); document.close();
  }});
</script>
</body></html>"""
        return HTMLResponse(html)
    except Exception as e:
        return HTMLResponse(f"<pre style='padding:24px;color:#b00'>❌ Fehler beim Abruf/Speichern:\n{e}</pre>", status_code=500)

# =============================================================================
# Organisationen & Personen-IDs (für Abgleich)
# =============================================================================
async def stream_organizations_by_filter(filter_id: int, page_limit: int = PAGE_LIMIT):
    start = 0
    while True:
        url = append_token(f"{PIPEDRIVE_API}/organizations?filter_id={filter_id}&start={start}&limit={page_limit}")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive API Fehler (Orgs {filter_id}): {r.text}")
        chunk = r.json().get("data") or []
        if not chunk:
            break
        yield chunk
        if len(chunk) < page_limit:
            break
        start += page_limit

async def stream_person_ids_by_filter(filter_id: int, page_limit: int = PAGE_LIMIT):
    start = 0
    while True:
        url = append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={page_limit}&sort=id")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive API Fehler (Persons {filter_id}): {r.text}")
        data = r.json().get("data") or []
        if not data:
            break
        yield [str(p.get("id")) for p in data if p.get("id") is not None]
        if len(data) < page_limit:
            break
        start += page_limit

# =============================================================================
# Abgleich – schreibt nk_master_ready & nk_delete_log (ohne Organisationsart-Regel)
# =============================================================================
async def _reconcile_impl() -> HTMLResponse:
    master = await load_df_text("nk_master_final")
    if master.empty:
        return HTMLResponse("<div style='padding:24px'>Keine Daten in nk_master_final.</div>")

    if len(master) > RECONCILE_MAX_ROWS:
        return HTMLResponse(
            f"<div style='padding:24px;color:#b00'>❌ Zu viele Datensätze ({len(master)}). "
            f"Bitte auf ≤ {RECONCILE_MAX_ROWS} begrenzen.</div>", status_code=400
        )

    col_person_id = "Person - ID" if "Person - ID" in master.columns else ("id" if "id" in master.columns else None)
    col_org_name = "Person - Organisation" if "Person - Organisation" in master.columns else ("Organisation" if "Organisation" in master.columns else None)

    delete_log: List[Dict[str, str]] = []

    # Regel – Orga-Abgleich (Filter 1245, 851, 1521) >=95%
    suspect_org_filters = [1245, 851, 1521]
    if col_org_name and col_org_name in master.columns:
        ext_buckets: Dict[str, List[str]] = {}
        for fid in suspect_org_filters:
            async for chunk in stream_organizations_by_filter(fid, PAGE_LIMIT):
                for o in chunk:
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
                drop_idx.append(idx)
                delete_log.append({"reason":"org_match_95",
                                   "id":str(row.get(col_person_id) or ""),
                                   "name":f"{row.get('Person - Vorname') or ''} {row.get('Person - Nachname') or ''}".strip(),
                                   "org_name":cand,"extra":f"Best Match: {best[0]} ({best[1]}%)"})
        if drop_idx:
            master = master.drop(index=drop_idx)

    # Regel – Personen-ID-Abgleich (Filter 1216, 1708)
    suspect_person_filters = [1216, 1708]
    if col_person_id:
        suspect_ids = set()
        for fid in suspect_person_filters:
            async for ids in stream_person_ids_by_filter(fid, PAGE_LIMIT):
                suspect_ids.update(ids)
        if suspect_ids:
            mask_pid = master[col_person_id].astype(str).isin(suspect_ids)
            removed = master[mask_pid].copy()
            for _, r in removed.iterrows():
                delete_log.append({"reason":"person_id_match",
                                   "id":str(r.get(col_person_id) or ""),
                                   "name":f"{r.get('Person - Vorname') or ''} {r.get('Person - Nachname') or ''}".strip(),
                                   "org_name":str(r.get(col_org_name) or ""),
                                   "extra":"ID in Filter 1216/1708"})
            master = master[~mask_pid].copy()

    await save_df_text(master, "nk_master_ready")
    log_df = pd.DataFrame(delete_log, columns=["reason","id","name","org_name","extra"])
    await save_df_text(log_df, "nk_delete_log")

    stats = {
        "after": len(master),
        "del_orgmatch": log_df[log_df["reason"]=="org_match_95"].shape[0],
        "del_pid": log_df[log_df["reason"]=="person_id_match"].shape[0],
        "deleted_total": log_df.shape[0],
    }
    table_html = log_df.head(50).to_html(classes="grid", index=False, border=0) if not log_df.empty else "<i>keine</i>"

    html = f"""<!doctype html><html lang="de"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Abgleich – Ergebnis</title>
<style>
  :root{{--bg:#f6f8fb;--card:#fff;--txt:#0f172a;--muted:#64748b;--border:#e2e8f0;--primary:#0ea5e9;--primary-h:#0284c7}}
  *{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--txt);font:16px/1.6 Inter,-apple-system,Segoe UI,Roboto,Arial,sans-serif}}
  header{{background:#fff;border-bottom:1px solid var(--border)}}
  .hwrap{{max-width:1120px;margin:0 auto;padding:14px 20px;display:flex;align-items:center;justify-content:space-between;gap:12px}}
  main{{max-width:1120px;margin:24px auto;padding:0 20px}}
  .card{{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:18px;box-shadow:0 2px 8px rgba(2,8,23,.04);margin-bottom:16px}}
  .gridStats{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}}
  .kpi{{border:1px solid var(--border);border-radius:12px;padding:14px;background:#fafbff}}
  .kpi b{{font-size:20px}}
  .btn{{background:var(--primary);border:none;color:#fff;border-radius:10px;padding:10px 14px;cursor:pointer;text-decoration:none;display:inline-flex;gap:8px;align-items:center}}
  .btn.secondary{{background:#334155}} .btn.secondary:hover{{opacity:.95}}
  table{{width:100%;border-collapse:separate;border-spacing:0;overflow:hidden;border-radius:12px;border:1px solid var(--border)}}
  th,td{{padding:10px 12px;border-bottom:1px solid var(--border);text-align:left;vertical-align:top}}
  th{{background:#f8fafc;font-weight:700}}
  tr:last-child td{{border-bottom:0}}
</style></head>
<body>
<header>
  <div class="hwrap">
    <div><b>Abgleich abgeschlossen</b></div>
    <div class="muted">Ergebnis in <b>nk_master_ready</b> · Log in <b>nk_delete_log</b></div>
  </div>
</header>

<main>
  <section class="card">
    <div class="gridStats">
      <div class="kpi"><div class="muted">Übrig</div><b>{stats["after"]}</b></div>
      <div class="kpi"><div class="muted">Orga-Match ≥95%</div><b>{stats["del_orgmatch"]}</b></div>
      <div class="kpi"><div class="muted">Person-ID in 1216/1708</div><b>{stats["del_pid"]}</b></div>
    </div>
    <div style="margin-top:12px"><a class="btn secondary" href="/neukontakte">Neue Auswahl</a></div>
  </section>

  <section class="card">
    <h3 style="margin:0 0 10px">Entfernte Datensätze (Top 50)</h3>
    {table_html}
  </section>
</main>
</body></html>"""
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
# Abgleich & Excel-Export (Direkt-Button) – Template-orientiert
# =============================================================================
def build_export_from_ready(master_ready: pd.DataFrame) -> pd.DataFrame:
    # Master-DF → in Template-Spalten bringen (fehlende Spalten leer)
    out = pd.DataFrame(columns=TEMPLATE_COLUMNS)
    for col in TEMPLATE_COLUMNS:
        if col in master_ready.columns:
            out[col] = master_ready[col]
        else:
            out[col] = ""  # fehlendes Feld leer
    # Falls „E-Mail“ leer und „Person - E-Mail-Adresse - Büro“ gefüllt → übernehmen
    mask = (out.get("E-Mail", pd.Series(dtype=str)) == "") & (out.get("Person - E-Mail-Adresse - Büro", pd.Series(dtype=str)) != "")
    if mask.any():
        out.loc[mask, "E-Mail"] = out.loc[mask, "Person - E-Mail-Adresse - Büro"]
    return out

@app.post("/neukontakte/export")
async def neukontakte_export(
    fachbereich: str = Body(...),
    take_count: Optional[int] = Body(None),
    batch_id: Optional[str] = Body(None),
    campaign: Optional[str] = Body(None),
    per_org_limit: int = Body(PER_ORG_DEFAULT_LIMIT),
    mode: str = Query("new"),
):
    # Auswahl + nk_master_final
    _ = await neukontakte_preview(fachbereich, take_count, batch_id, campaign, per_org_limit, mode)
    # Abgleich
    _ = await _reconcile_impl()
    # Export
    ready = await load_df_text("nk_master_ready")
    export_df = build_export_from_ready(ready)
    buf = io.BytesIO()
    export_df.to_excel(buf, index=False, sheet_name="Export")
    buf.seek(0)
    headers = {"Content-Disposition": "attachment; filename=BatchFlow_Export.xlsx"}
    return StreamingResponse(buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers=headers)

# =============================================================================
# OAuth
# =============================================================================
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
    return RedirectResponse("/campaign")

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
