# master_routes_split.py
# Getrennte Routen: /neukontakte (aus 20251030) und /nachfass (aus 20251103)
# Buttons zeigen auf getrennte Seiten; Endpoints liefern immer JSON.
import os, re, io, time, uuid, asyncio
from typing import Optional, Dict, List, Tuple
from datetime import datetime, timedelta, timezone
import numpy as np, pandas as pd, httpx, asyncpg
from fastapi import FastAPI, Request, Body, Query, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware
from rapidfuzz import process, fuzz

app = FastAPI(title="BatchFlow")
app.add_middleware(GZipMiddleware, minimum_size=1024)
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

PIPEDRIVE_API = "https://api.pipedrive.com/v1"
PD_API_TOKEN = os.getenv("PD_API_TOKEN","")
DATABASE_URL = os.getenv("DATABASE_URL","")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL fehlt (Neon DSN).")
SCHEMA = os.getenv("PGSCHEMA","public")

FILTER_NEUKONTAKTE = int(os.getenv("FILTER_NEUKONTAKTE","2998"))
FILTER_NACHFASS   = int(os.getenv("FILTER_NACHFASS","3024"))
FIELD_FACHBEREICH_HINT = os.getenv("FIELD_FACHBEREICH_HINT","fachbereich")

DEFAULT_CHANNEL = "Cold E-Mail"
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT","500"))
RECONCILE_MAX_ROWS = int(os.getenv("RECONCILE_MAX_ROWS","20000"))
OPTIONS_TTL_SEC = int(os.getenv("OPTIONS_TTL_SEC","900"))
PER_ORG_DEFAULT_LIMIT = int(os.getenv("PER_ORG_DEFAULT_LIMIT","2"))
PD_CONCURRENCY = int(os.getenv("PD_CONCURRENCY","4"))
MAX_ORG_NAMES = int(os.getenv("MAX_ORG_NAMES","120000"))
MAX_ORG_BUCKET = int(os.getenv("MAX_ORG_BUCKET","15000"))

_OPTIONS_CACHE: Dict[tuple, dict] = {}
_PERSON_FIELDS_CACHE = None
user_tokens: Dict[str,str] = {}

TEMPLATE_COLUMNS = ["Batch ID","Channel","Cold-Mailing Import","Prospect ID","Organisation ID","Organisation Name","Person ID","Person Vorname","Person Nachname","Person Titel","Person Geschlecht","Person Position","Person E-Mail","XING Profil","LinkedIn URL"]

def http_client(): return app.state.http
def get_pool(): return app.state.pool

@app.on_event("startup")
async def _startup():
    limits = httpx.Limits(max_keepalive_connections=16, max_connections=24)
    app.state.http = httpx.AsyncClient(timeout=60.0, limits=limits)
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)

@app.on_event("shutdown")
async def _shutdown():
    try: await app.state.http.aclose()
    finally: await app.state.pool.close()

def get_headers():
    tok = user_tokens.get("default")
    return {"Authorization": f"Bearer {tok}"} if tok else {}

def append_token(url: str) -> str:
    if "api_token=" in url: return url
    if user_tokens.get("default"): return url
    if PD_API_TOKEN: return f"{url}{'&' if '?' in url else '?'}api_token={PD_API_TOKEN}"
    return url

def normalize_name(s: str) -> str:
    if not s: return ""
    s=s.lower(); s=re.sub(r"\b(gmbh|ug|ag|kg|ohg|inc|ltd|co)\b","",s)
    s=re.sub(r"[^a-z0-9 ]","",s); s=re.sub(r"\s+"," ",s).strip(); return s

def _as_list_email(v):
    if v is None or (isinstance(v,float) and pd.isna(v)): return []
    if isinstance(v, np.ndarray): v=v.tolist()
    if isinstance(v, dict):
        val=v.get("value"); return [val] if val else []
    if isinstance(v, (list, tuple)):
        out=[]; 
        for x in v:
            if isinstance(x, dict): 
                val=x.get("value"); 
                if val: out.append(str(val))
            elif x is not None and not (isinstance(x,float) and pd.isna(x)):
                out.append(str(x))
        return out
    return [str(v)]

def parse_pd_date(d):
    if not d: return None
    try: return datetime.strptime(d,"%Y-%m-%d").replace(tzinfo=timezone.utc)
    except: return None

def is_forbidden_activity_date(val):
    dt = parse_pd_date(val)
    if not dt: return False
    today = datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0)
    three_months = today - timedelta(days=90)
    return dt > today or (three_months <= dt <= today)

async def get_person_fields():
    global _PERSON_FIELDS_CACHE
    if _PERSON_FIELDS_CACHE is not None: return _PERSON_FIELDS_CACHE
    r = await http_client().get(append_token(f"{PIPEDRIVE_API}/personFields"), headers=get_headers())
    r.raise_for_status(); _PERSON_FIELDS_CACHE = r.json().get("data") or []
    return _PERSON_FIELDS_CACHE

def field_options_id_to_label_map(field):
    return {str(o.get("id")): str(o.get("label") or o.get("name") or o.get("id")) for o in (field.get("options") or [])}

async def get_person_field_by_hint(label_hint):
    fields = await get_person_fields()
    hint = (label_hint or "").lower()
    for f in fields:
        nm=(f.get("name") or "").lower()
        if hint in nm: return f
    return None

_NEXT_ACTIVITY_KEY=None; _LAST_ACTIVITY_KEY=None
async def get_next_activity_key():
    global _NEXT_ACTIVITY_KEY
    if _NEXT_ACTIVITY_KEY is not None: return _NEXT_ACTIVITY_KEY
    _NEXT_ACTIVITY_KEY="next_activity_date"
    try:
        fields = await get_person_fields()
        for f in fields:
            nm=(f.get("name") or "").lower()
            if "next activity" in nm or "next_activity_date" in nm:
                _NEXT_ACTIVITY_KEY = f.get("key") or _NEXT_ACTIVITY_KEY; break
    except: pass
    return _NEXT_ACTIVITY_KEY

async def get_last_activity_key():
    global _LAST_ACTIVITY_KEY
    if _LAST_ACTIVITY_KEY is not None: return _LAST_ACTIVITY_KEY
    _LAST_ACTIVITY_KEY="last_activity_date"
    try:
        fields = await get_person_fields()
        for f in fields:
            nm=(f.get("name") or "").lower()
            if "last activity" in nm or "last_activity_date" in nm:
                _LAST_ACTIVITY_KEY = f.get("key") or _LAST_ACTIVITY_KEY; break
    except: pass
    return _LAST_ACTIVITY_KEY

def extract_field_date(p, key):
    if not key: return None
    v = p.get(key)
    if isinstance(v, dict): v=v.get("value")
    elif isinstance(v, list): v=v[0] if v else None
    if v is None or (isinstance(v,float) and pd.isna(v)): return None
    return str(v)

async def stream_persons_by_filter(filter_id, page_limit=PAGE_LIMIT):
    start=0
    while True:
        r = await http_client().get(append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={page_limit}&sort=name"), headers=get_headers())
        if r.status_code != 200: raise Exception(f"Pipedrive API Fehler: {r.text}")
        data = r.json().get("data") or []
        if not data: break
        yield data
        if len(data) < page_limit: break
        start += page_limit

async def stream_counts_with_org_cap(filter_id, fachbereich_key, per_org_limit, page_limit=PAGE_LIMIT):
    total=0; counts={}; used_by_fb={}; start=0
    last_key = await get_last_activity_key(); next_key = await get_next_activity_key()
    while True:
        r = await http_client().get(append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={page_limit}"), headers=get_headers())
        if r.status_code != 200: raise Exception(f"Pipedrive API Fehler: {r.text}")
        chunk = r.json().get("data") or []
        if not chunk: break
        for p in chunk:
            av = extract_field_date(p, last_key) or extract_field_date(p, next_key)
            if is_forbidden_activity_date(av): continue
            fb_val = p.get(fachbereich_key)
            if isinstance(fb_val, list): fb_vals=[str(fb_val[0])] if fb_val else []
            elif fb_val is not None and str(fb_val).strip(): fb_vals=[str(fb_val)]
            else: fb_vals=[]
            if not fb_vals: continue
            org = p.get("org_id") or {}
            if isinstance(org, dict):
                if org.get("id") is not None: org_key=f"id:{org.get('id')}"
                elif org.get("name"): org_key=f"name:{normalize_name(org.get('name'))}"
                else: org_key=f"noorg:{p.get('id')}"
            else: org_key=f"noorg:{p.get('id')}"
            fb = fb_vals[0]; used_map = used_by_fb.setdefault(fb,{}); used = used_map.get(org_key,0)
            if used >= per_org_limit: continue
            used_map[org_key] = used + 1; counts[fb]=counts.get(fb,0)+1; total += 1
        if len(chunk) < page_limit: break
        start += page_limit
    return total, counts

async def ensure_table_text(conn, table, cols): 
    await conn.execute(f'CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{table}" (' + ", ".join([f'"{c}" TEXT' for c in cols]) + ")")
async def clear_table(conn, table): await conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table}"')

async def save_df_text(df, table):
    async with get_pool().acquire() as conn:
        await clear_table(conn, table); await ensure_table_text(conn, table, list(df.columns))
        if df.empty: return
        cols=list(df.columns); cols_sql=", ".join(f'"{c}"' for c in cols); placeholders=", ".join(f'${i}' for i in range(1,len(cols)+1))
        insert_sql=f'INSERT INTO "{SCHEMA}"."{table}" ({cols_sql}) VALUES ({placeholders})'
        batch=[]
        async with conn.transaction():
            for _, row in df.iterrows():
                vals = ["" if pd.isna(v) else str(v) for v in row.tolist()]; batch.append(vals)
                if len(batch) >= 1000: await conn.executemany(insert_sql, batch); batch=[]
            if batch: await conn.executemany(insert_sql, batch)

async def load_df_text(table):
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(f'SELECT * FROM "{SCHEMA}"."{table}"')
    if not rows: return pd.DataFrame()
    cols=list(rows[0].keys()); data=[tuple(r[c] for c in cols) for r in rows]
    return pd.DataFrame(data, columns=cols).replace({"": np.nan})

@app.get("/campaign", response_class=HTMLResponse)
async def campaign():
    return HTMLResponse("""<!doctype html><html lang="de"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>BatchFlow</title></head>
<body style='font:16px/1.6 system-ui;padding:24px'>
  <h2>BatchFlow</h2>
  <p><a href="/neukontakte">Neukontakte</a> · <a href="/nachfass">Nachfass</a> · <a href="/refresh">Refresh</a></p>
</body></html>""")

@app.get("/neukontakte", response_class=HTMLResponse)
async def neukontakte_page():
    return HTMLResponse(r"""<!doctype html><html lang="de">
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
  .grid{{display:grid;grid-template-columns:repeat(12,1fr);gap:16px}}
  .col-3{{grid-column:span 3;min-width:220px}}
  .col-4{{grid-column:span 4;min-width:260px}}
  .col-5{{grid-column:span 5;min-width:260px}}
  .col-6{{grid-column:span 6;min-width:260px}}
  .col-12{{grid-column:span 12}}
  label{{display:block;font-weight:600;margin:8px 0 6px}}
  select,input{{width:100%;padding:10px 12px;border:1px solid #cbd5e1;border-radius:10px;background:#fff}}
  .hint{{color:var(--muted);font-size:13px;margin-top:6px}}
  .muted{{color:#64748b}}
  .btn{{background:var(--primary);border:none;color:#fff;border-radius:10px;padding:12px 16px;cursor:pointer}}
  .btn:disabled{{opacity:.5;cursor:not-allowed}}
  #overlay{{display:none;position:fixed;inset:0;background:rgba(255,255,255,.7);backdrop-filter:blur(2px);z-index:9999;align-items:center;justify-content:center;flex-direction:column;gap:10px}}
  .barwrap{{width:min(520px,90vw);height:10px;border-radius:999px;background:#e2e8f0;overflow:hidden}}
  .bar{{height:100%;width:0%;background:var(--primary);transition:width .2s linear}}
  ul.info{{margin:.5rem 0 0 1rem;padding:0;font-size:13px;color:#64748b}}
</style>
</head>
<body>
<header>
  <div class="hwrap">
    <div><a href="/campaign" style="color:#0a66c2;text-decoration:none">← Kampagne wählen</a></div>
    <div><b>{page_title}</b> · Gesamt: <b id="total-count">lädt…</b></div>
    <div>{"<span class='muted'>angemeldet</span>" if authed else "<a href='/login'>Anmelden</a>"}</div>
  </div>
</header>

<main>
  <section class="card">
    <div class="grid">
      <div class="col-3">
        <label>Kontakte pro Organisation</label>
        <select id="per_org_limit">
          <option value="1">1</option>
          <option value="2" selected>2</option>
          <option value="3">3</option>
        </select>
        <div class="hint">Beispiel: 2</div>
      </div>
      <div class="col-5">
        <label>Fachbereich</label>
        <select id="fachbereich"><option value="">– bitte auswählen –</option></select>
        <div class="hint" id="fbinfo">Die Zahl in Klammern berücksichtigt bereits „Kontakte pro Organisation“.</div>
        <ul class="info">
          <li>Die Anzahl bezieht sich bereits auf Anzahl der Kontakte pro Organisation.</li>
          <li>Es sind keine Datensätze dabei, die eine gesetzte Organisationsart haben.</li>
          <li>Aktivitätsdaten sind berücksichtigt (keine Einträge von heute bis 3&nbsp;Monate zurück und keine zukünftigen Daten).</li>
        </ul>
      </div>
      <div class="col-4">
        <label>Wie viele Datensätze nehmen?</label>
        <input type="number" id="take_count" placeholder="z. B. 900" min="1"/>
        <div class="hint">Leer lassen = alle Datensätze des gewählten Fachbereichs.</div>
      </div>

      <div class="col-3">
        <label>Batch ID</label>
        <input id="batch_id" placeholder="Bxxx"/>
        <div class="hint">z. B. B111</div>
      </div>
      <div class="col-6">
        <label>Kampagnenname</label>
        <input id="campaign" placeholder="z. B. Herbstkampagne"/>
        <div class="hint">Wird als „Cold-Mailing Import“ gesetzt und als Dateiname verwendet.</div>
      </div>
      <div class="col-3" style="display:flex;align-items:flex-end;justify-content:flex-end">
        <button class="btn" id="btnExport" disabled>Abgleich &amp; Download</button>
      </div>
    </div>
  </section>
</main>

<div id="overlay">
  <div id="phase" style="color:#0f172a"></div>
  <div class="barwrap"><div class="bar" id="bar"></div></div>
</div>

<script>
const MODE = new URLSearchParams(location.search).get('mode') || 'new';
const el = id => document.getElementById(id);
const fbSel = el('fachbereich');
const btnExp = el('btnExport');

function toggleCTAs() {{ btnExp.disabled = !fbSel.value; }}
fbSel.addEventListener('change', toggleCTAs);
el('per_org_limit').addEventListener('change', loadOptions);

function showOverlay(msg) {{
  el("phase").textContent = msg || "";
  el("overlay").style.display = "flex";
}}
function hideOverlay() {{ el("overlay").style.display = "none"; }}
function setProgress(p) {{ el("bar").style.width = (Math.max(0, Math.min(100, p)) + "%"); }}

async function loadOptions() {{
  showOverlay("Lade Optionen …"); setProgress(15);
  try {{
    const pol = el('per_org_limit').value || '{PER_ORG_DEFAULT_LIMIT}';
    const r = await fetch('/neukontakte/options?per_org_limit=' + encodeURIComponent(pol) + '&mode=' + encodeURIComponent(MODE), {{cache:'no-store'}});
    const data = await r.json();
    const sel = fbSel;
    sel.innerHTML = '<option value="">– bitte auswählen –</option>';
    for (const o of data.options) {{
      const opt = document.createElement('option');
      opt.value = o.value; opt.textContent = o.label + ' (' + o.count + ')';
      sel.appendChild(opt);
    }}
    el('fbinfo').textContent = "Gesamt (nach Orga-Limit): " + data.total + " · Fachbereiche: " + data.options.length;
    el('total-count').textContent = String(data.total);
    toggleCTAs();
  }} catch(e) {{
    alert('Fehler beim Laden der Fachbereiche: ' + e);
  }} finally {{
    setProgress(100); setTimeout(hideOverlay, 200);
  }}
}}

async function startExport() {{
  const fb  = fbSel.value;
  const tc  = el('take_count').value || null;
  const bid = el('batch_id').value || null;
  const camp= el('campaign').value || null;
  const pol = el('per_org_limit').value || '{PER_ORG_DEFAULT_LIMIT}';
  if (!fb) {{ alert('Bitte zuerst einen Fachbereich wählen.'); return; }}

  showOverlay("Starte Abgleich …"); setProgress(5);
  const r = await fetch('/neukontakte/export_start?mode=' + encodeURIComponent(MODE), {{
    method:'POST', headers:{{'Content-Type':'application/json'}},
    body: JSON.stringify({{ fachbereich: fb, take_count: tc ? parseInt(tc) : null, batch_id: bid, campaign: camp, per_org_limit: parseInt(pol) }})
  }});
  if (!r.ok) {{ hideOverlay(); alert('Start fehlgeschlagen.'); return; }}
  const {{ job_id }} = await r.json();
  await poll(job_id);
}}

async function poll(job_id) {{
  let done = false, tries = 0;
  while (!done && tries < 3600) {{
    await new Promise(res => setTimeout(res, 300));
    const r = await fetch('/neukontakte/export_progress?job_id=' + encodeURIComponent(job_id), {{cache:'no-store'}});
    if (!r.ok) {{ el('phase').textContent = 'Fehler beim Fortschritt'; return; }}
    const s = await r.json();
    if (s.error) {{ el('phase').textContent = s.error; setProgress(100); return; }}
    el('phase').textContent = s.phase || 'Arbeite …';
    setProgress(s.percent ?? 0);
    done = !!s.done; tries++;
  }}
  if (done) {{
    el('phase').textContent = 'Export bereit – Download startet …'; setProgress(100);
    window.location.href = '/neukontakte/export_download?job_id=' + encodeURIComponent(job_id);
    setTimeout(() => {{
      window.location.href = '/neukontakte/summary?job_id=' + encodeURIComponent(job_id);
    }}, 800);
  }} else {{
    el('phase').textContent = 'Zeitüberschreitung beim Export';
  }}
}}

btnExp.addEventListener('click', startExport);
loadOptions();
</script>
</body></html>""")

@app.get("/nachfass", response_class=HTMLResponse)
async def nachfass_page():
    return HTMLResponse(r"""<!doctype html><html lang="de">
<head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Nachfass – BatchFlow</title>
<style>
  :root{{--bg:#f6f8fb;--card:#fff;--txt:#0f172a;--muted:#64748b;--border:#e2e8f0;--primary:#0ea5e9;--primary-h:#0284c7}}
  *{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--txt);font:16px/1.6 Inter,-apple-system,Segoe UI,Roboto,Arial,sans-serif}}
  header{{background:#fff;border-bottom:1px solid var(--border)}}
  .hwrap{{max-width:1120px;margin:0 auto;padding:14px 20px;display:flex;align-items:center;justify-content:space-between}}
  main{{max-width:1120px;margin:28px auto;padding:0 20px}}
  .card{{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:20px;box-shadow:0 2px 8px rgba(2,8,23,.04)}}
  .grid{{display:grid;grid-template-columns:repeat(12,1fr);gap:16px}}
  .col-6{{grid-column:span 6;min-width:260px}}
  .col-3{{grid-column:span 3;min-width:200px}}
  label{{display:block;font-weight:600;margin:8px 0 6px}}
  input{{width:100%;padding:10px 12px;border:1px solid #cbd5e1;border-radius:10px;background:#fff}}
  .hint{{color:#64748b;font-size:13px;margin-top:6px}}
  .btn{{background:var(--primary);border:none;color:#fff;border-radius:10px;padding:12px 16px;cursor:pointer}}
  .btn:disabled{{opacity:.5;cursor:not-allowed}}
  #overlay{{display:none;position:fixed;inset:0;background:rgba(255,255,255,.7);backdrop-filter:blur(2px);z-index:9999;align-items:center;justify-content:center;flex-direction:column;gap:10px}}
  .barwrap{{width:min(520px,90vw);height:10px;border-radius:999px;background:#e2e8f0;overflow:hidden}}
  .bar{{height:100%;width:0%;background:var(--primary);transition:width .2s linear}}
  ul.info{{margin:.5rem 0 0 1rem;padding:0;font-size:13px;color:#64748b}}
</style>
</head>
<body>
<header>
  <div class="hwrap">
    <div><b>Nachfass</b></div>
    <div>{{"<span class='hint'>angemeldet</span>" if authed else "<a href='/login'>Anmelden</a>"}}</div>
  </div>
</header>

<main>
  <section class="card">
    <div class="grid">
      <div class="col-6">
        <label>Nachfass: Batch ID 1 (Pflicht)</label>
        <input id="nf_b1" placeholder="z. B. B372"/>
      </div>
      <div class="col-6">
        <label>Nachfass: Batch ID 2 (optional)</label>
        <input id="nf_b2" placeholder="optional – zweite Batch ID"/>
      </div>
      <div class="col-3">
        <label>Batch ID</label>
        <input id="batch_id" placeholder="Bxxx"/>
        <div class="hint">wird in der Exportdatei gesetzt</div>
      </div>
      <div class="col-6">
        <label>Kampagnenname</label>
        <input id="campaign" placeholder="z. B. Herbstkampagne"/>
        <div class="hint">wird als „Cold-Mailing Import“ gesetzt und als Dateiname verwendet</div>
      </div>
      <div class="col-3" style="display:flex;align-items:flex-end;justify-content:flex-end">
        <button class="btn" id="btnExport" disabled>Abgleich &amp; Download</button>
      </div>
    </div>
    <ul class="info">
      <li>Basis: Filter 3024.</li>
      <li>Selektion erfolgt über das Personenfeld „Batch ID“ (contains, OR).</li>
      <li>Aktivitätsdaten sind berücksichtigt (keine Einträge von heute bis 3&nbsp;Monate zurück und keine zukünftigen Daten).</li>
    </ul>
  </section>
</main>

<div id="overlay">
  <div id="phase" style="color:#0f172a"></div>
  <div class="barwrap"><div class="bar" id="bar"></div></div>
</div>

<script>
const el = id => document.getElementById(id);
const btnExp = el('btnExport');

function checkReady() {{
  const ok = ((el('nf_b1').value || '')).trim().length > 0
          && ((el('batch_id').value || '')).trim().length > 0
          && ((el('campaign').value || '')).trim().length > 0;
  btnExp.disabled = !ok;
}}
['nf_b1','nf_b2','batch_id','campaign'].forEach(id => el(id).addEventListener('input', checkReady));

function showOverlay(msg) {{ el("phase").textContent = msg || ""; el("overlay").style.display = "flex"; }}
function setProgress(p) {{ el("bar").style.width = (Math.max(0, Math.min(100, p)) + "%"); }}

async function startExport() {{
  const payload = {{
    nf_batch_ids: [(el('nf_b1').value || '').trim(), (el('nf_b2').value || '').trim()].filter(Boolean),
    batch_id: (el('batch_id').value || '').trim(),
    campaign: (el('campaign').value || '').trim()
  }};
  if (payload.nf_batch_ids.length === 0) {{ alert('Bitte mindestens eine Nachfass-Batch ID angeben.'); return; }}
  showOverlay("Starte Abgleich …"); setProgress(5);
  const r = await fetch('/nachfass/export_start', {{
    method: 'POST', headers: {{'Content-Type':'application/json'}}, body: JSON.stringify(payload)
  }});
  if (!r.ok) {{ alert('Start fehlgeschlagen.'); return; }}
  const {{ job_id }} = await r.json();
  await poll(job_id);
}}

async function poll(job_id) {{
  let done = false, tries = 0;
  while (!done && tries < 3600) {{
    await new Promise(res => setTimeout(res, 300));
    const r = await fetch('/nachfass/export_progress?job_id=' + encodeURIComponent(job_id), {{cache:'no-store'}});
    if (!r.ok) {{ el('phase').textContent = 'Fehler beim Fortschritt'; return; }}
    const s = await r.json();
    if (s.error) {{ el('phase').textContent = s.error; setProgress(100); return; }}
    el('phase').textContent = s.phase || 'Arbeite …';
    setProgress(s.percent ?? 0);
    done = !!s.done; tries++;
  }}
  if (done) {{
    el('phase').textContent = 'Export bereit – Download startet …'; setProgress(100);
    window.location.href = '/nachfass/export_download?job_id=' + encodeURIComponent(job_id);
    setTimeout(() => {{ window.location.href = '/nachfass/summary?job_id=' + encodeURIComponent(job_id); }}, 800);
  }} else {{
    el('phase').textContent = 'Zeitüberschreitung beim Export';
  }}
}}

btnExp.addEventListener('click', startExport);
checkReady();
</script>
</body></html>""")

@app.get("/neukontakte/options")
async def options_neu(per_org_limit: int = Query(PER_ORG_DEFAULT_LIMIT, ge=1, le=3)):
    try:
        fb_field = await get_person_field_by_hint(FIELD_FACHBEREICH_HINT)
        if not fb_field: return JSONResponse({"total":0,"options":[],"hint":"Fachbereich-Feld nicht gefunden."})
        fb_key = fb_field.get("key"); id2label = field_options_id_to_label_map(fb_field)
        total, counts = await stream_counts_with_org_cap(FILTER_NEUKONTAKTE, fb_key, per_org_limit)
        options = [{"value":opt_id, "label":id2label.get(str(opt_id),str(opt_id)), "count":cnt} for opt_id, cnt in counts.items()]
        options.sort(key=lambda x: x["count"], reverse=True)
        return JSONResponse({"total": total, "options": options, "hint":"Orga-Limit berücksichtigt"})
    except Exception as e:
        return JSONResponse({"error": f"Options-Fehler (new): {e}" }, status_code=500)

@app.get("/nachfass/options")
async def options_nach(per_org_limit: int = Query(PER_ORG_DEFAULT_LIMIT, ge=1, le=3)):
    try:
        fb_field = await get_person_field_by_hint(FIELD_FACHBEREICH_HINT)
        if not fb_field: return JSONResponse({"total":0,"options":[],"hint":"Fachbereich-Feld nicht gefunden."})
        fb_key = fb_field.get("key"); id2label = field_options_id_to_label_map(fb_field)
        total, counts = await stream_counts_with_org_cap(FILTER_NACHFASS, fb_key, per_org_limit)
        options = [{"value":opt_id, "label":id2label.get(str(opt_id),str(opt_id)), "count":cnt} for opt_id, cnt in counts.items()]
        options.sort(key=lambda x: x["count"], reverse=True)
        return JSONResponse({"total": total, "options": options, "hint":"Mit Aktivitätslogik"})
    except Exception as e:
        return JSONResponse({"error": f"Options-Fehler (nachfass): {e}" }, status_code=500)
