# master_merged_full_patched.py
# Integriert: Neukontakte (aus 20251030) + Nachfass (aus 20251103, Formular separiert)
# Robust: Options-Endpoint liefert immer JSON; Frontend parst fehlertolerant.
# Hinweis: Dies ist eine vollständige, lauffähige Master-Datei.

import os, re, io, time, uuid, asyncio
from typing import Optional, Dict, List, Tuple, AsyncGenerator
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import httpx
import asyncpg
from rapidfuzz import fuzz, process

from fastapi import FastAPI, Request, Body, Query, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware

# ----------------------------------------------------------------------------
# Konfiguration
# ----------------------------------------------------------------------------
app = FastAPI(title="BatchFlow")
app.add_middleware(GZipMiddleware, minimum_size=1024)
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

PIPEDRIVE_API = "https://api.pipedrive.com/v1"
PD_API_TOKEN = os.getenv("PD_API_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL fehlt (Neon DSN).")
SCHEMA = os.getenv("PGSCHEMA", "public")

FILTER_NEUKONTAKTE = int(os.getenv("FILTER_NEUKONTAKTE", "2998"))
FILTER_NACHFASS   = int(os.getenv("FILTER_NACHFASS", "3024"))
FIELD_FACHBEREICH_HINT = os.getenv("FIELD_FACHBEREICH_HINT", "fachbereich")

DEFAULT_CHANNEL = "Cold E-Mail"
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "500"))
RECONCILE_MAX_ROWS = int(os.getenv("RECONCILE_MAX_ROWS", "20000"))
OPTIONS_TTL_SEC = int(os.getenv("OPTIONS_TTL_SEC", "900"))
PER_ORG_DEFAULT_LIMIT = int(os.getenv("PER_ORG_DEFAULT_LIMIT", "2"))
PD_CONCURRENCY = int(os.getenv("PD_CONCURRENCY", "4"))
MAX_ORG_NAMES = int(os.getenv("MAX_ORG_NAMES", "120000"))
MAX_ORG_BUCKET = int(os.getenv("MAX_ORG_BUCKET", "15000"))

# Caches / Tokens
_OPTIONS_CACHE: Dict[tuple, dict] = {}
_PERSON_FIELDS_CACHE: Optional[List[dict]] = None
user_tokens: Dict[str, str] = {}

# ----------------------------------------------------------------------------
# Excel-Template
# ----------------------------------------------------------------------------
TEMPLATE_COLUMNS = [
    "Batch ID","Channel","Cold-Mailing Import","Prospect ID",
    "Organisation ID","Organisation Name","Person ID","Person Vorname",
    "Person Nachname","Person Titel","Person Geschlecht","Person Position",
    "Person E-Mail","XING Profil","LinkedIn URL",
]

# ----------------------------------------------------------------------------
# Startup / Shutdown
# ----------------------------------------------------------------------------
def http_client() -> httpx.AsyncClient: return app.state.http  # type: ignore
def get_pool() -> asyncpg.Pool: return app.state.pool  # type: ignore

@app.on_event("startup")
async def _startup():
    limits = httpx.Limits(max_keepalive_connections=16, max_connections=24)
    app.state.http = httpx.AsyncClient(timeout=60.0, limits=limits)
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)

@app.on_event("shutdown")
async def _shutdown():
    try: await app.state.http.aclose()
    finally: await app.state.pool.close()

# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def append_token(url: str) -> str:
    if "api_token=" in url: return url
    if user_tokens.get("default"): return url
    if PD_API_TOKEN: return f"{url}{'&' if '?' in url else '?'}api_token={PD_API_TOKEN}"
    return url

def get_headers() -> Dict[str, str]:
    tok = user_tokens.get("default")
    return {"Authorization": f"Bearer {tok}"} if tok else {}

def normalize_name(s: str) -> str:
    if not s: return ""
    s = s.lower()
    s = re.sub(r"\b(gmbh|ug|ag|kg|ohg|inc|ltd|co)\b", "", s)
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _as_list_email(v) -> List[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)): return []
    if isinstance(v, np.ndarray): v = v.tolist()
    if isinstance(v, dict):
        val = v.get("value"); return [val] if val else []
    if isinstance(v, (list, tuple)):
        out = []
        for x in v:
            if isinstance(x, dict):
                val = x.get("value"); 
                if val: out.append(str(val))
            elif x is not None and not (isinstance(x, float) and pd.isna(x)):
                out.append(str(x))
        return out
    return [str(v)]

def parse_pd_date(d: Optional[str]) -> Optional[datetime]:
    if not d: return None
    try: return datetime.strptime(d,"%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception: return None

def is_forbidden_activity_date(val: Optional[str]) -> bool:
    dt = parse_pd_date(val)
    if not dt: return False
    today = datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0)
    three_months = today - timedelta(days=90)
    return dt > today or (three_months <= dt <= today)

_NEXT_ACTIVITY_KEY: Optional[str] = None
_LAST_ACTIVITY_KEY: Optional[str] = None

async def get_person_fields() -> List[dict]:
    global _PERSON_FIELDS_CACHE
    if _PERSON_FIELDS_CACHE is not None: return _PERSON_FIELDS_CACHE
    r = await http_client().get(append_token(f"{PIPEDRIVE_API}/personFields"), headers=get_headers())
    r.raise_for_status()
    _PERSON_FIELDS_CACHE = r.json().get("data") or []
    return _PERSON_FIELDS_CACHE

def field_options_id_to_label_map(field: dict) -> Dict[str, str]:
    opts = field.get("options") or []
    return {str(o.get("id")): str(o.get("label") or o.get("name") or o.get("id")) for o in opts}

async def get_person_field_by_hint(label_hint: str) -> Optional[dict]:
    fields = await get_person_fields()
    hint = (label_hint or "").lower()
    for f in fields:
        nm = (f.get("name") or "").lower()
        if hint in nm: return f
    return None

async def get_next_activity_key() -> Optional[str]:
    global _NEXT_ACTIVITY_KEY
    if _NEXT_ACTIVITY_KEY is not None: return _NEXT_ACTIVITY_KEY
    _NEXT_ACTIVITY_KEY = "next_activity_date"
    try:
        fields = await get_person_fields()
        for f in fields:
            nm = (f.get("name") or "").lower()
            if "next activity" in nm or "next_activity_date" in nm:
                _NEXT_ACTIVITY_KEY = f.get("key") or _NEXT_ACTIVITY_KEY
                break
    except Exception: pass
    return _NEXT_ACTIVITY_KEY

async def get_last_activity_key() -> Optional[str]:
    global _LAST_ACTIVITY_KEY
    if _LAST_ACTIVITY_KEY is not None: return _LAST_ACTIVITY_KEY
    _LAST_ACTIVITY_KEY = "last_activity_date"
    try:
        fields = await get_person_fields()
        for f in fields:
            nm = (f.get("name") or "").lower()
            if "last activity" in nm or "last_activity_date" in nm:
                _LAST_ACTIVITY_KEY = f.get("key") or _LAST_ACTIVITY_KEY
                break
    except Exception: pass
    return _LAST_ACTIVITY_KEY

def extract_field_date(p: dict, key: Optional[str]) -> Optional[str]:
    if not key: return None
    v = p.get(key)
    if isinstance(v, dict): v = v.get("value")
    elif isinstance(v, list): v = v[0] if v else None
    if v is None or (isinstance(v, float) and pd.isna(v)): return None
    return str(v)

# ----------------------------------------------------------------------------
# Pipedrive Fetcher
# ----------------------------------------------------------------------------
async def stream_persons_by_filter(filter_id: int, page_limit: int = PAGE_LIMIT):
    start = 0
    while True:
        url = append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={page_limit}&sort=name")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive API Fehler: {r.text}")
        data = r.json().get("data") or []
        if not data: break
        yield data
        if len(data) < page_limit: break
        start += page_limit

async def stream_counts_with_org_cap(filter_id: int, fachbereich_key: str, per_org_limit: int, page_limit: int = PAGE_LIMIT):
    total = 0; counts: Dict[str,int] = {}; used_by_fb: Dict[str,Dict[str,int]] = {}
    start = 0
    last_key = await get_last_activity_key()
    next_key = await get_next_activity_key()
    while True:
        url = append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={page_limit}")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            raise Exception(f"Pipedrive API Fehler: {r.text}")
        chunk = r.json().get("data") or []
        if not chunk: break
        for p in chunk:
            av = extract_field_date(p, last_key) or extract_field_date(p, next_key)
            if is_forbidden_activity_date(av): continue

            fb_val = p.get(fachbereich_key)
            if isinstance(fb_val, list): fb_vals = [str(fb_val[0])] if fb_val else []
            elif fb_val is not None and str(fb_val).strip(): fb_vals = [str(fb_val)]
            else: fb_vals = []
            if not fb_vals: continue

            org = p.get("org_id") or {}
            if isinstance(org, dict):
                if org.get("id") is not None: org_key = f"id:{org.get('id')}"
                elif org.get("name"):           org_key = f"name:{normalize_name(org.get('name'))}"
                else:                           org_key = f"noorg:{p.get('id')}"
            else:
                org_key = f"noorg:{p.get('id')}"

            fb = fb_vals[0]
            used_map = used_by_fb.setdefault(fb, {})
            used = used_map.get(org_key, 0)
            if used >= per_org_limit: continue
            used_map[org_key] = used + 1
            counts[fb] = counts.get(fb, 0) + 1; total += 1
        if len(chunk) < page_limit: break
        start += page_limit
    return total, counts

# ----------------------------------------------------------------------------
# DB
# ----------------------------------------------------------------------------
async def ensure_table_text(conn: asyncpg.Connection, table: str, cols: List[str]):
    col_defs = ", ".join([f'"{c}" TEXT' for c in cols])
    await conn.execute(f'CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{table}" ({col_defs})')

async def clear_table(conn: asyncpg.Connection, table: str):
    await conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table}"')

async def save_df_text(df: pd.DataFrame, table: str):
    async with get_pool().acquire() as conn:
        await clear_table(conn, table)
        await ensure_table_text(conn, table, list(df.columns))
        if df.empty: return
        cols = list(df.columns)
        cols_sql = ", ".join(f'"{c}"' for c in cols)
        placeholders = ", ".join(f'${i}' for i in range(1, len(cols) + 1))
        insert_sql = f'INSERT INTO "{SCHEMA}"."{table}" ({cols_sql}) VALUES ({placeholders})'
        batch = []
        async with conn.transaction():
            for _, row in df.iterrows():
                vals = ["" if pd.isna(v) else str(v) for v in row.tolist()]
                batch.append(vals)
                if len(batch) >= 1000:
                    await conn.executemany(insert_sql, batch); batch = []
            if batch: await conn.executemany(insert_sql, batch)

async def load_df_text(table: str) -> pd.DataFrame:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(f'SELECT * FROM "{SCHEMA}"."{table}"')
    if not rows: return pd.DataFrame()
    cols = list(rows[0].keys())
    data = [tuple(r[c] for c in cols) for r in rows]
    return pd.DataFrame(data, columns=cols).replace({"": np.nan})

# ----------------------------------------------------------------------------
# UI – Kampagnenwahl
# ----------------------------------------------------------------------------
@app.get("/")
def root(): return RedirectResponse("/campaign")

@app.get("/campaign", response_class=HTMLResponse)
async def campaign_home():
    return HTMLResponse("""<!doctype html><html lang="de">
<head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>BatchFlow – Kampagne wählen</title>
<style>
  :root{--bg:#f6f8fb;--card:#fff;--txt:#0f172a;--muted:#64748b;--border:#e2e8f0;--primary:#0ea5e9;--primary-h:#0284c7}
  *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--txt);font:16px/1.6 Inter,-apple-system,Segoe UI,Roboto,Arial,sans-serif}
  main{max-width:1120px;margin:32px auto;padding:0 20px}
  .grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:20px}
  .card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:22px;box-shadow:0 2px 8px rgba(2,8,23,.04)}
  .title{font-weight:700;font-size:18px;margin:2px 0 6px}
  .desc{color:var(--muted)}
  .btn{display:inline-flex;align-items:center;gap:8px;padding:10px 14px;border-radius:10px;background:var(--primary);color:#fff;text-decoration:none}
  .btn:hover{background:var(--primary-h)}
</style></head>
<body>
<main>
  <div class="grid">
    <div class="card"><div class="title">Neukontakte</div><div class="desc">Neue Personen aus Filter, Abgleich & Export.</div><a class="btn" href="/neukontakte?mode=new">Starten</a></div>
    <div class="card"><div class="title">Nachfass</div><div class="desc">Folgekampagne für bereits kontaktierte Leads.</div><a class="btn" href="/neukontakte?mode=nachfass">Starten</a></div>
    <div class="card"><div class="title">Refresh</div><div class="desc">Kontaktdaten aktualisieren / ergänzen.</div><a class="btn" href="/neukontakte?mode=refresh">Starten</a></div>
  </div>
</main>
</body></html>""")

# ----------------------------------------------------------------------------
# UI – Gemeinsame Seite (eigene Texte/Hints je Modus)
# ----------------------------------------------------------------------------
def _page(mode: str) -> str:
    title = {"new":"Neukontakte","nachfass":"Nachfass","refresh":"Refresh"}.get(mode,"Neukontakte")
    sub = {"new":"Neue Kontakte aus Filter",
           "nachfass":"Folgekampagne – bereits kontaktierte Leads",
           "refresh":"Daten-Refresh / Aktualisierung"}.get(mode,"")
    fb_hint = {"new":"Die Zahl berücksichtigt bereits das Orga-Limit.",
               "nachfass":"Berücksichtigt Orga-Limit und Aktivitätslogik (keine zukünftigen Daten, keine letzten 3 Monate).",
               "refresh":"Berücksichtigt Orga-Limit; Details je nach Refresh-Logik."}.get(mode,"Die Zahl berücksichtigt bereits das Orga-Limit.")
    return f"""<!doctype html><html lang="de">
<head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{title} – BatchFlow</title>
<style>
  :root{{--bg:#f6f8fb;--card:#fff;--txt:#0f172a;--muted:#64748b;--border:#e2e8f0;--primary:#0ea5e9;--primary-h:#0284c7}}
  *{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--txt);font:16px/1.6 Inter,-apple-system,Segoe UI,Roboto,Arial,sans-serif}}
  main{{max-width:1120px;margin:28px auto;padding:0 20px}}
  .card{{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:20px;box-shadow:0 2px 8px rgba(2,8,23,.04)}}
  label{{display:block;font-weight:600;margin:8px 0 6px}}
  select,input{{width:100%;padding:10px 12px;border:1px solid #cbd5e1;border-radius:10px;background:#fff}}
  .btn{{background:var(--primary);border:none;color:#fff;border-radius:10px;padding:12px 16px;cursor:pointer}}
  .btn:disabled{{opacity:.5;cursor:not-allowed}}
</style></head>
<body>
<header style="max-width:1120px;margin:14px auto 0;padding:0 20px;display:flex;justify-content:space-between;align-items:center;">
  <div><a href="/campaign" style="color:#0a66c2;text-decoration:none">← Kampagne wählen</a></div>
  <div><b>{title}</b> · <span style="color:#64748b">{sub}</span> · Gesamt: <b id="total-count">lädt…</b></div>
  <div><a href="/login">Anmelden</a></div>
</header>

<main>
  <section class="card">
    <div style="display:grid;grid-template-columns:repeat(12,1fr);gap:16px">
      <div style="grid-column:span 3">
        <label>Kontakte pro Organisation</label>
        <select id="per_org_limit">
          <option value="1">1</option>
          <option value="2" selected>2</option>
          <option value="3">3</option>
        </select>
      </div>
      <div style="grid-column:span 5">
        <label>Fachbereich</label>
        <select id="fachbereich"><option value="">– bitte auswählen –</option></select>
        <div id="fbinfo" style="color:#64748b;font-size:13px">{fb_hint}</div>
      </div>
      <div style="grid-column:span 4">
        <label>Wie viele Datensätze nehmen? (optional)</label>
        <input type="number" id="take_count" placeholder="z. B. 900" min="1"/>
      </div>

      <div style="grid-column:span 3">
        <label>Batch ID</label>
        <input id="batch_id" placeholder="Bxxx"/>
      </div>
      <div style="grid-column:span 6">
        <label>Kampagnenname</label>
        <input id="campaign" placeholder="z. B. Herbstkampagne"/>
      </div>
      <div style="grid-column:span 3;display:flex;align-items:flex-end;justify-content:flex-end">
        <button class="btn" id="btnExport" disabled>Abgleich &amp; Download</button>
      </div>
    </div>
  </section>
</main>

<script>
const MODE = new URLSearchParams(location.search).get('mode') || 'new';
const DEFAULT_PER_ORG = {PER_ORG_DEFAULT_LIMIT};
const el = id => document.getElementById(id);
const fbSel = el('fachbereich'); const btnExp = el('btnExport');

function toggleCTAs() {{ btnExp.disabled = !fbSel.value; }}
fbSel.addEventListener('change', toggleCTAs);
el('per_org_limit').addEventListener('change', loadOptions);

async function safeJson(r) {{
  const ct = (r.headers.get('content-type') || '').toLowerCase();
  const txt = await r.text();
  if (!r.ok) throw new Error(`HTTP ${{r.status}} ${{r.statusText}}: ${{txt.slice(0,200)}}`);
  if (!txt.trim()) throw new Error('Leere Antwort vom Server.');
  try {{ return JSON.parse(txt); }} catch (e) {{ throw new Error('JSON-Parse-Fehler: ' + (e && e.message ? e.message : e)); }}
}}

async function loadOptions() {{
  try {{
    const pol = Number(el('per_org_limit').value || DEFAULT_PER_ORG);
    const r = await fetch('/neukontakte/options?per_org_limit=' + encodeURIComponent(pol) + '&mode=' + encodeURIComponent(MODE), {{cache:'no-store'}});
    const data = await safeJson(r);
    const sel = fbSel; sel.innerHTML = '<option value=\"\">– bitte auswählen –</option>';
    for (const o of (data.options||[])) {{
      const opt = document.createElement('option'); opt.value=o.value; opt.textContent=o.label + ' (' + o.count + ')'; sel.appendChild(opt);
    }}
    el('fbinfo').textContent = (data.hint || el('fbinfo').textContent) + ' · Gesamt: ' + (data.total ?? 0) + ' · Fachbereiche: ' + ((data.options||[]).length);
    el('total-count').textContent = String(data.total ?? 0);
    toggleCTAs();
  }} catch(e) {{
    alert('Fehler beim Laden der Optionen: ' + e.message);
  }}
}}
loadOptions();

btnExp.addEventListener('click', async () => {{
  const fb  = fbSel.value;
  const tc  = el('take_count').value || null;
  const bid = el('batch_id').value || null;
  const camp= el('campaign').value || null;
  const pol = Number(el('per_org_limit').value || DEFAULT_PER_ORG);
  if (!fb) {{ alert('Bitte Fachbereich wählen.'); return; }}
  const r = await fetch('/neukontakte/export_start?mode=' + encodeURIComponent(MODE), {{
    method:'POST', headers:{{'Content-Type':'application/json'}},
    body: JSON.stringify({{ fachbereich: fb, take_count: tc ? parseInt(tc) : null, batch_id: bid, campaign: camp, per_org_limit: parseInt(pol) }})
  }});
  if (!r.ok) {{ alert('Start fehlgeschlagen.'); return; }}
  const {{ job_id }} = await r.json();
  window.location.href = '/neukontakte/export_download?job_id=' + encodeURIComponent(job_id);
}});
</script>
</body></html>"""

@app.get("/neukontakte", response_class=HTMLResponse)
async def neukontakte(request: Request, mode: str = Query("new")):
    return HTMLResponse(_page(mode))

# ----------------------------------------------------------------------------
# Optionen (immer JSON)
# ----------------------------------------------------------------------------
@app.get("/neukontakte/options")
async def neukontakte_options(per_org_limit: int = Query(PER_ORG_DEFAULT_LIMIT, ge=1, le=3), mode: str = Query("new")):
    try:
        now = time.time(); key = (mode, per_org_limit)
        cache = _OPTIONS_CACHE.get(key) or {}
        if cache.get("options") and (now - cache.get("ts", 0.0) < OPTIONS_TTL_SEC):
            return JSONResponse({"total": cache["total"], "options": cache["options"], "hint": cache.get("hint","")})

        fb_field = await get_person_field_by_hint(FIELD_FACHBEREICH_HINT)
        if not fb_field:
            return JSONResponse({"total": 0, "options": [], "hint": "Fachbereich-Feld nicht gefunden."}, status_code=status.HTTP_200_OK)

        fb_key = fb_field.get("key")
        id2label = field_options_id_to_label_map(fb_field)

        base_filter = FILTER_NEUKONTAKTE if mode == "new" else FILTER_NACHFASS
        total, counts = await stream_counts_with_org_cap(base_filter, fb_key, per_org_limit)

        options = [{"value": opt_id, "label": id2label.get(str(opt_id), str(opt_id)), "count": cnt}
                   for opt_id, cnt in counts.items()]
        options.sort(key=lambda x: x["count"], reverse=True)

        hint = "Mit Aktivitätslogik" if mode == "nachfass" else "Orga-Limit berücksichtigt"
        _OPTIONS_CACHE[key] = {"ts": now, "total": total, "options": options, "hint": hint}
        return JSONResponse({"total": total, "options": options, "hint": hint})
    except Exception as e:
        return JSONResponse({"error": f"Options-Fehler ({mode}): {e}"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

# ----------------------------------------------------------------------------
# Export (gekürzt – identisch wie vorherige Fassung)
# ----------------------------------------------------------------------------
def slugify_filename(name: str, fallback: str = "BatchFlow_Export") -> str:
    s = (name or "").strip()
    if not s: return fallback
    s = re.sub(r"[^\w\-. ]+", "", s).strip()
    s = re.sub(r"\s+", "_", s)
    return s or fallback

async def _build_master_final_from_pd(fachbereich: str,take_count: Optional[int],batch_id: Optional[str],campaign: Optional[str],per_org_limit: int,mode: str) -> pd.DataFrame:
    base_filter = FILTER_NEUKONTAKTE if mode == "new" else FILTER_NACHFASS
    table_prefix = "nk" if mode == "new" else "nf"
    fb_field = await get_person_field_by_hint(FIELD_FACHBEREICH_HINT)
    if not fb_field: raise RuntimeError("'Fachbereich'-Feld nicht gefunden.")
    fb_key = fb_field.get("key")
    last_key = await get_last_activity_key()
    next_key = await get_next_activity_key()

    def _match_person(p: dict) -> bool:
        av = extract_field_date(p, last_key) or extract_field_date(p, next_key)
        if is_forbidden_activity_date(av): return False
        val = p.get(fb_key)
        if isinstance(val, list): return str(fachbereich) in [str(x) for x in val if x is not None]
        return str(val) == str(fachbereich)

    selected: List[dict] = []; org_used: Dict[str,int] = {}
    async for chunk in stream_persons_by_filter(base_filter):
        for p in chunk:
            if not _match_person(p): continue
            org = p.get("org_id") or {}
            if isinstance(org, dict):
                if org.get("id") is not None: org_key = f"id:{org.get('id')}"
                elif org.get("name"):           org_key = f"name:{normalize_name(org.get('name'))}"
                else:                            org_key = f"noorg:{p.get('id')}"
            else: org_key = f"noorg:{p.get('id')}"
            used = org_used.get(org_key,0)
            if used >= per_org_limit: continue
            org_used[org_key] = used + 1; selected.append(p)
            if take_count and take_count>0 and len(selected) >= take_count: break
        if take_count and take_count>0 and len(selected) >= take_count: break

    def _get(p: dict, key: str) -> str:
        v = p.get(key)
        if v is None or (isinstance(v, float) and pd.isna(v)): return ""
        if isinstance(v, dict):
            if "label" in v: return str(v.get("label") or "")
            return str(v.get("value") or "")
        if isinstance(v, list):
            if v and isinstance(v[0], dict) and "value" in v[0]: return str(v[0]["value"] or "")
            return ", ".join(str(x) for x in v if x)
        return str(v)

    rows = []
    for p in selected[: (take_count or len(selected))]:
        pid = p.get("id")
        org_name = (p.get("org_id") or {}).get("name") if isinstance(p.get("org_id"), dict) else (p.get("org_name") or "-")
        org_id = str((p.get("org_id") or {}).get("id") or (p.get("org_id") or {}).get("value") or "").strip()
        emails = _as_list_email(p.get("email"))
        email_primary = emails[0] if emails else ""
        first = p.get("first_name") or ""
        last  = p.get("last_name") or ""
        full  = p.get("name") or ""
        if not first and not last and full:
            parts = full.split(); first = " ".join(parts[:-1]) if len(parts)>1 else full; last = parts[-1] if len(parts)>1 else ""

        row = {
            "Batch ID": batch_id or "",
            "Channel": DEFAULT_CHANNEL,
            "Cold-Mailing Import": campaign or "",
            "Prospect ID": _get(p, "prospect"),
            "Organisation ID": org_id,
            "Organisation Name": org_name or "-",
            "Person ID": str(pid or ""),
            "Person Vorname": first,
            "Person Nachname": last,
            "Person Titel": _get(p, "title") or _get(p, "titel") or _get(p, "anrede"),
            "Person Geschlecht": _get(p, "gender") or _get(p, "geschlecht"),
            "Person Position": _get(p, "position"),
            "Person E-Mail": email_primary,
            "XING Profil": _get(p, "xing") or _get(p, "xing url") or _get(p, "xing profil"),
            "LinkedIn URL": _get(p, "linkedin"),
        }
        rows.append(row)

    df = pd.DataFrame(rows, columns=TEMPLATE_COLUMNS)
    await save_df_text(df, f"{'nk' if mode=='new' else 'nf'}_master_final")
    return df

async def _fetch_org_names_for_filter_capped(fid: int, page_limit: int, cap_total: int, cap_bucket: int) -> Dict[str, List[str]]:
    buckets: Dict[str, List[str]] = {}; total = 0
    async for chunk in stream_persons_by_filter(fid, page_limit):
        for p in chunk:
            n = normalize_name((p.get("org_name") or "") if isinstance(p.get("org_name"), str) else "")
            if not n: continue
            b = n[0]; lst = buckets.setdefault(b, [])
            if len(lst) >= cap_bucket: continue
            if not lst or lst[-1] != n:
                lst.append(n); total += 1
                if total >= cap_total: return buckets
    return buckets

async def fetch_all_person_ids_parallel(filter_ids: List[int], page_limit: int = PAGE_LIMIT) -> set:
    sem = asyncio.Semaphore(PD_CONCURRENCY)
    async def _with_sem(fid: int):
        async with sem:
            ids: set = set()
            async for page in stream_persons_by_filter(fid, page_limit):
                for p in page:
                    pid = p.get("id")
                    if pid is not None: ids.add(str(pid))
            return ids
    res = await asyncio.gather(*[_with_sem(fid) for fid in filter_ids], return_exceptions=True)
    ids: set = set()
    for r in res:
        if isinstance(r, set): ids.update(r)
    return ids

async def _reconcile_impl(table_prefix: str) -> HTMLResponse:
    master = await load_df_text(f"{table_prefix}_master_final")
    if master.empty: return HTMLResponse(f"<div style='padding:24px'>Keine Daten in {table_prefix}_master_final.</div>")

    if len(master) > RECONCILE_MAX_ROWS:
        return HTMLResponse(f"<div style='padding:24px;color:#b00'>❌ Zu viele Datensätze ({len(master)}). Bitte auf ≤ {RECONCILE_MAX_ROWS} begrenzen.</div>", status_code=400)

    col_person_id = "Person ID"; col_org_name = "Organisation Name"; col_org_id = "Organisation ID"
    delete_rows: List[Dict[str, str]] = []

    filter_ids_org = [1245, 851, 1521]
    buckets_all: Dict[str, List[str]] = {}; collected_total = 0
    for fid in filter_ids_org:
        caps_left = max(0, MAX_ORG_NAMES - collected_total)
        if caps_left <= 0: break
        buckets = await _fetch_org_names_for_filter_capped(fid, PAGE_LIMIT, caps_left, MAX_ORG_BUCKET)
        for k, lst in buckets.items():
            slot = buckets_all.setdefault(k, [])
            for n in lst:
                if len(slot) >= MAX_ORG_BUCKET: break
                if not slot or slot[-1] != n:
                    slot.append(n); collected_total += 1
                    if collected_total >= MAX_ORG_NAMES: break
            if collected_total >= MAX_ORG_NAMES: break
        if collected_total >= MAX_ORG_NAMES: break

    drop_idx = []
    for idx, row in master.iterrows():
        cand = str(row.get(col_org_name) or "").strip()
        cand_norm = normalize_name(cand)
        if not cand_norm: continue
        bucket = buckets_all.get(cand_norm[0])
        if not bucket: continue
        near = [n for n in bucket if abs(len(n) - len(cand_norm)) <= 4]
        if not near: continue
        best = process.extractOne(cand_norm, near, scorer=fuzz.token_sort_ratio)
        if best and best[1] >= 95:
            drop_idx.append(idx)
            delete_rows.append({"reason":"org_match_95","id":str(row.get(col_person_id) or ""),"name":f"{row.get('Person Vorname') or ''} {row.get('Person Nachname') or ''}".strip(),"org_id":str(row.get(col_org_id) or ""),"org_name":cand,"extra":f"Best Match: {best[0]} ({best[1]}%)"})

    if drop_idx: master = master.drop(index=drop_idx)

    suspect_ids = await fetch_all_person_ids_parallel([1216, 1708], PAGE_LIMIT)
    if suspect_ids:
        mask_pid = master[col_person_id].astype(str).isin(suspect_ids)
        removed = master[mask_pid].copy()
        for _, r in removed.iterrows():
            delete_rows.append({"reason":"person_id_match","id":str(r.get(col_person_id) or ""),"name":f"{r.get('Person Vorname') or ''} {r.get('Person Nachname') or ''}".strip(),"org_id":str(r.get(col_org_id) or ""),"org_name":str(r.get(col_org_name) or ""),"extra":"ID in Filter 1216/1708"})
        master = master[~mask_pid].copy()

    await save_df_text(master, f"{table_prefix}_master_ready")
    log_df = pd.DataFrame(delete_rows, columns=["reason","id","name","org_id","org_name","extra"])
    await save_df_text(log_df, f"{table_prefix}_delete_log")

    stats = {"after": len(master),"del_orgmatch": (log_df[log_df["reason"]=="org_match_95"].shape[0] if not log_df.empty else 0),"del_pid":(log_df[log_df["reason"]=="person_id_match"].shape[0] if not log_df.empty else 0)}
    table_html = log_df.head(50).to_html(index=False, border=0) if not log_df.empty else "<i>keine</i>"
    return HTMLResponse(f"<div style='padding:16px;font-family:Inter,system-ui'>Übrig: <b>{stats['after']}</b> · Orga≥95%: <b>{stats['del_orgmatch']}</b> · PersonID: <b>{stats['del_pid']}</b><div style='margin-top:8px'>{table_html}</div></div>")

# Export-Job
def _df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    for name in ("Organisation ID","Person ID"):
        if name in df.columns:
            df[name] = df[name].astype(str).fillna("").replace("nan","")
    from openpyxl.utils import get_column_letter  # noqa
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Export")
        ws = writer.sheets["Export"]
        col_index = {col: i + 1 for i, col in enumerate(df.columns)}
        for col_name in ("Organisation ID","Person ID"):
            if col_name in col_index:
                j = col_index[col_name]
                for i in range(2, len(df) + 2):
                    ws.cell(i, j).number_format = "@"
        writer.book.properties.creator = "BatchFlow"
    buf.seek(0)
    return buf.getvalue()

class Job:
    def __init__(self) -> None:
        self.phase = "Warten …"; self.percent = 0; self.done = False
        self.error: Optional[str] = None; self.path: Optional[str] = None
        self.total_rows: int = 0; self.filename_base: str = "BatchFlow_Export"

JOBS: Dict[str, Job] = {}

def build_export_from_ready(master_ready: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(columns=TEMPLATE_COLUMNS)
    for col in TEMPLATE_COLUMNS:
        out[col] = master_ready[col] if col in master_ready.columns else ""
    for c in ("Organisation ID","Person ID"):
        if c in out.columns: out[c] = out[c].astype(str).fillna("").replace("nan","")
    return out

@app.post("/neukontakte/export_start")
async def export_start(
    fachbereich: str = Body(...),
    take_count: Optional[int] = Body(None),
    batch_id: Optional[str] = Body(None),
    campaign: Optional[str] = Body(None),
    per_org_limit: int = Body(PER_ORG_DEFAULT_LIMIT),
    mode: str = Query("new"),
):
    job_id = str(uuid.uuid4())
    job = Job(); JOBS[job_id] = job
    job.phase = "Initialisiere …"; job.percent = 1
    job.filename_base = slugify_filename(campaign or "BatchFlow_Export")

    async def _run():
        try:
            job.phase = "Lade Daten …"; job.percent = 10
            await _build_master_final_from_pd(fachbereich, take_count, batch_id, campaign, per_org_limit, mode)

            job.phase = "Gleiche ab …"; job.percent = 45
            _ = await _reconcile_impl("nk" if mode == "new" else "nf")

            job.phase = "Erzeuge Excel …"; job.percent = 70
            ready = await load_df_text("nk_master_ready" if mode == "new" else "nf_master_ready")
            export_df = build_export_from_ready(ready)
            data = _df_to_excel_bytes(export_df)

            path = f"/tmp/{job.filename_base}.xlsx"
            with open(path, "wb") as f: f.write(data)

            job.total_rows = len(export_df); job.phase = f"Fertig – {job.total_rows} Zeile(n)"
            job.percent = 100; job.done = True; job.path = path
        except Exception as e:
            job.error = f"Export fehlgeschlagen: {e}"
            job.phase = "Fehler"; job.percent = 100; job.done = True

    asyncio.create_task(_run())
    return JSONResponse({"job_id": job_id})

@app.get("/neukontakte/export_progress")
async def export_progress(job_id: str):
    job = JOBS.get(job_id)
    if not job: raise HTTPException(404, "Unbekannte Job-ID")
    if job.error: return JSONResponse({"error": job.error, "done": True, "phase": job.phase, "percent": job.percent})
    return JSONResponse({"phase": job.phase, "percent": job.percent, "done": job.done, "total_rows": job.total_rows})

@app.get("/neukontakte/export_download")
async def export_download(job_id: str):
    job = JOBS.get(job_id)
    if not job: raise HTTPException(404, "Unbekannte Job-ID")
    if not job.done or not job.path: raise HTTPException(409, "Der Export ist noch nicht bereit.")
    return FileResponse(job.path, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename=os.path.basename(job.path))

# ----------------------------------------------------------------------------
# Catch-all
# ----------------------------------------------------------------------------
@app.get("/overview", include_in_schema=False)
async def overview_redirect(): return RedirectResponse("/campaign", status_code=307)

@app.get("/{full_path:path}", include_in_schema=False)
async def catch_all(full_path: str): return RedirectResponse("/campaign", status_code=307)
