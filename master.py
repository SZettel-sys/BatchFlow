# master.py — BatchFlow (Nachfass: vereinfachtes Formular, Selektion über Batch-ID)
# FastAPI + Neon (Postgres) + Pipedrive
# © bizforward

import os
import re
import io
import time
import uuid
from typing import Optional, Dict, List, Tuple, AsyncGenerator

import numpy as np
import pandas as pd
import httpx
import asyncpg
from rapidfuzz import fuzz, process
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Request, Body, Query, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware

# =============================================================================
# Konfiguration
# =============================================================================
app = FastAPI(title="BatchFlow")
app.add_middleware(GZipMiddleware, minimum_size=1024)
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
PD_CLIENT_ID = os.getenv("PD_CLIENT_ID", "")
PD_CLIENT_SECRET = os.getenv("PD_CLIENT_SECRET", "")
OAUTH_AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
OAUTH_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"
PD_API_TOKEN = os.getenv("PD_API_TOKEN", "")
PIPEDRIVE_API = "https://api.pipedrive.com/v1"

DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL fehlt (Neon DSN).")
SCHEMA = os.getenv("PGSCHEMA", "public")

# Filter
FILTER_NACHFASS = int(os.getenv("FILTER_NACHFASS", "3024"))

# Defaults
DEFAULT_CHANNEL = "Cold E-Mail"

# Performance
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "500"))
PD_CONCURRENCY = int(os.getenv("PD_CONCURRENCY", "4"))
RECONCILE_MAX_ROWS = int(os.getenv("RECONCILE_MAX_ROWS", "20000"))
MAX_ORG_NAMES = int(os.getenv("MAX_ORG_NAMES", "120000"))
MAX_ORG_BUCKET = int(os.getenv("MAX_ORG_BUCKET", "15000"))

# OAuth Speicher (einfach)
user_tokens: Dict[str, str] = {}

# =============================================================================
# Excel-Template (exakte Reihenfolge)
# =============================================================================
TEMPLATE_COLUMNS = [
    "Batch ID",
    "Channel",
    "Cold-Mailing Import",
    "Prospect ID",
    "Organisation ID",
    "Organisation Name",
    "Person ID",
    "Person Vorname",
    "Person Nachname",
    "Person Titel",
    "Person Geschlecht",
    "Person Position",
    "Person E-Mail",
    "XING Profil",
    "LinkedIn URL",
]

# =============================================================================
# Startup / Shutdown
# =============================================================================
def http_client() -> httpx.AsyncClient:
    return app.state.http  # type: ignore

def get_pool() -> asyncpg.Pool:
    return app.state.pool  # type: ignore

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
# Hilfsfunktionen
# =============================================================================
@app.get("/healthz")
async def healthz():
    try:
        async with get_pool().acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

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

def slugify_filename(name: str, fallback: str = "BatchFlow_Export") -> str:
    s = (name or "").strip()
    if not s:
        return fallback
    s = re.sub(r"[^\w\-. ]+", "", s).strip()
    s = re.sub(r"\s+", "_", s)
    return s or fallback

def parse_pd_date(d: Optional[str]) -> Optional[datetime]:
    if not d:
        return None
    try:
        return datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None

def is_forbidden_activity_date(val: Optional[str]) -> bool:
    """
    True => ausschließen, wenn Datum in Zukunft ODER in [heute-3M, heute]
    """
    dt = parse_pd_date(val)
    if not dt:
        return False
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    three_months = today - timedelta(days=90)
    return dt > today or (three_months <= dt <= today)

# =============================================================================
# Pipedrive Feld-Ermittlung
# =============================================================================
_PERSON_FIELDS_CACHE: Optional[List[dict]] = None

async def get_person_fields() -> List[dict]:
    global _PERSON_FIELDS_CACHE
    if _PERSON_FIELDS_CACHE is not None:
        return _PERSON_FIELDS_CACHE
    url = append_token(f"{PIPEDRIVE_API}/personFields")
    r = await http_client().get(url, headers=get_headers())
    r.raise_for_status()
    _PERSON_FIELDS_CACHE = r.json().get("data") or []
    return _PERSON_FIELDS_CACHE

def field_options_id_to_label_map(field: dict) -> Dict[str, str]:
    opts = field.get("options") or []
    mp: Dict[str, str] = {}
    for o in opts:
        oid = str(o.get("id"))
        lab = str(o.get("label") or o.get("name") or oid)
        mp[oid] = lab
    return mp

# Geschlecht, Titel, Position, usw. – heuristische Feldsuche
PERSON_FIELD_HINTS_TO_EXPORT = {
    "prospect": "Prospect ID",
    "gender": "Person Geschlecht",
    "geschlecht": "Person Geschlecht",
    "titel": "Person Titel",
    "title": "Person Titel",
    "anrede": "Person Titel",
    "position": "Person Position",
    "xing": "XING Profil",
    "xing url": "XING Profil",
    "xing profil": "XING Profil",
    "linkedin": "LinkedIn URL",
    "email büro": "Person E-Mail",
    "email buero": "Person E-Mail",
    "e-mail büro": "Person E-Mail",
    "e-mail buero": "Person E-Mail",
    "office email": "Person E-Mail",
}

# Aktivitätsfelder
_NEXT_ACTIVITY_KEY: Optional[str] = None
_LAST_ACTIVITY_KEY: Optional[str] = None

async def get_next_activity_key() -> Optional[str]:
    global _NEXT_ACTIVITY_KEY
    if _NEXT_ACTIVITY_KEY is not None:
        return _NEXT_ACTIVITY_KEY
    _NEXT_ACTIVITY_KEY = "next_activity_date"
    try:
        fields = await get_person_fields()
        want = ["next activity", "next_activity_date", "nächste aktivität", "naechste aktivitaet", "datum nächste aktivität"]
        wl = [(w, w.replace("ä","ae").replace("ö","oe").replace("ü","ue")) for w in want]
        for f in fields:
            nm = (f.get("name") or "").lower()
            if any((w in nm) or (wa in nm) for w, wa in wl):
                _NEXT_ACTIVITY_KEY = f.get("key") or _NEXT_ACTIVITY_KEY
                break
    except Exception:
        pass
    return _NEXT_ACTIVITY_KEY

async def get_last_activity_key() -> Optional[str]:
    global _LAST_ACTIVITY_KEY
    if _LAST_ACTIVITY_KEY is not None:
        return _LAST_ACTIVITY_KEY
    _LAST_ACTIVITY_KEY = "last_activity_date"
    try:
        fields = await get_person_fields()
        want = ["last activity", "last_activity_date", "letzte aktivität", "letzte aktivitaet", "datum letzte aktivität"]
        wl = [(w, w.replace("ä","ae").replace("ö","oe").replace("ü","ue")) for w in want]
        for f in fields:
            nm = (f.get("name") or "").lower()
            if any((w in nm) or (wa in nm) for w, wa in wl):
                _LAST_ACTIVITY_KEY = f.get("key") or _LAST_ACTIVITY_KEY
                break
    except Exception:
        pass
    return _LAST_ACTIVITY_KEY

def extract_field_date(p: dict, key: Optional[str]) -> Optional[str]:
    if not key:
        return None
    v = p.get(key)
    if isinstance(v, dict):
        v = v.get("value")
    elif isinstance(v, list):
        v = v[0] if v else None
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    return str(v)

# Batch-ID Feld (für Selektion)
_BATCH_FIELD_KEY: Optional[str] = None

async def get_batch_field_key() -> Optional[str]:
    """
    Sucht ein Personenfeld, dessen Name Batch/Batch ID enthält.
    """
    global _BATCH_FIELD_KEY
    if _BATCH_FIELD_KEY is not None:
        return _BATCH_FIELD_KEY
    fields = await get_person_fields()
    candidates = [
        "batch id", "batch-id", "batch_id",
        "batch",   # Fallback – bitte nur, wenn eindeutig
    ]
    for f in fields:
        nm = (f.get("name") or "").lower()
        if any(c in nm for c in candidates):
            _BATCH_FIELD_KEY = f.get("key")
            break
    return _BATCH_FIELD_KEY

def _contains_any_text(val, wanted: List[str]) -> bool:
    if not wanted:
        return True
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return False
    if isinstance(val, dict):
        val = val.get("value")
    if isinstance(val, (list, tuple, np.ndarray)):
        flat = []
        for x in val:
            if isinstance(x, dict):
                x = x.get("value")
            if x:
                flat.append(str(x))
        val = " | ".join(flat)
    s = str(val).lower()
    return any(k.lower() in s for k in wanted if k)

# =============================================================================
# Pipedrive Fetcher
# =============================================================================
async def stream_persons_by_filter(filter_id: int, page_limit: int = PAGE_LIMIT) -> AsyncGenerator[List[dict], None]:
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

# =============================================================================
# DB (Textspalten)
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
        batch: List[List[str]] = []
        async with conn.transaction():
            for _, row in df.iterrows():
                vals = ["" if pd.isna(v) else str(v) for v in row.tolist()]
                batch.append(vals)
                if len(batch) >= 1000:
                    await conn.executemany(insert_sql, batch)
                    batch = []
            if batch:
                await conn.executemany(insert_sql, batch)

async def load_df_text(table: str) -> pd.DataFrame:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(f'SELECT * FROM "{SCHEMA}"."{table}"')
    if not rows:
        return pd.DataFrame()
    cols = list(rows[0].keys())
    data = [tuple(r[c] for c in cols) for r in rows]
    return pd.DataFrame(data, columns=cols).replace({"": np.nan})

# =============================================================================
# UI – Nachfass (vereinfachtes Formular)
# =============================================================================
@app.get("/")
def root():
    return RedirectResponse("/nachfass")

@app.get("/nachfass", response_class=HTMLResponse)
async def nachfass_home():
    authed = bool(user_tokens.get("default") or PD_API_TOKEN)
    html = f"""<!doctype html><html lang="de">
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
function hideOverlay() {{ el("overlay").style.display = "none"; }}
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
  if (!r.ok) {{ hideOverlay(); alert('Start fehlgeschlagen.'); return; }}
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
</body></html>"""
    return HTMLResponse(html)


# =============================================================================
# Datenaufbau – Nachfass über Batch-ID
# =============================================================================
async def _build_nf_master_final(
    nf_batch_ids: List[str],
    batch_id: str,
    campaign: str,
) -> pd.DataFrame:
    # Feld-Keys & Mappings
    person_fields = await get_person_fields()
    hint_to_key: Dict[str, str] = {}
    gender_options_map: Dict[str, str] = {}
    for f in person_fields:
        nm = (f.get("name") or "").lower()
        for hint in PERSON_FIELD_HINTS_TO_EXPORT.keys():
            if hint in nm and hint not in hint_to_key:
                hint_to_key[hint] = f.get("key")
        if any(x in nm for x in ("gender", "geschlecht")):
            gender_options_map = field_options_id_to_label_map(f)

    def get_field(p: dict, hint: str) -> str:
        key = hint_to_key.get(hint)
        if not key:
            return ""
        v = p.get(key)
        if isinstance(v, dict) and "label" in v:
            return str(v.get("label") or "")
        if isinstance(v, list):
            if v and isinstance(v[0], dict) and "value" in v[0]:
                return str(v[0].get("value") or "")
            return ", ".join([str(x) for x in v if x])
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        sv = str(v)
        if hint in ("gender", "geschlecht") and gender_options_map:
            return gender_options_map.get(sv, sv)
        return sv

    last_key = await get_last_activity_key()
    next_key = await get_next_activity_key()
    batch_key = await get_batch_field_key()
    if not batch_key:
        raise RuntimeError("Personenfeld „Batch ID“ wurde nicht gefunden. Bitte benennen/prüfen.")

    selected: List[dict] = []
    async for chunk in stream_persons_by_filter(FILTER_NACHFASS):
        for p in chunk:
            # Aktivitäts-Vorfilter
            av = extract_field_date(p, last_key) or extract_field_date(p, next_key)
            if is_forbidden_activity_date(av):
                continue
            # Selektion über Batch-ID-Feld (contains – OR)
            if not _contains_any_text(p.get(batch_key), nf_batch_ids):
                continue
            selected.append(p)

    # Zeilen für Export aufbauen
    rows = []
    for p in selected:
        pid = p.get("id")
        first = p.get("first_name")
        last = p.get("last_name")
        full = p.get("name")
        vor, nach = split_name(first, last, full)

        # Organisation robust
        org_name, org_id = "-", ""
        org = p.get("org_id")
        if isinstance(org, dict):
            org_name = org.get("name") or p.get("org_name") or "-"
            oid = org.get("id") if org.get("id") is not None else org.get("value")
            if oid is not None and str(oid).strip():
                org_id = str(oid)
        elif isinstance(org, (int, str)) and str(org).strip():
            org_id = str(org).strip()
            org_name = (p.get("org_name") or org_name)
        else:
            org_name = (p.get("org_name") or org_name)

        # E-Mails
        def _as_list_email(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return []
            if isinstance(v, dict):
                v = v.get("value")
                return [v] if v else []
            if isinstance(v, (list, tuple, np.ndarray)):
                out = []
                for x in v:
                    if isinstance(x, dict):
                        x = x.get("value")
                    if x:
                        out.append(str(x))
                return out
            return [str(v)]
        emails = _as_list_email(p.get("email"))
        person_email = emails[0] if emails else ""

        rows.append({
            "Batch ID": batch_id or "",
            "Channel": DEFAULT_CHANNEL,
            "Cold-Mailing Import": campaign or "",
            "Prospect ID": get_field(p, "prospect"),
            "Organisation ID": org_id,
            "Organisation Name": org_name,
            "Person ID": str(pid or ""),
            "Person Vorname": vor,
            "Person Nachname": nach,
            "Person Titel": get_field(p, "titel") or get_field(p, "title") or get_field(p, "anrede"),
            "Person Geschlecht": get_field(p, "gender") or get_field(p, "geschlecht"),
            "Person Position": get_field(p, "position"),
            "Person E-Mail": person_email,
            "XING Profil": get_field(p, "xing") or get_field(p, "xing url") or get_field(p, "xing profil"),
            "LinkedIn URL": get_field(p, "linkedin"),
        })

    df = pd.DataFrame(rows, columns=TEMPLATE_COLUMNS)
    await save_df_text(df, "nk_master_final")
    return df

# =============================================================================
# Abgleich (wie bei Neukontakte)
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

async def _fetch_org_names_for_filter_capped(filter_id: int, page_limit: int, cap_total: int, cap_bucket: int) -> Dict[str, List[str]]:
    buckets: Dict[str, List[str]] = {}
    total = 0
    async for chunk in stream_organizations_by_filter(filter_id, page_limit):
        for o in chunk:
            n = normalize_name(o.get("name") or "")
            if not n:
                continue
            b = n[0]
            lst = buckets.setdefault(b, [])
            if len(lst) >= cap_bucket:
                continue
            if not lst or lst[-1] != n:
                lst.append(n)
                total += 1
                if total >= cap_total:
                    return buckets
    return buckets

def _pretty_reason(reason: str, extra: str = "") -> str:
    r = (reason or "").lower()
    if r == "org_match_95":
        base = "Organisations-Duplikat (≥95 %)"
    elif r == "person_id_match":
        base = "Person bereits kontaktiert (ID in 1216/1708)"
    elif r == "limit_per_org":
        base = "Mehr als zulässige Kontakte pro Organisation"
    else:
        base = r or "Entfernt"
    return f"{base}{(' – ' + extra) if extra else ''}"

async def _reconcile_impl() -> HTMLResponse:
    master = await load_df_text("nk_master_final")
    if master.empty:
        return HTMLResponse("<div style='padding:24px'>Keine Daten in nk_master_final.</div>")

    if len(master) > RECONCILE_MAX_ROWS:
        return HTMLResponse(
            f"<div style='padding:24px;color:#b00'>❌ Zu viele Datensätze ({len(master)}). "
            f"Bitte auf ≤ {RECONCILE_MAX_ROWS} begrenzen.</div>", status_code=400
        )

    col_person_id = "Person ID"
    col_org_name = "Organisation Name"
    col_org_id = "Organisation ID"
    delete_rows: List[Dict[str, str]] = []

    # Orga-Dubletten via ≥95 % (Filter 1245/851/1521)
    filter_ids_org = [1245, 851, 1521]
    buckets_all: Dict[str, List[str]] = {}
    collected_total = 0
    for fid in filter_ids_org:
        caps_left = max(0, MAX_ORG_NAMES - collected_total)
        if caps_left <= 0:
            break
        buckets = await _fetch_org_names_for_filter_capped(fid, PAGE_LIMIT, caps_left, MAX_ORG_BUCKET)
        for k, lst in buckets.items():
            slot = buckets_all.setdefault(k, [])
            for n in lst:
                if len(slot) >= MAX_ORG_BUCKET:
                    break
                if not slot or slot[-1] != n:
                    slot.append(n)
                    collected_total += 1
                    if collected_total >= MAX_ORG_NAMES:
                        break
            if collected_total >= MAX_ORG_NAMES:
                break
        if collected_total >= MAX_ORG_NAMES:
            break

    drop_idx = []
    for idx, row in master.iterrows():
        cand = str(row.get(col_org_name) or "").strip()
        cand_norm = normalize_name(cand)
        if not cand_norm:
            continue
        bucket = buckets_all.get(cand_norm[0])
        if not bucket:
            continue
        near = [n for n in bucket if abs(len(n) - len(cand_norm)) <= 4]
        if not near:
            continue
        best = process.extractOne(cand_norm, near, scorer=fuzz.token_sort_ratio)
        if best and best[1] >= 95:
            drop_idx.append(idx)
            delete_rows.append({
                "reason": "org_match_95",
                "id": str(row.get(col_person_id) or ""),
                "name": f"{row.get('Person Vorname') or ''} {row.get('Person Nachname') or ''}".strip(),
                "org_id": str(row.get(col_org_id) or ""),
                "org_name": cand,
                "extra": f"Best Match: {best[0]} ({best[1]}%)"
            })

    if drop_idx:
        master = master.drop(index=drop_idx)

    # Person-ID in Filtern 1216/1708 entfernen
    suspect_ids = set()
    async for page in stream_person_ids_by_filter(1216):
        suspect_ids.update(page)
    async for page in stream_person_ids_by_filter(1708):
        suspect_ids.update(page)

    if suspect_ids:
        mask_pid = master[col_person_id].astype(str).isin(suspect_ids)
        removed = master[mask_pid].copy()
        for _, r in removed.iterrows():
            delete_rows.append({
                "reason": "person_id_match",
                "id": str(r.get(col_person_id) or ""),
                "name": f"{r.get('Person Vorname') or ''} {r.get('Person Nachname') or ''}".strip(),
                "org_id": str(r.get(col_org_id) or ""),
                "org_name": str(r.get(col_org_name) or ""),
                "extra": "ID in Filter 1216/1708"
            })
        master = master[~mask_pid].copy()

    await save_df_text(master, "nk_master_ready")
    log_df = pd.DataFrame(delete_rows, columns=["reason", "id", "name", "org_id", "org_name", "extra"])
    await save_df_text(log_df, "nk_delete_log")

    # Kurzer Inline-Report
    stats = {
        "after": len(master),
        "del_orgmatch": log_df[log_df["reason"] == "org_match_95"].shape[0] if not log_df.empty else 0,
        "del_pid": log_df[log_df["reason"] == "person_id_match"].shape[0] if not log_df.empty else 0,
        "deleted_total": log_df.shape[0] if not log_df.empty else 0,
    }
    table_html = log_df.head(50).to_html(index=False, border=0) if not log_df.empty else "<i>keine</i>"
    return HTMLResponse(
        f"<div style='padding:16px;font-family:Inter,system-ui'>Übrig: <b>{stats['after']}</b> · "
        f"Orga≥95%: <b>{stats['del_orgmatch']}</b> · PersonID: <b>{stats['del_pid']}</b>"
        f"<div style='margin-top:8px'>{table_html}</div></div>"
    )

# =============================================================================
# Export – Job/Excel
# =============================================================================
def build_export_from_ready(master_ready: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(columns=TEMPLATE_COLUMNS)
    for col in TEMPLATE_COLUMNS:
        out[col] = master_ready[col] if col in master_ready.columns else ""
    for c in ("Organisation ID", "Person ID"):
        if c in out.columns:
            out[c] = out[c].astype(str).fillna("").replace("nan", "")
    return out

def _df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    # IDs als Text
    for name in ("Organisation ID", "Person ID"):
        if name in df.columns:
            df[name] = df[name].astype(str).fillna("").replace("nan", "")
    from openpyxl.utils import get_column_letter
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Export")
        ws = writer.sheets["Export"]
        col_index = {col: i + 1 for i, col in enumerate(df.columns)}
        for col_name in ("Organisation ID", "Person ID"):
            if col_name in col_index:
                j = col_index[col_name]
                for i in range(2, len(df) + 2):
                    ws.cell(i, j).number_format = "@"
        writer.book.properties.creator = "BatchFlow"
    buf.seek(0)
    return buf.getvalue()

class Job:
    def __init__(self) -> None:
        self.phase = "Warten …"
        self.percent = 0
        self.done = False
        self.error: Optional[str] = None
        self.path: Optional[str] = None
        self.total_rows: int = 0
        self.filename_base: str = "BatchFlow_Export"

JOBS: Dict[str, Job] = {}

@app.post("/nachfass/export_start")
async def export_start(
    nf_batch_ids: List[str] = Body(..., embed=True),
    batch_id: str = Body(...),
    campaign: str = Body(...),
):
    job_id = str(uuid.uuid4())
    job = Job()
    JOBS[job_id] = job
    job.phase = "Initialisiere …"; job.percent = 1
    job.filename_base = slugify_filename(campaign or "BatchFlow_Export")

    async def _run():
        try:
            job.phase = "Lade Daten …"; job.percent = 10
            await _build_nf_master_final(nf_batch_ids=nf_batch_ids, batch_id=batch_id, campaign=campaign)

            job.phase = "Gleiche ab …"; job.percent = 45
            _ = await _reconcile_impl()  # schreibt ready + log

            job.phase = "Erzeuge Excel …"; job.percent = 70
            ready = await load_df_text("nk_master_ready")
            export_df = build_export_from_ready(ready)
            data = _df_to_excel_bytes(export_df)

            path = f"/tmp/{job.filename_base}.xlsx"
            with open(path, "wb") as f:
                f.write(data)

            job.total_rows = len(export_df)
            job.phase = f"Fertig – {job.total_rows} Zeile(n)"
            job.percent = 100
            job.done = True
            job.path = path
        except Exception as e:
            job.error = f"Export fehlgeschlagen: {e}"
            job.phase = "Fehler"
            job.percent = 100
            job.done = True

    import asyncio
    asyncio.create_task(_run())
    return JSONResponse({"job_id": job_id})

@app.get("/nachfass/export_progress")
async def export_progress(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "Unbekannte Job-ID")
    if job.error:
        return JSONResponse({"error": job.error, "done": True, "phase": job.phase, "percent": job.percent})
    return JSONResponse({"phase": job.phase, "percent": job.percent, "done": job.done, "total_rows": job.total_rows})

@app.get("/nachfass/export_download")
async def export_download(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "Unbekannte Job-ID")
    if not job.done or not job.path:
        raise HTTPException(409, "Der Export ist noch nicht bereit.")
    return FileResponse(
        job.path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=os.path.basename(job.path),
    )

# =============================================================================
# Übersicht/Summary (Id, Name, Orga-ID/Name, Grund)
# =============================================================================
def _count_reason(df: pd.DataFrame, keys: List[str]) -> int:
    if df.empty or "reason" not in df.columns:
        return 0
    r = df["reason"].astype(str).str.lower()
    keys_l = [k.lower() for k in keys]
    return int(r.isin(keys_l).sum())

@app.get("/nachfass/summary", response_class=HTMLResponse)
async def nachfass_summary(job_id: str = Query(...)):
    ready = await load_df_text("nk_master_ready")
    log = await load_df_text("nk_delete_log")

    total_ready = int(len(ready)) if not ready.empty else 0
    cnt_org95 = _count_reason(log, ["org_match_95"])
    cnt_pid   = _count_reason(log, ["person_id_match"])
    removed_sum = cnt_org95 + cnt_pid

    if not log.empty:
        view = log.tail(50).copy()
        view["Grund"] = view.apply(lambda r: _pretty_reason(str(r.get("reason") or ""), str(r.get("extra") or "")), axis=1)
        view = view.rename(columns={
            "id": "Id",
            "name": "Name",
            "org_id": "Organisation ID",
            "org_name": "Organisation Name",
        })[["Id", "Name", "Organisation ID", "Organisation Name", "Grund"]]
        table_html = view.to_html(classes="grid", index=False, border=0)
    else:
        table_html = "<i>keine</i>"

    html = f"""
<!doctype html><html lang="de"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Übersicht – Nachfass</title>
<style>
  :root{{--bg:#f6f8fb;--card:#fff;--txt:#0f172a;--muted:#64748b;--border:#e2e8f0;--primary:#0ea5e9}}
  *{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--txt);font:16px/1.6 Inter,-apple-system,Segoe UI,Roboto,Arial,sans-serif}}
  main{{max-width:1120px;margin:28px auto;padding:0 20px}}
  .card{{background:#fff;border:1px solid var(--border);border-radius:14px;padding:18px;box-shadow:0 2px 8px rgba(2,8,23,.04);margin-bottom:18px}}
  .btn{{display:inline-block;background:#0ea5e9;color:#fff;padding:10px 14px;border-radius:10px;text-decoration:none}}
  h2{{margin:0 0 10px 0;font-size:18px}}
  table{{width:100%;border-collapse:collapse;margin-top:8px}}
  th,td{{padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:left;font-size:13px}}
  th{{background:#f8fafc}}
  .muted{{color:#64748b;font-size:13px}}
</style>
</head><body>
<main>
  <section class="card">
    <div><b>Ergebnis:</b> <b>{total_ready}</b> Zeilen in <code>nk_master_ready</code></div>
    <ul>
      <li>Entfernt (Orga-Match ≥95% – Filter 1245/851/1521): <b>{cnt_org95}</b></li>
      <li>Entfernt (Person-ID in Filtern 1216/1708): <b>{cnt_pid}</b></li>
      <li><b>Summe entfernt:</b> <b>{removed_sum}</b></li>
    </ul>
    <div style="margin-top:12px">
      <a class="btn" href="/nachfass">Zurück</a>
    </div>
  </section>

  <section class="card">
    <h2>Entfernte Datensätze</h2>
    {table_html}
    <div class="muted" style="margin-top:8px">Vollständiges Log in Neon: <code>nk_delete_log</code></div>
  </section>
</main>
</body></html>
"""
    return HTMLResponse(html)

# =============================================================================
# OAuth (optional)
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
    return RedirectResponse("/nachfass")

# =============================================================================
# Lokaler Start
# =============================================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("master:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=False)
