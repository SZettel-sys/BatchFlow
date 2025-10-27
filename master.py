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

# Filter/Felder
FILTER_NEUKONTAKTE = 2998
FIELD_FACHBEREICH_HINT = "fachbereich"
FIELD_ORGART_HINT = "organisationsart"

# UI/Defaults
DEFAULT_CHANNEL = "Cold E-Mail"             # <-- fix gesetzt
COLD_MAILING_IMPORT_LABEL = "Cold-Mailing Import"

# Performance
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "500"))              # weniger Roundtrips
RECONCILE_MAX_ROWS = int(os.getenv("RECONCILE_MAX_ROWS", "1000"))
OPTIONS_TTL_SEC = int(os.getenv("OPTIONS_TTL_SEC", "900"))    # Cache für /options
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
    """Einfache Heuristik für Vor- / Nachname."""
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
    """Streamt Personen aus einem Filter und sammelt jene, die predicate(person) erfüllen."""
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
    """Yieldet Organisationen aus einem Pipedrive-Filter."""
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
    """Yieldet Personen-IDs als Strings für einen Filter (speicherschonend)."""
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

# ---- Kern: Zählen nach „max N Kontakte pro Organisation“ --------------------
async def stream_counts_with_org_cap(
    filter_id: int,
    fachbereich_key: str,
    per_org_limit: int,
    page_limit: int = PAGE_LIMIT,
) -> Tuple[int, Dict[str, int]]:
    """
    Zählt total & Fachbereich-Counts, nachdem max. N Kontakte pro Organisation
    berücksichtigt wurden. Streamend & speicherschonend.
    """
    total = 0
    counts: Dict[str, int] = {}
    org_used: Dict[str, int] = {}  # org_key -> wie viele schon gezählt

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
            # Org-Key bestimmen (id wenn vorhanden, sonst normalisierter Name, sonst Person-ID)
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
                continue  # Orga-Kontingent erschöpft

            # Person zählt
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
# Optionen-Cache (abhängig von per_org_limit)
# =============================================================================
_OPTIONS_CACHE: Dict[int, dict] = {}

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
    authed = bool(user_tokens.get("default") or PD_API_TOKEN)

    html = f"""<!doctype html><html lang="de">
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
  .row{{display:grid;grid-template-columns:1fr 160px 220px 220px 220px auto;gap:16px;align-items:end}}  /* Kampagne ergänzt */
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
  <div><b>Neukontakte (Filter {FILTER_NEUKONTAKTE})</b> · Gesamt: <b id="total-count">lädt…</b></div>
  <div>{"<span class='muted'>angemeldet</span>" if authed else "<a href='/login'>Anmelden</a>"}</div>
</header>

<div class="wrap">
  <div class="card">
    <div class="row">
      <div style="grid-column:1/-1">
        <label>Fachbereich</label>
        <select id="fachbereich"><option value="">– bitte auswählen –</option></select>
        <div class="muted" id="fbinfo" style="margin-top:6px;">Die Zahl in Klammern berücksichtigt bereits „Pro Organisation“.</div>
      </div>
      <div>
        <label>Pro Organisation</label>
        <select id="per_org_limit">
          <option value="1">1</option>
          <option value="2" selected>2</option>
        </select>
      </div>
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
function showOverlay(){{document.getElementById("overlay").style.display="flex";}}
function hideOverlay(){{document.getElementById("overlay").style.display="none";}}

async function loadOptions(){{
  showOverlay();
  try {{
    const pol = document.getElementById('per_org_limit').value || '{PER_ORG_DEFAULT_LIMIT}';
    const r = await fetch('/neukontakte/options?per_org_limit=' + encodeURIComponent(pol), {{cache:'no-store'}});
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
    const r = await fetch('/neukontakte/preview', {{
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

# -----------------------------------------------------------------------------
@app.get("/neukontakte/options")
async def neukontakte_options(per_org_limit: int = Query(PER_ORG_DEFAULT_LIMIT, ge=1, le=2)):
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
# Vorschau – Auswahl speichern (mit Orga-Limit) + Name split + Kampagne + Channel
# =============================================================================
@app.post("/neukontakte/preview", response_class=HTMLResponse)
async def neukontakte_preview(
    fachbereich: str = Body(...),
    take_count: Optional[int] = Body(None),
    batch_id: Optional[str] = Body(None),
    campaign: Optional[str] = Body(None),
    per_org_limit: int = Body(PER_ORG_DEFAULT_LIMIT),
):
    try:
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

        # Personen einsammeln (ohne Orga-Limit), dann Orga-Limit anwenden
        raw = await fetch_persons_until_match(FILTER_NEUKONTAKTE, _match, max_collect=None)

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
                "Channel": DEFAULT_CHANNEL,                         # fix "Cold E-Mail"
                COLD_MAILING_IMPORT_LABEL: campaign or "",         # Kampagnenname hier speichern
                "id": pid,
                "Person - Vorname": first,
                "Person - Nachname": last,
                "E-Mail": email_str,
                "Organisation": org_name,
                "Fachbereich": fb_label,
            })

        df = pd.DataFrame(rows)
        await save_df_text(df, "nk_master_final")

        preview_table = (df.head(50).to_html(classes="grid", index=False, border=0)
                         if not df.empty else "<i>Keine Daten</i>")

        html = f"""<!doctype html><html lang="de"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Vorschau – Neukontakte</title>
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
    <h3>Vorschau – Fachbereich: <b>{fb_label}</b> | Batch: <b>{batch_id or "-"}</b> | Datensätze (mit Orga-Limit): <b>{len(df)}</b></h3>
    {preview_table}
    <p><a class="btn" href="/neukontakte">Zurück</a></p>
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
# Abgleich – nk_master_ready & nk_delete_log
# (Regel „max 2 pro Orga“ ENTFERNT, da bereits vorher angewandt)
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
            f"Bitte auf ≤ {RECONCILE_MAX_ROWS} begrenzen (RECONCILE_MAX_ROWS anpassbar).</div>",
            status_code=400
        )

    col_person_id = "id" if "id" in master.columns else None
    col_org_name = "Organisation" if "Organisation" in master.columns else None
    col_orgart = "Organisationsart" if "Organisationsart" in master.columns else None

    delete_log = []

    # Regel 2 – Organisationsart gesetzt -> löschen
    if col_orgart and col_orgart in master.columns:
        mask_orgtype = master[col_orgart].notna() & (master[col_orgart].astype(str).str.strip() != "")
        removed = master[mask_orgtype].copy()
        for _, r in removed.iterrows():
            # Name-Fallback: Vorname Nachname, sonst evtl. 'name'
            pname = (str(r.get("Person - Vorname") or "") + " " + str(r.get("Person - Nachname") or "")).strip() or str(r.get("name") or "")
            delete_log.append({"reason":"organisationsart_set","id":r.get(col_person_id),"name":pname,
                               "org_name":r.get(col_org_name),"extra":str(r.get(col_orgart))})
        master = master[~mask_orgtype].copy()

    # Regel 3 – Orga-Abgleich (Filter 1245, 851, 1521) >=95% mit Buckets
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

    # Regel 4 – Personen-ID-Abgleich (Filter 1216, 1708)
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

    # Speichern
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
      <a class="btn" href="/neukontakte">Zurück</a>
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
    return RedirectResponse("/neukontakte", status_code=307)

@app.get("/{full_path:path}", include_in_schema=False)
async def catch_all(full_path: str, request: Request):
    return RedirectResponse("/neukontakte", status_code=307)

# =============================================================================
# Start lokal
# =============================================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("master:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=False)
