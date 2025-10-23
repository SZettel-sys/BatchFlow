# master.py
import os
import re
from typing import Optional, Dict, List

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
if not DATABASE_URL:
    raise ValueError("DATABASE_URL fehlt")

# Felder / Filter
FILTER_NEUKONTAKTE = 2998
FIELD_FACHBEREICH_HINT = "fachbereich"
FIELD_ORGART_HINT = "organisationsart"

# Visual / Vorgaben
DEFAULT_CHANNEL = "Cold-Mail"
COLD_MAILING_IMPORT_LABEL = "Cold-Mailing Import"

# OAuth Tokens (einfach)
user_tokens: Dict[str, str] = {}

# -----------------------------------------------------------------------------
# App-Startup: HTTP-Client + DB-Pool
# -----------------------------------------------------------------------------
def http_client() -> httpx.AsyncClient:
    return app.state.http  # type: ignore[attr-defined]

def get_pool() -> asyncpg.Pool:
    return app.state.pool  # type: ignore[attr-defined]

@app.on_event("startup")
async def _startup():
    # HTTPX-Client mit Keep-Alive
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    app.state.http = httpx.AsyncClient(timeout=60.0, limits=limits)

    # AsyncPG-Verbindungspool (Neon)
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)

@app.on_event("shutdown")
async def _shutdown():
    # sauber schließen
    try:
        await app.state.http.aclose()
    finally:
        await app.state.pool.close()

# -----------------------------------------------------------------------------
# Health-Check (Render Health Check Path: /healthz)
# -----------------------------------------------------------------------------
@app.get("/healthz")
async def healthz():
    try:
        async with get_pool().acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

# -----------------------------------------------------------------------------
# Hilfsfunktionen (robustes Parsing & Ähnlichkeit)
# -----------------------------------------------------------------------------
def _as_list_email(value) -> List[str]:
    """
    Robust gegen None / str / dict / list / tuple / numpy.ndarray.
    Extrahiert E-Mail-Werte aus Pipedrive-Strukturen.
    """
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
    s = re.sub(r"\b(gmbh|ug|ag|kg|ohg|inc|ltd)\b", "", s)
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def sim(a: str, b: str) -> int:
    a = (a or "").strip()
    b = (b or "").strip()
    if not a or not b:
        return 0
    return fuzz.token_sort_ratio(a, b)

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
    r = await http_client().get(url, headers=get_headers())
    r.raise_for_status()
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
        url = append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={limit}&sort=name")
        r = await http_client().get(url, headers=get_headers())
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
        r = await http_client().get(url, headers=get_headers())
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
# DB helpers (mit Pool)
# -----------------------------------------------------------------------------
async def ensure_table_text(conn: asyncpg.Connection, table: str, cols: List[str]):
    col_defs = ", ".join([f'"{c}" TEXT' for c in cols])
    await conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')

async def clear_table(conn: asyncpg.Connection, table: str):
    await conn.execute(f'DROP TABLE IF EXISTS "{table}"')

async def save_df_text(df: pd.DataFrame, table: str):
    """
    Speichert DataFrame als TEXT-Spalten per executemany (schnell & stabil).
    Nutzt den globalen Pool.
    """
    async with get_pool().acquire() as conn:
        await clear_table(conn, table)
        await ensure_table_text(conn, table, list(df.columns))
        if df.empty:
            return

        cols = list(df.columns)
        cols_sql = ", ".join(f'"{c}"' for c in cols)
        placeholders = ", ".join(f'${i}' for i in range(1, len(cols) + 1))
        insert_sql = f'INSERT INTO "{table}" ({cols_sql}) VALUES ({placeholders})'

        records: List[List[str]] = []
        for _, row in df.iterrows():
            vals = ["" if pd.isna(v) else str(v) for v in row.tolist()]
            records.append(vals)

        async with conn.transaction():
            await conn.executemany(insert_sql, records)

async def load_df_text(table: str) -> pd.DataFrame:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(f'SELECT * FROM "{table}"')
    if not rows:
        return pd.DataFrame()
    cols = list(rows[0].keys())
    data = [tuple(r[c] for c in cols) for r in rows]
    df = pd.DataFrame(data, columns=cols).replace({"": np.nan})
    return df

# -----------------------------------------------------------------------------
# OAuth & Routing Basics
# -----------------------------------------------------------------------------
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
# Startseite (kurz & bündig)
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
  <div>
    <b>Neukontakte (Filter {FILTER_NEUKONTAKTE})</b> · Gesamt: <b>{total}</b>
  </div>
  <div>
    {"<span class='muted'>angemeldet</span>" if authed else "<a href='/login'>Anmelden</a>"}
  </div>
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
    const r = await fetch('/neukontakte/options');
    const data = await r.json();
    const sel = document.getElementById('fachbereich');
    sel.innerHTML = '<option value="">– bitte auswählen –</option>';
    data.options.forEach(o => {{
      const opt = document.createElement('option');
      opt.value = o.value;
      opt.textContent = o.label + ' (' + o.count + ')';
      sel.appendChild(opt);
    }});
    const fbinfo = document.getElementById('fbinfo');
    fbinfo.textContent = "Gesamt im Filter: " + data.total + " | Fachbereiche: " + data.options.length;
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
      body: JSON.stringify({{ fachbereich: fb, take_count: tc ? parseInt(tc) : null, batch_id: bid }})
    }});
    const html = await r.text();
    document.open(); document.write(html); document.close();
  }} catch(e) {{
    hideOverlay();
    alert('Fehler: ' + e);
  }}
}});

loadOptions();
</script>
</body></html>
"""
    return HTMLResponse(html)

async def count_persons_in_filter(filter_id: int) -> int:
    start, limit, total = 0, 500, 0
    while True:
        url = append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={limit}")
        r = await http_client().get(url, headers=get_headers())
        if r.status_code != 200:
            return total
        items = r.json().get("data") or []
        if not items:
            break
        total += len(items)
        if len(items) < limit:
            break
        start += limit
    return total

# -----------------------------------------------------------------------------
# Optionen (Fachbereichswerte)
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

    options = [{"value": k, "label": k, "count": v} for k, v in sorted(counts.items(), key=lambda x: x[1], reverse=True)]
    return JSONResponse({"total": total, "options": options})

# -----------------------------------------------------------------------------
# Vorschau – erzeugt nk_master_final
# -----------------------------------------------------------------------------
@app.post("/neukontakte/preview", response_class=HTMLResponse)
async def neukontakte_preview(
    fachbereich: str = Body(...),
    take_count: Optional[int] = Body(None),
    batch_id: Optional[str] = Body(None),
):
    try:
        fb_key = await get_field_key_by_hint(FIELD_FACHBEREICH_HINT)
        orgart_key = await get_field_key_by_hint(FIELD_ORGART_HINT)

        persons = await fetch_persons_by_filter(FILTER_NEUKONTAKTE)
        sel: List[dict] = []
        if fb_key:
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
        else:
            sel = persons

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

            row = {
                "Batch ID": batch_id or "",
                "Channel": DEFAULT_CHANNEL,
                COLD_MAILING_IMPORT_LABEL: DEFAULT_CHANNEL,
                "id": pid,
                "name": name,
                "E-Mail": email_str,
                "Organisation": org_name,
            }
            if orgart_key:
                row["Organisationsart"] = p.get(orgart_key)

            rows.append(row)

        df = pd.DataFrame(rows)
        await save_df_text(df, "nk_master_final")

        preview_table = (df.head(50).to_html(classes="grid", index=False, border=0) if not df.empty else "<i>Keine Daten</i>")
        html = f"""
<!doctype html><html lang="de"><head>
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
    <h3>Vorschau – Fachbereich: <b>{fachbereich}</b> | Batch: <b>{batch_id or "-"}</b> | Datensätze: <b>{len(df)}</b></h3>
    {preview_table}
    <p><a class="btn" href="/neukontakte">Zurück</a></p>
    <p><button class="btn" id="btnReconcile">Abgleich durchführen</button></p>
  </div>
<script>
document.getElementById('btnReconcile').addEventListener('click', async ()=>{{
  const r = await fetch('/neukontakte/reconcile', {{method:'POST'}});
  const html = await r.text();
  document.open(); document.write(html); document.close();
}});
</script>
</body></html>
"""
        return HTMLResponse(html)
    except Exception as e:
        return HTMLResponse(f"<pre style='padding:24px;color:#b00'>❌ Fehler beim Abruf/Speichern:\n{e}</pre>", status_code=500)

# -----------------------------------------------------------------------------
# Abgleich – schreibt nk_master_ready & nk_delete_log
# -----------------------------------------------------------------------------
@app.post("/neukontakte/reconcile", response_class=HTMLResponse)
async def neukontakte_reconcile():
    try:
        master = await load_df_text("nk_master_final")
        if master.empty:
            return HTMLResponse("<div style='padding:24px'>Keine Daten in nk_master_final.</div>")

        col_person_id = "id" if "id" in master.columns else None
        col_org_name = "Organisation" if "Organisation" in master.columns else None
        col_orgart = "Organisationsart" if "Organisationsart" in master.columns else None

        delete_log = []

        # Regel 2 – Organisationsart gesetzt -> löschen
        if col_orgart and col_orgart in master.columns:
            mask_orgtype = master[col_orgart].notna() & (master[col_orgart].astype(str).str.strip() != "")
            removed = master[mask_orgtype].copy()
            for _, r in removed.iterrows():
                delete_log.append({"reason":"organisationsart_set","id":r.get(col_person_id),"name":r.get("name"),"org_name":r.get(col_org_name),"extra":str(r.get(col_orgart))})
            master = master[~mask_orgtype].copy()

        # Regel 1 – max. 2 Personen pro Organisation
        if col_org_name and col_org_name in master.columns:
            master["_rank"] = master.groupby(col_org_name).cumcount() + 1
            over = master[master["_rank"] > 2].copy()
            for _, r in over.iterrows():
                delete_log.append({"reason":"limit_per_org","id":r.get(col_person_id),"name":r.get("name"),"org_name":r.get(col_org_name),"extra":"über 2 pro Organisation"})
            master = master[master["_rank"] <= 2].drop(columns=["_rank"], errors="ignore")

        # Regel 3 – Orga-Abgleich (Filter 1245, 851, 1521) >=95%
        suspect_org_filters = [1245, 851, 1521]
        if col_org_name and col_org_name in master.columns:
            ext_names = []
            for fid in suspect_org_filters:
                orgs = await fetch_organizations_by_filter(fid)
                ext_names.extend([o.get("name") for o in orgs if o.get("name")])
            # dedupe
            ext_names = list({(n or "").strip(): None for n in ext_names if isinstance(n, str) and n.strip()}.keys())

            if ext_names:
                drop_idx = []
                for idx, row in master.iterrows():
                    cand = str(row.get(col_org_name) or "").strip()
                    if not cand:
                        continue
                    best = process.extractOne(cand, ext_names, scorer=fuzz.token_sort_ratio)
                    if best and best[1] >= 95:
                        drop_idx.append(idx)
                        delete_log.append({"reason":"org_match_95","id":row.get(col_person_id),"name":row.get("name"),"org_name":cand,"extra":f"Best Match: {best[0]} ({best[1]}%)"})
                if drop_idx:
                    master = master.drop(index=drop_idx)

        # Regel 4 – Personen-Abgleich (Filter 1216, 1708) ID-Gleichheit
        suspect_person_filters = [1216, 1708]
        if col_person_id:
            suspect_ids = set()
            for fid in suspect_person_filters:
                persons = await fetch_persons_by_filter(fid)
                for p in persons:
                    pid = p.get("id")
                    if pid is not None:
                        suspect_ids.add(str(pid))
            if suspect_ids:
                mask_pid = master[col_person_id].astype(str).isin(suspect_ids)
                removed = master[mask_pid].copy()
                for _, r in removed.iterrows():
                    delete_log.append({"reason":"person_id_match","id":r.get(col_person_id),"name":r.get("name"),"org_name":r.get(col_org_name),"extra":"ID in Filter 1216/1708"})
                master = master[~mask_pid].copy()

        # Speichern
        await save_df_text(master, "nk_master_ready")
        log_df = pd.DataFrame(delete_log, columns=["reason","id","name","org_name","extra"])
        await save_df_text(log_df, "nk_delete_log")

        stats = {
            "after": len(master),
            "del_orgtype": log_df[log_df["reason"]=="organisationsart_set"].shape[0],
            "del_limit": log_df[log_df["reason"]=="limit_per_org"].shape[0],
            "del_orgmatch": log_df[log_df["reason"]=="org_match_95"].shape[0],
            "del_pid": log_df[log_df["reason"]=="person_id_match"].shape[0],
            "deleted_total": log_df.shape[0],
        }
        table_html = log_df.head(50).to_html(classes="grid", index=False, border=0) if not log_df.empty else "<i>keine</i>"

        html = f"""
<!doctype html><html lang="de"><head>
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
</style></head>
<body>
<div class="wrap">
  <div class="card">
    <div>Ergebnis: <b>{stats["after"]}</b> Zeilen in <b>nk_master_ready</b></div>
    <p class="muted">
      Entfernt (Organisationsart gesetzt): <b>{stats["del_orgtype"]}</b><br/>
      Entfernt (mehr als 2 pro Organisation): <b>{stats["del_limit"]}</b><br/>
      Entfernt (Orga-Match ≥95% – Filter 1245/851/1521): <b>{stats["del_orgmatch"]}</b><br/>
      Entfernt (Person-ID in Filtern 1216/1708): <b>{stats["del_pid"]}</b><br/>
      <b>Summe entfernt:</b> {stats["deleted_total"]}
    </p>
    <p><a class="btn" href="/neukontakte">Zurück</a></p>
  </div>

  <div class="card">
    <h3>Entfernte Datensätze (Top 50)</h3>
    {table_html}
    <p class="muted">Vollständiges Log in Neon: <b>nk_delete_log</b></p>
  </div>
</div>
</body></html>
"""
        return HTMLResponse(html)
    except Exception as e:
        return HTMLResponse(f"<pre style='padding:24px;color:#b00'>❌ Fehler beim Abgleich: {e}</pre>", status_code=500)

# -----------------------------------------------------------------------------
# Redirects für Pipedrive & Fallback
# -----------------------------------------------------------------------------
@app.get("/overview", include_in_schema=False)
async def overview_redirect(request: Request):
    return RedirectResponse("/neukontakte", status_code=307)

@app.get("/{full_path:path}", include_in_schema=False)
async def catch_all(full_path: str, request: Request):
    return RedirectResponse("/neukontakte", status_code=307)

# -----------------------------------------------------------------------------
# Lokaler Start
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("master:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=False)
