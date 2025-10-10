# master.py – Version 3 (mit Neon/PostgreSQL-Integration)
import os
from typing import Dict, Any, Optional, List
import httpx
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text

# =========================================
# Basis-Setup
# =========================================
app = FastAPI(title="BatchFlow – Master Aufbau mit Neon")

if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

BASE_URL = os.getenv("BASE_URL", "").strip()
if not BASE_URL:
    raise ValueError("❌ BASE_URL fehlt!")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL fehlt! Bitte setze die Neon-Verbindungs-URL.")

engine = create_async_engine(DATABASE_URL, echo=False, future=True)

API_URL = "https://api.pipedrive.com/v1"
OAUTH_AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
OAUTH_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"

CLIENT_ID = os.getenv("PD_CLIENT_ID", "").strip()
CLIENT_SECRET = os.getenv("PD_CLIENT_SECRET", "").strip()
API_TOKEN = os.getenv("PD_API_TOKEN", "").strip()
USE_TOKEN = bool(API_TOKEN)
user_tokens: Dict[str, str] = {}

# =========================================
# Pipedrive-Hilfsfunktionen
# =========================================
def oauth_headers() -> Dict[str, str]:
    token = user_tokens.get("default")
    return {"Authorization": f"Bearer {token}"} if token else {}

def pd_params(base: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    base = dict(base or {})
    if USE_TOKEN:
        base["api_token"] = API_TOKEN
    return base

async def pd_get(path: str, params: Optional[Dict[str, Any]] = None) -> dict:
    url = f"{API_URL}{path}"
    headers = {} if USE_TOKEN else oauth_headers()
    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.get(url, params=pd_params(params), headers=headers)
        if r.status_code != 200:
            raise RuntimeError(f"Fehler {r.status_code}: {r.text}")
        return r.json()

async def fetch_user_me() -> dict:
    data = await pd_get("/users/me")
    return data.get("data") or {}

# =========================================
# Pipedrive Personen holen (mit Pagination)
# =========================================
async def fetch_persons_by_filter(filter_id: int) -> List[dict]:
    all_persons = []
    start = 0
    more = True

    while more:
        data = await pd_get(
            "/persons/list",
            params={"filter_id": filter_id, "start": start, "limit": 500}
        )
        persons = data.get("data") or []
        all_persons.extend(persons)
        pagination = data.get("additional_data", {}).get("pagination", {})
        more = pagination.get("more_items_in_collection", False)
        start = pagination.get("next_start", 0)

    return all_persons

def extract_email(person: dict) -> str:
    emails = person.get("email") or []
    if isinstance(emails, list) and emails:
        for e in emails:
            if e.get("primary"):
                return e.get("value")
        return emails[0].get("value")
    return "-"

# =========================================
# Neon Datenbank-Funktionen
# =========================================
async def recreate_master_table():
    """Erstellt oder leert die temporäre Master-Tabelle."""
    async with engine.begin() as conn:
        await conn.execute(text("""
            DROP TABLE IF EXISTS master_temp;
            CREATE TABLE master_temp (
                person_id BIGINT,
                name TEXT,
                email TEXT,
                org_name TEXT,
                fachbereich TEXT,
                batch_id TEXT,
                channel TEXT
            );
        """))

async def insert_master_data(persons: List[dict]):
    """Speichert alle Personen in master_temp."""
    async with AsyncSession(engine) as session:
        for p in persons:
            email = extract_email(p)
            stmt = text("""
                INSERT INTO master_temp (person_id, name, email, org_name, fachbereich, batch_id, channel)
                VALUES (:id, :name, :email, :org, :fach, :batch, :channel)
            """)
            await session.execute(stmt, {
                "id": p.get("id"),
                "name": p.get("name") or "-",
                "email": email,
                "org": p.get("org_name") or "-",
                "fach": p.get("fachbereich_kampagne") or "-",
                "batch": p.get("batch_id") or "-",
                "channel": p.get("channel") or "-"
            })
        await session.commit()

# =========================================
# Routing
# =========================================
@app.get("/", response_class=HTMLResponse)
async def root():
    return RedirectResponse("/overview")

@app.get("/login")
def login():
    if USE_TOKEN:
        return RedirectResponse("/overview")
    return RedirectResponse(
        f"{OAUTH_AUTHORIZE_URL}?client_id={CLIENT_ID}&redirect_uri={BASE_URL}/oauth/callback"
    )

@app.get("/oauth/callback")
async def oauth_callback(code: str):
    if USE_TOKEN:
        return RedirectResponse("/overview")

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            OAUTH_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": f"{BASE_URL}/oauth/callback",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
        )
    data = resp.json()
    token = data.get("access_token")
    if not token:
        return HTMLResponse(f"<h3>❌ OAuth-Fehler:</h3><pre>{data}</pre>", status_code=400)

    user_tokens["default"] = token
    me = await fetch_user_me()
    if "Sandbox" in (me.get("company_name") or ""):
        user_tokens.clear()
        return HTMLResponse("<h3>🚫 Sandbox erkannt – bitte Live-System wählen.</h3>", status_code=400)
    return RedirectResponse("/overview")

# =========================================
# Übersicht (Filter-Auswahl)
# =========================================
@app.get("/overview", response_class=HTMLResponse)
async def overview(request: Request):
    if not USE_TOKEN and "default" not in user_tokens:
        return RedirectResponse("/login")

    mode = "API-Token (Live)" if USE_TOKEN else "OAuth"
    html = f"""
    <html lang="de">
    <head><meta charset="utf-8"><title>BatchFlow – Master</title>
    <style>
      body {{ font-family:Arial; background:#f7f9fb; }}
      .container {{ max-width:700px; margin:60px auto; background:#fff; padding:30px; border-radius:10px; box-shadow:0 2px 6px rgba(0,0,0,0.1); }}
      select {{ width:100%; padding:10px; border:1px solid #ccc; border-radius:6px; }}
      button {{ background:#009fe3; color:white; border:none; padding:10px 20px; border-radius:6px; cursor:pointer; }}
      button:hover {{ background:#007bb8; }}
      .pill {{ background:#e6f2ff; color:#06487a; padding:3px 10px; border-radius:12px; font-size:12px; }}
    </style></head>
    <body>
      <div class="container">
        <h2>📊 Erster Filter – Master aufbauen <span class="pill">{mode}</span></h2>
        <form method="post" action="/preview">
          <label>🔍 Auswahl der Selektion:</label>
          <select name="filter_id" required>
            <option value="1914">🟢 Neukontakte (1914)</option>
            <option value="1917">🟠 Nachfass (1917)</option>
            <option value="2495">🔵 Refresh (2495)</option>
          </select>
          <button type="submit">Scan starten</button>
        </form>
        <p style="margin-top:15px;"><a href="/check">🔎 Verbindung prüfen</a></p>
      </div>
    </body></html>
    """
    return HTMLResponse(html)

# =========================================
# Vorschau + Neon-Speicherung
# =========================================
@app.post("/preview", response_class=HTMLResponse)
async def preview(filter_id: int = Form(...)):
    try:
        persons = await fetch_persons_by_filter(filter_id)
        await recreate_master_table()
        await insert_master_data(persons)
    except Exception as e:
        return HTMLResponse(f"<h3>❌ Fehler beim Abruf/Speichern:</h3><pre>{e}</pre>", status_code=500)

    html = f"""
    <html><body style="font-family:Arial;margin:40px;">
      <h2>✅ Master-Tabelle erfolgreich aufgebaut</h2>
      <p>Filter-ID: <b>{filter_id}</b></p>
      <p>Gespeicherte Datensätze: <b>{len(persons)}</b></p>
      <p>Tabelle: <code>master_temp</code> (temporär in Neon)</p>
      <p><a href="/overview">⬅️ Zurück</a></p>
    </body></html>
    """
    return HTMLResponse(html)

# =========================================
# Verbindung prüfen
# =========================================
@app.get("/check", response_class=HTMLResponse)
async def check():
    mode = "API-Token (Live)" if USE_TOKEN else ("OAuth" if "default" in user_tokens else "nicht angemeldet")
    try:
        me = await fetch_user_me()
        company = me.get("company_name") or "-"
        user = me.get("name") or "-"
        email = me.get("email") or "-"
        msg = f"<p style='color:#0a7c3a'>✅ Verbindung OK</p>"
    except Exception as e:
        company = user = email = "-"
        msg = f"<p style='color:#a10'>❌ Fehler:<br>{e}</p>"
    html = f"""
    <html><body style="font-family:Arial;margin:40px;">
      <h2>Verbindungs-Check</h2>
      <p><b>Modus:</b> {mode}</p>
      <p><b>Unternehmen:</b> {company}</p>
      <p><b>User:</b> {user} ({email})</p>
      {msg}
      <p><a href="/overview">Zurück</a></p>
    </body></html>"""
    return HTMLResponse(html)
