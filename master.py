# master.py
import os
from typing import Dict, Any, Optional, List

import httpx
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

# =========================================
# App & Konfiguration
# =========================================
app = FastAPI(title="BatchFlow – Filter-Vorschau (Step 1)")

# Static nur mounten, wenn vorhanden
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

BASE_URL: str = os.getenv("BASE_URL", "").strip()
if not BASE_URL:
    raise ValueError("❌ BASE_URL fehlt in den Umgebungsvariablen.")

PIPEDRIVE_API_URL = "https://api.pipedrive.com/v1"
OAUTH_AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
OAUTH_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"

CLIENT_ID = os.getenv("PD_CLIENT_ID", "").strip()
CLIENT_SECRET = os.getenv("PD_CLIENT_SECRET", "").strip()

# Wenn gesetzt: direkter Live-Zugriff per API-Token, kein OAuth nötig
PD_API_TOKEN = os.getenv("PD_API_TOKEN", "").strip()
USE_API_TOKEN = bool(PD_API_TOKEN)

# Für OAuth-Flow (Token im Speicher – PoC/Single-user)
user_tokens: Dict[str, str] = {}

# =========================================
# Hilfsfunktionen
# =========================================
def oauth_headers() -> Dict[str, str]:
    """Bearer Header nur, wenn wir per OAuth arbeiten."""
    token = user_tokens.get("default")
    return {"Authorization": f"Bearer {token}"} if token else {}

def pd_params(base: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Basis-Query-Params: hängt api_token an, wenn wir im API-Token-Modus sind."""
    base = dict(base or {})
    if USE_API_TOKEN:
        base["api_token"] = PD_API_TOKEN
    return base

async def pd_get(path: str, params: Optional[Dict[str, Any]] = None) -> dict:
    """Zentrale GET-Hilfe für Pipedrive."""
    url = f"{PIPEDRIVE_API_URL}{path}"
    qp = pd_params(params)
    headers = {} if USE_API_TOKEN else oauth_headers()

    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url, params=qp, headers=headers)
        # Bei nicht-200 klare Fehlermeldung ausgeben
        if r.status_code != 200:
            raise RuntimeError(f"Pipedrive-Request fehlgeschlagen ({r.status_code}): {r.text}")
        return r.json()

async def fetch_persons_by_filter(filter_id: int) -> List[dict]:
    """
    Holt Personen anhand einer Filter-ID.
    WICHTIG: /persons/list verwenden (OAuth-kompatibel; funktioniert auch mit api_token).
    """
    data = await pd_get(
        "/persons/list",
        params={"filter_id": filter_id, "limit": 500}
    )
    return data.get("data") or []

async def fetch_user_me() -> dict:
    """Aktuellen User + Company prüfen."""
    data = await pd_get("/users/me")
    return data.get("data") or {}

# =========================================
# Routing – Basics
# =========================================
@app.get("/", response_class=HTMLResponse)
async def root():
    return RedirectResponse("/overview")

@app.get("/login")
def login():
    if USE_API_TOKEN:
        # Im Token-Modus ist Login nicht nötig
        return RedirectResponse("/overview")
    return RedirectResponse(
        f"{OAUTH_AUTHORIZE_URL}?client_id={CLIENT_ID}&redirect_uri={BASE_URL}/oauth/callback"
    )

@app.get("/oauth/callback")
async def oauth_callback(code: str):
    if USE_API_TOKEN:
        return RedirectResponse("/overview")

    async with httpx.AsyncClient(timeout=30) as client:
        token_resp = await client.post(
            OAUTH_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": f"{BASE_URL}/oauth/callback",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
        )
    token_data = token_resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        return HTMLResponse(f"<h3>❌ OAuth-Fehler: {token_data}</h3>", status_code=400)

    # Optional: Safety – Sandbox vermeiden
    try:
        user_tokens["default"] = access_token
        me = await fetch_user_me()
        if "Sandbox" in (me.get("company_name") or ""):
            user_tokens.clear()
            return HTMLResponse("<h3>🚫 Sandbox-Account erkannt. Bitte Live-Account wählen.</h3>", status_code=400)
    except Exception as e:
        user_tokens.clear()
        return HTMLResponse(f"<h3>❌ Token-Prüfung fehlgeschlagen: {e}</h3>", status_code=400)

    return RedirectResponse("/overview")

# =========================================
# UI – Overview
# =========================================
@app.get("/overview", response_class=HTMLResponse)
async def overview(request: Request):
    # Wenn kein API-Token: sicherstellen, dass OAuth bereits erfolgt ist
    if not USE_API_TOKEN and "default" not in user_tokens:
        return RedirectResponse("/login")

    mode = "API-Token (Live)" if USE_API_TOKEN else "OAuth"
    html = f"""
    <!DOCTYPE html>
    <html lang="de">
    <head>
      <meta charset="UTF-8" />
      <title>BatchFlow – Filter-Vorschau</title>
      <link rel="stylesheet" href="/static/style.css">
      <style>
        select, input[type=number], input[type=text] {{
          width: 100%;
          padding: 10px;
          border: 1px solid #ccc;
          border-radius: 8px;
          font-size: 15px;
          margin-bottom: 15px;
        }}
        .mode-pill {{
          display:inline-block; padding:6px 10px; border-radius:14px; font-size:12px;
          background:{'#e8f6f0' if USE_API_TOKEN else '#e6f0fb'}; color:{'#0f6c4f' if USE_API_TOKEN else '#0a3b8a'};
        }}
      </style>
    </head>
    <body>
      <header>
        <img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo">
      </header>

      <div class="container">
        <h1>📊 Erster Filter – Master aufbauen <span class="mode-pill">{mode}</span></h1>

        <div class="card">
          <form action="/preview" method="post">
            <label>🔍 Auswahl der Selektion:</label>
            <select name="filter_id" required>
              <option value="1914">🟢 Selektion Neukontakte (1914)</option>
              <option value="1917">🟠 Selektion Nachfass (1917)</option>
              <option value="2495">🔵 Selektion Refresh (2495)</option>
            </select>
            <div class="form-actions">
              <button class="btn-action" type="submit">Scan starten</button>
            </div>
          </form>
        </div>

        <p style="margin-top:18px;">
          <a class="btn-secondary" href="/check">🔎 Verbindung prüfen</a>
          {"&nbsp;&nbsp;<a class='btn-secondary' href='/login'>🔐 Neu anmelden</a>" if not USE_API_TOKEN else ""}
        </p>
      </div>
    </body>
    </html>
    """
    return HTMLResponse(html)

# =========================================
# Vorschau – einfache Statistik
# =========================================
@app.post("/preview", response_class=HTMLResponse)
async def preview(filter_id: int = Form(...)):
    try:
        persons = await fetch_persons_by_filter(int(filter_id))
    except Exception as e:
        return HTMLResponse(f"<h3>❌ Abruf fehlgeschlagen:</h3><pre>{e}</pre>", status_code=500)

    if not persons:
        return HTMLResponse("<h3>✅ Keine Datensätze im Filter gefunden.</h3>")

    # Mini-Statistik nach 'fachbereich_kampagne' (falls vorhanden)
    counts: Dict[str, int] = {}
    for p in persons:
        # robust lowercasing / nested fields tolerant
        fb = (
            p.get("fachbereich_kampagne")
            or p.get("custom_fields", {}).get("fachbereich_kampagne")
            or "-"
        )
        counts[str(fb)] = counts.get(str(fb), 0) + 1

    count_rows = "".join(
        f"<tr><td>{name}</td><td style='text-align:right'>{anz}</td></tr>"
        for name, anz in sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    )

    # kleine Personen-Vorschau
    def safe(x): return x if x is not None else "-"
    preview_rows = ""
    for p in persons[:50]:  # nur erste 50 zeigen
        preview_rows += f"<tr><td>{safe(p.get('id'))}</td><td>{safe(p.get('name'))}</td><td>{safe(p.get('email'))}</td><td>{safe(p.get('org_name'))}</td></tr>"

    html = f"""
    <!DOCTYPE html>
    <html lang="de">
    <head>
      <meta charset="UTF-8" />
      <title>BatchFlow – Vorschau</title>
      <link rel="stylesheet" href="/static/style.css">
      <style>
        table.summary {{ width:100%; border-collapse:collapse; background:white; border-radius:10px; overflow:hidden; }}
        table.summary th, table.summary td {{ padding:10px 14px; border:1px solid #eee; }}
        table.summary th {{ background:#f0f6fb; text-align:left; }}
      </style>
    </head>
    <body>
      <header><img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo"></header>
      <div class="container">
        <h1>📋 Vorschau – Filter {filter_id}</h1>

        <div class="card">
          <h3>Fachbereich-Kampagne – Verteilung</h3>
          <table class="summary">
            <thead><tr><th>Fachbereich</th><th style="text-align:right">Anzahl</th></tr></thead>
            <tbody>{count_rows}</tbody>
          </table>
        </div>

        <div class="card">
          <h3>Erste 50 Datensätze</h3>
          <table class="summary">
            <thead><tr><th>Person-ID</th><th>Name</th><th>E-Mail</th><th>Organisation</th></tr></thead>
            <tbody>{preview_rows}</tbody>
          </table>
          <p><small>Gesamt: {len(persons)} Datensätze</small></p>
        </div>

        <p><a class="btn-secondary" href="/overview">⬅️ Zurück</a></p>
      </div>
    </body>
    </html>
    """
    return HTMLResponse(html)

# =========================================
# Verbindung prüfen (zeigt Company/Modus)
# =========================================
@app.get("/check", response_class=HTMLResponse)
async def check():
    mode = "API-Token (Live)" if USE_API_TOKEN else ("OAuth" if "default" in user_tokens else "nicht angemeldet")
    try:
        me = await fetch_user_me()
        company = me.get("company_name") or "-"
        user = me.get("name") or "-"
        email = me.get("email") or "-"
        ok = True
        msg = ""
    except Exception as e:
        ok = False
        company = user = email = "-"
        msg = str(e)

    color = "#0a7c3a" if ok else "#a10"
    html = f"""
    <html><body style="font-family:Arial; margin:40px;">
      <h2>Verbindungscheck</h2>
      <p><b>Modus:</b> {mode}</p>
      <p><b>Unternehmen:</b> {company}</p>
      <p><b>User:</b> {user} ({email})</p>
      {"<p style='color:"+color+"'><b>✅ OK</b></p>" if ok else "<p style='color:"+color+"'><b>❌ Fehler:</b> "+msg+"</p>"}
      <p><a href="/overview">Zurück</a></p>
    </body></html>
    """
    return HTMLResponse(html)
