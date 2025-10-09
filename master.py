import os
import httpx
from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

# ================== 🔧 Konfiguration ==================
CLIENT_ID = os.getenv("PD_CLIENT_ID")
CLIENT_SECRET = os.getenv("PD_CLIENT_SECRET")
BASE_URL = os.getenv("BASE_URL")

if not BASE_URL:
    raise ValueError("❌ BASE_URL fehlt in Umgebungsvariablen")

REDIRECT_URI = f"{BASE_URL}/oauth/callback"
OAUTH_AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
OAUTH_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"
PIPEDRIVE_API_URL = "https://api.pipedrive.com/v1"

user_tokens = {}

# ================== 🔒 Token Helper ==================
def get_headers():
    token = user_tokens.get("default")
    if not token:
        raise ValueError("❌ Kein Zugriffstoken gefunden. Bitte zuerst /login aufrufen.")
    return {"Authorization": f"Bearer {token}"}

# ================== 🧩 Static Files ==================
app.mount("/static", StaticFiles(directory="static"), name="static")

# ================== 🏠 Root Redirect ==================
@app.get("/", response_class=HTMLResponse)
async def root():
    return RedirectResponse("/overview")

# ================== 🔐 OAuth Login ==================
@app.get("/login")
def login():
    return RedirectResponse(
        f"{OAUTH_AUTHORIZE_URL}?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
    )

@app.get("/oauth/callback")
async def oauth_callback(code: str):
    async with httpx.AsyncClient() as client:
        token_resp = await client.post(
            OAUTH_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": REDIRECT_URI,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
        )
    token_data = token_resp.json()
    access_token = token_data.get("access_token")

    if not access_token:
        return HTMLResponse(f"<h3>❌ Fehler beim Login: {token_data}</h3>")

    # Überprüfen, ob Token auf Live-Konto zeigt
    async with httpx.AsyncClient() as client:
        me = await client.get(
            "https://api.pipedrive.com/v1/users/me",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        user = me.json().get("data", {})
        if "Sandbox" in user.get("company_name", ""):
            return HTMLResponse("<h3>🚫 Du bist im Sandbox-Konto eingeloggt. Bitte Live-System wählen!</h3>")

    user_tokens["default"] = access_token
    return RedirectResponse("/overview")

# ================== 🔍 Personen aus Filter abrufen ==================
async def fetch_persons_by_filter(filter_id: int):
    headers = get_headers()
    url = "https://api.pipedrive.com/v1/persons"
    params = {"filter_id": filter_id, "limit": 500}

    async with httpx.AsyncClient() as client:
        r = await client.get(url, params=params, headers=headers)
        if r.status_code != 200:
            print(f"❌ Fehler beim Abrufen des Filters {filter_id}: {r.status_code} – {r.text}")
            r.raise_for_status()
        data = r.json()
        return data.get("data", [])

# ================== 📊 Übersicht – Filterauswahl ==================
@app.get("/overview", response_class=HTMLResponse)
async def overview(request: Request):
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    html = """
    <!DOCTYPE html>
    <html lang="de">
    <head>
      <meta charset="UTF-8">
      <title>BatchFlow – Kampagnen Vorbereitung</title>
      <link rel="stylesheet" href="/static/style.css">
      <style>
        select {
          width: 100%;
          padding: 10px;
          border: 1px solid #ccc;
          border-radius: 8px;
          font-size: 15px;
          margin-bottom: 15px;
        }
        option { padding: 5px; }
      </style>
    </head>
    <body>
      <header>
        <img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo">
      </header>

      <div class="container">
        <h1>📊 Erster Filter – Master aufbauen</h1>
        <div class="card">
          <form action="/preview" method="post">
            <label>🔍 Auswahl der Selektion:</label>
            <select name="filter_id" required>
              <option value="1914">🟢 Selektion Neukontakte</option>
              <option value="1917">🟠 Selektion Nachfass</option>
              <option value="2495">🔵 Selektion Refresh</option>
            </select>

            <div class="form-actions">
              <button type="submit" class="btn-action">Scan starten</button>
            </div>
          </form>
        </div>

        <p style="margin-top:25px;">
          <a href="/login" class="btn-secondary">🔐 Neu anmelden</a>
        </p>
      </div>
    </body>
    </html>
    """
    return HTMLResponse(html)

# ================== 🧾 Vorschau – Scan ausführen ==================
@app.post("/preview", response_class=HTMLResponse)
async def preview(filter_id: int = Form(...)):
    try:
        persons = await fetch_persons_by_filter(filter_id)
    except Exception as e:
        return HTMLResponse(f"<h3>❌ Fehler beim Abrufen: {e}</h3>")

    if not persons:
        return HTMLResponse("<h3>✅ Keine Datensätze gefunden oder Filter leer.</h3>")

    # Beispielhafte Übersicht (nur Name + E-Mail)
    rows = "".join(
        f"<tr><td>{p.get('name','-')}</td><td>{p.get('email','-')}</td></tr>"
        for p in persons[:50]  # nur Vorschau
    )

    html = f"""
    <html><body style='font-family:Arial; margin:40px;'>
    <h2>🔎 Vorschau der Filterdaten (max. 50 Einträge)</h2>
    <table border='1' cellspacing='0' cellpadding='6'>
      <tr><th>Name</th><th>E-Mail</th></tr>
      {rows}
    </table>
    <p><b>Gesamt:</b> {len(persons)} Datensätze</p>
    <a href="/overview">⬅️ Zurück</a>
    </body></html>
    """
    return HTMLResponse(html)

# ================== 🧠 Token-Check ==================
@app.get("/check", response_class=HTMLResponse)
async def check_user():
    headers = get_headers()
    async with httpx.AsyncClient() as client:
        resp = await client.get("https://api.pipedrive.com/v1/users/me", headers=headers)
        data = resp.json()

    if not data.get("success"):
        return HTMLResponse(f"<h3>❌ Fehler beim Token-Check: {data}</h3>")

    user = data["data"]
    html = f"""
    <html><body style='font-family:Arial; margin:40px;'>
    <h2>✅ Token aktiv</h2>
    <p><b>Name:</b> {user.get('name')}</p>
    <p><b>E-Mail:</b> {user.get('email')}</p>
    <p><b>Unternehmen:</b> {user.get('company_name')}</p>
    <hr>
    <small>Token gültig für Live-System: https://bizforwardgmbh.pipedrive.com</small>
    </body></html>
    """
    return HTMLResponse(html)
