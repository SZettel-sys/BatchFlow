import os
import httpx
import pandas as pd
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

# ======================================================
# ⚙️ FastAPI App Setup
# ======================================================
app = FastAPI(title="BatchFlow – Kampagnenvorbereitung")

# Static-Files (CSS, Logo)
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# ======================================================
# 🔐 Pipedrive OAuth Config
# ======================================================
CLIENT_ID = os.getenv("PD_CLIENT_ID")
CLIENT_SECRET = os.getenv("PD_CLIENT_SECRET")
BASE_URL = os.getenv("BASE_URL")
if not BASE_URL:
    raise ValueError("❌ BASE_URL fehlt")

REDIRECT_URI = f"{BASE_URL}/oauth/callback"
OAUTH_AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
OAUTH_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"
PIPEDRIVE_API_URL = "https://api.pipedrive.com/v1"

user_tokens = {}

# ======================================================
# 🔧 Hilfsfunktionen
# ======================================================
def get_headers():
    token = user_tokens.get("default")
    if not token:
        raise HTTPException(status_code=401, detail="Nicht eingeloggt")
    return {"Authorization": f"Bearer {token}"}


async def fetch_filter_data(filter_id: int, headers: dict):
    """Lädt Personen aus Pipedrive-Filter."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{PIPEDRIVE_API_URL}/persons",
            params={"filter_id": filter_id, "limit": 500},
            headers=headers,
        )
        resp.raise_for_status()
        return resp.json().get("data", []) or []


def clean_filter_data(df, fachbereich, limit, batch_id):
    """Bereinigt Daten nach den Regeln."""
    df = df[df["fachbereich_kampagne"] == fachbereich]
    df = df[df["organisationsart"].isna() | (df["organisationsart"] == "")]
    df = df.groupby("org_id").head(2)
    df["channel"] = "Cold-Mail"
    df["batch_id"] = batch_id
    df = df.head(limit)
    return df


def export_to_excel(df, batch_id):
    filename = f"{batch_id}_Bereinigt.xlsx"
    df.to_excel(filename, index=False)
    return filename


# ======================================================
# 🔐 OAuth Routes
# ======================================================
@app.get("/login")
def login():
    """Startet OAuth-Login bei Pipedrive."""
    url = f"{OAUTH_AUTHORIZE_URL}?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
    return RedirectResponse(url)


@app.get("/oauth/callback")
async def oauth_callback(code: str):
    """OAuth Callback von Pipedrive."""
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
    user_tokens["default"] = access_token
    return RedirectResponse("/overview")


# ======================================================
# 🏠 Übersicht (modernes Layout)
# ======================================================
@app.get("/overview", response_class=HTMLResponse)
async def overview(request: Request):
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    html = """
    <!DOCTYPE html>
    <html>
    <head>
      <title>BatchFlow – Kampagnen Vorbereitung</title>
      <link rel="stylesheet" href="/static/style.css">
    </head>
    <body>
      <header>
        <img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo">
      </header>

      <div class="container">
        <h1>📊 Kampagnen-Vorbereitung</h1>
        <p>Starte mit einem Pipedrive-Filter, um die Kontakte pro Fachbereich zu analysieren.</p>

        <div class="card">
          <form action="/preview" method="post">
            <label>🔍 Pipedrive Filter-ID:</label>
            <input type="number" name="filter_id" placeholder="z. B. 1917" required>

            <div class="form-actions">
              <button type="submit" class="btn-action">➡️ Scan starten</button>
            </div>
          </form>
        </div>

        <p style="margin-top:25px;">
          <a href="/login" class="btn-secondary">🔐 Login mit Pipedrive</a>
        </p>
      </div>
    </body>
    </html>
    """
    return HTMLResponse(html)


# ======================================================
# 📊 Vorschau: Fachbereiche anzeigen
# ======================================================
@app.post("/preview", response_class=HTMLResponse)
async def preview(filter_id: int = Form(...)):
    headers = get_headers()
    data = await fetch_filter_data(filter_id, headers)
    if not data:
        return HTMLResponse("<h3>❌ Keine Daten im Filter gefunden.</h3>")

    df = pd.json_normalize(data)
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    if "fachbereich_kampagne" not in df.columns:
        return HTMLResponse("<h3>❌ Feld 'Fachbereich-Kampagne' nicht gefunden.</h3>")

    summary = df.groupby("fachbereich_kampagne").size().reset_index(name="anzahl")

    rows = ""
    for _, row in summary.iterrows():
        rows += f"<tr><td>{row['fachbereich_kampagne']}</td><td>{row['anzahl']}</td></tr>"

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <title>BatchFlow – Vorschau</title>
      <link rel="stylesheet" href="/static/style.css">
    </head>
    <body>
      <header>
        <img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo">
      </header>

      <div class="container">
        <h1>📋 Fachbereich-Übersicht</h1>
        <p>Filter: <b>{filter_id}</b></p>
        <div class="card">
          <table class="summary">
            <thead><tr><th>Fachbereich-Kampagne</th><th>Anzahl</th></tr></thead>
            <tbody>{rows}</tbody>
          </table>
        </div>

        <div class="card">
          <form action="/prepare" method="post">
            <input type="hidden" name="filter_id" value="{filter_id}">
            <label>🎯 Fachbereich auswählen:</label>
            <input type="text" name="fachbereich" placeholder="z. B. Marketing" required>

            <label>📦 Anzahl Datensätze (Limit):</label>
            <input type="number" name="limit" value="900" required>

            <label>🏷️ Batch-ID:</label>
            <input type="text" name="batch_id" placeholder="z. B. B477" required>

            <div class="form-actions">
              <button type="submit" class="btn-action">📤 Bereinigen & Excel erzeugen</button>
            </div>
          </form>
        </div>

        <p><a href="/overview" class="btn-secondary">⬅️ Zurück</a></p>
      </div>
    </body>
    </html>
    """
    return HTMLResponse(html)


# ======================================================
# 🧮 Bereinigung & Export
# ======================================================
@app.post("/prepare")
async def prepare(
    filter_id: int = Form(...),
    fachbereich: str = Form(...),
    limit: int = Form(...),
    batch_id: str = Form(...),
):
    headers = get_headers()
    data = await fetch_filter_data(filter_id, headers)
    df = pd.json_normalize(data)
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    required = ["fachbereich_kampagne", "org_id", "organisationsart"]
    for col in required:
        if col not in df.columns:
            raise HTTPException(status_code=400, detail=f"Spalte '{col}' fehlt in Filterdaten.")

    df_clean = clean_filter_data(df, fachbereich, limit, batch_id)
    filename = export_to_excel(df_clean, batch_id)
    remaining = len(df_clean)

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <title>BatchFlow – Ergebnis</title>
      <link rel="stylesheet" href="/static/style.css">
    </head>
    <body>
      <header>
        <img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo">
      </header>

      <div class="container">
        <div class="alert">
          ✅ <b>Bereinigung abgeschlossen!</b><br><br>
          Batch-ID: <b>{batch_id}</b><br>
          Fachbereich: <b>{fachbereich}</b><br>
          Verbleibende Datensätze: <b>{remaining}</b>
        </div>
        <a href="/{filename}" download class="btn-action">⬇️ Excel herunterladen</a>
        <p style="margin-top:20px;"><a href="/overview" class="btn-secondary">🔁 Neue Verarbeitung</a></p>
      </div>
    </body>
    </html>
    """
    return HTMLResponse(html)
