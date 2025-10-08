import os
import httpx
import pandas as pd
from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="Pipedrive Batch Export")

# ================== Konfiguration ==================
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

app.mount("/static", StaticFiles(directory="static"), name="static")

# ================== OAuth ==================
@app.get("/login")
def login():
    url = f"{OAUTH_AUTHORIZE_URL}?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
    return RedirectResponse(url)

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
    user_tokens["default"] = access_token
    return RedirectResponse("/overview")

def get_headers():
    token = user_tokens.get("default")
    return {"Authorization": f"Bearer {token}"} if token else {}

# ================== Batch-Werte abrufen ==================
@app.get("/batch-list")
async def batch_list():
    """Liest alle vorhandenen Werte aus dem Feld 'Batch ID' von Kontakten."""
    headers = get_headers()
    if not headers:
        raise HTTPException(status_code=401, detail="Nicht eingeloggt")

    batches = set()
    async with httpx.AsyncClient() as client:
        # Wir holen z. B. bis zu 500 Kontakte
        resp = await client.get(f"{PIPEDRIVE_API_URL}/persons", params={"limit": 500}, headers=headers)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        for person in data:
            val = person.get("batch_id") or person.get("Batch ID")
            if val:
                if isinstance(val, str):
                    batches.add(val.strip())
                elif isinstance(val, list):
                    for v in val:
                        if isinstance(v, str):
                            batches.add(v.strip())
    return {"batches": sorted(batches)}

# ================== Übersicht / Frontend ==================
@app.get("/overview", response_class=HTMLResponse)
async def overview():
    headers = get_headers()
    if not headers:
        return HTMLResponse("<h3>Bitte zuerst <a href='/login'>mit Pipedrive einloggen</a>.</h3>")

    # Batch-Werte holen
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{BASE_URL}/batch-list")
            batches = resp.json().get("batches", [])
        except Exception:
            batches = []

    # HTML Dropdown
    options_html = "\n".join(f"<option value='{b}'>{b}</option>" for b in batches)

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Pipedrive Batch Export</title>
        <style>
            body {{ font-family: Arial; margin: 60px auto; max-width: 600px; }}
            h1 {{ color: #2a5bd7; }}
            input, select, button {{ padding: 8px; width: 100%; margin-top: 8px; }}
            button {{ background: #2a5bd7; color: white; border: none; cursor: pointer; }}
            button:hover {{ background: #1e46a1; }}
        </style>
    </head>
    <body>
        <h1>📊 Batch-Export</h1>
        <form action="/process" method="post">
            <label>Filter ID:</label>
            <input type="number" name="filter_id" value="1917" required>

            <label>Batch ID auswählen:</label>
            <select name="batch_id" required>
                <option value="">-- bitte wählen --</option>
                {options_html}
            </select>

            <button type="submit">➡️ Export starten</button>
        </form>
    </body>
    </html>
    """
    return HTMLResponse(content=html)

# ================== Verarbeitung ==================
@app.post("/process")
async def process(filter_id: int = Form(...), batch_id: str = Form(...)):
    """Exportiert alle Kontakte aus Filter, deren Batch ID dem gewählten Wert entspricht."""
    headers = get_headers()
    if not headers:
        raise HTTPException(status_code=401, detail="Nicht eingeloggt")

    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{PIPEDRIVE_API_URL}/persons",
                                params={"filter_id": filter_id, "limit": 500},
                                headers=headers)
        resp.raise_for_status()
        data = resp.json().get("data", [])

    if not data:
        return HTMLResponse("<h3>❌ Keine Daten gefunden</h3>")

    df = pd.json_normalize(data)
    # Filter nach Batch ID
    if "batch_id" in df.columns:
        df = df[df["batch_id"].astype(str).str.fullmatch(batch_id, case=False, na=False)]
    elif "Batch ID" in df.columns:
        df = df[df["Batch ID"].astype(str).str.fullmatch(batch_id, case=False, na=False)]

    if df.empty:
        return HTMLResponse(f"<h3>Keine Einträge mit Batch '{batch_id}' gefunden.</h3>")

    # Excel exportieren
    filename = f"pipedrive_batch_{batch_id}.xlsx"
    df.to_excel(filename, index=False)

    return FileResponse(filename,
                        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        filename=filename)
