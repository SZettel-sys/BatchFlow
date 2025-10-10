import os
import ssl
import httpx
import pandas as pd
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from dotenv import load_dotenv

# ========================================
# 🔧 Grundkonfiguration
# ========================================
load_dotenv()
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="static")

PD_CLIENT_ID = os.getenv("PD_CLIENT_ID")
PD_CLIENT_SECRET = os.getenv("PD_CLIENT_SECRET")
PD_API_TOKEN = os.getenv("PD_API_TOKEN")
BASE_URL = "https://api.pipedrive.com/v1"

# ========================================
# 🧠 Neon Datenbankverbindung mit SSL-Handling
# ========================================
DATABASE_URL = os.getenv("DATABASE_URL")

# async-kompatibel machen
if DATABASE_URL and DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

# SSL-Kontext einfügen, falls sslmode in URL enthalten
ssl_context = None
if DATABASE_URL and "sslmode=require" in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("?sslmode=require", "")
    ssl_context = ssl.create_default_context()

connect_args = {"ssl": ssl_context} if ssl_context else {}
engine = create_async_engine(DATABASE_URL, echo=False, future=True, connect_args=connect_args)

# ========================================
# 🔐 OAuth2 Login
# ========================================
user_tokens = {}

@app.get("/login")
async def login():
    redirect_uri = "https://batchflow-4wbo.onrender.com/oauth/callback"
    url = f"https://oauth.pipedrive.com/oauth/authorize?client_id={PD_CLIENT_ID}&redirect_uri={redirect_uri}"
    return RedirectResponse(url)

@app.get("/oauth/callback")
async def oauth_callback(code: str):
    token_url = "https://oauth.pipedrive.com/oauth/token"
    async with httpx.AsyncClient() as client:
        r = await client.post(token_url, data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": "https://batchflow-4wbo.onrender.com/oauth/callback",
            "client_id": PD_CLIENT_ID,
            "client_secret": PD_CLIENT_SECRET
        })
        data = r.json()
        user_tokens["default"] = data.get("access_token")
    return RedirectResponse("/overview")

# ========================================
# 🧩 Hilfsfunktionen
# ========================================
async def fetch_all_persons(filter_id: int, token: str):
    """Lädt ALLE Personen aus einem Pipedrive-Filter (nicht nur 500)"""
    persons = []
    start = 0
    limit = 500
    async with httpx.AsyncClient() as client:
        while True:
            url = f"{BASE_URL}/persons?filter_id={filter_id}&start={start}&limit={limit}"
            headers = {"Authorization": f"Bearer {token}"}
            r = await client.get(url, headers=headers)
            if r.status_code != 200:
                raise Exception(f"Pipedrive API Fehler: {r.text}")
            data = r.json().get("data", [])
            if not data:
                break
            persons.extend(data)
            start += limit
    return persons

async def save_temp_to_neon(df: pd.DataFrame, table_name: str):
    """Speichert ein DataFrame als temporäre Tabelle in Neon"""
    async with engine.begin() as conn:
        await conn.run_sync(lambda sync_conn: df.to_sql(
            table_name, sync_conn, if_exists='replace', index=False
        ))

# ========================================
# 🧭 Frontend /overview
# ========================================
@app.get("/overview", response_class=HTMLResponse)
async def overview(request: Request):
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    html = """
    <html>
    <head>
        <title>Erster Filter – Master aufbauen</title>
        <style>
            body { font-family: 'Source Sans Pro', Arial; background: #f4f6f8; margin: 0; color: #333; }
            header { background: #fff; padding: 20px; border-bottom: 1px solid #ddd; text-align:center; font-size: 22px; font-weight: bold; }
            .container { max-width: 800px; margin: 40px auto; background: #fff; padding: 30px; border-radius: 10px; box-shadow: 0 2px 6px rgba(0,0,0,0.1); }
            input[type=text] { width: 100%; padding: 10px; border-radius: 6px; border: 1px solid #ccc; font-size: 15px; }
            button { background: #009fe3; color: white; border: none; padding: 10px 20px; border-radius: 6px; font-size: 15px; cursor: pointer; }
            button:hover { background: #007bb8; }
            .error { color: red; font-weight: bold; margin-top: 20px; }
        </style>
    </head>
    <body>
        <header>📊 Erster Filter – Master aufbauen</header>
        <div class="container">
            <form method="post" action="/preview">
                <label>🔍 Pipedrive Filter-ID (Personen – Hauptfilter):</label><br>
                <input type="text" name="filter_id" placeholder="z.B. 1914"><br><br>
                <button type="submit">Scan starten</button>
            </form>
            <form action="/login" method="get">
                <button style="margin-top:15px;background:#666;">🔑 Neu anmelden</button>
            </form>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(html)

# ========================================
# 🧮 Vorschau /preview
# ========================================
@app.post("/preview", response_class=HTMLResponse)
async def preview(request: Request, filter_id: str = Form(...)):
    try:
        token = user_tokens.get("default", PD_API_TOKEN)
        persons = await fetch_all_persons(int(filter_id), token)

        # In DataFrame umwandeln
        df = pd.DataFrame(persons)
        df_display = df.head(50)[["id", "name", "email", "org_name"]] if not df.empty else None

        # Temporär speichern
        await save_temp_to_neon(df, "temp_master")

        # HTML-Vorschau
        html = """
        <html><body style="font-family:Arial;background:#f8fafc;">
        <h2 style="text-align:center;">✅ Erste 50 Datensätze</h2>
        <table border="1" cellspacing="0" cellpadding="6" style="margin:auto;border-collapse:collapse;">
        <tr><th>Person-ID</th><th>Name</th><th>E-Mail</th><th>Organisation</th></tr>
        """
        for _, row in df_display.iterrows():
            html += f"<tr><td>{row['id']}</td><td>{row['name']}</td><td>{row['email']}</td><td>{row['org_name']}</td></tr>"
        html += "</table></body></html>"
        return HTMLResponse(html)

    except Exception as e:
        return HTMLResponse(f"<h3 style='color:red;'>❌ Fehler beim Abruf/Speichern:</h3><p>{e}</p>")

# ========================================
# 🚀 Start (lokal)
# ========================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("master:app", host="0.0.0.0", port=8000, reload=True)
