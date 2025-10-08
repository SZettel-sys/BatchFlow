import os
import httpx
import asyncpg
import pandas as pd
from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

# ==============================================
# ⚙️ FastAPI Setup
# ==============================================
app = FastAPI(title="Pipedrive SQL Batch Cleaner (OAuth + SQL)")

if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# ==============================================
# 🔐 Konfiguration
# ==============================================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL fehlt (Neon Connection String)")

PD_CLIENT_ID = os.getenv("PD_CLIENT_ID")
PD_CLIENT_SECRET = os.getenv("PD_CLIENT_SECRET")
BASE_URL = os.getenv("BASE_URL", "https://deine-app.onrender.com")
REDIRECT_URI = f"{BASE_URL}/oauth/callback"

OAUTH_AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
OAUTH_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"
PIPEDRIVE_API_URL = "https://api.pipedrive.com/v1"

user_tokens = {}  # access_token pro Benutzer (Session)

# ==============================================
# 🔌 Hilfsfunktionen
# ==============================================
async def get_conn():
    """Verbindung zur Neon-Datenbank"""
    return await asyncpg.connect(DATABASE_URL)

async def fetch_filter_data(filter_id: int, headers: dict):
    """Lädt Daten aus einem Pipedrive-Filter"""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{PIPEDRIVE_API_URL}/persons",
            params={"filter_id": filter_id, "limit": 500},
            headers=headers
        )
        resp.raise_for_status()
        return resp.json().get("data", []) or []

async def load_into_temp_table(conn, table_name: str, data: list):
    """Schreibt ein JSON-Ergebnis in eine temporäre Tabelle"""
    await conn.execute(f"""
        CREATE TEMP TABLE {table_name} (
            contact_id BIGINT,
            name TEXT,
            email TEXT,
            org_name TEXT,
            batch_id TEXT
        );
    """)
    rows = [
        (
            p.get("id"),
            p.get("name"),
            str(p.get("email")),
            str(p.get("org_name")),
            str(p.get("batch_id"))
        )
        for p in data
    ]
    if rows:
        await conn.executemany(
            f"INSERT INTO {table_name}(contact_id, name, email, org_name, batch_id) VALUES ($1,$2,$3,$4,$5)",
            rows
        )

async def clean_batch(conn, exclude_tables: list):
    """Löscht alle Kontakte aus main_batch, die in anderen Tabellen vorkommen"""
    deleted_total = 0
    for t in exclude_tables:
        result = await conn.execute(f"""
            DELETE FROM main_batch
            WHERE contact_id IN (SELECT contact_id FROM {t});
        """)
        # result sieht aus wie 'DELETE <anzahl>'
        try:
            deleted = int(result.split()[-1])
        except Exception:
            deleted = 0
        deleted_total += deleted
    return deleted_total

async def export_result(conn, batch_id: str):
    """Exportiert das Endergebnis aus main_batch"""
    records = await conn.fetch("SELECT * FROM main_batch ORDER BY contact_id;")
    if not records:
        raise HTTPException(status_code=404, detail="Kein bereinigtes Ergebnis gefunden")

    df = pd.DataFrame(records, columns=["contact_id", "name", "email", "org_name", "batch_id"])
    filename = f"cleaned_batch_{batch_id}.xlsx"
    df.to_excel(filename, index=False)
    return filename, len(df)

def get_headers():
    """Header für Pipedrive-Requests erzeugen"""
    token = user_tokens.get("default")
    if not token:
        raise HTTPException(status_code=401, detail="Nicht eingeloggt")
    return {"Authorization": f"Bearer {token}"}

# ==============================================
# 🌐 OAuth Routes
# ==============================================
@app.get("/login")
def login():
    """Startet OAuth-Login bei Pipedrive"""
    url = f"{OAUTH_AUTHORIZE_URL}?client_id={PD_CLIENT_ID}&redirect_uri={REDIRECT_URI}"
    return RedirectResponse(url)

@app.get("/oauth/callback")
async def oauth_callback(code: str):
    """Empfängt OAuth-Callback von Pipedrive"""
    async with httpx.AsyncClient() as client:
        token_resp = await client.post(
            OAUTH_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": REDIRECT_URI,
                "client_id": PD_CLIENT_ID,
                "client_secret": PD_CLIENT_SECRET,
            },
        )
    token_data = token_resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        return HTMLResponse(f"<h3>❌ Fehler beim Login: {token_data}</h3>")
    user_tokens["default"] = access_token
    return RedirectResponse("/overview")

# ==============================================
# 🧭 Frontend
# ==============================================
@app.get("/overview", response_class=HTMLResponse)
def overview():
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Pipedrive Batch Cleaner (OAuth + SQL)</title>
        <style>
            body { font-family: Arial; margin: 60px auto; max-width: 600px; }
            h1 { color: #2a5bd7; }
            input, button { padding: 8px; width: 100%; margin-top: 8px; }
            button { background: #2a5bd7; color: white; border: none; cursor: pointer; }
            button:hover { background: #1e46a1; }
        </style>
    </head>
    <body>
        <h1>📊 Pipedrive Batch Cleaner (SQL)</h1>
        <form action="/process" method="post">
            <label>Hauptfilter ID (Batch):</label>
            <input type="number" name="filter_id_main" placeholder="z. B. 1917" required>

            <label>Batch ID:</label>
            <input type="text" name="batch_id" placeholder="z. B. B434_E-Mail_2025" required>

            <label>Weitere Filter IDs (kommasepariert):</label>
            <input type="text" name="exclude_filters" placeholder="z. B. 1918,1919,1920">

            <button type="submit">➡️ Verarbeitung starten</button>
        </form>
        <p style="margin-top:2em;"><a href="/login">🔐 Login mit Pipedrive</a></p>
    </body>
    </html>
    """
    return HTMLResponse(content=html)

# ==============================================
# 🔁 Hauptprozess
# ==============================================
@app.post("/process", response_class=HTMLResponse)
async def process(
    filter_id_main: int = Form(...),
    batch_id: str = Form(...),
    exclude_filters: str = Form("")
):
    headers = get_headers()
    conn = await get_conn()

    try:
        # === 1️⃣ Hauptdaten laden
        main_data = await fetch_filter_data(filter_id_main, headers)
        if not main_data:
            raise HTTPException(status_code=404, detail="Keine Hauptdaten gefunden")

        # Nur Einträge mit der richtigen Batch-ID
        main_data = [p for p in main_data if str(p.get("batch_id", "")).strip() == batch_id]
        await load_into_temp_table(conn, "main_batch", main_data)
        print(f"✅ Hauptbatch geladen: {len(main_data)} Kontakte")

        # === 2️⃣ Weitere Filter (Abgleich)
        exclude_tables = []
        if exclude_filters.strip():
            for i, fid in enumerate(exclude_filters.split(","), start=1):
                fid = fid.strip()
                if not fid:
                    continue
                data = await fetch_filter_data(int(fid), headers)
                table_name = f"exclude_{i}"
                await load_into_temp_table(conn, table_name, data)
                exclude_tables.append(table_name)
                print(f"📋 Filter {fid} geladen: {len(data)} Kontakte")

        # === 3️⃣ SQL-Abgleich durchführen
        deleted_total = 0
        if exclude_tables:
            deleted_total = await clean_batch(conn, exclude_tables)

        # === 4️⃣ Endergebnis exportieren
        filename, remaining = await export_result(conn, batch_id)

    finally:
        await conn.close()

    # === 5️⃣ Ergebnis-Seite anzeigen
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Batch-Ergebnis</title>
        <style>
            body {{ font-family: Arial; margin: 60px auto; max-width: 600px; }}
            h1 {{ color: #2a5bd7; }}
            a.button {{ background:#2a5bd7; color:white; padding:10px 16px; text-decoration:none; border-radius:6px; }}
            a.button:hover {{ background:#1e46a1; }}
            .info {{ margin-top:20px; }}
        </style>
    </head>
    <body>
        <h1>✅ Batch-Ergebnis</h1>
        <p>Batch-ID: <b>{batch_id}</b></p>
        <p>Gespeicherte Kontakte: <b>{remaining}</b></p>
        <p>Gelöschte Datensätze: <b style="color:red;">{deleted_total}</b></p>
        <div class="info">
            <a href="/{filename}" download class="button">⬇️ Ergebnis herunterladen</a>
        </div>
        <p style="margin-top:2em;"><a href="/overview">🔁 Neuen Durchlauf starten</a></p>
    </body>
    </html>
    """
    return HTMLResponse(content=html)
