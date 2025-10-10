# master.py – Version 3.2 (stabil, asyncpg-basiert)
# Autor: ChatGPT – angepasst für Neon + Pipedrive Filter
import os
import httpx
import asyncpg
import pandas as pd
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

# =====================================================
# ENV Variablen laden
# =====================================================
load_dotenv()

PD_API_TOKEN = os.getenv("PD_API_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not PD_API_TOKEN:
    raise ValueError("❌ PD_API_TOKEN fehlt")
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL fehlt")

# =====================================================
# App & Templates
# =====================================================
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# =====================================================
# Hilfsfunktionen
# =====================================================
async def get_conn():
    """Stellt eine Verbindung zur Neon-Datenbank her"""
    return await asyncpg.connect(DATABASE_URL)

async def save_to_neon(df: pd.DataFrame, table_name: str):
    """Speichert DataFrame in Neon"""
    conn = await get_conn()
    await conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    # dynamische Tabellenerstellung
    cols = ", ".join([f'"{c}" TEXT' for c in df.columns])
    await conn.execute(f"CREATE TABLE {table_name} ({cols})")
    # Daten einfügen
    for _, row in df.iterrows():
        values = [str(v) if v is not None else "" for v in row]
        placeholders = ", ".join(f"${i+1}" for i in range(len(values)))
        await conn.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", *values)
    await conn.close()

async def fetch_all_persons(filter_id: int):
    """Holt alle Personen aus Pipedrive-Filter (inkl. Pagination)"""
    persons = []
    start = 0
    limit = 500
    base_url = "https://api.pipedrive.com/v1"
    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            url = f"{base_url}/persons?filter_id={filter_id}&start={start}&limit={limit}&api_token={PD_API_TOKEN}"
            r = await client.get(url)
            if r.status_code != 200:
                raise Exception(f"Pipedrive API Fehler: {r.text}")
            data = r.json().get("data", [])
            if not data:
                break
            persons.extend(data)
            if len(data) < limit:
                break
            start += limit
    return persons

# =====================================================
# Routes
# =====================================================
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/preview", response_class=HTMLResponse)
async def preview(request: Request, filter_type: str = Form(...)):
    try:
        filters = {
            "Neukontakte": 1914,
            "Nachfass": 1917,
            "Refresh": 2495
        }
        filter_id = filters.get(filter_type)
        if not filter_id:
            raise Exception("Ungültiger Filter ausgewählt")

        persons = await fetch_all_persons(filter_id)
        if not persons:
            raise Exception("Keine Datensätze gefunden")

        df = pd.DataFrame(persons)

        # E-Mail schön extrahieren
        if "email" in df.columns:
            df["E-Mail"] = df["email"].apply(
                lambda x: x[0]["value"] if isinstance(x, list) and x else None
            )
        if "org_name" in df.columns:
            df.rename(columns={"org_name": "Organisation"}, inplace=True)

        # in Neon speichern
        await save_to_neon(df, "temp_master")

        # Vorschau
        cols = [c for c in ["id", "name", "E-Mail", "Organisation"] if c in df.columns]
        preview_df = df[cols] if cols else df.head(50)
        html_table = preview_df.head(50).to_html(classes="table table-striped", index=False)

        return templates.TemplateResponse(
            "preview.html",
            {"request": request, "filter_type": filter_type, "table": html_table, "count": len(df)}
        )

    except Exception as e:
        return HTMLResponse(
            f"<h3 style='color:red;'>❌ Fehler beim Abruf/Speichern:</h3><pre>{str(e)}</pre>",
            status_code=500
        )

@app.get("/overview", response_class=HTMLResponse)
async def overview_redirect(request: Request):
    """Kompatibel mit Pipedrive-Redirect /overview"""
    return templates.TemplateResponse("index.html", {"request": request})
