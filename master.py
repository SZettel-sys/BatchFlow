# master.py – Version 3.1.1 (API-Token + /overview Redirect + Neon Speicher)
# Autor: ChatGPT – 2025-10-10

import os
import httpx
import pandas as pd
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv

# ---------------------------------------------------------------------
# ENV Variablen laden
# ---------------------------------------------------------------------
load_dotenv()

BASE_URL = "https://api.pipedrive.com/v1"
PD_API_TOKEN = os.getenv("PD_API_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# Async Engine (Neon)
engine = create_async_engine(DATABASE_URL, echo=False, future=True)

# FastAPI Setup
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ---------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------
async def fetch_all_persons(filter_id: int, token: str):
    """Lädt alle Personen aus Pipedrive-Filter mit API-Token"""
    persons = []
    start = 0
    limit = 500
    async with httpx.AsyncClient() as client:
        while True:
            url = f"{BASE_URL}/persons?filter_id={filter_id}&start={start}&limit={limit}&api_token={token}"
            r = await client.get(url)
            if r.status_code != 200:
                raise Exception(f"Pipedrive API Fehler: {r.text}")
            data = r.json().get("data", [])
            if not data:
                break
            persons.extend(data)
            start += limit
    return persons


async def save_to_neon(df: pd.DataFrame, table_name: str):
    """Speichert DataFrame in Neon als temporäre Tabelle"""
    async with engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {table_name};"))
        cols = ", ".join([f'"{c}" TEXT' for c in df.columns])
        await conn.execute(text(f"CREATE TABLE {table_name} ({cols});"))
        for _, row in df.iterrows():
            values = ", ".join([f"'{str(v).replace(\"'\", \"''\")}'" for v in row])
            await conn.execute(text(f"INSERT INTO {table_name} VALUES ({values});"))
        await conn.commit()

# ---------------------------------------------------------------------
# Webrouten
# ---------------------------------------------------------------------

@app.get("/overview", response_class=HTMLResponse)
async def overview_redirect(request: Request):
    """Leitet Pipedrive-Redirect /overview zur Startseite weiter"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/preview", response_class=HTMLResponse)
async def preview(request: Request, filter_type: str = Form(...)):
    try:
        # Filter IDs definieren
        filters = {
            "Neukontakte": 1914,
            "Nachfass": 1917,
            "Refresh": 2495
        }

        filter_id = filters.get(filter_type)
        if not filter_id:
            raise Exception("Ungültiger Filter-Typ ausgewählt.")

        persons = await fetch_all_persons(filter_id, PD_API_TOKEN)
        if not persons:
            raise Exception("Keine Datensätze gefunden oder Filter leer.")

        df = pd.DataFrame(persons)
        await save_to_neon(df, "temp_master")

        # E-Mail & Organisation schön extrahieren, falls vorhanden
        if "email" in df.columns:
            df["E-Mail"] = df["email"].apply(lambda x: x[0]["value"] if isinstance(x, list) and x else None)
        if "org_name" in df.columns:
            df.rename(columns={"org_name": "Organisation"}, inplace=True)

        # Nur ausgewählte Felder anzeigen, wenn vorhanden
        cols = [c for c in ["id", "name", "E-Mail", "Organisation"] if c in df.columns]
        preview_df = df[cols] if cols else df.head(50)

        html_table = preview_df.head(50).to_html(classes="table table-striped", index=False)

        return templates.TemplateResponse(
            "preview.html",
            {
                "request": request,
                "filter_type": filter_type,
                "table": html_table,
                "count": len(df)
            }
        )

    except Exception as e:
        return HTMLResponse(
            f"<h3 style='color:red;'>❌ Fehler beim Abruf/Speichern:</h3><pre>{str(e)}</pre>",
            status_code=500
        )

# ---------------------------------------------------------------------
# Ende der Datei
# ---------------------------------------------------------------------
