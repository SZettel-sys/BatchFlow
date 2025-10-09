# master.py
import os
import uuid
import httpx
import pandas as pd
from typing import List, Tuple
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from rapidfuzz import fuzz
import asyncpg

# ==========================
# FastAPI + Static
# ==========================
app = FastAPI(title="BatchFlow – Vorbereitung & Abgleich")

if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# ==========================
# Config / OAuth
# ==========================
CLIENT_ID = os.getenv("PD_CLIENT_ID")
CLIENT_SECRET = os.getenv("PD_CLIENT_SECRET")
BASE_URL = os.getenv("BASE_URL")
if not BASE_URL:
    raise ValueError("❌ BASE_URL fehlt")

REDIRECT_URI = f"{BASE_URL}/oauth/callback"
OAUTH_AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
OAUTH_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"
API = "https://api.pipedrive.com/v1"

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL fehlt (Neon)")

TOKENS = {}  # sehr simpel für PoC (pro Session)

# ==========================
# DB Helpers
# ==========================
async def db() -> asyncpg.Connection:
    return await asyncpg.connect(DATABASE_URL)

async def ensure_schema(conn: asyncpg.Connection):
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS master_tmp (
        run_id UUID,
        contact_id BIGINT,
        person_name TEXT,
        email TEXT,
        org_id BIGINT,
        org_name TEXT,
        organisationsart TEXT,
        channel TEXT,
        batch_id TEXT
    );
    CREATE TABLE IF NOT EXISTS org_tmp (
        run_id UUID,
        org_id BIGINT,
        org_name TEXT,
        source TEXT
    );
    CREATE TABLE IF NOT EXISTS person_tmp (
        run_id UUID,
        person_id BIGINT,
        source TEXT
    );
    CREATE TABLE IF NOT EXISTS org_conflicts (
        run_id UUID,
        contact_id BIGINT,
        master_org_name TEXT,
        other_org_name TEXT,
        score INT,
        decision TEXT  -- NULL (offen), 'keep', 'delete'
    );
    """)

# ==========================
# Pipedrive Helpers
# ==========================
def headers():
    token = TOKENS.get("default")
    if not token:
        raise HTTPException(status_code=401, detail="Nicht eingeloggt")
    return {"Authorization": f"Bearer {token}"}

async def fetch_persons_by_filter(filter_id: int) -> List[dict]:
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.get(f"{API}/persons", params={"filter_id": filter_id, "limit": 500}, headers=headers())
        r.raise_for_status()
        return r.json().get("data") or []

async def fetch_orgs_by_filter(filter_id: int) -> List[dict]:
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.get(f"{API}/organizations", params={"filter_id": filter_id, "limit": 500}, headers=headers())
        r.raise_for_status()
        return r.json().get("data") or []

async def fetch_leads(status: str) -> List[dict]:
    # status: 'open' (inbox) oder 'archived'
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.get(f"{API}/leads", params={"limit": 500, "status": status}, headers=headers())
        r.raise_for_status()
        return r.json().get("data") or []

# ==========================
# Business: Master bauen
# ==========================
def normalize_df_persons(raw: List[dict]) -> pd.DataFrame:
    df = pd.json_normalize(raw)
    # weiche Feldnamen angleichen
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    # häufige Mappings
    # Falls deine Pipedrive-Customfelder andere Keys haben, hier angleichen:
    rename = {}
    if "org_name" not in df.columns:
        # häufig: "org_id.name" etc.
        for c in df.columns:
            if c.endswith("organization_name") or c.endswith("org_id.name"):
                rename[c] = "org_name"
    if rename:
        df = df.rename(columns=rename)
    return df

def clean_master(df: pd.DataFrame, fachbereich: str, limit: int, batch_id: str) -> pd.DataFrame:
    # Pflichtspalten prüfen
    need = ["org_id", "org_name"]
    for col in need:
        if col not in df.columns:
            df[col] = None

    # Feldnamen deiner Beispiele
    # 'fachbereich_kampagne' + 'organisationsart'
    fb = "fachbereich_kampagne" if "fachbereich_kampagne" in df.columns else None
    orgart = "organisationsart" if "organisationsart" in df.columns else None

    if fb:
        df = df[df[fb].astype(str) == str(fachbereich)]
    if orgart:
        df = df[df[orgart].isna() | (df[orgart] == "")]

    # Max 2 Kontakte pro Organisation
    df = df.sort_values(by=["org_id"]).groupby("org_id", dropna=False).head(2)

    # Channel & Batch setzen
    df["channel"] = "Cold-Mail"
    df["batch_id"] = batch_id

    # Limit
    df = df.head(limit)

    # standardisierte Ausgaben
    out = pd.DataFrame({
        "contact_id": df.get("id"),
        "person_name": df.get("name"),
        "email": df.get("email").astype(str) if "email" in df.columns else None,
        "org_id": df.get("org_id"),
        "org_name": df.get("org_name"),
        "organisationsart": df.get("organisationsart") if orgart else "",
        "channel": df["channel"],
        "batch_id": df["batch_id"],
    })
    return out

async def insert_master(conn: asyncpg.Connection, run_id: uuid.UUID, df: pd.DataFrame):
    rows = [(
        run_id, int(x.contact_id) if pd.notna(x.contact_id) else None,
        str(x.person_name) if pd.notna(x.person_name) else None,
        str(x.email) if pd.notna(x.email) else None,
        int(x.org_id) if pd.notna(x.org_id) else None,
        str(x.org_name) if pd.notna(x.org_name) else None,
        str(x.orgorganisationsart) if False else str(x.organisational) if False else str(x.organisational) if False else str(x.org_name)  # placeholder, fixed below
    ) for x in []]  # placeholder – wir setzen die korrekte Liste darunter

    # sauberer Neuaufbau der rows (obiges Placeholder nur, um Linter zu beruhigen)
    rows = []
    for _, r in df.iterrows():
        rows.append((
            run_id,
            int(r["contact_id"]) if pd.notna(r["contact_id"]) else None,
            str(r["person_name"]) if pd.notna(r["person_name"]) else None,
            str(r["email"]) if pd.notna(r["email"]) else None,
            int(r["org_id"]) if pd.notna(r["org_id"]) else None,
            str(r["org_name"]) if pd.notna(r["org_name"]) else None,
            str(r["organisationsart"]) if pd.notna(r["organisationsart"]) else "",
            "Cold-Mail",
            str(r["batch_id"])
        ))

    if rows:
        await conn.executemany("""
        INSERT INTO master_tmp(run_id, contact_id, person_name, email, org_id, org_name, organisationsart, channel, batch_id)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        """, rows)

async def insert_orgs(conn: asyncpg.Connection, run_id: uuid.UUID, data: List[dict], source: str):
    df = pd.json_normalize(data)
    if df.empty:
        return
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    # org_name
    if "name" not in df.columns:
        return
    rows = []
    for _, r in df.iterrows():
        rows.append((
            run_id,
            int(r["id"]) if "id" in df.columns and pd.notna(r["id"]) else None,
            str(r["name"]),
            source
        ))
    await conn.executemany("""
    INSERT INTO org_tmp(run_id, org_id, org_name, source)
    VALUES ($1,$2,$3,$4)
    """, rows)

async def insert_person_ids(conn: asyncpg.Connection, run_id: uuid.UUID, person_ids: List[int], source: str):
    if not person_ids:
        return
    rows = [(run_id, int(pid), source) for pid in person_ids]
    await conn.executemany("""
    INSERT INTO person_tmp(run_id, person_id, source)
    VALUES ($1,$2,$3)
    """, rows)

# ==========================
# Kontakt-Abgleich (ID)
# ==========================
async def auto_remove_by_person_ids(conn: asyncpg.Connection, run_id: uuid.UUID) -> int:
    res = await conn.execute("""
        DELETE FROM master_tmp m
        WHERE m.run_id = $1
          AND m.contact_id IN (SELECT person_id FROM person_tmp WHERE run_id = $1)
    """, run_id)
    # res like "DELETE 42"
    try:
        return int(res.split()[-1])
    except Exception:
        return 0

# ==========================
# Organisations-Abgleich (Fuzzy)
# ==========================
async def build_org_conflicts(conn: asyncpg.Connection, run_id: uuid.UUID, threshold: int = 90) -> int:
    # hole alle master orgs + alle org_tmp
    masters = await conn.fetch("SELECT contact_id, org_name FROM master_tmp WHERE run_id=$1 AND org_name IS NOT NULL", run_id)
    others = await conn.fetch("SELECT org_name FROM org_tmp WHERE run_id=$1 AND org_name IS NOT NULL", run_id)

    def norm(s: str) -> str:
        return (s or "").strip().lower()

    pairs = []
    other_names = [o["org_name"] for o in others]
    for m in masters:
        mname = m["org_name"]
        for on in other_names:
            score = fuzz.token_sort_ratio(norm(mname), norm(on))
            if score >= threshold:
                pairs.append((run_id, m["contact_id"], mname, on, int(score), None))

    if pairs:
        # leere/alte Conflicts des Laufs löschen
        await conn.execute("DELETE FROM org_conflicts WHERE run_id=$1", run_id)
        await conn.executemany("""
            INSERT INTO org_conflicts(run_id, contact_id, master_org_name, other_org_name, score, decision)
            VALUES ($1,$2,$3,$4,$5,$6)
        """, pairs)
        return len(pairs)
    return 0

async def apply_org_decisions(conn: asyncpg.Connection, run_id: uuid.UUID) -> Tuple[int, int]:
    # 'decision' NULL → standardmäßig 'delete' interpretieren, außer explizit 'keep'
    # lösche aus master_tmp, wenn zugehöriger Konflikt nicht keep ist
    deleted = await conn.execute("""
        DELETE FROM master_tmp AS m
        USING org_conflicts AS c
        WHERE m.run_id=$1 AND c.run_id=$1
          AND m.contact_id=c.contact_id
          AND COALESCE(c.decision,'delete')='delete'
    """, run_id)
    try:
        d = int(deleted.split()[-1])
    except Exception:
        d = 0
    kept = await conn.fetchval("""
        SELECT COUNT(*) FROM org_conflicts
        WHERE run_id=$1 AND COALESCE(decision,'delete')='keep'
    """, run_id)
    return d, int(kept or 0)

# ==========================
# OAuth Routes
# ==========================
@app.get("/login")
def login():
    return RedirectResponse(f"{OAUTH_AUTHORIZE_URL}?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}")

@app.get("/oauth/callback")
async def oauth_callback(code: str):
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(OAUTH_TOKEN_URL, data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        })
    data = r.json()
    token = data.get("access_token")
    if not token:
        return HTMLResponse(f"<h3>❌ OAuth-Fehler: {data}</h3>")
    TOKENS["default"] = token
    return RedirectResponse("/overview")

# ==========================
# UI – Overview
# ==========================
@app.get("/overview", response_class=HTMLResponse)
async def overview(request: Request):
    if "default" not in TOKENS:
        return RedirectResponse("/login")

    html = """
    <!DOCTYPE html>
    <html>
    <head>
      <title>BatchFlow – Vorbereitung & Abgleich</title>
      <link rel="stylesheet" href="/static/style.css">
    </head>
    <body>
      <header><img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo"></header>
      <div class="container">
        <h1>📊 Erster Filter – Master aufbauen</h1>
        <div class="card">
          <form action="/preview" method="post">
            <label>🔍 Pipedrive Filter-ID (Personen – Hauptfilter):</label>
            <input type="number" name="filter_id" placeholder="z. B. 1917" required>
            <div class="form-actions">
              <button class="btn-action" type="submit">Scan starten</button>
            </div>
          </form>
        </div>
        <p><a class="btn-secondary" href="/login">🔐 Neu anmelden</a></p>
      </div>
    </body>
    </html>
    """
    return HTMLResponse(html)

# ==========================
# Vorschau pro Fachbereich
# ==========================
@app.post("/preview", response_class=HTMLResponse)
async def preview(filter_id: int = Form(...)):
    persons = await fetch_persons_by_filter(filter_id)
    if not persons:
        return HTMLResponse("<h3>❌ Keine Daten im Filter gefunden.</h3>")
    df = normalize_df_persons(persons)
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    if "fachbereich_kampagne" not in df.columns:
        return HTMLResponse("<h3>❌ Feld 'Fachbereich-Kampagne' nicht gefunden.</h3>")

    summary = df.groupby("fachbereich_kampagne").size().reset_index(name="anzahl")
    rows = "".join(f"<tr><td>{r['fachbereich_kampagne']}</td><td>{int(r['anzahl'])}</td></tr>"
                   for _, r in summary.iterrows())

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <title>BatchFlow – Vorschau</title>
      <link rel="stylesheet" href="/static/style.css">
    </head>
    <body>
      <header><img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo"></header>
      <div class="container">
        <h1>📋 Fachbereich-Übersicht</h1>
        <div class="card">
          <p>Hauptfilter: <b>{filter_id}</b></p>
          <table class="summary">
            <thead><tr><th>Fachbereich-Kampagne</th><th>Anzahl</th></tr></thead>
            <tbody>{rows}</tbody>
          </table>
        </div>
        <div class="card">
          <form action="/build_master" method="post">
            <input type="hidden" name="filter_id" value="{filter_id}">
            <label>🎯 Fachbereich:</label>
            <input type="text" name="fachbereich" placeholder="z. B. Marketing" required>
            <label>📦 Anzahl (Limit):</label>
            <input type="number" name="limit" value="900" required>
            <label>🏷️ Batch-ID:</label>
            <input type="text" name="batch_id" placeholder="z. B. B477" required>
            <div class="form-actions">
              <button class="btn-action" type="submit">Master-Tabelle erstellen</button>
            </div>
          </form>
        </div>
        <p><a href="/overview" class="btn-secondary">⬅️ Zurück</a></p>
      </div>
    </body>
    </html>
    """
    return HTMLResponse(html)

# ==========================
# Master aufbauen + Abgleiche
# ==========================
ORG_FILTERS = [1245, 851, 1521]
PERSON_FILTERS = [1216, 1708]  # zusätzlich zu Leads inbox/archived

@app.post("/build_master", response_class=HTMLResponse)
async def build_master(
    filter_id: int = Form(...),
    fachbereich: str = Form(...),
    limit: int = Form(...),
    batch_id: str = Form(...)
):
    run_id = uuid.uuid4()
    persons = await fetch_persons_by_filter(filter_id)
    if not persons:
        return HTMLResponse("<h3>❌ Keine Daten im Hauptfilter.</h3>")

    df_raw = normalize_df_persons(persons)
    df_master = clean_master(df_raw, fachbereich, limit, batch_id)

    # DB-Schritt
    conn = await db()
    try:
        await ensure_schema(conn)
        # Lauf-Altlasten für dieselbe Batch optional löschen (nicht zwingend):
        # await conn.execute("DELETE FROM master_tmp WHERE batch_id=$1", batch_id)

        await insert_master(conn, run_id, df_master)

        # --- Kontakt-Abgleich (Personen-ID) vorbereiten ---
        # Leads inbox + archived
        leads_inbox = await fetch_leads("open")
        leads_arch = await fetch_leads("archived")
        # aus leads ggf. person_id ziehen
        def lead_person_ids(lst):
            ids = []
            for L in lst:
                pid = (L.get("person_id") or {}).get("value")
                if pid:
                    ids.append(pid)
            return ids
        await insert_person_ids(conn, run_id, lead_person_ids(leads_inbox), "leads_inbox")
        await insert_person_ids(conn, run_id, lead_person_ids(leads_arch), "leads_archived")

        # Personen-Filter 1216, 1708
        for fid in PERSON_FILTERS:
            pdata = await fetch_persons_by_filter(fid)
            pids = [p.get("id") for p in pdata if p.get("id") is not None]
            await insert_person_ids(conn, run_id, pids, f"persons_filter_{fid}")

        # Auto-Remove per Personen-ID
        removed_by_person = await auto_remove_by_person_ids(conn, run_id)

        # --- Organisations-Abgleich (Org-Filter) ---
        for ofid in ORG_FILTERS:
            odata = await fetch_orgs_by_filter(ofid)
            await insert_orgs(conn, run_id, odata, f"org_filter_{ofid}")

        conflicts = await build_org_conflicts(conn, run_id, threshold=90)

        remaining = await conn.fetchval("SELECT COUNT(*) FROM master_tmp WHERE run_id=$1", run_id)

    finally:
        await conn.close()

    # Review-Seite
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <title>BatchFlow – Review</title>
      <link rel="stylesheet" href="/static/style.css">
      <style>.muted{{color:#666}}</style>
    </head>
    <body>
      <header><img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo"></header>
      <div class="container">
        <h1>🔎 Review & Freigabe</h1>
        <div class="card">
          <p><b>Batch:</b> {batch_id} &middot; <b>Fachbereich:</b> {fachbereich}</p>
          <p><b>Nach Kontakt-Abgleich entfernt:</b> {removed_by_person}</p>
          <p><b>Konflikte (Org-Ähnlichkeit ≥ 90%):</b> {conflicts}</p>
          <p><b>Verbleibend in Master:</b> {remaining}</p>
        </div>
        <div class="card">
          <form action="/review_orgs" method="get">
            <input type="hidden" name="run_id" value="{run_id}">
            <button class="btn-action" type="submit">Konflikte prüfen & Entscheidungen setzen</button>
          </form>
        </div>
      </div>
    </body>
    </html>
    """
    return HTMLResponse(html)

# ==========================
# Org-Konflikte prüfen
# ==========================
@app.get("/review_orgs", response_class=HTMLResponse)
async def review_orgs(run_id: str):
    conn = await db()
    try:
        rows = await conn.fetch("""
            SELECT contact_id, master_org_name, other_org_name, score, COALESCE(decision,'delete') AS decision
            FROM org_conflicts WHERE run_id=$1
            ORDER BY score DESC, master_org_name
        """, uuid.UUID(run_id))
    finally:
        await conn.close()

    if not rows:
        return HTMLResponse("<h3>✅ Keine Organisations-Konflikte. Du kannst direkt exportieren.</h3>")

    trs = ""
    for r in rows:
        cid = r["contact_id"]
        mname = r["master_org_name"] or "-"
        oname = r["other_org_name"] or "-"
        score = r["score"]
        decision = r["decision"]
        keep_checked = "checked" if decision == "keep" else ""
        del_checked  = "checked" if decision != "keep" else ""
        trs += f"""
        <tr>
          <td>{cid}</td>
          <td>{mname}</td>
          <td>{oname}</td>
          <td>{score}%</td>
          <td>
            <label><input type="radio" name="d_{cid}" value="keep" {keep_checked}> behalten</label>
            <label style="margin-left:10px;"><input type="radio" name="d_{cid}" value="delete" {del_checked}> löschen</label>
          </td>
        </tr>
        """

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <title>BatchFlow – Org-Konflikte</title>
      <link rel="stylesheet" href="/static/style.css">
    </head>
    <body>
      <header><img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo"></header>
      <div class="container">
        <h1>⚖️ Organisations-Konflikte</h1>
        <form action="/apply_orgs" method="post">
          <input type="hidden" name="run_id" value="{run_id}">
          <div class="card">
            <table class="summary">
              <thead><tr><th>Contact-ID</th><th>Master Org</th><th>Andere Org</th><th>Score</th><th>Entscheidung</th></tr></thead>
              <tbody>
                {trs}
              </tbody>
            </table>
          </div>
          <div class="form-actions">
            <button class="btn-action" type="submit">Entscheidungen anwenden</button>
          </div>
        </form>
        <p style="margin-top:20px;">
          <form action="/export" method="get">
            <input type="hidden" name="run_id" value="{run_id}">
            <button class="btn-secondary" type="submit">Nur exportieren (aktuelle Entscheidungen gelten)</button>
          </form>
        </p>
      </div>
    </body>
    </html>
    """
    return HTMLResponse(html)

# ==========================
# Entscheidungen anwenden
# ==========================
@app.post("/apply_orgs", response_class=HTMLResponse)
async def apply_orgs(request: Request, run_id: str = Form(...)):
    form = await request.form()
    # sammle Entscheidungen
    decisions = []
    for k, v in form.items():
        if k.startswith("d_"):  # d_<contact_id>
            try:
                cid = int(k.split("_", 1)[1])
            except Exception:
                continue
            decisions.append((cid, v))

    conn = await db()
    try:
        # Entscheidungen persistieren
        for cid, dec in decisions:
            await conn.execute("""
                UPDATE org_conflicts
                SET decision=$3
                WHERE run_id=$1 AND contact_id=$2
            """, uuid.UUID(run_id), cid, dec)

        deleted, kept = await apply_org_decisions(conn, uuid.UUID(run_id))
        remaining = await conn.fetchval("SELECT COUNT(*) FROM master_tmp WHERE run_id=$1", uuid.UUID(run_id))
    finally:
        await conn.close()

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <title>BatchFlow – angewendet</title>
      <link rel="stylesheet" href="/static/style.css">
    </head>
    <body>
      <header><img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo"></header>
      <div class="container">
        <div class="card">
          <h2>✅ Entscheidungen angewendet</h2>
          <p>Gelöscht (Org-Konflikte): <b>{deleted}</b> &middot; Behalten: <b>{kept}</b></p>
          <p>Verbleibend im Master: <b>{remaining}</b></p>
        </div>
        <div class="form-actions">
          <form action="/export" method="get">
            <input type="hidden" name="run_id" value="{run_id}">
            <button class="btn-action" type="submit">⬇️ Endergebnis exportieren</button>
          </form>
        </div>
      </div>
    </body>
    </html>
    """
    return HTMLResponse(html)

# ==========================
# Export
# ==========================
@app.get("/export")
async def export(run_id: str):
    conn = await db()
    try:
        recs = await conn.fetch("""
            SELECT contact_id, person_name, email, org_id, org_name, channel, batch_id
            FROM master_tmp WHERE run_id=$1
            ORDER BY org_name, contact_id
        """, uuid.UUID(run_id))
    finally:
        await conn.close()

    if not recs:
        return HTMLResponse("<h3>❌ Keine Daten zum Export.</h3>")

    df = pd.DataFrame(recs, columns=["contact_id","person_name","email","org_id","org_name","channel","batch_id"])
    filename = f"Master_Endergebnis_{run_id}.xlsx"
    df.to_excel(filename, index=False)
    return FileResponse(filename, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename=filename)
