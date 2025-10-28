# master.py
import os
import io
import csv
import uuid
import asyncio
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from starlette.middleware.cors import CORSMiddleware

try:
    import asyncpg  # type: ignore
except Exception:
    asyncpg = None

from jinja2 import Environment, DictLoader, select_autoescape

# -----------------------------------------------------------------------------
# HTML (Jinja2) – kompaktes, klares Formular + Progress + "Entfernte Datensätze"
# -----------------------------------------------------------------------------
TEMPLATE_HTML = r"""
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>BatchFlow – Kampagnen-Tool</title>
  <style>
    :root { --bg:#f6f7fb; --card:#fff; --muted:#64748b; --text:#0f172a; --line:#e5e7eb; --primary:#0ea5e9; --ink:#111827; }
    html,body { height:100% }
    body { margin:0; background:var(--bg); color:var(--text); font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; }
    header { display:flex; justify-content:space-between; align-items:center; padding:18px 22px; border-bottom:1px solid var(--line); background:#fff; }
    header .crumb { color:var(--muted); font-size:14px; }
    header .right { color:var(--muted); font-size:14px; }
    main { padding:24px; display:flex; justify-content:center; }
    .wrap { width:min(100%, 980px); }
    h1 { font-size:18px; margin:10px 0 18px; }
    .card { background:var(--card); border:1px solid var(--line); border-radius:16px; padding:18px; box-shadow:0 1px 1px rgba(0,0,0,0.02); }
    .grid { display:grid; grid-template-columns: repeat(12, 1fr); gap:14px; }
    .col-4 { grid-column: span 4; min-width:260px }
    .col-6 { grid-column: span 6; min-width:260px }
    .col-12 { grid-column: span 12; }
    label { display:block; font-size:13px; color:var(--muted); margin:0 0 6px }
    input[type="text"], input[type="number"], select {
      width:100%; padding:10px 12px; border:1px solid var(--line); border-radius:10px; background:#fff; color:var(--text);
    }
    .hint { font-size:12px; color:var(--muted); margin-top:4px }
    .row { display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
    .pill { display:inline-flex; align-items:center; gap:6px; border:1px solid var(--line); padding:2px 10px; border-radius:999px; font-size:12px; color:var(--muted) }
    .actions { display:flex; gap:10px; margin-top:10px }
    button { border-radius:10px; padding:10px 14px; border:1px solid #0b1220; background:#0b1220; color:#fff; cursor:pointer; }
    button.primary { background:var(--primary); border-color:var(--primary); }
    #status { min-height:22px; font-size:13px; margin-top:10px; }
    #status .ok { color:#065f46 }
    #status .err { color:#b91c1c }
    .progress { width:100%; height:10px; border-radius:999px; background:#e2e8f0; overflow:hidden; margin-top:6px; }
    .bar { height:100%; width:0%; background:var(--primary); transition: width .2s linear; }
    .hidden { display:none }

    table { width:100%; border-collapse: collapse; margin-top: 12px; }
    th, td { text-align:left; padding:8px 10px; border-bottom:1px solid #f1f5f9; font-size:12px }
    th { background:#f8fafc; position:sticky; top:0; }
    .small { font-size:12px; color:var(--muted) }
  </style>
</head>
<body>
  <header>
    <div class="crumb">← Kampagne wählen</div>
    <div class="right">angemeldet</div>
  </header>

  <main>
    <div class="wrap">
      <h1>Neukontakte</h1>

      <div class="card">
        <form id="bf-form" class="grid">
          <div class="col-4">
            <label for="limitPerOrg">Kontakte pro Organisation</label>
            <select id="limitPerOrg" name="limitPerOrg">
              <option>1</option><option selected>2</option><option>3</option><option>4</option><option>5</option>
            </select>
            <div class="hint">Beispiel: 2</div>
          </div>

          <div class="col-6">
            <label for="batchId">Batch ID</label>
            <input type="text" id="batchId" name="batchId" placeholder="z. B. B111" />
            <div class="hint">Beispiel: B111</div>
          </div>

          <div class="col-6">
            <label for="campaign">Kampagnenname</label>
            <input type="text" id="campaign" name="campaign" placeholder="z. B. Q4_2025_Neukunden" required />
            <div class="hint">Wird als „Cold-Mailing Import“ in der Exportdatei gesetzt.</div>
          </div>

          <div class="col-4">
            <label for="mode">Modus</label>
            <select id="mode" name="mode">
              <option>Neukontakte</option>
              <option>Nachfass</option>
              <option>Refresh</option>
            </select>
          </div>

          <div class="col-12 row">
            <span class="pill">Channel: <strong>Cold E-Mail</strong></span>
            <div class="actions">
              <button type="button" id="btn-export" class="primary">Abgleich &amp; Download</button>
            </div>
          </div>

          <div class="col-12" id="status">
            <div id="phase" class="ok"></div>
            <div class="progress hidden" id="prog"><div class="bar" id="bar"></div></div>
          </div>

          <div class="col-12">
            <div class="small">Nach dem Download werden die im Abgleich entfernten Datensätze unten aufgelistet.</div>
            <div id="removed"></div>
          </div>
        </form>
      </div>
    </div>
  </main>

  {% raw %}
  <script>
    function setPhase(msg, ok=true) {
      const p = document.getElementById("phase");
      p.textContent = msg || "";
      p.className = ok ? "ok" : "err";
    }
    function showProgress(on) { document.getElementById("prog").classList.toggle("hidden", !on); }
    function setProgress(percent) { document.getElementById("bar").style.width = `${Math.max(0, Math.min(100, percent))}%`; }

    function table(headings, rows) {
      const t = document.createElement("table");
      const trh = document.createElement("tr");
      headings.forEach(h => { const th = document.createElement("th"); th.textContent = h; trh.appendChild(th); });
      t.appendChild(trh);
      rows.forEach(r => {
        const tr = document.createElement("tr");
        headings.forEach(k => { const td = document.createElement("td"); td.textContent = r[k] ?? ""; tr.appendChild(td); });
        t.appendChild(tr);
      });
      return t;
    }

    function renderRemoved(rows) {
      const wrap = document.getElementById("removed");
      wrap.innerHTML = "";
      if (!rows || rows.length === 0) {
        wrap.textContent = "Keine entfernten Datensätze erfasst.";
        return;
      }
      const headings = ["Grund","Prospect ID","Organisation ID","Person ID","Person E-Mail","Match Info"];
      wrap.appendChild(table(headings, rows));
    }

    async function startExport() {
      setPhase(""); showProgress(false); setProgress(0);
      const payload = new URLSearchParams({
        campaign: document.getElementById("campaign").value.trim(),
        mode: document.getElementById("mode").value,
        limitPerOrg: document.getElementById("limitPerOrg").value,
        batchId: document.getElementById("batchId").value.trim()
      });
      if (!payload.get("campaign")) { setPhase("Bitte Kampagnenname angeben.", false); return; }

      setPhase("Starte Abgleich …");
      const r = await fetch("/export_start", { method:"POST", headers:{ "Content-Type":"application/x-www-form-urlencoded" }, body: payload });
      if (!r.ok) { setPhase(await r.text() || "Fehler beim Start.", false); return; }
      const { job_id } = await r.json();
      await poll(job_id);
    }

    async function poll(job_id) {
      showProgress(true);
      let done = false, tries = 0;
      while (!done && tries < 3600) {
        await new Promise(res => setTimeout(res, 300));
        const r = await fetch(`/export_progress?job_id=${encodeURIComponent(job_id)}`);
        if (!r.ok) { setPhase(await r.text() || "Fehler beim Fortschritt.", false); return; }
        const s = await r.json();
        if (s.error) { setPhase(s.error, false); return; }
        setPhase(s.phase || "Arbeite …");
        setProgress(s.percent ?? 0);
        done = !!s.done;
        tries++;
      }
      if (done) {
        setPhase("Export bereit – Download startet …");
        window.location.href = `/export_download?job_id=${encodeURIComponent(job_id)}`;
        setTimeout(async () => {
          const rr = await fetch(`/export_removed?job_id=${encodeURIComponent(job_id)}`);
          if (rr.ok) { const data = await rr.json(); renderRemoved(data.removed || []); setPhase(`Fertig – ${data.total_rows ?? 0} Zeile(n) exportiert`); }
        }, 800);
      } else {
        setPhase("Zeitüberschreitung beim Export.", false);
      }
    }

    document.getElementById("btn-export").addEventListener("click", startExport);
  </script>
  {% endraw %}
</body>
</html>
"""

env = Environment(loader=DictLoader({"index.html": TEMPLATE_HTML}),
                  autoescape=select_autoescape(["html", "xml"]))

def render_template(name: str, **context) -> HTMLResponse:
    return HTMLResponse(env.get_template(name).render(**context))

# -----------------------------------------------------------------------------
# FastAPI + DB
# -----------------------------------------------------------------------------
app = FastAPI(title="BatchFlow")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)

DATABASE_URL = os.getenv("DATABASE_URL")
POOL: Optional["asyncpg.pool.Pool"] = None  # type: ignore

@app.on_event("startup")
async def startup():
    global POOL
    if asyncpg and DATABASE_URL:
        try:
            POOL = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=1, max_size=5)
        except Exception as e:
            print(f"[BatchFlow] Hinweis: Konnte DB-Pool nicht erstellen: {e}")

@app.on_event("shutdown")
async def shutdown():
    global POOL
    if POOL:
        await POOL.close()
        POOL = None

# -----------------------------------------------------------------------------
# Export-Spalten & Hilfen
# -----------------------------------------------------------------------------
EXPORT_COLUMNS = [
    "Batch ID","Channel","Cold-Mailing Import","Prospect ID","Organisation ID","Organisation Name",
    "Person ID","Person Vorname","Person Nachname","Person Titel","Person Geschlecht",
    "Person Position","Person E-Mail","XING Profil","LinkedIn URL",
]

def map_gender(val: Any) -> str:
    if val is None or val == "": return "unbekannt"
    s = str(val).strip().lower()
    if s in {"1","m","male","mann","herr"}: return "männlich"
    if s in {"2","w","f","female","frau"}: return "weiblich"
    if s in {"3","d","x","divers","nichtbinär","nonbinary"}: return "divers"
    return s

def normalize_row(raw: Dict[str, Any], campaign: str, batch_id_const: str) -> Dict[str, Any]:
    return {
        "Batch ID": raw.get("batch_id") or batch_id_const or "",
        "Channel": "Cold E-Mail",
        "Cold-Mailing Import": campaign or "",
        "Prospect ID": raw.get("prospect_id") or "",
        "Organisation ID": raw.get("organisation_id") or raw.get("org_id") or "",
        "Organisation Name": raw.get("organisation_name") or raw.get("org_name") or "",
        "Person ID": raw.get("person_id") or "",
        "Person Vorname": raw.get("person_vorname") or raw.get("first_name") or "",
        "Person Nachname": raw.get("person_nachname") or raw.get("last_name") or "",
        "Person Titel": raw.get("person_titel") or raw.get("salutation") or "",
        "Person Geschlecht": map_gender(raw.get("person_geschlecht") or raw.get("gender")),
        "Person Position": raw.get("person_position") or raw.get("position") or "",
        "Person E-Mail": raw.get("person_email") or raw.get("email") or "",
        "XING Profil": raw.get("xing_profil") or raw.get("xing_url") or "",
        "LinkedIn URL": raw.get("linkedin_url") or raw.get("linkedin") or "",
    }

# -----------------------------------------------------------------------------
# Abgleich-Parameter (Filter IDs)
# -----------------------------------------------------------------------------
ORG_FILTERS = (1245, 851, 1521)
PERSON_FILTERS = (1216, 1708)

# -----------------------------------------------------------------------------
# SQL-Bausteine (ohne Views; Quelle = nk_master_ready)
# -----------------------------------------------------------------------------
SQL_ENABLE_TRGM = "CREATE EXTENSION IF NOT EXISTS pg_trgm;"
SQL_ENABLE_FUZZY = "CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;"

# Basis-Selection mit pro-Orga-Limit (Window-Function)
SQL_BASE_READY = """
WITH base AS (
  SELECT
    COALESCE(batch_id,'') AS batch_id,
    prospect_id,
    COALESCE(organisation_id, org_id) AS organisation_id,
    COALESCE(organisation_name, org_name) AS organisation_name,
    person_id,
    COALESCE(person_vorname, first_name) AS first_name,
    COALESCE(person_nachname, last_name) AS last_name,
    salutation,
    gender,
    position,
    email,
    xing_url,
    linkedin_url,
    ROW_NUMBER() OVER (PARTITION BY COALESCE(organisation_id, org_id) ORDER BY person_id) AS rn
  FROM nk_master_ready
)
SELECT * FROM base WHERE rn <= $1
"""

# Konsolidierte Filter-Tabellen (erwartet):
# - pd_orgs_filters(filter_id int, organisation_id text, organisation_name text)
# - pd_persons_filters(filter_id int, person_id text, email text)
# Falls sie fehlen, wird der Abgleich elegant übersprungen.
async def table_exists(conn, table: str) -> bool:
    return bool(await conn.fetchval("""
        SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)
    """, table))

async def ensure_delete_log(conn):
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS nk_delete_log (
      id bigserial primary key,
      job_id text,
      prospect_id text,
      organisation_id text,
      person_id text,
      person_email text,
      reason text,
      match_info text,
      created_at timestamptz default now()
    );
    """)

async def fetch_rows_with_match_and_log(conn, limit_per_org: int, job_id: str) -> Dict[str, Any]:
    # Basisdaten (limitiert pro Orga)
    base_rows = await conn.fetch(SQL_BASE_READY, limit_per_org)
    rows = [dict(r) for r in base_rows]

    # Wenn keine Filtertabellen existieren, gibt es keinen Abgleich
    has_org = await table_exists(conn, "pd_orgs_filters")
    has_per = await table_exists(conn, "pd_persons_filters")

    removed: List[Dict[str, Any]] = []
    if not (has_org or has_per):
        return {"kept": rows, "removed": removed}

    # Extensions (für Similarity/Levenshtein)
    await conn.execute(SQL_ENABLE_TRGM)
    await conn.execute(SQL_ENABLE_FUZZY)

    # Temp-Tabelle mit den selektierten Kandidaten (nur für den Match, beschleunigt Join)
    await conn.execute("DROP TABLE IF EXISTS tmp_bf_candidates;")
    await conn.execute("""
        CREATE TEMP TABLE tmp_bf_candidates AS
        SELECT * FROM (""" + SQL_BASE_READY + """) AS t;
    """, limit_per_org)

    # ORG-Match (Similarity ≥ 0.95) gegen Filter 1245/851/1521
    org_removed = []
    if has_org:
        org_removed = await conn.fetch(f"""
            WITH org_f AS (
              SELECT DISTINCT organisation_id, organisation_name
              FROM pd_orgs_filters
              WHERE filter_id = ANY($1::int[])
            )
            SELECT c.prospect_id, c.organisation_id, c.person_id, c.email AS person_email,
                   'ORG_MATCH_95' AS reason,
                   CONCAT('sim=', to_char(similarity(lower(COALESCE(c.organisation_name,'')),
                                                     lower(COALESCE(org_f.organisation_name,''))), 'FM0D00'),
                          ' ↔ ', org_f.organisation_name) AS match_info
            FROM tmp_bf_candidates c
            JOIN org_f
              ON (
                 similarity(lower(COALESCE(c.organisation_name,'')),
                            lower(COALESCE(org_f.organisation_name,''))) >= 0.95
              );
        """, list(ORG_FILTERS))

    # PERSON-Match (ID identisch) gegen Filter 1216/1708
    per_removed = []
    if has_per:
        per_removed = await conn.fetch(f"""
            WITH per_f AS (
              SELECT DISTINCT person_id, email FROM pd_persons_filters
              WHERE filter_id = ANY($1::int[])
            )
            SELECT c.prospect_id, c.organisation_id, c.person_id, c.email AS person_email,
                   'PERSON_ID_MATCH' AS reason,
                   'id='||COALESCE(c.person_id,'') AS match_info
            FROM tmp_bf_candidates c
            JOIN per_f ON per_f.person_id = c.person_id;
        """, list(PERSON_FILTERS))

    # vereinigen & loggen
    removed_all = { (r["person_id"] or "", r["prospect_id"] or ""): dict(r) for r in org_removed + per_removed }
    removed = list(removed_all.values())

    if removed:
        await ensure_delete_log(conn)
        await conn.executemany("""
            INSERT INTO nk_delete_log (job_id, prospect_id, organisation_id, person_id, person_email, reason, match_info)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
        """, [
            (job_id,
             r.get("prospect_id") or "",
             r.get("organisation_id") or "",
             r.get("person_id") or "",
             r.get("person_email") or "",
             r.get("reason") or "",
             r.get("match_info") or "",
            )
            for r in removed
        ])

    # Übrig bleibende (= Export)
    # Entferne per person_id (falls fehlt, per prospect_id als Fallback)
    removed_pids = { r.get("person_id") for r in removed if r.get("person_id") }
    removed_prids = { r.get("prospect_id") for r in removed if r.get("prospect_id") }

    kept = []
    for r in rows:
        pid = r.get("person_id")
        prid = r.get("prospect_id")
        if (pid and pid in removed_pids) or (not pid and prid in removed_prids):
            continue
        kept.append(r)

    return {"kept": kept, "removed": removed}

async def fetch_removed_rows(job_id: str) -> List[Dict[str, Any]]:
    if not POOL or not asyncpg:
        return []
    try:
        async with POOL.acquire() as conn:
            exists = await table_exists(conn, "nk_delete_log")
            if not exists:
                return []
            recs = await conn.fetch("""
                SELECT job_id, prospect_id, organisation_id, person_id, person_email, reason, match_info, created_at
                FROM nk_delete_log
                WHERE ($1 = '' OR job_id = $1)
                ORDER BY created_at DESC
                LIMIT 2000;
            """, job_id or "")
            out = []
            for r in recs:
                d = dict(r)
                out.append({
                    "Grund": d.get("reason",""),
                    "Prospect ID": d.get("prospect_id",""),
                    "Organisation ID": d.get("organisation_id",""),
                    "Person ID": d.get("person_id",""),
                    "Person E-Mail": d.get("person_email",""),
                    "Match Info": d.get("match_info",""),
                })
            return out
    except Exception as e:
        print(f"[BatchFlow] DB-Fehler (fetch_removed_rows): {e}")
        return []

def csv_bytes(rows: List[Dict[str, Any]], campaign: str, batch_id: str) -> bytes:
    buff = io.StringIO()
    writer = csv.DictWriter(buff, fieldnames=EXPORT_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for raw in rows:
        row = normalize_row(raw, campaign, batch_id_const=batch_id)
        row["Channel"] = "Cold E-Mail"
        row["Cold-Mailing Import"] = campaign
        writer.writerow({k: row.get(k, "") for k in EXPORT_COLUMNS})
    return buff.getvalue().encode("utf-8-sig")

# -----------------------------------------------------------------------------
# Jobs
# -----------------------------------------------------------------------------
class Job:
    def __init__(self) -> None:
        self.phase: str = "Warten …"
        self.percent: int = 0
        self.done: bool = False
        self.error: Optional[str] = None
        self.path: Optional[str] = None
        self.total_rows: int = 0
        self.job_id: str = ""

JOBS: Dict[str, Job] = {}

async def run_export(job_id: str, mode: str, limit_per_org: int, campaign: str, batch_id: str):
    job = JOBS[job_id]
    try:
        job.phase = "Lade & gleiche ab …"; job.percent = 10

        if not POOL or not asyncpg:
            # Kein DB: leerer Export
            data = csv_bytes([], campaign=campaign, batch_id=batch_id)
            path = f"/tmp/{uuid.uuid4()}_batchflow_export_{mode}_{campaign}.csv".replace(" ", "_")
            with open(path, "wb") as f: f.write(data)
            job.phase = "Fertig – 0 Zeilen"; job.percent = 100; job.done = True; job.path = path; job.total_rows = 0
            return

        async with POOL.acquire() as conn:
            res = await fetch_rows_with_match_and_log(conn, limit_per_org=limit_per_org, job_id=job_id)
            kept: List[Dict[str, Any]] = res["kept"]
            job.total_rows = len(kept)

            job.phase = "Erzeuge CSV …"; job.percent = 70
            data = csv_bytes(kept, campaign=campaign, batch_id=batch_id)

            job.phase = "Schreibe Datei …"; job.percent = 90
            filename = f"batchflow_export_{mode}_{campaign}.csv".replace(" ", "_")
            path = f"/tmp/{uuid.uuid4()}_{filename}"
            with open(path, "wb") as f:
                f.write(data)

        job.phase = f"Fertig – {job.total_rows} Zeile(n)"; job.percent = 100
        job.done = True; job.path = path
    except Exception as e:
        job.error = f"Export fehlgeschlagen: {e}"
        job.phase = "Fehler"; job.done = True; job.percent = 100

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def index(_: Request):
    return render_template("index.html")

@app.post("/export_start")
async def export_start(
    campaign: str = Form(...),
    mode: str = Form(...),
    limitPerOrg: int = Form(...),
    batchId: str = Form(""),
):
    job_id = str(uuid.uuid4())
    job = Job(); job.job_id = job_id
    JOBS[job_id] = job
    job.phase = "Initialisiere …"; job.percent = 1
    asyncio.create_task(run_export(job_id, mode=mode, limit_per_org=int(limitPerOrg), campaign=campaign, batch_id=batchId or ""))
    return JSONResponse({"job_id": job_id})

@app.get("/export_progress")
async def export_progress(job_id: str):
    job = JOBS.get(job_id)
    if not job: raise HTTPException(404, "Unbekannte Job-ID")
    if job.error:
        return JSONResponse({"error": job.error, "done": True, "phase": job.phase, "percent": job.percent})
    return JSONResponse({"phase": job.phase, "percent": job.percent, "done": job.done, "total_rows": job.total_rows})

@app.get("/export_download")
async def export_download(job_id: str):
    job = JOBS.get(job_id)
    if not job: raise HTTPException(404, "Unbekannte Job-ID")
    if not job.done or not job.path: raise HTTPException(409, "Der Export ist noch nicht bereit.")
    return FileResponse(job.path, media_type="text/csv", filename=os.path.basename(job.path))

@app.get("/export_removed")
async def export_removed(job_id: str):
    rows = await fetch_removed_rows(job_id)
    total_rows = JOBS.get(job_id).total_rows if job_id in JOBS else None
    return JSONResponse({"removed": rows, "total_rows": total_rows})
