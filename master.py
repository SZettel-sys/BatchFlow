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

# Optional: asyncpg (Neon/Postgres). Ohne DB läuft die App, exportiert aber leer.
try:
    import asyncpg  # type: ignore
except Exception:  # pragma: no cover
    asyncpg = None

# --------------------------
# Jinja2 (In-Memory Template)
# --------------------------
from jinja2 import Environment, DictLoader, select_autoescape

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
      width:100%; padding:10px 12px; border:1px solid var(--line); border-radius:10px; background:#fff; color:#0b1220;
    }
    .hint { font-size:12px; color:var(--muted); margin-top:4px }
    .row { display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
    .pill { display:inline-flex; align-items:center; gap:6px; border:1px solid var(--line); padding:2px 10px; border-radius:999px; font-size:12px; color:#475569 }
    .actions { display:flex; gap:10px; margin-top:10px }
    button { border-radius:10px; padding:10px 14px; border:1px solid #0b1220; background:#0b1220; color:#fff; cursor:pointer; }
    button.primary { background:var(--primary); border-color:var(--primary); }
    #status { min-height:22px; font-size:13px; margin-top:10px; }
    #status .ok { color:#065f46 }
    #status .err { color:#b91c1c }
    .progress { width:100%; height:10px; border-radius:999px; background:#e2e8f0; overflow:hidden; margin-top:6px; }
    .bar { height:100%; width:0%; background:var(--primary); transition: width .2s linear; }
    .hidden { display:none }
    table { width:100%; border-collapse:collapse; margin-top:12px; }
    th, td { text-align:left; padding:9px 10px; border-bottom:1px solid #f1f5f9; font-size:13px }
    th { background:#f8fafc; position:sticky; top:0; z-index:1 }
    #removedWrap { margin-top:16px; }
    .subhead { font-size:14px; color:#334155; margin:14px 0 6px; font-weight:600 }
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
        </form>

        <div id="removedWrap" class="hidden">
          <div class="subhead">Aufgrund des Abgleichs entfernte Datensätze</div>
          <div id="removed"></div>
        </div>
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

    function renderRemoved(rows) {
      const wrap = document.getElementById("removedWrap");
      const tgt  = document.getElementById("removed");
      tgt.innerHTML = "";
      if (!rows || rows.length === 0) { tgt.textContent = "Keine Datensätze entfernt."; }
      else {
        const cols = ["Grund","Person/ID","Name","Organisation","Extra"];
        const keys = ["reason","id","name","org_name","extra"];
        const table = document.createElement("table");
        const thead = document.createElement("thead");
        const trh = document.createElement("tr");
        cols.forEach(c => { const th=document.createElement("th"); th.textContent=c; trh.appendChild(th); });
        thead.appendChild(trh);
        const tbody = document.createElement("tbody");
        rows.forEach(r => {
          const tr = document.createElement("tr");
          keys.forEach(k => { const td=document.createElement("td"); td.textContent = r[k] ?? ""; tr.appendChild(td); });
          tbody.appendChild(tr);
        });
        table.appendChild(thead); table.appendChild(tbody);
        tgt.appendChild(table);
      }
      wrap.classList.remove("hidden");
    }

    async function startExport() {
      setPhase(""); showProgress(false); setProgress(0);
      document.getElementById("removedWrap").classList.add("hidden");

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
      while (!done && tries < 2400) {
        await new Promise(res => setTimeout(res, 250));
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
        const url = `/export_download?job_id=${encodeURIComponent(job_id)}`;
        const a = document.createElement("a");
        a.href = url; a.download = ""; document.body.appendChild(a); a.click(); a.remove();

        // entfernte Datensätze
        const rr = await fetch(`/export_removed?job_id=${encodeURIComponent(job_id)}`);
        if (rr.ok) {
          const data = await rr.json();
          renderRemoved(data.rows || []);
        } else {
          renderRemoved([]);
        }
        setProgress(100);
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

env = Environment(
    loader=DictLoader({"index.html": TEMPLATE_HTML}),
    autoescape=select_autoescape(["html", "xml"])
)

def render_template(name: str, **context) -> HTMLResponse:
    tpl = env.get_template(name)
    return HTMLResponse(tpl.render(**context))

# --------------------------
# FastAPI & CORS
# --------------------------
app = FastAPI(title="BatchFlow")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)

# --------------------------
# DB (optional, Neon)
# --------------------------
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

# --------------------------
# Export-Spalten (exakte Reihenfolge)
# --------------------------
EXPORT_COLUMNS = [
    "Batch ID",
    "Channel",
    "Cold-Mailing Import",
    "Prospect ID",
    "Organisation ID",
    "Organisation Name",
    "Person ID",
    "Person Vorname",
    "Person Nachname",
    "Person Titel",
    "Person Geschlecht",
    "Person Position",
    "Person E-Mail",
    "XING Profil",
    "LinkedIn URL",
]

def map_gender(val: Any) -> str:
    """IDs/Kürzel → Klartext."""
    if val is None or val == "":
        return "unbekannt"
    s = str(val).strip().lower()
    if s in {"1", "m", "male", "mann", "herr"}:
        return "männlich"
    if s in {"2", "w", "f", "female", "frau"}:
        return "weiblich"
    if s in {"3", "d", "x", "diverse", "nichtbinär", "nonbinary"}:
        return "divers"
    return s

def normalize_row_from_ready(raw: Dict[str, Any], campaign: str, batch_id_const: str) -> Dict[str, Any]:
    """Mappt Zeile aus nk_master_ready auf die 1:1 Exportspalten (mit Fixes/Defaults)."""
    # Spalten kommen bereits mit denselben Namen – wir setzen nur Defaults/Fixes.
    out = {k: raw.get(k) or "" for k in EXPORT_COLUMNS}
    if not out["Batch ID"]:
        out["Batch ID"] = batch_id_const or ""
    out["Channel"] = "Cold E-Mail"
    out["Cold-Mailing Import"] = campaign or ""
    # Geschlecht in Klartext
    out["Person Geschlecht"] = map_gender(out.get("Person Geschlecht"))
    return out

# --------------------------
# SQL (nur deine Tabellen + gequotete Spalten)
# --------------------------
SQL_FETCH_READY = """
SELECT *
FROM (
  SELECT
    COALESCE("Batch ID",'')              AS "Batch ID",
    COALESCE("Prospect ID",'')           AS "Prospect ID",
    COALESCE("Organisation ID",'')       AS "Organisation ID",
    COALESCE("Organisation Name",'')     AS "Organisation Name",
    COALESCE("Person ID",'')             AS "Person ID",
    COALESCE("Person Vorname",'')        AS "Person Vorname",
    COALESCE("Person Nachname",'')       AS "Person Nachname",
    COALESCE("Person Titel",'')          AS "Person Titel",
    COALESCE("Person Geschlecht",'')     AS "Person Geschlecht",
    COALESCE("Person Position",'')       AS "Person Position",
    COALESCE("Person E-Mail",'')         AS "Person E-Mail",
    COALESCE("XING Profil",'')           AS "XING Profil",
    COALESCE("LinkedIn URL",'')          AS "LinkedIn URL",
    ROW_NUMBER() OVER (PARTITION BY "Organisation ID" ORDER BY "Person ID") AS rn
  FROM nk_master_ready
  WHERE
    (CASE
      WHEN $1 = 'Neukontakte' THEN TRUE
      WHEN $1 = 'Nachfass' THEN TRUE
      WHEN $1 = 'Refresh' THEN TRUE
      ELSE TRUE
    END)
) t
WHERE t.rn <= $2
LIMIT $3;
"""

async def fetch_ready_rows(mode: str, limit_per_org: int, hard_limit: int) -> List[Dict[str, Any]]:
    """Zeilen aus nk_master_ready; ohne DB → []."""
    if not POOL or not asyncpg:
        return []
    try:
        rows: List[Dict[str, Any]] = []
        async with POOL.acquire() as conn:
            recs = await conn.fetch(SQL_FETCH_READY, mode, limit_per_org, hard_limit)
            for r in recs:
                rows.append(dict(r))
        return rows
    except Exception as e:
        print(f"[BatchFlow] DB-Fehler fetch_ready_rows: {e}")
        return []

# Entfernte Datensätze aus nk_delete_log (keine Timestamps vorhanden → einfache Liste, begrenzt)
SQL_FETCH_REMOVED = """
SELECT reason, id, name, org_name, extra
FROM nk_delete_log
LIMIT 500;
"""

async def fetch_removed() -> List[Dict[str, Any]]:
    if not POOL or not asyncpg:
        return []
    try:
        out: List[Dict[str, Any]] = []
        async with POOL.acquire() as conn:
            recs = await conn.fetch(SQL_FETCH_REMOVED)
            for r in recs:
                out.append(dict(r))
        return out
    except Exception as e:
        print(f"[BatchFlow] DB-Fehler fetch_removed: {e}")
        return []

# --------------------------
# In-Memory Job-Store
# --------------------------
class Job:
    def __init__(self, campaign: str, batch_id: str) -> None:
        self.phase: str = "Warten …"
        self.percent: int = 0
        self.done: bool = False
        self.error: Optional[str] = None
        self.path: Optional[str] = None
        self.total_rows: int = 0
        self.campaign = campaign
        self.batch_id = batch_id
        self.removed_rows: List[Dict[str, Any]] = []

JOBS: Dict[str, Job] = {}

def csv_bytes_from_ready(rows: List[Dict[str, Any]], campaign: str, batch_id: str) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=EXPORT_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for raw in rows:
        row = normalize_row_from_ready(raw, campaign=campaign, batch_id_const=batch_id)
        writer.writerow({k: row.get(k, "") for k in EXPORT_COLUMNS})
    return buf.getvalue().encode("utf-8-sig")

async def run_export(job_id: str, mode: str, limit_per_org: int):
    job = JOBS.get(job_id)
    if not job:
        return
    try:
        job.phase = "Lade Daten …"; job.percent = 5
        HARD_LIMIT = 20000

        # 1) Daten holen
        rows = await fetch_ready_rows(mode=mode, limit_per_org=limit_per_org, hard_limit=HARD_LIMIT)
        job.total_rows = len(rows)

        # 2) CSV erzeugen
        job.phase = "Erzeuge CSV …"; job.percent = 40
        data = csv_bytes_from_ready(rows, campaign=job.campaign, batch_id=job.batch_id)

        # 3) Datei speichern
        job.percent = 80
        filename = f"batchflow_export_{mode}_{job.campaign}.csv".replace(" ", "_")
        path = f"/tmp/{uuid.uuid4()}_{filename}"
        with open(path, "wb") as f:
            f.write(data)
        job.path = path

        # 4) Entfernte Datensätze laden (nachdem dein Abgleich gelaufen ist)
        job.phase = "Protokoll auslesen …"; job.percent = 90
        job.removed_rows = await fetch_removed()

        job.phase = f"Fertig – {job.total_rows} Zeile(n)"
        job.percent = 100
        job.done = True
    except Exception as e:
        job.error = f"Export fehlgeschlagen: {e}"
        job.phase = "Fehler"
        job.done = True
        job.percent = 100

# --------------------------
# Routes
# --------------------------
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
    job = Job(campaign=campaign, batch_id=batchId or "")
    JOBS[job_id] = job
    job.phase = "Initialisiere …"; job.percent = 1
    asyncio.create_task(run_export(job_id, mode=mode, limit_per_org=int(limitPerOrg)))
    return JSONResponse({"job_id": job_id})

@app.get("/export_progress")
async def export_progress(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "Unbekannte Job-ID")
    if job.error:
        return JSONResponse({"error": job.error, "done": True, "phase": job.phase, "percent": job.percent})
    return JSONResponse({
        "phase": job.phase,
        "percent": job.percent,
        "done": job.done,
        "total_rows": job.total_rows
    })

@app.get("/export_download")
async def export_download(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "Unbekannte Job-ID")
    if not job.done or not job.path:
        raise HTTPException(409, "Der Export ist noch nicht bereit.")
    return FileResponse(job.path, media_type="text/csv", filename=os.path.basename(job.path))

@app.get("/export_removed")
async def export_removed(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "Unbekannte Job-ID")
    # falls sich das Log erst NACH dem Export füllt, nochmal live lesen
    if POOL and asyncpg:
        latest = await fetch_removed()
        if latest:
            job.removed_rows = latest
    return JSONResponse({"rows": job.removed_rows})
