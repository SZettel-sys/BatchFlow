# master_nf_directfilter.py — BatchFlow (FastAPI + Pipedrive + Neon)
# Optimiert: direkte Batch-ID-Suche via /persons/search + Fortschrittsupdates

import os
import re
import io
import sys
import time
import uuid
from typing import Optional, Dict, List, Tuple, AsyncGenerator

import numpy as np
import pandas as pd
import httpx
import asyncpg
from rapidfuzz import fuzz, process
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Request, Body, Query, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware

# =============================================================================
# Konfiguration
# =============================================================================
app = FastAPI(title="BatchFlow")
app.add_middleware(GZipMiddleware, minimum_size=1024)
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
PD_CLIENT_ID = os.getenv("PD_CLIENT_ID", "")
PD_CLIENT_SECRET = os.getenv("PD_CLIENT_SECRET", "")
OAUTH_AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
OAUTH_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"
PD_API_TOKEN = os.getenv("PD_API_TOKEN", "")
PIPEDRIVE_API = "https://api.pipedrive.com/v1"

DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL fehlt (Neon DSN).")
SCHEMA = os.getenv("PGSCHEMA", "public")

# Filter/Felder
FILTER_NEUKONTAKTE = int(os.getenv("FILTER_NEUKONTAKTE", "2998"))
FILTER_NACHFASS   = int(os.getenv("FILTER_NACHFASS", "3024"))
FIELD_FACHBEREICH_HINT = os.getenv("FIELD_FACHBEREICH_HINT", "fachbereich")

# UI/Defaults
DEFAULT_CHANNEL = "Cold E-Mail"

# Performance-Limits
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "500"))
NF_PAGE_LIMIT = int(os.getenv("NF_PAGE_LIMIT", "100"))
NF_MAX_ROWS = int(os.getenv("NF_MAX_ROWS", "10000"))
RECONCILE_MAX_ROWS = int(os.getenv("RECONCILE_MAX_ROWS", "20000"))
PER_ORG_DEFAULT_LIMIT = int(os.getenv("PER_ORG_DEFAULT_LIMIT", "2"))
PD_CONCURRENCY = int(os.getenv("PD_CONCURRENCY", "4"))

# Orga-Dedupe-Limits
MAX_ORG_NAMES = int(os.getenv("MAX_ORG_NAMES", "120000"))
MAX_ORG_BUCKET = int(os.getenv("MAX_ORG_BUCKET", "15000"))

user_tokens: Dict[str, str] = {}

TEMPLATE_COLUMNS = [
    "Batch ID","Channel","Cold-Mailing Import",
    "Prospect ID","Organisation ID","Organisation Name",
    "Person ID","Person Vorname","Person Nachname",
    "Person Titel","Person Geschlecht","Person Position",
    "Person E-Mail","XING Profil","LinkedIn URL"
]

PERSON_FIELD_HINTS_TO_EXPORT = {
    "prospect":"Prospect ID","gender":"Person Geschlecht","geschlecht":"Person Geschlecht",
    "titel":"Person Titel","title":"Person Titel","anrede":"Person Titel",
    "position":"Person Position","xing":"XING Profil","xing url":"XING Profil",
    "xing profil":"XING Profil","linkedin":"LinkedIn URL",
    "email büro":"Person E-Mail","email buero":"Person E-Mail",
    "office email":"Person E-Mail"
}

# =============================================================================
# Startup / Shutdown
# =============================================================================
def http_client() -> httpx.AsyncClient:
    return app.state.http  # type: ignore

def get_pool() -> asyncpg.Pool:
    return app.state.pool  # type: ignore

@app.on_event("startup")
async def _startup():
    import asyncio
    limits = httpx.Limits(max_keepalive_connections=8, max_connections=16)
    app.state.http = httpx.AsyncClient(timeout=60.0, limits=limits)
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
    print("[Startup] BatchFlow initialisiert.")

@app.on_event("shutdown")
async def _shutdown():
    try:
        await app.state.http.aclose()
    finally:
        await app.state.pool.close()

# =============================================================================
# Helpers
# =============================================================================
def normalize_name(s: str) -> str:
    if not s: return ""
    s = s.lower()
    s = re.sub(r"[^a-z0-9 ]","",s)
    s = re.sub(r"\s+"," ",s).strip()
    return s

def parse_pd_date(d: Optional[str]) -> Optional[datetime]:
    if not d: return None
    try: return datetime.strptime(d,"%Y-%m-%d").replace(tzinfo=timezone.utc)
    except: return None

def is_forbidden_activity_date(val: Optional[str]) -> bool:
    dt = parse_pd_date(val)
    if not dt: return False
    today = datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0)
    three_months = today - timedelta(days=90)
    return dt > today or (three_months <= dt <= today)

def _as_list_email(value) -> List[str]:
    if value is None or (isinstance(value,float) and pd.isna(value)): return []
    if isinstance(value,dict): v=value.get("value"); return [v] if v else []
    if isinstance(value,(list,tuple,np.ndarray)):
        out=[]; 
        for x in value:
            if isinstance(x,dict): x=x.get("value")
            if x: out.append(str(x))
        return out
    return [str(value)]

def slugify_filename(name:str,fallback="BatchFlow_Export")->str:
    s=(name or "").strip()
    if not s: return fallback
    s=re.sub(r"[^\w\-. ]+","",s).strip()
    s=re.sub(r"\s+","_",s)
    return s or fallback

# =============================================================================
# DB-Funktionen
# =============================================================================
async def ensure_table_text(conn: asyncpg.Connection, table: str, cols: List[str]):
    col_defs = ", ".join([f'"{c}" TEXT' for c in cols])
    await conn.execute(f'CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{table}" ({col_defs})')

async def clear_table(conn: asyncpg.Connection, table: str):
    await conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table}"')

async def save_df_text(df: pd.DataFrame, table: str):
    async with get_pool().acquire() as conn:
        await clear_table(conn, table)
        await ensure_table_text(conn, table, list(df.columns))
        if df.empty: return
        cols = list(df.columns)
        cols_sql = ", ".join(f'"{c}"' for c in cols)
        placeholders = ", ".join(f'${i}' for i in range(1,len(cols)+1))
        insert_sql = f'INSERT INTO "{SCHEMA}"."{table}" ({cols_sql}) VALUES ({placeholders})'
        batch=[]
        async with conn.transaction():
            for _,row in df.iterrows():
                vals=["" if pd.isna(v) else str(v) for v in row.tolist()]
                batch.append(vals)
                if len(batch)>=1000:
                    await conn.executemany(insert_sql,batch); batch=[]
            if batch: await conn.executemany(insert_sql,batch)

async def load_df_text(table:str)->pd.DataFrame:
    async with get_pool().acquire() as conn:
        rows=await conn.fetch(f'SELECT * FROM "{SCHEMA}"."{table}"')
    if not rows: return pd.DataFrame()
    cols=list(rows[0].keys())
    data=[tuple(r[c] for c in cols) for r in rows]
    return pd.DataFrame(data,columns=cols).replace({"":np.nan})
# =============================================================================
# Pipedrive API Helper & Fetcher
# =============================================================================
def get_headers() -> Dict[str,str]:
    token = user_tokens.get("default","")
    if token:
        return {"Authorization":f"Bearer {token}"}
    return {}

def append_token(url:str)->str:
    if "api_token=" in url: return url
    if user_tokens.get("default"): return url
    if PD_API_TOKEN:
        sep="&" if "?" in url else "?"
        return f"{url}{sep}api_token={PD_API_TOKEN}"
    return url

async def get_person_fields()->List[dict]:
    url=append_token(f"{PIPEDRIVE_API}/personFields")
    r=await http_client().get(url,headers=get_headers())
    if r.status_code!=200: raise Exception(f"Pipedrive Fehler: {r.text}")
    return r.json().get("data") or []

# -----------------------------------------------------------------------------
# Direkte Batch-ID-Suche (optimiert)
# -----------------------------------------------------------------------------
async def stream_persons_by_batch_id(batch_key:str,batch_ids:List[str],
                                     page_limit:int=NF_PAGE_LIMIT)->AsyncGenerator[List[dict],None]:
    """
    Holt Personen direkt über /persons/search (serverseitig gefiltert).
    """
    for bid in batch_ids:
        start=0
        total=0
        while True:
            url=append_token(f"{PIPEDRIVE_API}/persons/search?term={bid}&fields={batch_key}&start={start}&limit={page_limit}")
            r=await http_client().get(url,headers=get_headers())
            if r.status_code!=200:
                raise Exception(f"Pipedrive API Fehler bei Batch {bid}: {r.text}")
            items=r.json().get("data",{}).get("items",[])
            if not items: break
            persons=[it.get("item") for it in items if it.get("item")]
            yield persons
            total+=len(persons)
            print(f"[Nachfass] Batch {bid} – {total} Treffer geladen…")
            if len(persons)<page_limit: break
            start+=page_limit
        print(f"[Nachfass] Batch {bid} abgeschlossen ({total} Personen).")

# -----------------------------------------------------------------------------
# Klassische Streams (für Abgleich etc.)
# -----------------------------------------------------------------------------
async def stream_organizations_by_filter(filter_id:int,page_limit:int=PAGE_LIMIT):
    start=0
    while True:
        url=append_token(f"{PIPEDRIVE_API}/organizations?filter_id={filter_id}&start={start}&limit={page_limit}")
        r=await http_client().get(url,headers=get_headers())
        if r.status_code!=200: raise Exception(f"Pipedrive Orgs Fehler: {r.text}")
        data=r.json().get("data") or []
        if not data: break
        yield data
        if len(data)<page_limit: break
        start+=page_limit

async def stream_person_ids_by_filter(filter_id:int,page_limit:int=PAGE_LIMIT):
    start=0
    while True:
        url=append_token(f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={page_limit}&sort=id")
        r=await http_client().get(url,headers=get_headers())
        if r.status_code!=200: raise Exception(f"Pipedrive Persons Fehler: {r.text}")
        data=r.json().get("data") or []
        if not data: break
        yield [str(p.get("id")) for p in data if p.get("id")]
        if len(data)<page_limit: break
        start+=page_limit

# =============================================================================
# Nachfass: Build & Progress
# =============================================================================
async def _build_nf_master_final(nf_batch_ids:List[str],batch_id:str,campaign:str,job_obj=None)->pd.DataFrame:
    """
    Baut Nachfass-Daten direkt über /persons/search.
    Fortschritt wird laufend in job_obj.phase / job_obj.percent geschrieben.
    """
    fields=await get_person_fields()
    batch_key=None
    for f in fields:
        nm=(f.get("name") or "").lower()
        if "batch" in nm:
            batch_key=f.get("key"); break
    if not batch_key:
        raise RuntimeError("Personenfeld 'Batch ID' wurde nicht gefunden.")
    print(f"[Nachfass] Verwende Feld-Key: {batch_key}")

    hint_to_key={}
    for f in fields:
        nm=(f.get("name") or "").lower()
        for hint in PERSON_FIELD_HINTS_TO_EXPORT.keys():
            if hint in nm and hint not in hint_to_key:
                hint_to_key[hint]=f.get("key")

    rows=[]; total=0
    batches_done=0
    for bid in nf_batch_ids:
        batches_done+=1
        if job_obj:
            job_obj.phase=f"Lade Batch {bid} …"; job_obj.percent=10*batches_done
        async for chunk in stream_persons_by_batch_id(batch_key,[bid]):
            for p in chunk:
                if total>=NF_MAX_ROWS: break
                pid=p.get("id")
                org=p.get("org_id") or {}
                org_name=org.get("name") or "-"
                org_id=str(org.get("id") or "")
                name=p.get("name") or ""
                first=p.get("first_name") or ""
                last=p.get("last_name") or ""
                emails=_as_list_email(p.get("email"))
                email=emails[0] if emails else ""
                rows.append({
                    "Batch ID":batch_id,"Channel":DEFAULT_CHANNEL,
                    "Cold-Mailing Import":campaign,"Prospect ID":"",
                    "Organisation ID":org_id,"Organisation Name":org_name,
                    "Person ID":str(pid),"Person Vorname":first,
                    "Person Nachname":last,"Person Titel":"",
                    "Person Geschlecht":"","Person Position":"",
                    "Person E-Mail":email,"XING Profil":"",
                    "LinkedIn URL":""
                })
                total+=1
        print(f"[Nachfass] Batch {bid}: {total} Zeilen gesammelt.")
    if job_obj:
        job_obj.phase=f"Daten gesammelt: {total} Zeilen"; job_obj.percent=40
    df=pd.DataFrame(rows,columns=TEMPLATE_COLUMNS)
    await save_df_text(df,"nf_master_final")
    return df
# =============================================================================
# Abgleich & Export (Nachfass)
# =============================================================================
async def _fetch_org_names_for_filter_capped(filter_id:int,page_limit:int,cap_total:int,cap_bucket:int)->Dict[str,List[str]]:
    buckets={}; total=0
    async for chunk in stream_organizations_by_filter(filter_id,page_limit):
        for o in chunk:
            n=normalize_name(o.get("name") or "")
            if not n: continue
            b=n[0]
            lst=buckets.setdefault(b,[])
            if len(lst)>=cap_bucket: continue
            if not lst or lst[-1]!=n:
                lst.append(n); total+=1
                if total>=cap_total: return buckets
    return buckets

async def _reconcile_nf(job_obj=None):
    master=await load_df_text("nf_master_final")
    if master.empty:
        await save_df_text(pd.DataFrame(),"nf_master_ready")
        await save_df_text(pd.DataFrame(columns=["reason","id","name","org_id","org_name","extra"]),"nf_delete_log")
        return

    delete_rows=[]; col_person_id="Person ID"; col_org_name="Organisation Name"; col_org_id="Organisation ID"

    # Orga-Dubletten via ≥95 %
    if job_obj: job_obj.phase="Orga-Abgleich …"; job_obj.percent=60
    filter_ids_org=[1245]  # nur 1 Filter (schneller)
    buckets_all={}; total=0
    for fid in filter_ids_org:
        caps_left=max(0,MAX_ORG_NAMES-total)
        if caps_left<=0: break
        b=await _fetch_org_names_for_filter_capped(fid,PAGE_LIMIT,caps_left,MAX_ORG_BUCKET)
        for k,lst in b.items():
            slot=buckets_all.setdefault(k,[])
            for n in lst:
                if len(slot)>=MAX_ORG_BUCKET: break
                if not slot or slot[-1]!=n:
                    slot.append(n); total+=1
                    if total>=MAX_ORG_NAMES: break
            if total>=MAX_ORG_NAMES: break
        if total>=MAX_ORG_NAMES: break

    drop_idx=[]
    for idx,row in master.iterrows():
        cand=str(row.get(col_org_name) or "").strip()
        norm=normalize_name(cand)
        if not norm: continue
        bucket=buckets_all.get(norm[0]); 
        if not bucket: continue
        near=[n for n in bucket if abs(len(n)-len(norm))<=4]
        if not near: continue
        best=process.extractOne(norm,near,scorer=fuzz.token_sort_ratio)
        if best and best[1]>=95:
            drop_idx.append(idx)
            delete_rows.append({
                "reason":"org_match_95","id":str(row.get(col_person_id) or ""),
                "name":f"{row.get('Person Vorname') or ''} {row.get('Person Nachname') or ''}".strip(),
                "org_id":str(row.get(col_org_id) or ""),"org_name":cand,
                "extra":f"Best Match: {best[0]} ({best[1]}%)"
            })
    if drop_idx: master=master.drop(index=drop_idx)

    # Entferne Person-IDs aus Filtern 1216/1708
    if job_obj: job_obj.phase="Person-ID-Abgleich …"; job_obj.percent=75
    suspect=set()
    async for p in stream_person_ids_by_filter(1216,PAGE_LIMIT): suspect.update(p)
    async for p in stream_person_ids_by_filter(1708,PAGE_LIMIT): suspect.update(p)
    if suspect:
        mask=master[col_person_id].astype(str).isin(suspect)
        removed=master[mask].copy()
        for _,r in removed.iterrows():
            delete_rows.append({
                "reason":"person_id_match","id":str(r.get(col_person_id) or ""),
                "name":f"{r.get('Person Vorname') or ''} {r.get('Person Nachname') or ''}".strip(),
                "org_id":str(r.get(col_org_id) or ""),"org_name":str(r.get(col_org_name) or ""),
                "extra":"ID in 1216/1708"
            })
        master=master[~mask].copy()

    await save_df_text(master,"nf_master_ready")
    log=pd.DataFrame(delete_rows,columns=["reason","id","name","org_id","org_name","extra"])
    await save_df_text(log,"nf_delete_log")
    if job_obj: job_obj.phase="Abgleich abgeschlossen"; job_obj.percent=90

# =============================================================================
# Excel Export
# =============================================================================
def build_export_from_ready(df:pd.DataFrame)->pd.DataFrame:
    out=pd.DataFrame(columns=TEMPLATE_COLUMNS)
    for col in TEMPLATE_COLUMNS:
        out[col]=df[col] if col in df.columns else ""
    for c in ("Organisation ID","Person ID"):
        if c in out.columns: out[c]=out[c].astype(str).fillna("").replace("nan","")
    return out

def _df_to_excel_bytes(df:pd.DataFrame)->bytes:
    import openpyxl
    buf=io.BytesIO()
    with pd.ExcelWriter(buf,engine="openpyxl") as writer:
        df.to_excel(writer,index=False,sheet_name="Export")
        ws=writer.sheets["Export"]
        col_index={c:i+1 for i,c in enumerate(df.columns)}
        for name in ("Organisation ID","Person ID"):
            if name in col_index:
                j=col_index[name]
                for i in range(2,len(df)+2):
                    ws.cell(i,j).number_format="@"
        writer.book.properties.creator="BatchFlow"
    buf.seek(0)
    return buf.getvalue()

# =============================================================================
# Jobverwaltung & Fortschritt
# =============================================================================
class Job:
    def __init__(self):
        self.phase="Warten …"; self.percent=0; self.done=False
        self.error=None; self.path=None; self.total_rows=0
        self.filename_base="BatchFlow_Export"
JOBS:Dict[str,Job]={}

@app.post("/nachfass/export_start")
async def export_start(nf_batch_ids:List[str]=Body(...,embed=True),
                       batch_id:str=Body(...),campaign:str=Body(...)):
    job_id=str(uuid.uuid4()); job=Job(); JOBS[job_id]=job
    job.phase="Initialisiere …"; job.percent=1
    job.filename_base=slugify_filename(campaign or "BatchFlow_Export")

    import asyncio
    async def _run():
        try:
            job.phase="Lade Daten …"; job.percent=10
            await _build_nf_master_final(nf_batch_ids,batch_id,campaign,job_obj=job)
            job.phase="Abgleich …"; job.percent=55
            await _reconcile_nf(job_obj=job)
            job.phase="Excel …"; job.percent=80
            ready=await load_df_text("nf_master_ready")
            exp=build_export_from_ready(ready)
            data=_df_to_excel_bytes(exp)
            path=f"/tmp/{job.filename_base}.xlsx"
            with open(path,"wb") as f: f.write(data)
            job.path=path; job.total_rows=len(exp)
            job.phase=f"Fertig – {len(exp)} Zeilen"; job.percent=100; job.done=True
        except Exception as e:
            job.error=f"Fehler: {e}"; job.done=True; job.percent=100; job.phase="Fehler"
            print("[Nachfass Fehler]",e)
    asyncio.create_task(_run())
    return JSONResponse({"job_id":job_id})

@app.get("/nachfass/export_progress")
async def export_progress(job_id:str):
    job=JOBS.get(job_id)
    if not job: raise HTTPException(404,"Unbekannte Job-ID")
    if job.error:
        return JSONResponse({"error":job.error,"done":True,"phase":job.phase,"percent":job.percent})
    return JSONResponse({"phase":job.phase,"percent":job.percent,"done":job.done,"total_rows":job.total_rows})

@app.get("/nachfass/export_download")
async def export_download(job_id:str):
    job=JOBS.get(job_id)
    if not job: raise HTTPException(404,"Unbekannte Job-ID")
    if not job.done or not job.path:
        raise HTTPException(409,"Export ist noch nicht fertig.")
    return FileResponse(job.path,media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        filename=os.path.basename(job.path))
# =============================================================================
# Summary & UI
# =============================================================================
def _count_reason(df:pd.DataFrame,keys:List[str])->int:
    if df.empty or "reason" not in df.columns: return 0
    r=df["reason"].astype(str).str.lower()
    return int(r.isin([k.lower() for k in keys]).sum())

@app.get("/nachfass/summary",response_class=HTMLResponse)
async def nachfass_summary(job_id:str=Query(...)):
    ready=await load_df_text("nf_master_ready")
    log=await load_df_text("nf_delete_log")
    total_ready=len(ready) if not ready.empty else 0
    cnt_org95=_count_reason(log,["org_match_95"])
    cnt_pid=_count_reason(log,["person_id_match"])
    removed_sum=cnt_org95+cnt_pid
    if not log.empty:
        view=log.tail(50).copy()
        def _pretty(r): 
            return f"{r.get('reason')} – {r.get('extra') or ''}"
        view["Grund"]=view.apply(_pretty,axis=1)
        view=view.rename(columns={
            "id":"Id","name":"Name","org_id":"Organisation ID","org_name":"Organisation Name"
        })[["Id","Name","Organisation ID","Organisation Name","Grund"]]
        table_html=view.to_html(classes="grid",index=False,border=0)
    else:
        table_html="<i>keine</i>"

    html=f"""
<!doctype html><html lang="de"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Nachfass – Ergebnis</title>
<style>
  body{{margin:0;background:#f6f8fb;font:16px/1.6 Inter,sans-serif;color:#0f172a}}
  main{{max-width:1100px;margin:30px auto;padding:0 20px}}
  .card{{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:18px;
         box-shadow:0 2px 8px rgba(2,8,23,.04);margin-bottom:18px}}
  h2{{margin:0 0 10px;font-size:18px}}
  table{{width:100%;border-collapse:collapse;margin-top:8px}}
  th,td{{padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:left;font-size:13px}}
  th{{background:#f8fafc}}
  .btn{{display:inline-block;background:#0ea5e9;color:#fff;padding:10px 14px;
        border-radius:10px;text-decoration:none}}
</style>
</head><body>
<main>
  <section class="card">
    <div><b>Ergebnis:</b> {total_ready} Zeilen in <code>nf_master_ready</code></div>
    <ul>
      <li>Orga-Match ≥95 %: <b>{cnt_org95}</b></li>
      <li>Person-ID in 1216/1708: <b>{cnt_pid}</b></li>
      <li><b>Summe entfernt:</b> {removed_sum}</li>
    </ul>
    <div style="margin-top:12px"><a class="btn" href="/campaign">Zur Übersicht</a></div>
  </section>
  <section class="card">
    <h2>Entfernte Datensätze</h2>
    {table_html}
    <div style="color:#64748b;font-size:13px;margin-top:8px">
      Vollständiges Log in Neon: <code>nf_delete_log</code>
    </div>
  </section>
</main>
</body></html>"""
    return HTMLResponse(html)

# =============================================================================
# UI – Formularseite
# =============================================================================
@app.get("/nachfass",response_class=HTMLResponse)
async def nachfass_page():
    authed=bool(user_tokens.get("default") or PD_API_TOKEN)
    authed_html="<span style='color:#0a66c2'>angemeldet</span>" if authed else "<a href='/login'>Anmelden</a>"
    html=f"""
<!doctype html><html lang="de"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Nachfass – BatchFlow</title>
<style>
  body{{margin:0;background:#f6f8fb;font:16px/1.6 Inter,sans-serif;color:#0f172a}}
  header{{background:#fff;border-bottom:1px solid #e2e8f0}}
  .hwrap{{max-width:1100px;margin:0 auto;padding:14px 20px;display:flex;
          justify-content:space-between;align-items:center}}
  main{{max-width:1100px;margin:30px auto;padding:0 20px}}
  .card{{background:#fff;border:1px solid #e2e8f0;border-radius:14px;
         padding:20px;box-shadow:0 2px 8px rgba(2,8,23,.04)}}
  .grid{{display:grid;grid-template-columns:repeat(12,1fr);gap:16px}}
  .col-6{{grid-column:span 6}}.col-3{{grid-column:span 3}}.col-12{{grid-column:span 12}}
  label{{display:block;font-weight:600;margin:8px 0 6px}}
  textarea,input{{width:100%;padding:10px 12px;border:1px solid #cbd5e1;border-radius:10px}}
  .btn{{background:#0ea5e9;color:#fff;border:none;border-radius:10px;
        padding:12px 16px;cursor:pointer}}.btn:hover{{background:#0284c7}}
  #overlay{{display:none;position:fixed;inset:0;background:rgba(255,255,255,.7);
            backdrop-filter:blur(2px);z-index:9999;align-items:center;
            justify-content:center;flex-direction:column;gap:10px}}
  .barwrap{{width:min(520px,90vw);height:10px;border-radius:999px;background:#e2e8f0;overflow:hidden}}
  .bar{{height:100%;width:0%;background:#0ea5e9;transition:width .2s linear}}
</style></head>
<body>
<header><div class="hwrap">
  <div><a href="/campaign" style="color:#0a66c2;text-decoration:none">← Kampagne</a></div>
  <div><b>Nachfass</b></div><div>{authed_html}</div>
</div></header>
<main>
<section class="card">
  <div class="grid">
    <div class="col-6">
      <label>Batch IDs (1–2 Werte)</label>
      <textarea id="nf_batch_ids" rows="3" placeholder="z. B. B111, B096"></textarea>
      <div class="hint">Komma- oder zeilengetrennt (z. B. B111,B096)</div>
    </div>
    <div class="col-3"><label>Batch ID (Exportspalte)</label>
      <input id="batch_id" placeholder="Bxxx"/></div>
    <div class="col-3"><label>Kampagnenname</label>
      <input id="campaign" placeholder="z. B. Follow-Up KW45"/></div>
    <div class="col-12" style="display:flex;justify-content:flex-end">
      <button class="btn" id="btnExportNf">Abgleich &amp; Download</button>
    </div>
  </div>
</section>
</main>
<div id="overlay"><div id="phase"></div>
  <div class="barwrap"><div class="bar" id="bar"></div></div></div>
<script>
const el=id=>document.getElementById(id);
function showOverlay(msg){{el("phase").textContent=msg||"";el("overlay").style.display="flex";}}
function hideOverlay(){{el("overlay").style.display="none";}}
function setProgress(p){{el("bar").style.width=(Math.max(0,Math.min(100,p))+"%");}}
function _parseIDs(raw){{if(!raw)return[];return raw.split(/[\\n,;]/).map(s=>s.trim()).filter(Boolean).slice(0,2);}}
async function startExportNf(){{const raw=el('nf_batch_ids').value||'';const ids=_parseIDs(raw);
 const bid=el('batch_id').value||'';const camp=el('campaign').value||'';
 if(ids.length===0){{alert('Bitte mind. eine Batch ID eingeben.');return;}}
 showOverlay("Starte …");setProgress(5);
 const r=await fetch('/nachfass/export_start',{method:'POST',headers:{'Content-Type':'application/json'},
  body:JSON.stringify({nf_batch_ids:ids,batch_id:bid,campaign:camp})});
 if(!r.ok){{hideOverlay();alert('Start fehlgeschlagen');return;}}
 const {{job_id}}=await r.json();await poll(job_id);}}
async function poll(id){{let done=false,tries=0;
 while(!done&&tries<3600){{await new Promise(r=>setTimeout(r,500));
 const r=await fetch('/nachfass/export_progress?job_id='+encodeURIComponent(id));
 if(!r.ok) continue;const s=await r.json();
 if(s.error){{el('phase').textContent=s.error;setProgress(100);return;}}
 el('phase').textContent=s.phase||'Arbeite …';setProgress(s.percent??0);
 done=!!s.done;tries++;}}
 if(done){{el('phase').textContent='Fertig – Download startet …';setProgress(100);
 window.location.href='/nachfass/export_download?job_id='+encodeURIComponent(id);
 setTimeout(()=>window.location.href='/nachfass/summary?job_id='+encodeURIComponent(id),800);}}
 else el('phase').textContent='Zeitüberschreitung';}}
el('btnExportNf').addEventListener('click',startExportNf);
</script>
</body></html>"""
    return HTMLResponse(html)

# =============================================================================
# Catch-All
# =============================================================================
@app.get("/overview",include_in_schema=False)
async def overview_redirect(request:Request):
    return RedirectResponse("/campaign",status_code=307)

@app.get("/{full_path:path}",include_in_schema=False)
async def catch_all(full_path:str,request:Request):
    return RedirectResponse("/campaign",status_code=307)

if __name__=="__main__":
    import uvicorn
    uvicorn.run("master_nf_directfilter:app",host="0.0.0.0",port=int(os.getenv("PORT",8000)),reload=False)
