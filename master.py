import os
import re
import time
from typing import Optional, Tuple

import httpx
import asyncpg
import pandas as pd
import numpy as np
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.middleware.gzip import GZipMiddleware

# =========================
#   Konfiguration
# =========================
PD_API_TOKEN = os.getenv("PD_API_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")  # postgresql://.../neondb?sslmode=require
if not PD_API_TOKEN:
    raise ValueError("PD_API_TOKEN fehlt")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL fehlt")

PIPEDRIVE_API = "https://api.pipedrive.com/v1"
FILTER_NEUKONTAKTE = 2998  # Personen-Filter für Neukontakte

# Feldnamen (sichtbare Namen in Pipedrive)
FIELD_NAME_FACHBEREICH = "Fachbereich_Kampagne"
FIELD_NAME_BATCH       = "Batch ID"         # nur im Ergebnis-DF, NICHT in Pipedrive
FIELD_NAME_CHANNEL     = "Channel"          # nur im Ergebnis-DF, NICHT in Pipedrive
FIELD_NAME_ORGART      = "Organisationsart" # für Cleanup (Spaltenname im DF)
FIELD_NAME_IMPORT      = "Cold-Mailing Import"  # zusätzliche Spalte im Ergebnis
CHANNEL_VALUE          = "Cold-Mail"
IMPORT_VALUE           = "Ja"  # fester Marker für die 3. Spalte

# Caching / Performance
HTTP_TIMEOUT      = 60.0
PERSON_FIELDS_TTL = 900     # 15 min
FILTER_TTL        = 120     # 2 min

# =========================
#   App & Middleware
# =========================
app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=500)

# globaler HTTP-Client (Keep-Alive)
http_client = httpx.AsyncClient(timeout=HTTP_TIMEOUT)

# In-Memory Caches
_person_fields_cache = {"data": None, "ts": 0.0}
_filter_cache = {}  # {filter_id: {"data":[...], "ts": float}}

# =========================
#   Helpers
# =========================
def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

def to_int(v, default=0) -> int:
    try:
        if v is None:
            return default
        s = str(v).strip()
        return int(s) if s != "" else default
    except:
        return default

async def get_conn():
    return await asyncpg.connect(DATABASE_URL)

async def save_df_text(df: pd.DataFrame, table: str):
    """Speichert DataFrame mit TEXT-Spalten in Neon (einfach & robust)."""
    conn = await get_conn()
    try:
        await conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        if df.empty:
            await conn.execute(f'CREATE TABLE "{table}" ("_empty" TEXT)')
            return
        cols = ", ".join([f'"{c}" TEXT' for c in df.columns])
        await conn.execute(f'CREATE TABLE "{table}" ({cols})')

        col_list = list(df.columns)
        placeholders = ", ".join(f"${i+1}" for i in range(len(col_list)))
        stmt = f'INSERT INTO "{table}" ({", ".join([f"""\"{c}\"""" for c in col_list])}) VALUES ({placeholders})'
        records = [
            tuple("" if pd.isna(row[c]) else str(row[c]) for c in col_list)
            for _, row in df.iterrows()
        ]
        if records:
            await conn.executemany(stmt, records)
    finally:
        await conn.close()

def extract_email(value):
    """Robuster Email-Extractor für Pipedrive-Feld 'email' (Liste/Dict/NaN)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, dict):
        return value.get("value") or None
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        first = value[0]
        if isinstance(first, dict):
            return first.get("value") or None
        return str(first) if first is not None else None
    return str(value) if value is not None else None

def apply_basic_cleanup(df: pd.DataFrame) -> pd.DataFrame:
    """Bereinigung laut Vorgabe:
       - Wenn in Spalte 'Organisationsart' ein Wert steht → Zeile entfernen
       - Pro Organisation maximal 2 Kontakte
    """
    # 1) Organisationsart
    if FIELD_NAME_ORGART in df.columns:
        df = df[df[FIELD_NAME_ORGART].isna() | (df[FIELD_NAME_ORGART] == "")]

    # 2) max. 2 Kontakte je Organisation
    org_col = "org_id" if "org_id" in df.columns else ("Organisation" if "Organisation" in df.columns else None)
    if org_col and org_col in df.columns:
        df["_rank"] = df.groupby(org_col).cumcount() + 1
        df = df[df["_rank"] <= 2].drop(columns=["_rank"], errors="ignore")

    return df

# =========================
#   Pipedrive Zugriff
# =========================
async def get_person_fields():
    """Feld-Definitionen der Personen (mit Cache)."""
    now = time.time()
    if _person_fields_cache["data"] and now - _person_fields_cache["ts"] < PERSON_FIELDS_TTL:
        return _person_fields_cache["data"]

    url = f"{PIPEDRIVE_API}/personFields?api_token={PD_API_TOKEN}"
    r = await http_client.get(url)
    r.raise_for_status()
    data = r.json().get("data") or []

    mapping = {}
    for f in data:
        name, key = f.get("name"), f.get("key")
        options = f.get("options") or []
        mapping[name] = {"key": key, "options": options, "name": name}
        mapping[_norm(name)] = {"key": key, "options": options, "name": name}

    _person_fields_cache.update(data=mapping, ts=now)
    return mapping

def _resolve_option_label(value: str, options: list) -> Tuple[str, str]:
    """Hilfsfunktion: gibt (value, label) zurück (value bleibt unverändert),
       label wird aus den Options ermittelt oder als value zurückgegeben."""
    if not options:
        return value, value
    for o in options:
        oid = str(o.get("id"))
        lab = str(o.get("label", ""))
        if value == oid or value == lab:
            return value, lab or value
    return value, value

async def fetch_persons_by_filter(filter_id: int):
    """Alle Personen eines Filters (mit Cache & Pagination)."""
    now = time.time()
    c = _filter_cache.get(filter_id)
    if c and now - c["ts"] < FILTER_TTL:
        return c["data"]

    persons, start, limit = [], 0, 500
    while True:
        url = f"{PIPEDRIVE_API}/persons?filter_id={filter_id}&start={start}&limit={limit}&api_token={PD_API_TOKEN}"
        r = await http_client.get(url)
        if r.status_code != 200:
            raise Exception(f"Pipedrive API Fehler: {r.text}")
        chunk = r.json().get("data") or []
        if not chunk:
            break
        persons.extend(chunk)
        if len(chunk) < limit:
            break
        start += limit

    _filter_cache[filter_id] = {"data": persons, "ts": now}
    return persons

# =========================
#   Navigation / Landing
# =========================
@app.get("/", response_class=HTMLResponse)
async def root_redirect():
    return HTMLResponse('<meta http-equiv="refresh" content="0;url=/neukontakte">')

@app.get("/overview", response_class=HTMLResponse)
async def old_redirect():
    return HTMLResponse('<meta http-equiv="refresh" content="0;url=/neukontakte">')

# =========================
#   UI: Formular „Neukontakte“
# =========================
@app.get("/neukontakte", response_class=HTMLResponse)
async def neukontakte_form(request: Request):
    fields = await get_person_fields()

    fach = fields.get(_norm(FIELD_NAME_FACHBEREICH)) or fields.get(FIELD_NAME_FACHBEREICH)
    if not fach:
        return HTMLResponse("<div style='padding:24px;color:#b00'>❌ Feld „Fachbereich_Kampagne“ nicht gefunden.</div>", status_code=500)

    # Optionen + Zähler (Zahl in Klammern)
    persons = await fetch_persons_by_filter(FILTER_NEUKONTAKTE)
    total_count = len(persons)

    key_fach = fach["key"]
    counts = {}
    for p in persons:
        v = p.get(key_fach)
        s = str(v).strip() if v is not None else ""
        if s:
            counts[s] = counts.get(s, 0) + 1

    options = fach.get("options") or []
    display = []
    if options:
        for o in options:
            lab, oid = o.get("label", ""), str(o.get("id"))
            c = counts.get(oid, counts.get(lab, 0))
            display.append((oid, f"{lab} ({c or 0})"))
    else:
        for val, c in sorted(counts.items(), key=lambda x: x[0].lower()):
            display.append((val, f"{val} ({c})"))

    # 1. Eintrag: bitte auswählen
    options_html = "<option value='' selected>-- bitte auswählen --</option>\n"
    options_html += "\n".join([f"<option value='{oid}'>{label}</option>" for oid, label in display])

    html = f"""
<!doctype html>
<html lang="de"><head>
<meta charset="utf-8"/>
<title>Neukontakte (Filter {FILTER_NEUKONTAKTE})</title>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<style>
  body{{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Inter,Arial,sans-serif;background:#f5f7fa;margin:0;color:#1f2937}}
  .wrap{{max-width:1180px;margin:28px auto;padding:0 16px}}
  .bar{{display:flex;align-items:center;gap:12px}}
  .pill{{background:#e6f2ff;color:#0a66c2;padding:8px 12px;border-radius:999px;font-weight:600}}
  .card{{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:22px;margin-top:16px;box-shadow:0 1px 3px rgba(0,0,0,.05)}}
  label{{display:block;font-weight:600;margin:8px 0 6px}}
  select,input{{width:100%;padding:10px 12px;border:1px solid #d1d5db;border-radius:8px;outline:none}}
  select:focus,input:focus{{border-color:#0ea5e9;box-shadow:0 0 0 3px rgba(14,165,233,.25)}}
  .row{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px}}
  .hint{{font-size:12px;color:#6b7280;margin-top:6px}}
  .btn{{background:#0ea5e9;border:none;color:#fff;border-radius:8px;padding:10px 16px;cursor:pointer}}
  .btn:hover{{background:#0284c7}}
  .right{{text-align:right;margin-top:16px}}
  .muted{{color:#6b7280}}
</style>
</head><body>
<div class="wrap">
  <div class="bar">
    <a class="pill" href="/neukontakte">🌟 Neukontakte (Filter {FILTER_NEUKONTAKTE})</a>
    <div class="muted">Gesamt im Filter: <b>{total_count}</b></div>
  </div>

  <form class="card" method="post" action="/neukontakte/preview" id="nkform">
    <input type="hidden" name="fachbereich_label" id="fachbereich_label" />
    <label for="fach">Fachbereich</label>
    <select id="fach" name="fachbereich_value" required>
      {options_html}
    </select>
    <div class="hint">Die Zahl in Klammern zeigt die vorhandenen Datensätze im Filter.</div>

    <div class="row">
      <div>
        <label for="take">Wie viele Datensätze nehmen?</label>
        <input id="take" name="take_count" inputmode="numeric" placeholder="z. B. 900"/>
        <div class="hint">Leer lassen = alle Datensätze des gewählten Fachbereichs.</div>
      </div>
      <div>
        <label for="batch">Batch ID</label>
        <input id="batch" name="batch_id" placeholder="Bxxx" required/>
        <div class="hint">Beispiel: B477</div>
      </div>
      <div class="right" style="align-self:end">
        <button class="btn" type="submit">Vorschau laden</button>
      </div>
    </div>
  </form>
</div>
<script>
// beim Absenden den sichtbaren Namen (ohne Klammerzahl) in ein Hidden-Feld schreiben
document.getElementById("nkform").addEventListener("submit", function(){
  const sel = document.getElementById("fach");
  const txt = sel.options[sel.selectedIndex]?.text || "";
  // "Marketing (1234)" -> "Marketing"
  const lbl = txt.replace(/\\s*\\(.*\\)\\s*$/,"").trim();
  document.getElementById("fachbereich_label").value = lbl;
});
</script>
</body></html>
"""
    return HTMLResponse(html)

# =========================
#   Vorschau (keine Writes)
# =========================
@app.post("/neukontakte/preview", response_class=HTMLResponse)
async def neukontakte_preview(
    request: Request,
    fachbereich_value: str = Form(...),
    batch_id: str = Form(...),
    take_count: Optional[str] = Form(None),
    fachbereich_label: Optional[str] = Form(None)
):
    try:
        take_n = to_int(take_count, 0)

        fields = await get_person_fields()
        fach = fields.get(_norm(FIELD_NAME_FACHBEREICH)) or fields.get(FIELD_NAME_FACHBEREICH)
        if not fach:
            return HTMLResponse("<div style='padding:24px;color:#b00'>❌ Feld „Fachbereich_Kampagne“ nicht gefunden.</div>", status_code=500)

        key_fach = fach["key"]
        # Label ermitteln (Fallback falls Hidden leer ist)
        _, label_from_options = _resolve_option_label(str(fachbereich_value), fach.get("options") or [])
        fach_label = fachbereich_label or label_from_options or str(fachbereich_value)

        persons = await fetch_persons_by_filter(FILTER_NEUKONTAKTE)
        sel = [p for p in persons if key_fach in p and (str(p[key_fach]) == str(fachbereich_value))]
        if take_n > 0:
            sel = sel[:take_n]

        if not sel:
            return HTMLResponse("<div style='padding:24px'>Keine Datensätze für diese Auswahl.</div>")

        df = pd.DataFrame(sel)

        # Lesbare Spalten
        if "email" in df.columns:
            df["E-Mail"] = df["email"].apply(extract_email)
        if "org_name" in df.columns:
            df.rename(columns={"org_name": "Organisation"}, inplace=True)
        if "org_id" in df.columns and FIELD_NAME_ORGART not in df.columns:
            df[FIELD_NAME_ORGART] = ""

        # Für Vorschau hinzufügen (aber nichts zu Pipedrive schreiben)
        df["Batch ID (Vorschau)"] = batch_id
        df["Channel (Vorschau)"]  = CHANNEL_VALUE
        df[f"{FIELD_NAME_IMPORT}"] = IMPORT_VALUE

        # Spaltenreihenfolge: 1) Batch, 2) Channel, 3) Cold-Mailing Import, danach Rest
        first_cols = ["Batch ID (Vorschau)", "Channel (Vorschau)", FIELD_NAME_IMPORT]
        rest_cols  = [c for c in df.columns if c not in first_cols]
        preview_df = df[first_cols + rest_cols]

        # Tabellendarstellung (erste 50)
        table_html = preview_df.head(50).to_html(classes="grid", index=False, border=0)

        html = f"""
<!doctype html><html lang="de"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Vorschau – Neukontakte</title>
<style>
  body{{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Inter,Arial,sans-serif;background:#f5f7fa;margin:0;color:#1f2937}}
  .wrap{{max-width:1180px;margin:28px auto;padding:0 16px}}
  .card{{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:22px;margin-top:16px;box-shadow:0 1px 3px rgba(0,0,0,.05)}}
  .grid{{width:100%;border-collapse:collapse}}
  .grid th,.grid td{{border:1px solid #e5e7eb;padding:8px 10px;text-align:left}}
  .grid th{{background:#f3f4f6}}
  .btn{{background:#0ea5e9;border:none;color:#fff;border-radius:8px;padding:10px 16px;cursor:pointer}}
  .btn:hover{{background:#0284c7}}
  .row{{display:flex;gap:10px;justify-content:space-between;align-items:center}}
  .muted{{color:#6b7280}}
</style></head><body>
<div class="wrap">
  <div class="card row">
    <div>
      <b>Vorschau</b><br/>
      Fachbereich: <b>{fach_label}</b> &nbsp;|&nbsp;
      Batch: <b>{batch_id}</b> &nbsp;|&nbsp;
      Datensätze: <b>{len(df)}</b>
    </div>
    <form method="post" action="/neukontakte/run">
      <input type="hidden" name="fachbereich_value" value="{fachbereich_value}"/>
      <input type="hidden" name="fachbereich_label" value="{fach_label}"/>
      <input type="hidden" name="batch_id" value="{batch_id}"/>
      <input type="hidden" name="take_count" value="{take_n}"/>
      <button class="btn" type="submit">Abgleich starten</button>
      <a class="btn" href="/neukontakte" style="background:#6b7280;margin-left:8px">Zurück</a>
    </form>
  </div>
  <div class="card">
    <div class="muted" style="margin-bottom:8px">Die ersten 50 Datensätze:</div>
    {table_html}
  </div>
</div>
</body></html>
"""
        return HTMLResponse(html)
    except Exception as e:
        return HTMLResponse(f"<pre style='padding:24px;color:#b00'>❌ Fehler: {e}</pre>", status_code=500)

# =========================
#   „Abgleich“ = nur speichern (keine Writes nach Pipedrive)
# =========================
@app.post("/neukontakte/run", response_class=HTMLResponse)
async def neukontakte_run(
    request: Request,
    fachbereich_value: str = Form(...),
    batch_id: str = Form(...),
    take_count: Optional[str] = Form(None),
    fachbereich_label: Optional[str] = Form(None)
):
    try:
        take_n = to_int(take_count, 0)

        fields = await get_person_fields()
        fach = fields.get(_norm(FIELD_NAME_FACHBEREICH)) or fields.get(FIELD_NAME_FACHBEREICH)
        if not fach:
            return HTMLResponse("<div style='padding:24px;color:#b00'>❌ Feld „Fachbereich_Kampagne“ nicht gefunden.</div>", status_code=500)

        key_fach = fach["key"]

        persons = await fetch_persons_by_filter(FILTER_NEUKONTAKTE)
        sel = [p for p in persons if key_fach in p and (str(p[key_fach]) == str(fachbereich_value))]
        if take_n > 0:
            sel = sel[:take_n]
        if not sel:
            return HTMLResponse("<div style='padding:24px'>Keine Datensätze für diese Auswahl.</div>")

        # DataFrame bauen
        df = pd.DataFrame(sel)
        if "email" in df.columns:
            df["E-Mail"] = df["email"].apply(extract_email)
        if "org_name" in df.columns:
            df.rename(columns={"org_name": "Organisation"}, inplace=True)
        if FIELD_NAME_ORGART not in df.columns:
            df[FIELD_NAME_ORGART] = ""

        # Cleanup-Regeln
        df = apply_basic_cleanup(df)

        # Ergebnis-Felder für Export
        df[FIELD_NAME_BATCH]   = batch_id
        df[FIELD_NAME_CHANNEL] = CHANNEL_VALUE
        df[FIELD_NAME_IMPORT]  = IMPORT_VALUE

        # Reihenfolge auch im gespeicherten Resultat: Batch, Channel, Import, dann Rest
        first_cols = [FIELD_NAME_BATCH, FIELD_NAME_CHANNEL, FIELD_NAME_IMPORT]
        rest_cols  = [c for c in df.columns if c not in first_cols]
        df = df[first_cols + rest_cols]

        # In Neon speichern (Endergebnis)
        await save_df_text(df, "nk_master_final")

        fach_label = fachbereich_label or _resolve_option_label(str(fachbereich_value), fach.get("options") or [])[1]

        html = f"""
<!doctype html><html lang="de"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Abgleich abgeschlossen</title>
<style>
  body{{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Inter,Arial,sans-serif;background:#f5f7fa;margin:0;color:#1f2937}}
  .wrap{{max-width:1180px;margin:28px auto;padding:0 16px}}
  .card{{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:22px;margin-top:16px;box-shadow:0 1px 3px rgba(0,0,0,.05)}}
  .btn{{background:#0ea5e9;border:none;color:#fff;border-radius:8px;padding:10px 16px;cursor:pointer}}
  .btn:hover{{background:#0284c7}}
</style></head><body>
<div class="wrap">
  <div class="card">
    <h3>✅ Abgleich abgeschlossen</h3>
    <p>Fachbereich: <b>{fach_label}</b><br>
       Batch: <b>{batch_id}</b><br>
       Gespeicherte Zeilen (nach Cleanup): <b>{len(df)}</b>
    </p>
    <a class="btn" href="/neukontakte">Zurück</a>
  </div>
</div>
</body></html>
"""
        return HTMLResponse(html)
    except Exception as e:
        return HTMLResponse(f"<pre style='padding:24px;color:#b00'>❌ Fehler beim Speichern: {e}</pre>", status_code=500)
