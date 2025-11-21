# GOYN Platform v17 - Ordered Review & Download
import os
import json
import sqlite3
import pandas as pd
import threading
from pathlib import Path
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, StreamingResponse
from starlette.middleware.cors import CORSMiddleware
from jinja2 import Environment, BaseLoader

# --- SETUP ---
DATA_DIR = Path("data")
DB_FILE = DATA_DIR / "goyn_data.db"
CONFIG_FILE = "config.json"

# Load Config
with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)

app = FastAPI(title=CONFIG["app_title"])
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"])
env = Environment(loader=BaseLoader(), autoescape=True)

# --- DATABASE ENGINE (SQLite) ---
def init_db():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reports (
            community TEXT,
            year TEXT,
            period TEXT,
            indicator_id TEXT,
            value TEXT,
            unit TEXT,
            last_updated TEXT,
            PRIMARY KEY (community, year, period, indicator_id)
        )
    ''')
    conn.commit()
    conn.close()

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

# --- GOOGLE SHEETS SYNC (Invisible to User) ---
def sync_to_google_background():
    if not CONFIG.get("google_sheets", {}).get("enabled", False):
        return
    thread = threading.Thread(target=_run_sync)
    thread.start()

def _run_sync():
    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        
        conn = get_db_connection()
        df = pd.read_sql_query("SELECT * FROM reports", conn)
        conn.close()
        
        creds_file = CONFIG["google_sheets"]["credentials_file"]
        sheet_name = CONFIG["google_sheets"]["sheet_name"]
        
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
        client = gspread.authorize(creds)
        
        try: sheet = client.open(sheet_name).sheet1
        except: sheet = client.create(sheet_name).sheet1
        
        sheet.clear()
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"☁️  Background Sync Complete")
    except Exception as e:
        print(f"⚠️ Sync Error: {e}")

# --- DATA OPERATIONS ---
def get_value_sql(community, year, period, indicator_id):
    conn = get_db_connection()
    row = conn.execute(
        "SELECT value FROM reports WHERE community=? AND year=? AND period=? AND indicator_id=?",
        (community, year, period, indicator_id)
    ).fetchone()
    conn.close()
    return row['value'] if row else ""

def upsert_value_sql(community, year, period, indicator_id, value, unit):
    if str(year) != CONFIG["active_year"]: return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = get_db_connection()
    conn.execute('''
        INSERT INTO reports (community, year, period, indicator_id, value, unit, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(community, year, period, indicator_id) 
        DO UPDATE SET value=excluded.value, unit=excluded.unit, last_updated=excluded.last_updated
    ''', (community, year, period, indicator_id, str(value), unit, now))
    conn.commit()
    conn.close()
    sync_to_google_background()

# --- HTML TEMPLATES ---

# 1. ROW (Input Field)
ROW_HTML_STR = """
<form hx-post="/save" hx-swap="outerHTML" hx-trigger="change" class="group">
    <div class="flex flex-col md:flex-row md:items-center gap-4 p-3 rounded-xl border border-transparent hover:border-[#D3E2DF] hover:bg-[#F9FAFB] transition-all duration-500 {% if saved %}saved-flash{% endif %}">
        <div class="md:w-1/2">
            <label class="block font-bold text-[#00497B] text-sm">{{ ind.name }}</label>
        </div>
        <input type="hidden" name="community" value="{{ community }}">
        <input type="hidden" name="year" value="{{ year }}">
        <input type="hidden" name="period" value="{{ period }}">
        <input type="hidden" name="section_id" value="{{ section_id }}">
        <input type="hidden" name="indicator_id" value="{{ ind.id }}">
        <input type="hidden" name="unit" value="{{ ind.unit }}">
        <div class="md:w-1/3 relative">
            {% set prefix = ind.id.rsplit('_', 1)[0] %}
            <input id="{{ ind.id }}"
                {% if editable and not ind.derived and (ind.id.endswith('_male') or ind.id.endswith('_female') or ind.id.endswith('_nb')) %}oninput="autoSum('{{ prefix }}')"{% endif %}
                type="{{ 'text' if ind.unit == 'Text' else 'number' }}" step="any" name="value" value="{{ ind.value }}" 
                class="w-full pl-4 pr-12 py-2 border border-gray-300 rounded-lg font-mono text-sm text-gray-800 outline-none focus:border-[#00A0CC] focus:ring-2 focus:ring-cyan-100 {% if not editable %}locked{% else %}bg-white{% endif %} {% if ind.derived %}bg-yellow-50 text-[#00497B] font-bold cursor-default{% endif %}"
                placeholder="-" {% if not editable or ind.derived %}disabled{% endif %}>
            <span class="absolute right-4 top-2.5 text-gray-400 text-xs font-medium pointer-events-none">{{ ind.unit }}</span>
            {% if saved %}<span class="absolute right-1 top-[-8px] text-[10px] text-[#0AA066] font-bold bg-white px-1 rounded border border-[#0AA066]">Saved</span>{% endif %}
        </div>
    </div>
</form>
"""

# 2. INTERFACE (The Form)
INTERFACE_HTML = f"""
<div id="main-container">
    <div class="flex flex-wrap gap-2 mb-8 border-b border-gray-300 pb-4">
        {{% for s in sections %}}
          <button hx-get="/switch" hx-target="#main-container"
            hx-vals='{{"community":"{{{{community}}}}", "year":"{{{{year}}}}", "period":"{{{{period}}}}", "section_id":"{{{{s.id}}}}"}}'
            class="px-4 py-2 rounded-full text-xs font-bold border transition {{% if s.id == section_id %}} bg-[#00497B] text-white border-[#00497B] transform scale-105 shadow-md {{% else %}} bg-white text-slate-600 border-slate-200 hover:border-[#00A0CC] hover:text-[#00A0CC] {{% endif %}}">
            {{{{ s.name }}}}
          </button>
        {{% endfor %}}
    </div>
    <div class="bg-white rounded-2xl shadow-sm border border-gray-200 overflow-hidden fade-in">
        <div class="p-6 bg-[#EBF1F2] border-b border-gray-200">
            <h2 class="text-2xl font-bold text-[#00497B]">{{{{ section.name }}}}</h2>
            <p class="text-gray-500 text-sm mt-1">{{{{ section.description }}}}</p>
        </div>
        <div class="p-6 space-y-4">
            {{% for ind in rendered %}}{ROW_HTML_STR}{{% endfor %}}
        </div>
    </div>
    <script>
        function autoSum(prefix) {{
            let m = parseFloat(document.getElementById(prefix + '_male').value) || 0;
            let f = parseFloat(document.getElementById(prefix + '_female').value) || 0;
            let nb = parseFloat(document.getElementById(prefix + '_nb').value) || 0;
            let tot = document.getElementById(prefix + '_total');
            if(tot) {{ tot.value = m + f + nb; }}
        }}
    </script>
</div>
"""

# 3. REVIEW PAGE (The Summary)
REVIEW_HTML = """
<!DOCTYPE html>
<html lang="en">
<head><title>Report Summary</title><script src="https://cdn.tailwindcss.com"></script></head>
<body class="bg-slate-100 p-8 font-sans">
    <div class="max-w-4xl mx-auto bg-white shadow-xl rounded-2xl overflow-hidden">
        <div class="bg-[#00497B] text-white p-8 flex justify-between items-center">
            <div><h1 class="text-3xl font-bold">{{ community }}</h1><p class="opacity-80">Report: {{ year }} - {{ period }}</p></div>
            <div class="text-right text-sm"><p class="opacity-70">Generated</p><p class="font-bold">{{ today }}</p></div>
        </div>
        
        <div class="p-8">
            <div class="bg-green-50 border border-green-200 text-green-800 p-4 rounded-lg mb-6 flex items-center gap-3">
                <span class="text-2xl">✅</span>
                <div>
                    <p class="font-bold text-lg">Report submitted successfully</p>
                    <p class="text-sm opacity-80">Your data has been recorded. You can download a copy below.</p>
                </div>
            </div>

            {% if rows %}
            <div class="overflow-x-auto border rounded-lg border-slate-200">
                <table class="w-full text-sm">
                    <thead class="text-xs text-slate-500 uppercase bg-slate-50">
                        <tr>
                            <th class="p-4 text-left w-2/3">Indicator</th>
                            <th class="p-4 text-right w-1/3">Value</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-100">
                        {% for r in rows %}
                        <tr class="hover:bg-slate-50 transition">
                            <td class="p-4 text-slate-700 font-medium">{{ r.name }}</td>
                            <td class="p-4 text-right font-mono font-bold text-[#00497B]">{{ r.value }} <span class="text-xs text-slate-400 font-normal ml-1">{{ r.unit }}</span></td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
            
            <div class="mt-8 flex justify-end pt-6 border-t border-slate-100">
                <a href="/download_report?community={{community}}&year={{year}}&period={{period}}" class="px-6 py-3 bg-[#00A0CC] text-white font-bold rounded-lg hover:bg-[#008bb0] shadow flex gap-2 items-center transition">
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-5 h-5">
                      <path stroke-linecap="round" stroke-linejoin="round" d="M3 16.5v2.25A2.25 2.25 0 0 0 5.25 21h13.5A2.25 2.25 0 0 0 21 18.75V16.5M12 9.75V1.5m0 0L7.5 6M12 1.5l4.5 4.5" />
                    </svg>
                    <span>Download Copy (CSV)</span>
                </a>
            </div>

            {% else %}
            <div class="text-center py-16 text-slate-400 border-2 border-dashed border-slate-200 rounded-xl">No data entered for this period.</div>
            {% endif %}
        </div>
    </div>
</body>
</html>
"""

BASE_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"><title>{{ title }}</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://unpkg.com/htmx.org@1.9.10"></script>
  <style>
    :root { --navy: #00497B; --cyan: #00A0CC; --green: #0AA066; --bg: #EBF1F2; }
    body { background-color: var(--bg); color: #334155; font-family: sans-serif; }
    .nav-header { background-color: var(--navy); }
    .btn-primary { background-color: var(--cyan); color: white; }
    .saved-flash { animation: flashGreen 1.5s; }
    @keyframes flashGreen { 0% { background-color: #F0FDF4; border-color: var(--green); } 100% { background-color: white; border-color: transparent; } }
    .locked { background-color: #F1F5F9; color: #94A3B8; cursor: not-allowed; }
    .active-badge { background-color: #DCFCE7; color: #166534; border: 1px solid #BBF7D0; }
    .fade-in { animation: fadeIn 0.3s ease-out; }
    @keyframes fadeIn { from { opacity: 0.5; transform: translateY(5px); } to { opacity: 1; transform: translateY(0); } }
  </style>
</head>
<body class="flex flex-col min-h-screen">
  <nav class="nav-header text-white p-4 shadow-lg sticky top-0 z-50">
    <div class="max-w-7xl mx-auto flex flex-col md:flex-row justify-between items-center gap-4">
      <div class="flex items-center gap-3">
        <div class="w-10 h-10 bg-white text-[#00497B] rounded-full flex items-center justify-center font-bold text-xl">G</div>
        <div><h1 class="text-lg font-bold">{{ title }}</h1></div>
      </div>
      <form action="/" method="get" class="flex flex-wrap gap-2 text-sm">
        <select name="community" class="text-black rounded px-2 py-1 font-bold">{% for c in communities %}<option {% if c==community %}selected{% endif %}>{{c}}</option>{% endfor %}</select>
        <select name="year" class="text-black rounded px-2 py-1">{% for y in years %}<option {% if y==year %}selected{% endif %}>{{y}}</option>{% endfor %}</select>
        <select name="period" class="text-black rounded px-2 py-1">{% for p in periods %}<option {% if p==period %}selected{% endif %}>{{p}}</option>{% endfor %}</select>
        <button class="btn-primary px-4 py-1 rounded font-bold shadow">LOAD</button>
      </form>
    </div>
  </nav>
  <div class="max-w-6xl mx-auto w-full p-6 flex-grow">
    {% if community == "Select Community" %}
      <div class="text-center py-20 bg-white rounded-3xl shadow mt-10">
        <h2 class="text-2xl font-bold text-[#00497B]">Welcome</h2>
        <p class="text-slate-500 mt-2">Select a community to begin reporting.</p>
      </div>
    {% else %}
      <div class="flex justify-between items-center mb-8 bg-white p-4 rounded-xl shadow-sm border-l-4 {% if editable %}border-[#0AA066]{% else %}border-slate-400{% endif %}">
        <div><h2 class="text-xl font-bold text-[#00497B]">{{ community }}</h2><p class="text-sm text-slate-500">{{ year }} • {{ period }}</p></div>
        <div>
            {% if not editable %}<span class="px-3 py-1 rounded-full text-xs font-bold bg-slate-200 text-slate-500">🔒 READ-ONLY</span>
            {% else %}
            <span class="px-3 py-1 rounded-full text-xs font-bold active-badge mr-2">● ACTIVE</span>
            <a href="/review?community={{community}}&year={{year}}&period={{period}}" target="_blank" class="bg-[#00497B] text-white px-4 py-2 rounded text-xs font-bold hover:bg-[#003860] shadow">REVIEW & SUBMIT</a>
            {% endif %}
        </div>
      </div>
      {{ interface_html | safe }}
    {% endif %}
  </div>
</body>
</html>
"""

# --- ROUTES ---
init_db()

@app.get("/", response_class=HTMLResponse)
async def index(req: Request, community: str="Select Community", year: str=CONFIG["active_year"], period: str="Annual Total", section_id: str=CONFIG["sections"][0]["id"]):
    if community == "Select Community":
        tmpl = env.from_string(BASE_HTML)
        return tmpl.render(title=CONFIG["app_title"], communities=CONFIG["communities"], years=CONFIG["years"], periods=CONFIG["periods"], community=community, year=year, period=period)
        
    section = next((s for s in CONFIG["sections"] if s["id"] == section_id), CONFIG["sections"][0])
    rendered = []
    for ind in section["indicators"]:
        item = dict(ind)
        item["value"] = get_value_sql(community, year, period, ind["id"])
        rendered.append(item)

    tmpl = env.from_string(INTERFACE_HTML)
    interface = tmpl.render(section=section, rendered=rendered, community=community, year=year, period=period, editable=(str(year)==CONFIG["active_year"]), section_id=section_id, sections=CONFIG["sections"])
    
    tmpl = env.from_string(BASE_HTML)
    return tmpl.render(title=CONFIG["app_title"], communities=CONFIG["communities"], years=CONFIG["years"], periods=CONFIG["periods"], community=community, year=year, period=period, interface_html=interface, editable=(str(year)==CONFIG["active_year"]))

@app.get("/switch", response_class=HTMLResponse)
async def switch(community: str, year: str, period: str, section_id: str):
    section = next((s for s in CONFIG["sections"] if s["id"] == section_id), CONFIG["sections"][0])
    rendered = []
    for ind in section["indicators"]:
        item = dict(ind)
        item["value"] = get_value_sql(community, year, period, ind["id"])
        rendered.append(item)
    
    tmpl = env.from_string(INTERFACE_HTML)
    return tmpl.render(section=section, rendered=rendered, community=community, year=year, period=period, editable=(str(year)==CONFIG["active_year"]), section_id=section_id, sections=CONFIG["sections"])

@app.post("/save", response_class=HTMLResponse)
async def save(community: str=Form(...), year: str=Form(...), period: str=Form(...), section_id: str=Form(...), indicator_id: str=Form(...), value: str=Form(""), unit: str=Form(...)):
    upsert_value_sql(community, year, period, indicator_id, value, unit)
    
    section = next((s for s in CONFIG["sections"] if s["id"] == section_id), CONFIG["sections"][0])
    ind_def = next((i for i in section["indicators"] if i["id"] == indicator_id), None)
    item = dict(ind_def)
    item["value"] = value
    
    tmpl = env.from_string(ROW_HTML_STR)
    return tmpl.render(ind=item, community=community, year=year, period=period, section_id=section_id, editable=True, saved=True)

@app.get("/review", response_class=HTMLResponse)
async def review(community: str, year: str, period: str):
    # 1. Fetch Raw Data from DB
    conn = get_db_connection()
    rows_sql = conn.execute("SELECT indicator_id, value, unit FROM reports WHERE community=? AND year=? AND period=?", (community, year, period)).fetchall()
    conn.close()
    
    # 2. Convert DB List to Dictionary for Fast Lookup
    # Key: indicator_id, Value: The row data
    data_map = {row['indicator_id']: row for row in rows_sql}
    
    # 3. Iterate through CONFIG to build the display list in CORRECT ORDER
    display_rows = []
    
    for section in CONFIG["sections"]:
        for ind in section["indicators"]:
            ind_id = ind["id"]
            # If this indicator exists in the user's data and has a value
            if ind_id in data_map and data_map[ind_id]['value']:
                display_rows.append({
                    "name": ind["name"], # Use name from Config (Clean)
                    "value": data_map[ind_id]['value'],
                    "unit": ind["unit"]
                })

    tmpl = env.from_string(REVIEW_HTML)
    return tmpl.render(community=community, year=year, period=period, rows=display_rows, today=datetime.now().strftime("%Y-%m-%d"))

@app.get("/download_report")
async def download_report(community: str, year: str, period: str):
    conn = get_db_connection()
    # We fetch only the data for this specific community/period
    df = pd.read_sql_query(
        "SELECT community, year, period, indicator_id, value, unit FROM reports WHERE community=? AND year=? AND period=?", 
        conn, params=(community, year, period)
    )
    conn.close()
    
    # Add Indicator Names for clarity in the CSV
    # We create a map of ID -> Name from the config
    id_to_name = {}
    for s in CONFIG["sections"]:
        for i in s["indicators"]:
            id_to_name[i["id"]] = i["name"]
    
    # Map the names
    df["indicator_name"] = df["indicator_id"].map(id_to_name)
    
    # Reorder columns to make it user friendly
    df = df[["community", "year", "period", "indicator_id", "indicator_name", "value", "unit"]]
    
    stream = df.to_csv(index=False).encode("utf-8")
    filename = f"GOYN_Report_{community}_{year}_{period}.csv"
    return StreamingResponse(iter([stream]), media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="{filename}"'})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)