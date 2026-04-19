"""
5G Security Job Board - FastAPI Backend (api.py)
Wraps persistence_pg, object_store, Gemini AI and exposes a REST API for Swift/iOS.
"""

from fastapi import FastAPI, HTTPException, Depends, UploadFile, File, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
from typing import Optional, List
import os, json, datetime, urllib.parse, requests, smtplib, threading, uuid
from io import BytesIO
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

from persistence_pg import load_state, save_state_to_db, init_db
from object_store import upload_bytes, get_view_url

try:
    from google import genai
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

try:
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    from reportlab.lib import colors
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False

# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(title="5G Security Job Board API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
def startup_event():
    try: init_db()
    except Exception as e: print(f"DB init warning: {e}")

# ── State Cache ────────────────────────────────────────────────────────────────

_state_cache: dict = {}
_state_version: int = 0
_state_lock = threading.Lock()

def get_state() -> dict:
    global _state_cache, _state_version
    with _state_lock:
        if not _state_cache:
            data, version = load_state()
            _state_cache = data
            _state_version = version
        return _state_cache

def save_state(invalidate_briefing: bool = True):
    global _state_cache, _state_version
    with _state_lock:
        if invalidate_briefing:
            _state_cache["briefing"] = "Data required to generate briefing."
        _state_version = save_state_to_db(_state_cache)

def reload_state():
    global _state_cache
    with _state_lock:
        _state_cache = {}

# ── Auth ───────────────────────────────────────────────────────────────────────

def verify_google_token(authorization: str = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header.")
    token = authorization.split(" ", 1)[1]
    try:
        r = requests.get("https://www.googleapis.com/oauth2/v1/userinfo",
                         headers={"Authorization": f"Bearer {token}"}, timeout=10)
        if r.status_code != 200:
            raise HTTPException(status_code=401, detail="Invalid Google token.")
        return r.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Token validation failed: {e}")

def require_admin(user: dict = Depends(verify_google_token)) -> dict:
    state = get_state()
    if user.get("email", "").lower() not in [e.lower() for e in state.get("adminEmails", [])]:
        raise HTTPException(status_code=403, detail="Admin access required.")
    return user

# ── Models ─────────────────────────────────────────────────────────────────────

class JobIn(BaseModel):
    title: str; description: str; type: str; priority: str
    locationId: Optional[str] = None; techId: Optional[str] = None; date: str

class JobUpdate(BaseModel):
    title: Optional[str] = None; description: Optional[str] = None
    type: Optional[str] = None; priority: Optional[str] = None
    locationId: Optional[str] = None; techId: Optional[str] = None
    date: Optional[str] = None; status: Optional[str] = None

class TechIn(BaseModel):
    name: str; email: str; initials: str
    color: Optional[str] = "#52525b"; skills: Optional[List[str]] = []

class TechUpdate(BaseModel):
    name: Optional[str] = None; email: Optional[str] = None
    initials: Optional[str] = None; color: Optional[str] = None
    skills: Optional[List[str]] = None

class LocationIn(BaseModel):
    name: str; address: str
    contact_name: Optional[str] = ""; contact_phone: Optional[str] = ""
    contact_email: Optional[str] = ""

class LocationUpdate(BaseModel):
    name: Optional[str] = None; address: Optional[str] = None
    contact_name: Optional[str] = None; contact_phone: Optional[str] = None
    contact_email: Optional[str] = None

class ReportIn(BaseModel):
    content: str; techsOnSite: Optional[str] = ""; timeArrived: Optional[str] = ""
    timeDeparted: Optional[str] = ""; hoursWorked: Optional[str] = ""
    partsUsed: Optional[str] = ""; billableItems: Optional[str] = ""
    completion_checklist: Optional[List[str]] = []
    photos: Optional[List[str]] = []; signature_key: Optional[str] = None

class ChatIn(BaseModel):
    message: str; history: Optional[List[dict]] = []

class AdminEmailIn(BaseModel):
    email: str

# ── Helpers ────────────────────────────────────────────────────────────────────

def get_api_key(): return os.environ.get("GEMINI_API_KEY") or os.environ.get("API_KEY")
def _tech(tid): return next((t for t in get_state()["techs"] if t["id"] == tid), None)
def _loc(lid): return next((l for l in get_state()["locations"] if l["id"] == lid), None)
def _job(jid): return next((j for j in get_state()["jobs"] if j["id"] == jid), None)

def get_model(api_key):
    if not HAS_GENAI: return None, "gemini-1.5-flash"
    client = genai.Client(api_key=api_key)
    try:
        models = list(client.models.list())
        valid = [m for m in models if hasattr(m, 'supported_generation_methods') and
                 'generateContent' in (m.supported_generation_methods or [])]
        if not valid: valid = [m for m in models if 'gemini' in m.name.lower()]
        for pat in ['gemini-1.5-flash', 'gemini-2.0-flash', 'gemini-1.5-pro', 'gemini-pro']:
            best = next((m for m in valid if pat in m.name), None)
            if best: return client, best.name
        if valid: return client, valid[0].name
    except Exception: pass
    return client, 'gemini-1.5-flash'

def weather_for(address):
    try:
        enc = urllib.parse.quote(address.strip())
        r = requests.get(f"https://geocoding-api.open-meteo.com/v1/search?name={enc}&count=1&language=en&format=json", timeout=5)
        data = r.json()
        if not data.get("results"):
            parts = [p.strip() for p in address.split(",")]
            city = parts[1] if len(parts) >= 3 else parts[0]
            r = requests.get(f"https://geocoding-api.open-meteo.com/v1/search?name={urllib.parse.quote(city)}&count=1&language=en&format=json", timeout=5)
            data = r.json()
        res = data.get("results", [])
        if not res: return None
        lat, lon = res[0]["latitude"], res[0]["longitude"]
        wx = requests.get(f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m,weather_code&temperature_unit=fahrenheit&timezone=auto", timeout=5).json()
        cur = wx.get("current", {}); temp = cur.get("temperature_2m"); code = cur.get("weather_code")
        cmap = {0:"☀️ Clear",1:"⛅ Partly Cloudy",2:"⛅ Partly Cloudy",3:"⛅ Partly Cloudy",
                45:"🌫️ Foggy",48:"🌫️ Foggy",51:"🌧️ Drizzle",53:"🌧️ Drizzle",55:"🌧️ Drizzle",
                61:"🌧️ Rain",63:"🌧️ Rain",65:"🌧️ Rain",71:"❄️ Snow",73:"❄️ Snow",75:"❄️ Snow",
                95:"⛈️ Thunderstorm",96:"⛈️ Thunderstorm",99:"⛈️ Thunderstorm"}
        cond = cmap.get(int(code), "Unknown") if code is not None else "Unknown"
        return f"{cond} {temp}°F"
    except Exception: return None

def smtp_cfg(): return (os.environ.get("SMTP_SERVER"), os.environ.get("SMTP_PORT", 587),
                        os.environ.get("SMTP_EMAIL"), os.environ.get("SMTP_PASSWORD"))

def smtp_connect(srv, port, email, pwd):
    port = int(port)
    s = smtplib.SMTP_SSL(srv, port) if port == 465 else smtplib.SMTP(srv, port)
    if port != 465: s.starttls()
    s.login(email, pwd); return s

def send_email(subject, body, recipients, pdf_bytes=None, pdf_name="report.pdf"):
    srv, port, sender, pwd = smtp_cfg()
    if not all([srv, sender, pwd]): return False, "SMTP not configured"
    try:
        s = smtp_connect(srv, port, sender, pwd)
        for r in recipients:
            msg = MIMEMultipart(); msg["From"] = sender; msg["To"] = r; msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))
            if pdf_bytes:
                att = MIMEApplication(pdf_bytes, _subtype="pdf")
                att.add_header("Content-Disposition", "attachment", filename=pdf_name)
                msg.attach(att)
            s.send_message(msg)
        s.quit(); return True, "OK"
    except Exception as e: return False, str(e)

def make_pdf(job, tech, loc, report):
    if not HAS_REPORTLAB: return None
    buf = BytesIO(); p = canvas.Canvas(buf, pagesize=letter); w, h = letter
    p.setFillColor(colors.darkred); p.setFont("Helvetica-Bold", 20)
    p.drawString(50, h-50, "5G Security - Job Completion Report")
    p.setFillColor(colors.black); p.setFont("Helvetica", 10)
    p.drawString(50, h-65, f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    y = h-100; p.setFont("Helvetica-Bold", 12); p.drawString(50, y, "JOB DETAILS")
    p.line(50, y-5, w-50, y-5); y -= 25; p.setFont("Helvetica", 11)
    p.drawString(50, y, f"Title: {job.get('title','')}"); y -= 15
    p.drawString(50, y, f"Location: {loc['name'] if loc else 'Unknown'}"); y -= 15
    p.drawString(50, y, f"Tech: {tech['name'] if tech else 'Unassigned'}"); y -= 15
    p.drawString(50, y, f"Status: {job.get('status','')}"); y -= 40
    p.setFont("Helvetica-Bold", 12); p.drawString(50, y, "FIELD REPORT")
    p.line(50, y-5, w-50, y-5); y -= 25; p.setFont("Helvetica", 11)
    for lbl, k in [("Techs On Site","techsOnSite"),("Arrived","timeArrived"),
                   ("Departed","timeDeparted"),("Hours","hoursWorked"),
                   ("Parts","partsUsed"),("Billable","billableItems")]:
        p.drawString(50, y, f"{lbl}: {report.get(k,'N/A')}"); y -= 20
    notes = report.get("content",""); y -= 20
    p.setFont("Helvetica-Bold", 12); p.drawString(50, y, "NOTES")
    p.line(50, y-5, w-50, y-5); y -= 25; p.setFont("Helvetica", 10)
    to = p.beginText(50, y)
    for chunk in [notes[i:i+80] for i in range(0, len(notes), 80)]: to.textLine(chunk)
    p.drawText(to); p.save(); return buf.getvalue()

def gen_briefing(state):
    api_key = get_api_key()
    if not api_key: return "⚠️ GEMINI_API_KEY not configured."
    if not HAS_GENAI: return "⚠️ google-genai not installed."
    active = [j for j in state["jobs"] if j["status"] != "Completed"]
    critical = [j for j in active if j["priority"] in ["Critical","High"]]
    today = datetime.datetime.now().strftime("%B %d, %Y")
    prompt = f"""You are Operations Manager for 5G Security (cameras, access control, alarms, cabling).
Generate a Morning Briefing. Today: {today}
Active: {len(active)} | Critical/High: {len(critical)}
Techs: {', '.join([t['name'] for t in state['techs']])}
Jobs: {chr(10).join([f"- {j['title']} ({j['priority']})" for j in active])}
Start with **Morning Briefing: 5G Security - {today}**
Cover: 1. Security Focus 2. Critical Jobs 3. Safety Tip. Max 150 words. Use **bold** not # headers."""
    try:
        client, model = get_model(api_key)
        return client.models.generate_content(model=model, contents=prompt).text
    except Exception as e: return f"Error: {e}"

# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/health")
def health(): return {"status": "ok", "timestamp": datetime.datetime.now().isoformat()}

@app.get("/auth/me")
def get_me(user: dict = Depends(verify_google_token)):
    state = get_state(); email = user.get("email","").lower()
    is_admin = email in [e.lower() for e in state.get("adminEmails",[])]
    tech = next((t for t in state["techs"] if t["email"].lower() == email), None)
    if not state["adminEmails"]:
        state["adminEmails"].append(user["email"]); save_state(invalidate_briefing=False); is_admin = True
    return {"user": user, "is_admin": is_admin, "tech": tech}

@app.get("/jobs")
def list_jobs(search: Optional[str] = None, user: dict = Depends(verify_google_token)):
    jobs = get_state()["jobs"]
    if search:
        q = search.lower()
        jobs = [j for j in jobs if q in j.get("title","").lower() or q in j.get("description","").lower()]
    return {"jobs": jobs}

@app.post("/jobs", status_code=201)
def create_job(job: JobIn, user: dict = Depends(require_admin)):
    state = get_state()
    nj = {"id": f"j{len(state['jobs'])+100}_{datetime.datetime.now().timestamp()}",
          "title": job.title, "description": job.description, "type": job.type,
          "priority": job.priority, "status": "Pending", "locationId": job.locationId,
          "techId": job.techId, "date": job.date, "reports": []}
    state["jobs"].insert(0, nj); save_state()
    if job.techId and job.locationId:
        t = _tech(job.techId); l = _loc(job.locationId)
        if t and l:
            send_email(f"Assignment: {job.title}",
                       f"Hello {t['name']},\n\nNew: {job.title} ({job.priority})\n\n{l['name']}\n{l['address']}\n\n{job.description}",
                       [t["email"]])
    return nj

@app.get("/jobs/{job_id}")
def get_job_detail(job_id: str, user: dict = Depends(verify_google_token)):
    j = _job(job_id)
    if not j: raise HTTPException(404, "Job not found")
    return j

@app.patch("/jobs/{job_id}")
def update_job(job_id: str, updates: JobUpdate, user: dict = Depends(verify_google_token)):
    state = get_state()
    idx = next((i for i,j in enumerate(state["jobs"]) if j["id"]==job_id), -1)
    if idx == -1: raise HTTPException(404, "Job not found")
    for f,v in updates.dict(exclude_none=True).items(): state["jobs"][idx][f] = v
    save_state(); return state["jobs"][idx]

@app.delete("/jobs/{job_id}", status_code=204)
def delete_job(job_id: str, user: dict = Depends(require_admin)):
    state = get_state(); before = len(state["jobs"])
    state["jobs"] = [j for j in state["jobs"] if j["id"] != job_id]
    if len(state["jobs"]) == before: raise HTTPException(404, "Job not found")
    save_state()

@app.post("/jobs/{job_id}/reports")
def add_report(job_id: str, report: ReportIn, user: dict = Depends(verify_google_token)):
    state = get_state()
    idx = next((i for i,j in enumerate(state["jobs"]) if j["id"]==job_id), -1)
    if idx == -1: raise HTTPException(404, "Job not found")
    rd = report.dict(); rd["id"] = str(uuid.uuid4())
    rd["timestamp"] = datetime.datetime.now().isoformat(); rd["authorEmail"] = user.get("email")
    api_key = get_api_key()
    if api_key and HAS_GENAI and report.content:
        try:
            client, model = get_model(api_key)
            rd["ai_summary"] = client.models.generate_content(model=model,
                contents=f"Summarize these tech notes for '{state['jobs'][idx]['title']}' in ~50 words for a client report:\n\n{report.content}").text
        except Exception: pass
    state["jobs"][idx].setdefault("reports", []).append(rd)
    save_state(invalidate_briefing=False)
    t = _tech(state["jobs"][idx].get("techId"))
    l = _loc(state["jobs"][idx].get("locationId"))
    pdf = make_pdf(state["jobs"][idx], t, l, rd)
    admins = state.get("adminEmails", [])
    if admins:
        job_title = state["jobs"][idx]["title"]
        status = state["jobs"][idx].get("status", "In Progress")
        if status == "Completed":
            subject = f"✅ Job Completed: {job_title}"
            body = f"Job has been completed. See attached PDF report."
        else:
            subject = f"📋 Daily Report: {job_title}"
            body = f"A field report has been submitted for '{job_title}' (Status: {status}). See attached PDF."
        send_email(subject, body, admins, pdf, f"Report_{job_id}.pdf")
    return rd

@app.get("/jobs/{job_id}/pdf")
def download_pdf(job_id: str, user: dict = Depends(verify_google_token)):
    j = _job(job_id)
    if not j: raise HTTPException(404, "Job not found")
    if not j.get("reports"): raise HTTPException(404, "No reports")
    pdf = make_pdf(j, _tech(j.get("techId")), _loc(j.get("locationId")), j["reports"][-1])
    if not pdf: raise HTTPException(500, "PDF generation unavailable")
    return StreamingResponse(BytesIO(pdf), media_type="application/pdf",
                             headers={"Content-Disposition": f"attachment; filename=Report_{job_id}.pdf"})

@app.get("/jobs/{job_id}/ics")
def download_ics(job_id: str, user: dict = Depends(verify_google_token)):
    j = _job(job_id)
    if not j: raise HTTPException(404, "Job not found")
    l = _loc(j.get("locationId"))
    try:
        ds = datetime.datetime.fromisoformat(j["date"]) if "T" in j["date"] else datetime.datetime.strptime(j["date"][:10],"%Y-%m-%d").replace(hour=9)
        de = ds + datetime.timedelta(hours=2); fmt = "%Y%m%dT%H%M%S"
        ics = f"BEGIN:VCALENDAR\nVERSION:2.0\nBEGIN:VEVENT\nUID:{j['id']}@5gsecurity.app\nDTSTAMP:{datetime.datetime.now().strftime(fmt)}\nDTSTART:{ds.strftime(fmt)}\nDTEND:{de.strftime(fmt)}\nSUMMARY:🛡️ {j['title']}\nLOCATION:{l['name']+' - '+l['address'] if l else 'Unknown'}\nEND:VEVENT\nEND:VCALENDAR"
        return StreamingResponse(BytesIO(ics.encode()), media_type="text/calendar",
                                 headers={"Content-Disposition": f"attachment; filename=job_{job_id}.ics"})
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/techs")
def list_techs(user: dict = Depends(verify_google_token)): return {"techs": get_state()["techs"]}

@app.post("/techs", status_code=201)
def create_tech(tech: TechIn, user: dict = Depends(require_admin)):
    state = get_state()
    colors = ['#7f1d1d','#3f3f46','#b91c1c','#52525b','#991b1b','#7c2d12','#292524']
    nt = {"id": f"t{len(state['techs'])+1}_{datetime.datetime.now().timestamp()}",
          "name": tech.name, "email": tech.email, "initials": tech.initials,
          "color": tech.color or colors[len(state["techs"]) % len(colors)], "skills": tech.skills or []}
    state["techs"].append(nt); save_state(invalidate_briefing=False); return nt

@app.patch("/techs/{tech_id}")
def update_tech(tech_id: str, updates: TechUpdate, user: dict = Depends(require_admin)):
    state = get_state()
    idx = next((i for i,t in enumerate(state["techs"]) if t["id"]==tech_id), -1)
    if idx == -1: raise HTTPException(404, "Tech not found")
    for f,v in updates.dict(exclude_none=True).items(): state["techs"][idx][f] = v
    save_state(invalidate_briefing=False); return state["techs"][idx]

@app.delete("/techs/{tech_id}", status_code=204)
def delete_tech(tech_id: str, user: dict = Depends(require_admin)):
    state = get_state(); before = len(state["techs"])
    state["techs"] = [t for t in state["techs"] if t["id"] != tech_id]
    if len(state["techs"]) == before: raise HTTPException(404, "Tech not found")
    save_state(invalidate_briefing=False)

@app.get("/locations")
def list_locations(user: dict = Depends(verify_google_token)): return {"locations": get_state()["locations"]}

@app.post("/locations", status_code=201)
def create_location(loc: LocationIn, user: dict = Depends(require_admin)):
    state = get_state()
    nl = {"id": f"l{len(state['locations'])+1}_{datetime.datetime.now().timestamp()}",
          "name": loc.name, "address": loc.address, "contact_name": loc.contact_name or "",
          "contact_phone": loc.contact_phone or "", "contact_email": loc.contact_email or ""}
    w = weather_for(loc.address)
    if w: nl["weather"] = w
    state["locations"].append(nl); save_state(invalidate_briefing=False); return nl

@app.patch("/locations/{loc_id}")
def update_location(loc_id: str, updates: LocationUpdate, user: dict = Depends(require_admin)):
    state = get_state()
    idx = next((i for i,l in enumerate(state["locations"]) if l["id"]==loc_id), -1)
    if idx == -1: raise HTTPException(404, "Location not found")
    for f,v in updates.dict(exclude_none=True).items(): state["locations"][idx][f] = v
    save_state(invalidate_briefing=False); return state["locations"][idx]

@app.delete("/locations/{loc_id}", status_code=204)
def delete_location(loc_id: str, user: dict = Depends(require_admin)):
    state = get_state(); before = len(state["locations"])
    state["locations"] = [l for l in state["locations"] if l["id"] != loc_id]
    if len(state["locations"]) == before: raise HTTPException(404, "Location not found")
    save_state(invalidate_briefing=False)

@app.get("/locations/{loc_id}/weather")
def get_location_weather(loc_id: str, user: dict = Depends(verify_google_token)):
    l = _loc(loc_id)
    if not l: raise HTTPException(404, "Location not found")
    return {"weather": weather_for(l["address"])}

@app.get("/briefing")
def get_briefing(user: dict = Depends(verify_google_token)):
    state = get_state(); b = state.get("briefing","")
    if b == "Data required to generate briefing." and state["jobs"]:
        b = gen_briefing(state); state["briefing"] = b; save_state(invalidate_briefing=False)
    return {"briefing": b}

@app.post("/briefing/regenerate")
def regen_briefing(user: dict = Depends(verify_google_token)):
    state = get_state(); b = gen_briefing(state)
    state["briefing"] = b; save_state(invalidate_briefing=False); return {"briefing": b}

@app.post("/chat")
def chat(msg: ChatIn, user: dict = Depends(verify_google_token)):
    api_key = get_api_key()
    if not api_key or not HAS_GENAI: raise HTTPException(503, "AI not available")
    state = get_state(); client, model = get_model(api_key)
    ctx = f"You are an AI assistant for the 5G Security Job Board.\nJobs: {json.dumps(state['jobs'],default=str)}\nTechs: {json.dumps(state['techs'])}\nLocations: {json.dumps(state['locations'])}"
    contents = [{"role":"user","parts":[ctx]}] + (msg.history or []) + [{"role":"user","parts":[msg.message]}]
    try: return {"reply": client.models.generate_content(model=model, contents=contents).text}
    except Exception as e: raise HTTPException(500, str(e))

@app.post("/upload/photo")
async def upload_photo(file: UploadFile = File(...), folder: str = "photos", user: dict = Depends(verify_google_token)):
    data = await file.read()
    key = f"{folder}/{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
    result = upload_bytes(data, key, file.content_type or "image/jpeg")
    if not result: raise HTTPException(500, "Upload failed")
    return {"key": result}

@app.post("/upload/signature")
async def upload_signature(file: UploadFile = File(...), user: dict = Depends(verify_google_token)):
    data = await file.read()
    key = f"signatures/{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_sig.png"
    result = upload_bytes(data, key, "image/png")
    if not result: raise HTTPException(500, "Upload failed")
    return {"key": result}

@app.get("/files/url")
def get_file_url(key: str, user: dict = Depends(verify_google_token)):
    url = get_view_url(key)
    if not url: raise HTTPException(404, "File not found")
    return {"url": url}

@app.get("/admin/emails")
def get_admin_emails(user: dict = Depends(require_admin)):
    return {"adminEmails": get_state().get("adminEmails", [])}

@app.post("/admin/emails")
def add_admin_email(body: AdminEmailIn, user: dict = Depends(require_admin)):
    state = get_state()
    if body.email not in state["adminEmails"]: state["adminEmails"].append(body.email); save_state(invalidate_briefing=False)
    return {"adminEmails": state["adminEmails"]}

@app.delete("/admin/emails/{email}")
def remove_admin_email(email: str, user: dict = Depends(require_admin)):
    state = get_state(); state["adminEmails"] = [e for e in state["adminEmails"] if e != email]
    save_state(invalidate_briefing=False); return {"adminEmails": state["adminEmails"]}

@app.get("/admin/export/json")
def export_json(user: dict = Depends(require_admin)): return JSONResponse(content=get_state())

@app.get("/admin/export/csv")
def export_csv(user: dict = Depends(require_admin)):
    import csv, io
    jobs = get_state().get("jobs", [])
    if not jobs: return StreamingResponse(BytesIO(b"No jobs"), media_type="text/csv")
    out = io.StringIO(); w = csv.DictWriter(out, fieldnames=jobs[0].keys()); w.writeheader(); w.writerows(jobs)
    return StreamingResponse(BytesIO(out.getvalue().encode()), media_type="text/csv",
                             headers={"Content-Disposition": "attachment; filename=jobs_export.csv"})

@app.post("/admin/reminders/send")
def send_reminders(user: dict = Depends(require_admin)):
    state = get_state(); srv, port, sender, pwd = smtp_cfg()
    if not all([srv, sender, pwd]): raise HTTPException(503, "SMTP not configured")
    today = datetime.datetime.now().strftime("%Y-%m-%d"); count = 0
    try:
        s = smtp_connect(srv, port, sender, pwd)
        for t in state["techs"]:
            jobs = [j for j in state["jobs"] if j.get("techId")==t["id"] and j.get("status")!="Completed"]
            if not jobs: continue
            lines = "\n".join([f"- {j['title']} ({j['priority']})" for j in jobs])
            msg = MIMEMultipart(); msg["From"]=sender; msg["To"]=t["email"]
            msg["Subject"]=f"📅 Daily Reminder - {today}"
            msg.attach(MIMEText(f"Hello {t['name']},\n\nYour assignments for {today}:\n{lines}\n\nCheck the job board for details.", "plain"))
            s.send_message(msg); count += 1
        s.quit(); state["last_reminder_date"] = today; save_state(invalidate_briefing=False)
        return {"sent": count}
    except Exception as e: raise HTTPException(500, str(e))

@app.post("/admin/import")
def import_data(body: dict, user: dict = Depends(require_admin)):
    state = get_state()
    for k in ["jobs","techs","locations","adminEmails"]:
        if k in body: state[k] = body[k]
    save_state(invalidate_briefing=True); reload_state(); return {"status": "imported"}
