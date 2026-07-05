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

from persistence_pg import load_state, save_state_to_db, init_db, StaleStateError, get_db_version
from object_store import upload_bytes, get_view_url

# Cloud hosts run on UTC — stamp timestamps in the company timezone instead.
# Override with the APP_TIMEZONE env var (IANA name, e.g. "America/Chicago").
try:
    from zoneinfo import ZoneInfo
    _APP_TZ = ZoneInfo(os.getenv("APP_TIMEZONE", "America/Chicago"))
except Exception:
    _APP_TZ = None

def now_local():
    if _APP_TZ:
        return datetime.datetime.now(_APP_TZ).replace(tzinfo=None)
    return datetime.datetime.now()

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
        # First load populates the cache.
        if not _state_cache:
            data, version = load_state()
            _state_cache = data
            _state_version = version
            return _state_cache
        # Otherwise do a cheap version check and only reload the full state when
        # another writer (the Streamlit app, another API worker) moved the DB
        # forward. This is what keeps the iOS app in sync with web-side changes.
        try:
            db_ver = get_db_version()
            if db_ver is not None and db_ver != _state_version:
                data, version = load_state()
                _state_cache = data
                _state_version = version
        except Exception:
            pass
        return _state_cache

def save_state(invalidate_briefing: bool = True):
    global _state_cache, _state_version
    with _state_lock:
        if invalidate_briefing:
            _state_cache["briefing"] = "Data required to generate briefing."
        try:
            _state_version = save_state_to_db(_state_cache, expected_version=_state_version or None)
        except StaleStateError:
            # Another writer (e.g. the Streamlit app) saved first. Drop the stale
            # cache so the client's retry runs against fresh data.
            _state_cache = {}
            _state_version = 0
            raise HTTPException(status_code=409, detail="Data changed on the server. Please retry the request.")

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
        user = r.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Token validation failed: {e}")

    # ACCESS CONTROL: a valid Google account is not enough - the email must be a
    # registered admin or tech (or match ALLOWED_EMAIL_DOMAIN) to use the API.
    email = (user.get("email") or "").lower()
    state = get_state()
    is_admin = email in [e.lower() for e in state.get("adminEmails", [])]
    is_tech = any((t.get("email") or "").lower() == email for t in state.get("techs", []))
    allowed_domain = os.getenv("ALLOWED_EMAIL_DOMAIN", "")
    domain_ok = bool(allowed_domain) and email.endswith("@" + allowed_domain.lower().lstrip("@"))
    if not (is_admin or is_tech or domain_ok):
        raise HTTPException(status_code=403, detail="Account not registered on the 5G Security Job Board.")
    return user

def require_admin(user: dict = Depends(verify_google_token)) -> dict:
    state = get_state()
    if user.get("email", "").lower() not in [e.lower() for e in state.get("adminEmails", [])]:
        raise HTTPException(status_code=403, detail="Admin access required.")
    return user

# ── Models ─────────────────────────────────────────────────────────────────────

class JobIn(BaseModel):
    title: str; description: str; type: str; priority: str
    locationId: Optional[str] = None; techId: Optional[str] = None; date: str
    company: Optional[str] = "security"
    photos: Optional[List[str]] = []

class JobUpdate(BaseModel):
    title: Optional[str] = None; description: Optional[str] = None
    type: Optional[str] = None; priority: Optional[str] = None
    locationId: Optional[str] = None; techId: Optional[str] = None
    date: Optional[str] = None; status: Optional[str] = None

class TechIn(BaseModel):
    name: str; email: str; initials: str
    color: Optional[str] = "#52525b"; skills: Optional[List[str]] = []
    company: Optional[str] = "security"

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

class PartIn(BaseModel):
    name: str
    qty: Optional[int] = 1
    status: Optional[str] = "Needed"
    vendor: Optional[str] = ""
    cost: Optional[str] = ""
    notes: Optional[str] = ""

class PartUpdate(BaseModel):
    name: Optional[str] = None
    qty: Optional[int] = None
    status: Optional[str] = None
    vendor: Optional[str] = None
    cost: Optional[str] = None
    notes: Optional[str] = None

class MoveReportIn(BaseModel):
    target_job_id: str

class JobPhotosIn(BaseModel):
    keys: List[str]

class JobPhotoDeleteIn(BaseModel):
    key: str

# ── Helpers ────────────────────────────────────────────────────────────────────

def get_api_key(): return os.environ.get("GEMINI_API_KEY") or os.environ.get("API_KEY")
def _tech(tid): return next((t for t in get_state()["techs"] if t["id"] == tid), None)
def _loc(lid): return next((l for l in get_state()["locations"] if l["id"] == lid), None)
def _job(jid): return next((j for j in get_state()["jobs"] if j["id"] == jid), None)

# ── Company / construction access ────────────────────────────────────────────
def job_company(j): return (j or {}).get("company", "security")

def construction_role(user, state):
    """'manager' (5G Construction lead), 'crew' (construction tech), or None."""
    email = (user.get("email") or "").lower()
    if email in [e.lower() for e in state.get("construction_emails", [])]:
        return "manager"
    t = next((t for t in state.get("techs", []) if (t.get("email") or "").lower() == email), None)
    if t and t.get("company") == "construction":
        return "crew"
    return None

def user_companies(user, state):
    """Set of companies this user may see. Admins see both; construction-only
    users see only construction; everyone else sees only security."""
    email = (user.get("email") or "").lower()
    if email in [e.lower() for e in state.get("adminEmails", [])]:
        return {"security", "construction"}
    if construction_role(user, state) is not None:
        return {"construction"}
    return {"security"}

def resolve_company(user, state, requested):
    """Which single company to show. Validates the request against access;
    defaults to construction for construction-only users, else security."""
    allowed = user_companies(user, state)
    if requested:
        if requested not in allowed:
            raise HTTPException(403, "Not allowed to view that company.")
        return requested
    return "construction" if allowed == {"construction"} else "security"

def get_model(api_key):
    if not HAS_GENAI: return None, "gemini-flash-latest"
    client = genai.Client(api_key=api_key)
    try:
        models = list(client.models.list())
        exclude = ('tts', 'image', 'audio', 'live', 'embed', 'veo', 'imagen', 'aqa')

        def text_capable(m):
            # google-genai SDK uses 'supported_actions'; legacy SDK used 'supported_generation_methods'
            acts = getattr(m, 'supported_actions', None) or getattr(m, 'supported_generation_methods', None)
            return not acts or 'generateContent' in acts or 'generate_content' in acts

        valid = [m for m in models if 'gemini' in m.name.lower()
                 and not any(x in m.name.lower() for x in exclude) and text_capable(m)]
        for pat in ['gemini-2.5-flash', 'gemini-flash-latest', 'gemini-2.0-flash', 'gemini-2.5-pro', 'flash']:
            best = (next((m for m in valid if m.name.lower().split('/')[-1] == pat), None)
                    or next((m for m in valid if pat in m.name.lower() and 'preview' not in m.name.lower()), None)
                    or next((m for m in valid if pat in m.name.lower()), None))
            if best: return client, best.name
        if valid: return client, valid[0].name
    except Exception: pass
    return client, 'gemini-flash-latest'

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

# ── Branded email ────────────────────────────────────────────────────────────
EMAIL_PRIORITY_COLORS = {"Critical": "#ef4444", "High": "#dc2626", "Medium": "#b45309", "Low": "#52525b"}

def _email_esc(s):
    return str(s if s is not None else "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def _email_brand_mark():
    url = os.getenv("LOGO_URL")
    if url:
        return f'<img src="{url}" alt="5G Security" style="height:34px;display:inline-block;">'
    return '<span style="color:#ffffff;font-size:22px;font-weight:bold;letter-spacing:1px;">5G SECURITY</span>'

def _email_shell(header_label, inner_html):
    return f"""<html><body style="margin:0;padding:0;background-color:#f4f4f5;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f4f5;">
<tr><td align="center" style="padding:24px 12px;">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background-color:#ffffff;border-radius:8px;overflow:hidden;font-family:Arial,Helvetica,sans-serif;border:1px solid #e4e4e7;">
    <tr><td style="background-color:#18181b;padding:22px 32px;border-bottom:4px solid #b91c1c;">
        {_email_brand_mark()}<br>
        <span style="color:#a1a1aa;font-size:13px;">{_email_esc(header_label)}</span>
    </td></tr>
    <tr><td style="padding:28px 32px;">{inner_html}</td></tr>
    <tr><td style="background-color:#f4f4f5;padding:14px 32px;color:#71717a;font-size:11px;border-top:1px solid #e4e4e7;">
        5G Security &nbsp;|&nbsp; Cameras &middot; Access Control &middot; Alarm Systems &middot; Cabling
    </td></tr>
</table></td></tr></table></body></html>"""

def _email_rows(rows):
    out = ""
    for label, value in rows:
        out += (f'<tr><td style="padding:8px 12px;background-color:#f4f4f5;color:#71717a;font-size:11px;'
                f'font-weight:bold;text-transform:uppercase;border-bottom:1px solid #e4e4e7;width:120px;">{_email_esc(label)}</td>'
                f'<td style="padding:8px 12px;color:#27272a;font-size:14px;border-bottom:1px solid #e4e4e7;">{_email_esc(value)}</td></tr>')
    return f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin:18px 0;border:1px solid #e4e4e7;border-radius:6px;border-collapse:separate;overflow:hidden;">{out}</table>'

def build_assignment_html(job, tech, loc):
    p_color = EMAIL_PRIORITY_COLORS.get(job.get("priority"), "#52525b")
    first = (tech.get("name") or "there").split()[0] if tech else "there"
    loc_name = loc.get("name", "Unknown") if loc else "Unknown"
    loc_addr = loc.get("address", "") if loc else ""
    app_url = os.getenv("APP_URL", "").rstrip("/")
    map_url = f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote(loc_addr)}" if loc_addr else None

    buttons = ""
    if map_url:
        buttons += f'<a href="{map_url}" style="display:inline-block;background-color:#b91c1c;color:#ffffff;padding:11px 22px;border-radius:6px;text-decoration:none;font-weight:bold;font-size:14px;margin-right:10px;">&#128205; Get Directions</a>'
    if app_url:
        buttons += f'<a href="{app_url}" style="display:inline-block;background-color:#18181b;color:#ffffff;padding:11px 22px;border-radius:6px;text-decoration:none;font-weight:bold;font-size:14px;">Open Job Board</a>'

    desc = _email_esc(job.get("description", "")).replace("\n", "<br>")
    inner = (f'<p style="color:#27272a;font-size:14px;margin:0 0 18px 0;">Hello {_email_esc(first)}, you have a new job assignment:</p>'
             f'<h2 style="color:#18181b;font-size:19px;margin:0 0 10px 0;">{_email_esc(job.get("title",""))}</h2>'
             f'<span style="display:inline-block;background-color:{p_color};color:#ffffff;padding:3px 12px;border-radius:12px;font-size:12px;font-weight:bold;">{_email_esc(job.get("priority","N/A"))} Priority</span>'
             + _email_rows([("Location", loc_name), ("Address", loc_addr), ("Type", job.get("type", "N/A")), ("Scheduled", str(job.get("date", ""))[:10])])
             + f'<p style="color:#71717a;font-size:11px;font-weight:bold;text-transform:uppercase;margin:0 0 6px 0;">Description</p>'
             f'<p style="color:#27272a;font-size:14px;line-height:1.6;margin:0 0 24px 0;border-left:3px solid #b91c1c;padding-left:12px;">{desc}</p>'
             + buttons)
    return _email_shell("New Job Assignment", inner)

def build_completion_html(job, tech, loc, report):
    inner = (f'<p style="color:#27272a;font-size:14px;margin:0 0 6px 0;">A job has been marked <b>Completed</b>.</p>'
             + _email_rows([("Job", job.get("title", "")),
                            ("Technician", tech.get("name", "Unknown") if tech else "Unknown"),
                            ("Location", loc.get("name", "Unknown") if loc else "Unknown"),
                            ("Hours", report.get("hoursWorked", "N/A"))])
             + '<p style="color:#71717a;font-size:13px;margin:0;">&#128206; The full completion report is attached as a PDF.</p>')
    return _email_shell("Job Completed", inner)

def build_reminder_html(tech, jobs_with_locs, today):
    first = (tech.get("name") or "there").split()[0] if tech else "there"
    cards = ""
    for job, loc in jobs_with_locs:
        p_color = EMAIL_PRIORITY_COLORS.get(job.get("priority"), "#52525b")
        loc_name = loc.get("name", "Unknown Location") if loc else "Unknown Location"
        loc_addr = loc.get("address", "") if loc else ""
        cards += (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
                  f'style="border:1px solid #e4e4e7;border-left:4px solid {p_color};border-radius:6px;border-collapse:separate;margin:0 0 12px 0;">'
                  f'<tr><td style="padding:14px 16px;">'
                  f'<span style="color:#18181b;font-size:15px;font-weight:bold;">{_email_esc(job.get("title",""))}</span>'
                  f'<span style="display:inline-block;background:{p_color};color:#fff;padding:2px 10px;border-radius:10px;font-size:11px;font-weight:bold;margin-left:8px;">{_email_esc(job.get("priority",""))}</span><br>'
                  f'<span style="color:#71717a;font-size:12px;">Status: {_email_esc(job.get("status",""))}</span><br>'
                  f'<span style="color:#27272a;font-size:13px;font-weight:bold;">&#128205; {_email_esc(loc_name)}</span><br>'
                  f'<span style="font-size:12px;color:#71717a;">{_email_esc(loc_addr)}</span>'
                  f'</td></tr></table>')
    plural = "s" if len(jobs_with_locs) != 1 else ""
    inner = (f'<p style="color:#27272a;font-size:14px;margin:0 0 18px 0;">Good morning {_email_esc(first)} &mdash; '
             f'you have <b>{len(jobs_with_locs)} active assignment{plural}</b> today:</p>{cards}'
             f'<p style="color:#71717a;font-size:12px;margin:12px 0 0 0;">Check the job board for full details and to log your work.</p>')
    return _email_shell(f"Daily Assignment Reminder &mdash; {_email_esc(today)}", inner)

def send_email(subject, body, recipients, pdf_bytes=None, pdf_name="report.pdf", html=None):
    srv, port, sender, pwd = smtp_cfg()
    if not all([srv, sender, pwd]): return False, "SMTP not configured"
    try:
        s = smtp_connect(srv, port, sender, pwd)
        for r in recipients:
            # mixed( alternative(plain, html), pdf ) so HTML renders and the PDF still attaches
            alt = MIMEMultipart("alternative")
            alt.attach(MIMEText(body, "plain"))
            if html:
                alt.attach(MIMEText(html, "html"))
            msg = MIMEMultipart("mixed")
            msg["From"] = sender; msg["To"] = r; msg["Subject"] = subject
            msg.attach(alt)
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
    p.drawString(50, h-65, f"Generated: {now_local().strftime('%Y-%m-%d %H:%M')}")
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
    today = now_local().strftime("%B %d, %Y")
    prompt = f"""You are Operations Manager for 5G Security (cameras, access control, alarms, cabling).
Generate an Operations Briefing. Today: {today}
Active: {len(active)} | Critical/High: {len(critical)}
Techs: {', '.join([t['name'] for t in state['techs']])}
Jobs: {chr(10).join([f"- {j['title']} ({j['priority']})" for j in active])}
Start with **Operations Briefing: 5G Security - {today}**
Cover: 1. Security Focus 2. Critical Jobs 3. Safety Tip. Max 150 words. Use **bold** not # headers."""
    try:
        client, model = get_model(api_key)
        return client.models.generate_content(model=model, contents=prompt).text
    except Exception as e: return f"Error: {e}"

# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/health")
def health(): return {"status": "ok", "timestamp": now_local().isoformat()}

@app.get("/auth/me")
def get_me(user: dict = Depends(verify_google_token)):
    state = get_state(); email = user.get("email","").lower()
    is_admin = email in [e.lower() for e in state.get("adminEmails",[])]
    tech = next((t for t in state["techs"] if t["email"].lower() == email), None)
    if not state["adminEmails"]:
        state["adminEmails"].append(user["email"]); save_state(invalidate_briefing=False); is_admin = True
    return {"user": user, "is_admin": is_admin, "tech": tech,
            "construction_role": construction_role(user, state),
            "companies": sorted(user_companies(user, state))}

@app.get("/jobs")
def list_jobs(search: Optional[str] = None, company: Optional[str] = None,
              user: dict = Depends(verify_google_token)):
    state = get_state()
    show = resolve_company(user, state, company)
    jobs = [j for j in state["jobs"] if job_company(j) == show]
    if search:
        q = search.lower()
        jobs = [j for j in jobs if q in j.get("title","").lower() or q in j.get("description","").lower()]
    return {"jobs": jobs}

@app.post("/jobs", status_code=201)
def create_job(job: JobIn, user: dict = Depends(verify_google_token)):
    state = get_state()
    company = job.company or "security"
    # Access: must be allowed the company; security jobs = admin only,
    # construction jobs = admin OR the construction lead (manager).
    if company not in user_companies(user, state):
        raise HTTPException(403, "Not allowed to create jobs for that company.")
    email = user.get("email", "").lower()
    is_admin = email in [e.lower() for e in state.get("adminEmails", [])]
    if company == "security" and not is_admin:
        raise HTTPException(403, "Admin access required.")
    if company == "construction" and not (is_admin or construction_role(user, state) == "manager"):
        raise HTTPException(403, "Construction lead access required.")
    nj = {"id": f"j{len(state['jobs'])+100}_{now_local().timestamp()}",
          "title": job.title, "description": job.description, "type": job.type,
          "priority": job.priority, "status": "Pending", "locationId": job.locationId,
          "techId": job.techId, "date": job.date, "reports": [], "company": company,
          "photos": job.photos or []}
    state["jobs"].insert(0, nj); save_state()
    if job.techId and job.locationId:
        t = _tech(job.techId); l = _loc(job.locationId)
        if t and l:
            send_email(f"New Job Assignment: {job.title}",
                       f"Hello {t['name']},\n\nNew: {job.title} ({job.priority})\n\n{l['name']}\n{l['address']}\n\n{job.description}",
                       [t["email"]],
                       html=build_assignment_html(nj, t, l))
    return nj

@app.get("/jobs/{job_id}")
def get_job_detail(job_id: str, user: dict = Depends(verify_google_token)):
    j = _job(job_id)
    if not j: raise HTTPException(404, "Job not found")
    if job_company(j) not in user_companies(user, get_state()):
        raise HTTPException(403, "Not allowed to view this job.")
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
    rd["timestamp"] = now_local().isoformat(); rd["authorEmail"] = user.get("email")
    # Match the web app's report shape so the Streamlit history view is happy
    rd["techId"] = state["jobs"][idx].get("techId") or "unknown"
    api_key = get_api_key()
    if api_key and HAS_GENAI and report.content:
        try:
            client, model = get_model(api_key)
            rd["ai_summary"] = client.models.generate_content(model=model,
                contents=f"Summarize these tech notes for '{state['jobs'][idx]['title']}' in ~50 words for a client report:\n\n{report.content}").text
        except Exception: pass
    state["jobs"][idx].setdefault("reports", []).append(rd)
    save_state(invalidate_briefing=False)
    if state["jobs"][idx].get("status") == "Completed":
        t = _tech(state["jobs"][idx].get("techId")); l = _loc(state["jobs"][idx].get("locationId"))
        pdf = make_pdf(state["jobs"][idx], t, l, rd)
        admins = state.get("adminEmails", [])
        if admins: send_email(f"✅ Job Completed: {state['jobs'][idx]['title']}",
                              f"Job completed. See attached PDF.", admins, pdf, f"Report_{job_id}.pdf",
                              html=build_completion_html(state["jobs"][idx], t, l, rd))
    return rd

# ── Report management (delete / move) ────────────────────────────────────────
def _job_idx_checked(state, job_id, user):
    idx = next((i for i, j in enumerate(state["jobs"]) if j["id"] == job_id), -1)
    if idx == -1: raise HTTPException(404, "Job not found")
    if job_company(state["jobs"][idx]) not in user_companies(user, state):
        raise HTTPException(403, "Not allowed to modify this job.")
    return idx

@app.delete("/jobs/{job_id}/reports/{report_id}", status_code=204)
def delete_report(job_id: str, report_id: str, user: dict = Depends(verify_google_token)):
    state = get_state()
    idx = _job_idx_checked(state, job_id, user)
    reports = state["jobs"][idx].get("reports", [])
    new_reports = [r for r in reports if r.get("id") != report_id]
    if len(new_reports) == len(reports): raise HTTPException(404, "Report not found")
    state["jobs"][idx]["reports"] = new_reports
    save_state(invalidate_briefing=False)

@app.post("/jobs/{job_id}/reports/{report_id}/move")
def move_report(job_id: str, report_id: str, body: MoveReportIn, user: dict = Depends(verify_google_token)):
    state = get_state()
    src = _job_idx_checked(state, job_id, user)
    dst = _job_idx_checked(state, body.target_job_id, user)  # also verifies access to target
    report = next((r for r in state["jobs"][src].get("reports", []) if r.get("id") == report_id), None)
    if not report: raise HTTPException(404, "Report not found")
    state["jobs"][dst].setdefault("reports", []).append(report)
    state["jobs"][src]["reports"] = [r for r in state["jobs"][src]["reports"] if r.get("id") != report_id]
    save_state(invalidate_briefing=False)
    return {"status": "moved", "to": body.target_job_id}

# ── Time clock ───────────────────────────────────────────────────────────────
@app.post("/jobs/{job_id}/clock-in")
def clock_in(job_id: str, user: dict = Depends(verify_google_token)):
    state = get_state()
    idx = _job_idx_checked(state, job_id, user)
    email = (user.get("email") or "").lower()
    entries = state["jobs"][idx].setdefault("time_entries", [])
    if any(e for e in entries if (e.get("userEmail") or "").lower() == email and not e.get("clock_out")):
        raise HTTPException(409, "Already clocked in on this job.")
    entry = {"id": f"tc{now_local().timestamp()}", "userEmail": user.get("email"),
             "tech_name": user.get("name") or user.get("email"),
             "clock_in": now_local().isoformat(), "clock_out": None}
    entries.append(entry)
    save_state(invalidate_briefing=False)
    return entry

@app.post("/jobs/{job_id}/clock-out")
def clock_out(job_id: str, user: dict = Depends(verify_google_token)):
    state = get_state()
    idx = _job_idx_checked(state, job_id, user)
    email = (user.get("email") or "").lower()
    entry = next((e for e in state["jobs"][idx].get("time_entries", [])
                  if (e.get("userEmail") or "").lower() == email and not e.get("clock_out")), None)
    if not entry: raise HTTPException(404, "Not currently clocked in.")
    entry["clock_out"] = now_local().isoformat()
    save_state(invalidate_briefing=False)
    return entry

# ── Parts ────────────────────────────────────────────────────────────────────
@app.post("/jobs/{job_id}/parts", status_code=201)
def add_part(job_id: str, part: PartIn, user: dict = Depends(verify_google_token)):
    state = get_state()
    idx = _job_idx_checked(state, job_id, user)
    p = {"id": f"p{now_local().timestamp()}", "name": part.name, "qty": part.qty or 1,
         "status": part.status or "Needed", "vendor": part.vendor or "", "cost": part.cost or "",
         "notes": part.notes or "", "added_by": user.get("email"), "updated_at": now_local().isoformat()}
    state["jobs"][idx].setdefault("parts", []).append(p)
    save_state(invalidate_briefing=False)
    return p

@app.patch("/jobs/{job_id}/parts/{part_id}")
def update_part(job_id: str, part_id: str, updates: PartUpdate, user: dict = Depends(verify_google_token)):
    state = get_state()
    idx = _job_idx_checked(state, job_id, user)
    p = next((x for x in state["jobs"][idx].get("parts", []) if x.get("id") == part_id), None)
    if not p: raise HTTPException(404, "Part not found")
    for f, v in updates.dict(exclude_none=True).items(): p[f] = v
    p["updated_at"] = now_local().isoformat(); p["added_by"] = user.get("email")
    save_state(invalidate_briefing=False)
    return p

@app.delete("/jobs/{job_id}/parts/{part_id}", status_code=204)
def delete_part(job_id: str, part_id: str, user: dict = Depends(verify_google_token)):
    state = get_state()
    idx = _job_idx_checked(state, job_id, user)
    parts = state["jobs"][idx].get("parts", [])
    new_parts = [x for x in parts if x.get("id") != part_id]
    if len(new_parts) == len(parts): raise HTTPException(404, "Part not found")
    state["jobs"][idx]["parts"] = new_parts
    save_state(invalidate_briefing=False)

@app.get("/jobs/{job_id}/photos")
def list_job_photos(job_id: str, user: dict = Depends(verify_google_token)):
    state = get_state()
    idx = _job_idx_checked(state, job_id, user)
    job = state["jobs"][idx]
    # Merge job-level photos with every photo across the job's reports, deduped,
    # so existing jobs surface photos posted through field reports too.
    keys, seen = [], set()
    for k in job.get("photos", []) or []:
        if k and k not in seen: seen.add(k); keys.append(k)
    for r in job.get("reports", []) or []:
        for k in (r.get("photos") or []):
            if k and k not in seen: seen.add(k); keys.append(k)
    return {"photos": keys}

@app.post("/jobs/{job_id}/photos", status_code=201)
def add_job_photos(job_id: str, body: JobPhotosIn, user: dict = Depends(verify_google_token)):
    state = get_state()
    idx = _job_idx_checked(state, job_id, user)
    photos = state["jobs"][idx].setdefault("photos", [])
    for k in body.keys:
        if k and k not in photos: photos.append(k)
    save_state(invalidate_briefing=False)
    return {"photos": photos}

@app.delete("/jobs/{job_id}/photos")
def delete_job_photo(job_id: str, body: JobPhotoDeleteIn, user: dict = Depends(verify_google_token)):
    state = get_state()
    idx = _job_idx_checked(state, job_id, user)
    photos = state["jobs"][idx].get("photos", []) or []
    if body.key not in photos:
        raise HTTPException(404, "Photo not on this job (report photos are managed on the report).")
    state["jobs"][idx]["photos"] = [k for k in photos if k != body.key]
    save_state(invalidate_briefing=False)
    return {"photos": state["jobs"][idx]["photos"]}

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
        ics = f"BEGIN:VCALENDAR\nVERSION:2.0\nBEGIN:VEVENT\nUID:{j['id']}@5gsecurity.app\nDTSTAMP:{now_local().strftime(fmt)}\nDTSTART:{ds.strftime(fmt)}\nDTEND:{de.strftime(fmt)}\nSUMMARY:🛡️ {j['title']}\nLOCATION:{l['name']+' - '+l['address'] if l else 'Unknown'}\nEND:VEVENT\nEND:VCALENDAR"
        return StreamingResponse(BytesIO(ics.encode()), media_type="text/calendar",
                                 headers={"Content-Disposition": f"attachment; filename=job_{job_id}.ics"})
    except Exception as e: raise HTTPException(500, str(e))

@app.get("/techs")
def list_techs(company: Optional[str] = None, user: dict = Depends(verify_google_token)):
    techs = get_state()["techs"]
    if company:
        techs = [t for t in techs if t.get("company", "security") == company]
    return {"techs": techs}

@app.post("/techs", status_code=201)
def create_tech(tech: TechIn, user: dict = Depends(require_admin)):
    state = get_state()
    colors = ['#7f1d1d','#3f3f46','#b91c1c','#52525b','#991b1b','#7c2d12','#292524']
    nt = {"id": f"t{len(state['techs'])+1}_{now_local().timestamp()}",
          "name": tech.name, "email": tech.email, "initials": tech.initials,
          "color": tech.color or colors[len(state["techs"]) % len(colors)],
          "skills": tech.skills or [], "company": tech.company or "security"}
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
    nl = {"id": f"l{len(state['locations'])+1}_{now_local().timestamp()}",
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
    key = f"{folder}/{now_local().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
    result = upload_bytes(data, key, file.content_type or "image/jpeg")
    if not result: raise HTTPException(500, "Upload failed")
    return {"key": result}

@app.post("/upload/signature")
async def upload_signature(file: UploadFile = File(...), user: dict = Depends(verify_google_token)):
    data = await file.read()
    key = f"signatures/{now_local().strftime('%Y%m%d_%H%M%S')}_sig.png"
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
    today = now_local().strftime("%Y-%m-%d"); count = 0
    try:
        s = smtp_connect(srv, port, sender, pwd)
        for t in state["techs"]:
            jobs = [j for j in state["jobs"] if j.get("techId")==t["id"] and j.get("status")!="Completed"]
            if not jobs: continue
            lines = "\n".join([f"- {j['title']} ({j['priority']})" for j in jobs])
            jobs_with_locs = [(j, _loc(j.get("locationId"))) for j in jobs]
            msg = MIMEMultipart("alternative")
            msg["From"]=sender; msg["To"]=t["email"]; msg["Subject"]=f"📅 Daily Reminder - {today}"
            msg.attach(MIMEText(f"Hello {t['name']},\n\nYour assignments for {today}:\n{lines}\n\nCheck the job board for details.", "plain"))
            msg.attach(MIMEText(build_reminder_html(t, jobs_with_locs, today), "html"))
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
