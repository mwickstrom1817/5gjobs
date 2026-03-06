import streamlit as st
import traceback

# --- DEBUG: trap reruns so we can see who is calling them ---
if st.session_state.get("_debug_trap_rerun", True):
    _real_rerun = st.rerun

    def _debug_rerun():
        stack = "".join(traceback.format_stack(limit=25))
        st.session_state["_last_rerun_stack"] = stack
        st.error("DEBUG: st.rerun() was called. Stack trace below:")
        st.code(stack)
        st.stop()

    st.rerun = _debug_rerun
import google.generativeai as genai
import datetime
import base64
import os
import json
import smtplib
import urllib.parse
import requests
import pandas as pd
import calendar
import numpy as np
import threading
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from PIL import Image
from io import BytesIO
from persistence_pg import (
    ensure_loaded_into_session,
    commit_from_session,
    force_overwrite_from_session,
    load_state,
)
from object_store import upload_streamlit_file, upload_bytes, get_view_url

import io
from reportlab.lib.utils import ImageReader

# Try importing ReportLab for PDF generation
try:
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    from reportlab.lib import colors
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False

# Try importing Streamlit Drawable Canvas for signatures
try:
    from streamlit_drawable_canvas import st_canvas
    HAS_CANVAS = True
except ImportError:
    HAS_CANVAS = False

# --- CONFIGURATION & STYLING ---
st.set_page_config(
    page_title="5G Security Job Board",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS to match the React App's Zinc/Red/Black theme
st.markdown("""
   <style>
   /* Main Background */
   .stApp {
       background-color: #09090b;
       color: #e4e4e7;
   }
   
   /* Inputs */
   .stTextInput > div > div > input, .stTextArea > div > div > textarea, .stSelectbox > div > div > div, .stNumberInput > div > div > input, .stMultiSelect > div > div > div {
       background-color: #000000;
       color: white;
       border-color: #27272a;
   }

   /* Time Input */
   input[type="time"] {
       background-color: #000000;
       color: white;
   }

   /* Sidebar */
   [data-testid="stSidebar"] {
       background-color: #18181b;
       border-right: 1px solid #27272a;
   }

   /* Buttons */
   .stButton > button {
       background-color: #b91c1c;
       color: white;
       border: none;
       border-radius: 8px;
       font-weight: bold;
   }
   .stButton > button:hover {
       background-color: #991b1b;
       color: white;
       border-color: #7f1d1d;
   }

   /* Custom Job Card Style */
   .job-card {
       background-color: #18181b;
       border: 1px solid #27272a;
       padding: 15px;
       border-radius: 10px;
       border-left: 5px solid #52525b;
       margin-bottom: 10px;
       transition: transform 0.2s;
   }
   .priority-Critical { border-left-color: #ef4444 !important; }
   .priority-High { border-left-color: #dc2626 !important; }
   .priority-Medium { border-left-color: #7f1d1d !important; }
   .priority-Low { border-left-color: #52525b !important; }

   /* Tabs */
   .stTabs [data-baseweb="tab-list"] {
       gap: 10px;
   }
   .stTabs [data-baseweb="tab"] {
       background-color: #18181b;
       border-radius: 4px;
       color: #a1a1aa;
   }
   .stTabs [aria-selected="true"] {
       background-color: #b91c1c !important;
       color: white !important;
   }
   
   /* Login Screen Container */
   .login-container {
       display: flex;
       justify-content: center;
       align-items: center;
       height: 70vh;
       text-align: center;
   }
   .login-box {
       background-color: #18181b;
       border: 1px solid #27272a;
       padding: 40px;
       border-radius: 12px;
       max-width: 400px;
       width: 100%;
       box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.5);
   }
   </style>
""", unsafe_allow_html=True)

# --- DB SESSION INITIALIZER (Neon/Postgres persistence) ---
def init_db_session():
    """Ensures st.session_state.db and st.session_state._db_version exist."""
    try:
        ensure_loaded_into_session()  # <-- add this line
        _sync_session_to_db()
        force_overwrite_from_session(invalidate_briefing=False)
        
    except Exception as e:
        # If DB is down, app can still run in a degraded mode
        print(f"DB init warning: {e}")

init_db_session()


# --- PERSISTENCE LAYER (Neon Postgres) ---
def load_data():
    """Loads data from Neon (app_state row)."""
    ensure_loaded_into_session()
    # make a copy to avoid accidental reference weirdness
    data = dict(st.session_state.db)
    return data

def _sync_session_to_db():
    """Push your convenience session_state fields into st.session_state.db before saving."""
    ensure_loaded_into_session()
    st.session_state.db["jobs"] = st.session_state.jobs
    st.session_state.db["techs"] = st.session_state.techs
    st.session_state.db["locations"] = st.session_state.locations
    st.session_state.db["briefing"] = st.session_state.briefing
    st.session_state.db["adminEmails"] = st.session_state.adminEmails
    st.session_state.db["last_reminder_date"] = st.session_state.get("last_reminder_date")

def save_state(invalidate_briefing=True):
    """Saves current session state to Neon (conflict-safe)."""
    if invalidate_briefing:
        st.session_state.briefing = "Data required to generate briefing."
    _sync_session_to_db()
    commit_from_session(invalidate_briefing=invalidate_briefing)
st.write("save_state is:", save_state.__code__.co_filename, save_state.__code__.co_firstlineno)
# --- SESSION STATE INITIALIZATION ---
if "jobs" not in st.session_state:
    db_data = load_data()
    st.session_state.jobs = db_data["jobs"]
    st.session_state.techs = db_data["techs"]
    st.session_state.locations = db_data["locations"]
    st.session_state.briefing = db_data["briefing"]
    st.session_state.adminEmails = db_data["adminEmails"]
    st.session_state.last_reminder_date = db_data.get("last_reminder_date")

if "chat_history" not in st.session_state:
    st.session_state.chat_history = [
        {
            "role": "model",
            "parts": ["Hello! I have access to your database. Ask me about active jobs, tech locations, or history."]
        }
    ]
# Tech Colors for UI
TECH_COLORS = ['#7f1d1d', '#3f3f46', '#b91c1c', '#52525b', '#991b1b', '#7c2d12', '#292524']

# --- AUTHENTICATION ---

def authenticate():
    """Handles Google OAuth2 Flow. Returns user_info dict if logged in, else None."""
    
    # 1. If already logged in, return user info
    if "user_info" in st.session_state:
        return st.session_state.user_info

    # 2. Setup OAuth Config
    # Retrieve from secrets or env
    client_id = st.secrets.get("GOOGLE_CLIENT_ID") or os.getenv("GOOGLE_CLIENT_ID")
    client_secret = st.secrets.get("GOOGLE_CLIENT_SECRET") or os.getenv("GOOGLE_CLIENT_SECRET")
    redirect_uri = st.secrets.get("GOOGLE_REDIRECT_URI") or os.getenv("GOOGLE_REDIRECT_URI")

    if not (client_id and client_secret and redirect_uri):
        st.error("🔒 Google OAuth is not configured. Please add `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, and `GOOGLE_REDIRECT_URI` to `.streamlit/secrets.toml`.")
        return None

    # 3. Check for Auth Code from Google Redirect
    # Compatibility with different Streamlit versions for query params
    code = None
    try:
        if "code" in st.query_params:
            code = st.query_params["code"]
    except:
        # Fallback for older Streamlit versions
        try:
            query_params = st.experimental_get_query_params()
            code = query_params.get("code", [None])[0]
        except:
            pass

if code:
    # Prevent infinite loops if the URL keeps the same code param
    if st.session_state.get("_oauth_last_code") == code:
        st.warning("OAuth code already processed; ignoring to prevent rerun loop.")
        code = None
    else:
        st.session_state["_oauth_last_code"] = code

if code:
    try:
        token_url = "https://oauth2.googleapis.com/token"
        data = {
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }
        r = requests.post(token_url, data=data)
        r.raise_for_status()
        tokens = r.json()
        access_token = tokens["access_token"]

        user_r = requests.get(
            "https://www.googleapis.com/oauth2/v1/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15,
        )
        user_r.raise_for_status()
        user_info = user_r.json()

        st.session_state.user_info = user_info

        # Clear query params (new + old API) without exploding
        try:
            st.query_params.clear()
        except Exception:
            try:
                st.experimental_set_query_params()
            except Exception:
                pass

        # Only rerun ONCE after successful login
        st.rerun()

    except Exception as e:
        st.error(f"Authentication Failed: {e}")
        # Important: do NOT keep rerunning with the same bad code
        # Clear query params so we can show the login button again
        try:
            st.query_params.clear()
        except Exception:
            try:
                st.experimental_set_query_params()
            except Exception:
                pass
            return None

    # 4. Show Login Button
    auth_url = "https://accounts.google.com/o/oauth2/v2/auth"
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "openid email profile",
        "access_type": "online", # Use online to avoid refresh token complexity unless needed
        "prompt": "select_account" # Force account selection to avoid auto-selecting wrong account
    }
    
    login_url = f"{auth_url}?{urllib.parse.urlencode(params)}"

    st.markdown(f"""
       <div class="login-container">
           <div class="login-box">
               <h1 style="color:white; margin-bottom: 10px;">5G Security Job Board</h1>
               <p style="color:#a1a1aa; margin-bottom: 30px;">Operational Dashboard</p>
               <a href="{login_url}" target="_top" rel="noopener noreferrer" style="
                   display: inline-block;
                   background-color: #DB4437; 
                   color: white; 
                   padding: 12px 24px; 
                   text-decoration: none; 
                   border-radius: 6px; 
                   font-weight: bold;
                   font-family: sans-serif;
                   box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
               ">
                   Sign in with Google
               </a>
               <p style="font-size: 0.8em; color: #52525b; margin-top: 20px;">
                   Ensure <code>{redirect_uri}</code> is added to <br/>
                   "Authorized redirect URIs" in Google Cloud Console.
               </p>
           </div>
       </div>
   """, unsafe_allow_html=True)
    
    return None

def logout():
    if "user_info" in st.session_state:
        del st.session_state.user_info
    st.rerun()

# --- HELPER FUNCTIONS ---

@st.cache_resource
class SystemLogger:




    def __init__(self):
        self.logs = []
        self.lock = threading.Lock()
        
    def log(self, message):
        with self.lock:
            ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.logs.insert(0, f"[{ts}] {message}")
            if len(self.logs) > 50:
                self.logs.pop()
    
    def get_logs(self):
        with self.lock:
            return list(self.logs)

def get_logger():
    return SystemLogger()

def keep_awake():
    """
    Background thread to keep the app active.
    Pings the server every 2 minutes to prevent idle timeouts.
    """
    def run():
        logger = get_logger()
        # Wait a bit for server to fully start
        time.sleep(10)
        
        # Primary endpoint that we know works
        primary_url = "http://localhost:8501/_stcore/health"
        
        while True:
            try:
                requests.get(primary_url, timeout=5)
                msg = f"Keep-awake ping successful to {primary_url}"
                # Only log to console periodically
                if datetime.datetime.now().minute % 10 == 0:
                    print(f"{datetime.datetime.now()}: {msg}")
                logger.log(msg)
            except Exception as e:
                # Fallback only if primary fails
                try:
                    fallback_url = "http://127.0.0.1:8501/_stcore/health"
                    requests.get(fallback_url, timeout=5)
                    msg = f"Primary ping failed, fallback successful to {fallback_url}"
                    logger.log(msg)
                except Exception:
                    msg = f"Keep-awake ping failed: {e}"
                    print(f"{datetime.datetime.now()}: {msg}")
                    logger.log(msg)
            
            # Wait for next ping
            time.sleep(120) 
            
    # Check if thread is already running to avoid duplicates on rerun
    # We use a new name to ensure we don't conflict with old zombie threads if any
    thread_name = "keep_awake_v2"
    
    # Check for old threads and log them (we can't kill them easily, but good to know)
    for t in threading.enumerate():
        if t.name == "keep_awake_thread":
            print(f"WARNING: Found zombie thread '{t.name}' still running.")
        if t.name == thread_name:
            return

    print(f"Starting {thread_name} background thread...")
    thread = threading.Thread(target=run, name=thread_name, daemon=True)
    thread.start()

def get_tech(tech_id):
    return next((t for t in st.session_state.techs if t['id'] == tech_id), None)

def get_location(loc_id):
    return next((l for l in st.session_state.locations if l['id'] == loc_id), None)

def resolve_image_source(photo_source: str):
    """
    Supports:
    - R2 object keys like 'photos/...', 'signatures/...'
    - legacy local paths (if any remain)
    """
    if not photo_source:
        return None

    # If it looks like an R2 key, turn into a signed URL
    if isinstance(photo_source, str) and (photo_source.startswith("photos/") or photo_source.startswith("signatures/")):
        return get_view_url(photo_source)

    # fallback: local paths or base64 (legacy)
    return photo_source


def save_image_locally(uploaded_file):
    """Uploads an uploaded file/camera input to R2 and returns the object key."""
    return upload_streamlit_file(uploaded_file, folder="photos")

def get_google_maps_url(address):
    """Generates a Google Maps Search URL based on address."""
    if not address: return None
    return f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote(address)}"

def get_api_key():
    # Try getting from Streamlit secrets, then Env, then return None
    if "GEMINI_API_KEY" in st.secrets:
        return st.secrets["GEMINI_API_KEY"]
    return os.getenv("API_KEY")

def get_available_model(api_key):
    """
    Dynamically lists models available to the API key and returns the best one.
    This prevents 404 errors by only selecting models that actually exist.
    """
    genai.configure(api_key=api_key)
    
    try:
        # Get all models available to this API Key
        all_models = list(genai.list_models())
        
        # Filter for models that support 'generateContent'
        valid_models = [m for m in all_models if 'generateContent' in m.supported_generation_methods]
        
        # Preference logic: Try to find latest Flash -> Pro -> generic Gemini
        
        # 1. Prefer 1.5 Flash
        best_model = next((m for m in valid_models if 'gemini-1.5-flash' in m.name), None)
        
        # 2. Prefer 2.0 Flash (Experimental)
        if not best_model:
            best_model = next((m for m in valid_models if 'gemini-2.0-flash' in m.name), None)
            
        # 3. Prefer 1.5 Pro
        if not best_model:
            best_model = next((m for m in valid_models if 'gemini-1.5-pro' in m.name), None)
            
        # 4. Fallback to generic 'gemini-pro' or 'gemini-1.0-pro'
        if not best_model:
            best_model = next((m for m in valid_models if 'gemini-pro' in m.name), None)
            
        # 5. Last resort: just take the first valid model
        if not best_model and valid_models:
            best_model = valid_models[0]
            
        if best_model:
            return genai.GenerativeModel(best_model.name)
            
        return genai.GenerativeModel('gemini-pro')

    except Exception as e:
        # If list_models fails (e.g. API key doesn't have list permission), fallback
        return genai.GenerativeModel('gemini-pro')

def generate_technician_summary(notes, job_title):
    """Uses Gemini to summarize the daily work for the PDF Report."""
    api_key = get_api_key()
    if not api_key: return None
    model = get_available_model(api_key)
    prompt = f"Summarize the following technician notes for job '{job_title}' into a concise, professional paragraph (approx 50 words) suitable for a client report:\n\n{notes}"
    try:
        response = model.generate_content(prompt)
        return response.text
    except:
        return None

def create_mailto_link(job, tech, location):
    """Generates a mailto link for client-side email sending."""
    subject = f"Assignment: {job['title']}"
    body = f"""Hello {tech['name']},

New Assignment:
{job['title']} ({job['priority']})

Location:
{location['name']}
{location['address']}

Details:
{job['description']}
"""
    # Use quote_via=quote to ensure spaces are encoded correctly for mail clients
    qs = urllib.parse.urlencode({'subject': subject, 'body': body}, quote_via=urllib.parse.quote)
    return f"mailto:{tech['email']}?{qs}"

def suggest_address_with_gemini(partial_address):
    """Uses Gemini to autocomplete/validate an address."""
    api_key = get_api_key()
    if not api_key: return partial_address
    model = get_available_model(api_key)
    prompt = f"You are an address autocomplete tool. The user typed: '{partial_address}'. Return the most likely full address. If ambiguous, return the best guess. Return ONLY the address text, no other words."
    try:
        response = model.generate_content(prompt)
        return response.text.strip()
    except:
        return partial_address

def download_data_as_csv():
    # Convert jobs to CSV
    if st.session_state.jobs:
        df = pd.DataFrame(st.session_state.jobs)
        return df.to_csv(index=False).encode('utf-8')
    return None

def download_data_as_json():
    # Dump current state to JSON
    data = {
        "jobs": st.session_state.jobs,
        "techs": st.session_state.techs,
        "locations": st.session_state.locations,
        "briefing": st.session_state.briefing,
        "adminEmails": st.session_state.adminEmails,
        "last_reminder_date": st.session_state.get("last_reminder_date")
    }
    return json.dumps(data, indent=2)

# --- PDF GENERATION ---
def generate_job_pdf(job, tech, location, report):
    """Generates a PDF bytes object for the completed job."""
    if not HAS_REPORTLAB:
        return None

    buffer = BytesIO()
    p = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter

    # Title
    p.setFillColor(colors.darkred)
    p.setFont("Helvetica-Bold", 20)
    p.drawString(50, height - 50, "5G Security - Job Completion Report")

    p.setFillColor(colors.black)
    p.setFont("Helvetica", 10)
    p.drawString(50, height - 65, f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")

    # Job Info
    y = height - 100
    p.setFont("Helvetica-Bold", 12)
    p.drawString(50, y, "JOB DETAILS")
    p.line(50, y - 5, width - 50, y - 5)

    y -= 25
    p.setFont("Helvetica", 11)
    p.drawString(50, y, f"Title: {job['title']}")
    y -= 15
    p.drawString(50, y, f"Location: {location['name'] if location else 'Unknown'} - {location['address'] if location else ''}")
    y -= 15
    p.drawString(50, y, f"Tech Assigned: {tech['name'] if tech else 'Unassigned'}")
    y -= 15
    p.drawString(50, y, f"Status: {job['status']}")

    # Report Data
    y -= 40
    p.setFont("Helvetica-Bold", 12)
    p.drawString(50, y, "FIELD REPORT DATA")
    p.line(50, y - 5, width - 50, y - 5)

    y -= 25
    p.setFont("Helvetica", 11)

    data_points = [
        f"Techs On Site: {report.get('techsOnSite', 'N/A')}",
        f"Time Arrived: {report.get('timeArrived', 'N/A')}",
        f"Time Finished: {report.get('timeDeparted', 'N/A')}",
        f"Hours Worked: {report.get('hoursWorked', 'N/A')}",
        f"Parts Used: {report.get('partsUsed', 'N/A')}",
        f"Billable Items: {report.get('billableItems', 'N/A')}",
    ]

    for point in data_points:
        p.drawString(50, y, point)
        y -= 20

    # AI Summary
    ai_summary = report.get("ai_summary")
    if ai_summary:
        y -= 20
        p.setFont("Helvetica-Bold", 12)
        p.drawString(50, y, "WORK SUMMARY (AI Generated)")
        p.line(50, y - 5, width - 50, y - 5)
        y -= 20
        p.setFont("Helvetica-Oblique", 10)

        text_object = p.beginText(50, y)
        max_width = 80
        words = ai_summary.split()
        current_line = []
        line_count = 0

        for word in words:
            current_line.append(word)
            if len(" ".join(current_line)) > max_width:
                text_object.textLine(" ".join(current_line[:-1]))
                current_line = [word]
                line_count += 1

        if current_line:
            text_object.textLine(" ".join(current_line))
            line_count += 1

        p.drawText(text_object)
        y -= (line_count * 14) + 10

    # Completion Checklist
    checklist = report.get("completion_checklist")
    if checklist:
        y -= 20
        p.setFont("Helvetica-Bold", 12)
        p.drawString(50, y, "COMPLETION CHECKLIST")
        p.line(50, y - 5, width - 50, y - 5)
        y -= 20
        p.setFont("Helvetica", 10)

        for item in checklist:
            p.drawString(50, y, f"[x] {item}")
            y -= 15

    # Content/Notes
    y -= 20
    p.setFont("Helvetica-Bold", 12)
    p.drawString(50, y, "TECHNICIAN NOTES")
    p.line(50, y - 5, width - 50, y - 5)
    y -= 25
    p.setFont("Helvetica", 10)

    notes = report.get("content", "")
    text_object = p.beginText(50, y)
    text_object.setFont("Helvetica", 10)

    max_width = 80
    words = notes.split()
    current_line = []

    for word in words:
        current_line.append(word)
        if len(" ".join(current_line)) > max_width:
            text_object.textLine(" ".join(current_line))
            current_line = []

    if current_line:
        text_object.textLine(" ".join(current_line))

    p.drawText(text_object)

    # Signature
    signature_key = report.get("signature_key")
    if signature_key:
        try:
            sig_url = get_view_url(signature_key, expires_seconds=3600)
            sig_bytes = requests.get(sig_url, timeout=10).content
            sig_reader = ImageReader(BytesIO(sig_bytes))

            y -= 60
            p.drawImage(sig_reader, 50, y, width=150, height=50, mask="auto")
            p.setFont("Helvetica-Oblique", 8)
            p.drawString(50, y - 10, "Customer Digital Signature")
            y -= 20
        except Exception as e:
            print(f"Error adding signature to PDF: {e}")

    # Photos Section
    photos = report.get("photos", [])
    if photos:
        p.showPage()

        y = height - 50
        p.setFont("Helvetica-Bold", 14)
        p.drawString(50, y, "SITE PHOTOS")
        y -= 30

        x_start = 50
        img_width = 250
        img_height = 200
        gap_x = 20
        gap_y = 20

        col = 0

        for photo_key in photos:
            try:
                photo_url = get_view_url(photo_key, expires_seconds=3600)
                img_bytes = requests.get(photo_url, timeout=15).content
                img_reader = ImageReader(BytesIO(img_bytes))

                if y - img_height < 50:
                    p.showPage()
                    y = height - 50
                    p.setFont("Helvetica-Bold", 14)
                    p.drawString(50, y, "SITE PHOTOS (Cont.)")
                    y -= 30
                    col = 0

                x = x_start + (col * (img_width + gap_x))
                draw_y = y - img_height

                p.drawImage(
                    img_reader,
                    x,
                    draw_y,
                    width=img_width,
                    height=img_height,
                    preserveAspectRatio=True,
                    anchor="c",
                )

                col += 1
                if col > 1:
                    col = 0
                    y -= (img_height + gap_y)

            except Exception as e:
                print(f"Error adding remote photo to PDF: {e}")

    p.showPage()
    p.save()

    buffer.seek(0)
    return buffer.getvalue()

def send_assignment_email(job, tech, location):
    """Sends an email notification via SMTP, returning True if successful."""
    # Helper to resolve config priority: Session > Secrets > Env
    def get_config_val(key, default=None):
        if 'smtp_settings' in st.session_state and st.session_state.smtp_settings.get(key):
            return st.session_state.smtp_settings[key]
        if key in st.secrets:
            return st.secrets[key]
        return os.getenv(key) or default

    smtp_server = get_config_val("SMTP_SERVER")
    smtp_port = get_config_val("SMTP_PORT", 587)
    sender_email = get_config_val("SMTP_EMAIL")
    sender_password = get_config_val("SMTP_PASSWORD")

    # Prepare email content
    subject = f"New Job Assignment: {job['title']}"
    body = f"""
   Hello {tech['name']},

   You have been assigned a new job task.

   JOB DETAILS
   --------------------------------------------------
   Title:    {job['title']}
   Priority: {job['priority']}
   Type:     {job['type']}
   
   LOCATION
   --------------------------------------------------
   Name:    {location['name']}
   Address: {location['address']}

   DESCRIPTION
   --------------------------------------------------
   {job['description']}

   Please check the 5G Security Job Board for full details.
   """

    # If no credentials, we return False to trigger fallback UI
    if not (smtp_server and sender_email and sender_password):
        print(f"SMTP not configured. Skipping auto-email for: {tech['email']}")
        return False

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = tech['email']
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        if int(smtp_port) == 465:
            server = smtplib.SMTP_SSL(smtp_server, int(smtp_port))
            server.ehlo()
        else:
            server = smtplib.SMTP(smtp_server, int(smtp_port))
            server.ehlo()
            server.starttls()
            server.ehlo()

        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        st.toast(f"📧 Email successfully sent to {tech['name']}", icon="✅")
        return True
    except Exception as e:
        st.error(f"Failed to send email: {str(e)}")
        print(f"Email Error: {str(e)}")
        return False

def send_completion_email(job, tech, location, report_data):
    """Sends an email notification to Admins when a job is completed, with PDF attachment."""
    # Helper to resolve config priority: Session > Secrets > Env
    def get_config_val(key, default=None):
        if 'smtp_settings' in st.session_state and st.session_state.smtp_settings.get(key):
            return st.session_state.smtp_settings[key]
        if key in st.secrets:
            return st.secrets[key]
        return os.getenv(key) or default

    smtp_server = get_config_val("SMTP_SERVER")
    smtp_port = get_config_val("SMTP_PORT", 587)
    sender_email = get_config_val("SMTP_EMAIL")
    sender_password = get_config_val("SMTP_PASSWORD")
    
    # Get Admin Emails
    recipients = st.session_state.adminEmails
    if not recipients:
        return

    # Generate PDF
    pdf_bytes = generate_job_pdf(job, tech, location, report_data)

    # Prepare email content
    subject = f"✅ Job Completed: {job['title']}"
    body = f"""
   JOB COMPLETED NOTIFICATION
   
   Job:      {job['title']}
   Tech:     {tech['name'] if tech else 'Unknown'}
   Location: {location['name'] if location else 'Unknown'}
   
   The job has been marked as Completed.
   Please see the attached PDF report for full details.
   """

    if not (smtp_server and sender_email and sender_password):
        print("SMTP not configured. Skipping admin completion email.")
        return

    try:
        if int(smtp_port) == 465:
            server = smtplib.SMTP_SSL(smtp_server, int(smtp_port))
            server.ehlo()
        else:
            server = smtplib.SMTP(smtp_server, int(smtp_port))
            server.ehlo()
            server.starttls()
            server.ehlo()

        server.login(sender_email, sender_password)
        
        for recipient in recipients:
            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['To'] = recipient
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            
            if pdf_bytes:
                attachment = MIMEApplication(pdf_bytes, _subtype="pdf")
                attachment.add_header('Content-Disposition', 'attachment', filename=f"Report_{job['id']}.pdf")
                msg.attach(attachment)

            server.send_message(msg)
            
        server.quit()
        st.toast("📧 Completion notification sent to Admins", icon="✅")
    except Exception as e:
        print(f"Email Error: {str(e)}")
        st.error(f"Failed to send email: {str(e)}")

def send_daily_report_email(job, tech, location, report_data):
    """Sends a Daily Report email to Admins with PDF attachment."""
    # Helper to resolve config priority: Session > Secrets > Env
    def get_config_val(key, default=None):
        if 'smtp_settings' in st.session_state and st.session_state.smtp_settings.get(key):
            return st.session_state.smtp_settings[key]
        if key in st.secrets:
            return st.secrets[key]
        return os.getenv(key) or default

    smtp_server = get_config_val("SMTP_SERVER")
    smtp_port = get_config_val("SMTP_PORT", 587)
    sender_email = get_config_val("SMTP_EMAIL")
    sender_password = get_config_val("SMTP_PASSWORD")
    
    # Get Admin Emails
    recipients = st.session_state.adminEmails
    if not recipients:
        st.warning("No admin emails configured.")
        return

    # Generate PDF
    pdf_bytes = generate_job_pdf(job, tech, location, report_data)

    # Prepare email content
    subject = f"📝 Daily Report: {job['title']}"
    body = f"""
   DAILY FIELD REPORT
   
   Job:      {job['title']}
   Tech:     {tech['name'] if tech else 'Unknown'}
   Location: {location['name'] if location else 'Unknown'}
   Date:     {datetime.datetime.now().strftime('%Y-%m-%d')}
   
   Please see the attached PDF report for today's details.
   """

    if not (smtp_server and sender_email and sender_password):
        st.error("SMTP not configured.")
        return

    try:
        if int(smtp_port) == 465:
            server = smtplib.SMTP_SSL(smtp_server, int(smtp_port))
            server.ehlo()
        else:
            server = smtplib.SMTP(smtp_server, int(smtp_port))
            server.ehlo()
            server.starttls()
            server.ehlo()

        server.login(sender_email, sender_password)
        
        for recipient in recipients:
            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['To'] = recipient
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            
            if pdf_bytes:
                attachment = MIMEApplication(pdf_bytes, _subtype="pdf")
                attachment.add_header('Content-Disposition', 'attachment', filename=f"DailyReport_{job['id']}_{datetime.datetime.now().strftime('%Y%m%d')}.pdf")
                msg.attach(attachment)

            server.send_message(msg)
            
        server.quit()
        st.toast("📧 Daily Report sent to Admins", icon="✅")
    except Exception as e:
        print(f"Email Error: {str(e)}")
        st.error(f"Failed to send email: {str(e)}")

def send_daily_reminders():
    """Sends daily reminder emails to techs with active assignments (Mon-Fri only)."""
    
    # 1. Check Date & Time
    now = datetime.datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    weekday = now.weekday() # 0=Mon, 4=Fri, 5=Sat, 6=Sun
    
    # Only run Mon-Fri (0-4)
    if weekday > 4:
        return

    # Check if already ran today
    if st.session_state.get("last_reminder_date") == today_str:
        return

    # 2. Get SMTP Config
    def get_config_val(key, default=None):
        if 'smtp_settings' in st.session_state and st.session_state.smtp_settings.get(key):
            return st.session_state.smtp_settings[key]
        if key in st.secrets:
            return st.secrets[key]
        return os.getenv(key) or default

    smtp_server = get_config_val("SMTP_SERVER")
    smtp_port = get_config_val("SMTP_PORT", 587)
    sender_email = get_config_val("SMTP_EMAIL")
    sender_password = get_config_val("SMTP_PASSWORD")

    if not (smtp_server and sender_email and sender_password):
        return # Cannot send email

    # 3. Iterate Techs
    techs_emailed = 0
    try:
        # Connect once
        if int(smtp_port) == 465:
            server = smtplib.SMTP_SSL(smtp_server, int(smtp_port))
            server.ehlo()
        else:
            server = smtplib.SMTP(smtp_server, int(smtp_port))
            server.ehlo()
            server.starttls()
            server.ehlo()
        
        server.login(sender_email, sender_password)

        for tech in st.session_state.techs:
            # Find active jobs for this tech
            active_jobs = [j for j in st.session_state.jobs 
                           if j['techId'] == tech['id'] and j['status'] != 'Completed']
            
            if not active_jobs:
                continue

            # Compose Email
            subject = f"📅 Daily Assignment Reminder - {today_str}"
            
            job_list_text = ""
            for job in active_jobs:
                loc = get_location(job['locationId'])
                loc_name = loc['name'] if loc else "Unknown Location"
                loc_addr = loc['address'] if loc else ""
                
                job_list_text += f"""
- {job['title']} ({job['priority']})
  Location: {loc_name} - {loc_addr}
  Status: {job['status']}
"""

            body = f"""Hello {tech['name']},

Here is your summary of active assignments for today ({today_str}):
{job_list_text}

Please check the 5G Security Job Board for full details and to log your work.
"""
            
            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['To'] = tech['email']
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            
            server.send_message(msg)
            techs_emailed += 1
            
        server.quit()
        
        # Update State
        st.session_state.last_reminder_date = today_str
        save_state(invalidate_briefing=False)
        
        if techs_emailed > 0:
            st.toast(f"📧 Sent daily reminders to {techs_emailed} technicians.", icon="✅")
            
    except Exception as e:
        print(f"Daily Reminder Error: {e}")

def generate_morning_briefing():
    """Generates the morning briefing using Gemini."""
    api_key = get_api_key()
    if not api_key:
        return "⚠️ API Key missing. Please set GEMINI_API_KEY in secrets.toml or environment."
    
    if not st.session_state.jobs:
        return "No active jobs to analyze. Please add jobs via the 'New Job' button."

    # Use dynamic model selector
    model = get_available_model(api_key)

    active_jobs = [j for j in st.session_state.jobs if j['status'] != 'Completed']
    critical_jobs = [j for j in active_jobs if j['priority'] in ['Critical', 'High']]
    
    current_date = datetime.datetime.now().strftime("%B %d, %Y")

    prompt = f"""
      You are the Operations Manager for 5G Security. Generate a concise "Morning Briefing" for the dashboard.
      5G Security is a company that specializes in cameras and NVR systems, access control, alarm systems, and infrastructure cabling. We dont do work on 5G Towers.
     
     Today's Date: {current_date}

     Data:
     - Active Jobs: {len(active_jobs)}
     - Critical: {len(critical_jobs)}
     - Techs: {', '.join([t['name'] for t in st.session_state.techs])}
     
     Active Job List:
     {chr(10).join([f"- {j['title']} ({j['priority']})" for j in active_jobs])}

     Format: 
     Start with the header: **Morning Briefing: 5G Security - {current_date}**

     Then:
     1. Security Focus (Motivation)
     2. Critical Focus (Briefly summarize the active jobs list, highlighting critical ones if any)
     3. Safety Tip.

     Max 150 words. No markdown headers (#), use Bold instead.
   """
    
    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"Error generating briefing: {str(e)}"

# --- DIALOGS (MODALS) ---

@st.dialog("Create New Job")
def add_job_dialog():
    if not st.session_state.locations:
        st.error("Please create a Location in the Admin tab first.")
        if st.button("Close"): st.rerun()
        return

    with st.form("new_job_form"):
        title = st.text_input("Job Title")
        desc = st.text_area("Description")
        
        c1, c2 = st.columns(2)
        job_type = c1.selectbox("Type", ["Service", "Project"])
        priority = c2.selectbox("Priority", ["Medium", "Low", "High", "Critical"])
        
        # Date Selection
        job_date = st.date_input("Scheduled Date", value=datetime.datetime.now())
        
        # Location Selection
        loc_map = {l['name']: l['id'] for l in st.session_state.locations}
        loc_name = st.selectbox("Location", list(loc_map.keys()))
        
        # Tech Selection
        tech_map = {t['name']: t['id'] for t in st.session_state.techs}
        tech_map["Unassigned"] = None
        tech_name = st.selectbox("Assign Tech", list(tech_map.keys()))

        submitted = st.form_submit_button("Save Job")
        if submitted and title:
            # Combine date with current time for ISO format
            full_date = datetime.datetime.combine(job_date, datetime.datetime.now().time())
            
            new_job = {
                'id': f"j{len(st.session_state.jobs) + 100}_{datetime.datetime.now().timestamp()}",
                'title': title,
                'description': desc,
                'type': job_type,
                'priority': priority,
                'status': 'Pending',
                'locationId': loc_map[loc_name],
                'techId': tech_map[tech_name],
                'date': full_date.isoformat(),
                'reports': []
            }
            st.session_state.jobs.insert(0, new_job)
            
            # Send Email Notification
            email_status_msg = ""
            selected_tech_id = tech_map[tech_name]
            
            if selected_tech_id:
                tech = get_tech(selected_tech_id)
                loc = get_location(loc_map[loc_name])
                if tech and loc:
                    success = send_assignment_email(new_job, tech, loc)
                    if not success:
                        email_status_msg = "SMTP not configured. Use the 'Email' button in Job Details to notify manually."

            # Invalidate briefing so it regenerates with new data
            st.session_state.briefing = "Data required to generate briefing."
            save_state()  # Save changes
            
            if email_status_msg:
                st.toast(email_status_msg, icon="ℹ️")
            else:
                st.toast("Job created successfully!", icon="✅")
                
            st.rerun()

@st.dialog("Edit Job Details")
def edit_job_dialog(job_id):
    # Find job directly from session state
    job_index = next((i for i, j in enumerate(st.session_state.jobs) if j['id'] == job_id), -1)
    if job_index == -1:
        st.error("Job not found")
        return
    
    job = st.session_state.jobs[job_index]

    with st.form(key=f"edit_job_form_{job_id}"):
        title = st.text_input("Job Title", value=job['title'])
        desc = st.text_area("Description", value=job['description'])
        
        c1, c2 = st.columns(2)
        
        # Type
        type_opts = ["Service", "Project"]
        curr_type_idx = 0
        if job['type'] in type_opts:
            curr_type_idx = type_opts.index(job['type'])
        job_type = c1.selectbox("Type", type_opts, index=curr_type_idx)
        
        # Priority
        prio_opts = ["Medium", "Low", "High", "Critical"]
        curr_prio_idx = 0
        if job['priority'] in prio_opts:
            curr_prio_idx = prio_opts.index(job['priority'])
        priority = c2.selectbox("Priority", prio_opts, index=curr_prio_idx)
        
        # Date Selection
        try:
            # Handle both full ISO strings and YYYY-MM-DD
            if 'T' in job['date']:
                existing_dt = datetime.datetime.fromisoformat(job['date'])
                existing_date = existing_dt.date()
                existing_time = existing_dt.time()
            else:
                existing_dt = datetime.datetime.strptime(job['date'][:10], "%Y-%m-%d")
                existing_date = existing_dt.date()
                existing_time = datetime.datetime.now().time()
        except:
            existing_date = datetime.datetime.now().date()
            existing_time = datetime.datetime.now().time()
            
        job_date = st.date_input("Scheduled Date", value=existing_date)
        
        # Location Selection
        loc_map = {l['name']: l['id'] for l in st.session_state.locations}
        loc_options = list(loc_map.keys())
        
        current_loc_id = job.get('locationId')
        current_loc_name = next((k for k, v in loc_map.items() if v == current_loc_id), None)
        
        loc_index = 0
        if current_loc_name and current_loc_name in loc_options:
            loc_index = loc_options.index(current_loc_name)
            
        if loc_options:
            loc_name = st.selectbox("Location", loc_options, index=loc_index)
        else:
            st.warning("No locations found.")
            loc_name = None
        
        # Tech Selection
        tech_map = {t['name']: t['id'] for t in st.session_state.techs}
        tech_map["Unassigned"] = None
        tech_options = list(tech_map.keys())
        
        current_tech_id = job.get('techId')
        current_tech_name = next((k for k, v in tech_map.items() if v == current_tech_id), "Unassigned")
        
        tech_index = 0
        if current_tech_name in tech_options:
            tech_index = tech_options.index(current_tech_name)
            
        tech_name = st.selectbox("Assign Tech", tech_options, index=tech_index)

        if st.form_submit_button("Update Job"):
            if title:
                st.session_state.jobs[job_index]['title'] = title
                st.session_state.jobs[job_index]['description'] = desc
                st.session_state.jobs[job_index]['type'] = job_type
                st.session_state.jobs[job_index]['priority'] = priority
                
                # Update Date (preserve time if possible, or use current time)
                full_date = datetime.datetime.combine(job_date, existing_time)
                st.session_state.jobs[job_index]['date'] = full_date.isoformat()
                
                if loc_name:
                    st.session_state.jobs[job_index]['locationId'] = loc_map[loc_name]
                
                st.session_state.jobs[job_index]['techId'] = tech_map[tech_name]
                
                # Invalidate briefing so it regenerates with new data
                st.session_state.briefing = "Data required to generate briefing."
                save_state()  # Save changes
                
                st.toast("Job updated successfully!", icon="✅")
                st.rerun()
            else:
                st.error("Title is required.")

def render_completion_confirmation(job_index, report_payload):
    job = st.session_state.jobs[job_index]
    st.write(f"**Job:** {job['title']}")
    st.warning("You are marking this job as **Completed**. This will archive the job and notify admins.")

    st.write("#### ✅ Completion Checklist")

    c1 = st.checkbox("🧹 Messes Cleaned")
    c2 = st.checkbox("🧱 Tiles Replaced")
    c3 = st.checkbox("🗑️ Trash Taken Out")

    st.write("#### ✍️ Customer Signature")
    signature_data = None

    if HAS_CANVAS:
        canvas_result = st_canvas(
            fill_color="rgba(255, 165, 0, 0.3)",
            stroke_width=2,
            stroke_color="#000000",
            background_color="#ffffff",
            update_streamlit=True,
            height=150,
            drawing_mode="freedraw",
            key=f"sig_canvas_{job['id']}",
        )

        if canvas_result.image_data is not None:
            signature_data = canvas_result.image_data
    else:
        st.warning("Signature pad not available (library missing). Please type name below.")
        signed_name = st.text_input("Customer Name (Signed)")

    st.write("#### 📝 Final Notes")
    final_note = st.text_area("Add any final closing notes (optional):")

    c_confirm, c_cancel = st.columns(2)

    if c_confirm.button("Confirm & Close Job", type="primary"):
        checklist = []
        if c1:
            checklist.append("Messes Cleaned")
        if c2:
            checklist.append("Tiles Replaced")
        if c3:
            checklist.append("Trash Taken Out")

        # Handle Signature (R2)
        if HAS_CANVAS and signature_data is not None:
            if signature_data.sum() > 0:
                try:
                    img = Image.fromarray(signature_data.astype("uint8"), "RGBA")
                    buf = io.BytesIO()
                    img.save(buf, format="PNG")
                    buf.seek(0)

                    sig_key = f"signatures/{job['id']}_{datetime.datetime.utcnow().timestamp()}.png"
                    upload_bytes(buf.getvalue(), sig_key, content_type="image/png")

                    report_payload["signature_key"] = sig_key
                    checklist.append("Customer Signed (Digital)")
                except Exception as e:
                    st.error(f"Error uploading signature: {e}")
        elif not HAS_CANVAS and "signed_name" in locals() and signed_name:
            checklist.append(f"Customer Signed: {signed_name}")

        report_payload["completion_checklist"] = checklist

        if final_note:
            report_payload["content"] += f"\n\n[Closing Note]: {final_note}"

        if report_payload.get("content"):
            with st.spinner("Generating AI Summary..."):
                summary = generate_technician_summary(report_payload["content"], job["title"])
                if summary:
                    report_payload["ai_summary"] = summary

        st.session_state.jobs[job_index]["reports"].append(report_payload)
        st.session_state.jobs[job_index]["status"] = "Completed"
        st.session_state.briefing = "Data required to generate briefing."

        tech = get_tech(job["techId"])
        loc = get_location(job["locationId"])
        send_completion_email(job, tech, loc, report_payload)

        save_state()

        if f"completion_pending_{job['id']}" in st.session_state:
            del st.session_state[f"completion_pending_{job['id']}"]

        st.success("Job Completed & Closed!")
        st.rerun()

    if c_cancel.button("Cancel"):
        if f"completion_pending_{job['id']}" in st.session_state:
            del st.session_state[f"completion_pending_{job['id']}"]
        st.rerun()
        
@st.dialog("Job Details & Report", width="large")
def job_details_dialog(job_id):
    # Find job directly from session state
    job_index = next((i for i, j in enumerate(st.session_state.jobs) if j['id'] == job_id), -1)
    if job_index == -1:
        st.error("Job not found")
        return
    
    # Check for pending completion confirmation
    pending_key = f"completion_pending_{job_id}"
    if pending_key in st.session_state:
        render_completion_confirmation(job_index, st.session_state[pending_key])
        return

    job = st.session_state.jobs[job_index]
    loc = get_location(job['locationId'])
    tech = get_tech(job['techId'])

    # Header
    c1, c2 = st.columns([3, 1])
    with c1:
        st.subheader(f"{job['title']}")
        
        # Map Link Logic
        if loc:
            map_url = loc.get('mapsUrl') or get_google_maps_url(loc['address'])
            if map_url:
                st.markdown(f"📍 **[{loc['name']}]({map_url})**")
            else:
                st.markdown(f"📍 **{loc['name']}**")
            st.caption(loc['address'])
        else:
             st.caption(f"📍 Unknown | 👤 {tech['name'] if tech else 'Unassigned'}")
        
        # MAILTO LINK BUTTON: Provides manual alternative if SMTP is missing
        if tech and loc:
            mailto_url = create_mailto_link(job, tech, loc)
            st.link_button("📧 Email Assignment to Tech", mailto_url)

    with c2:
        st.markdown("**Current Status:**")
        status_color = {
            "Pending": "gray",
            "In Progress": "orange", 
            "Completed": "green"
        }.get(job['status'], "gray")
        st.markdown(f":{status_color}-background[{job['status']}]")

    tab_history, tab_progress, tab_daily, tab_creds = st.tabs(["📋 Details & History", "📸 In-Progress", "📝 Daily Report", "🔐 IPs & Passwords"])

    with tab_creds:
        st.write("#### 🔐 Site Credentials & Network Info")
        st.caption("Securely store logins and IP addresses for this job.")
        
        # Ensure 'credentials' key exists
        if 'credentials' not in job:
            job['credentials'] = {}
            
        creds = job['credentials']
        
        with st.form(key=f"creds_form_{job_id}"):
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Windows Login**")
                win_user = st.text_input("Username", value=creds.get('windows_user', ''), key=f"win_u_{job_id}")
                win_pass = st.text_input("Password", value=creds.get('windows_pass', ''), key=f"win_p_{job_id}")
                
                st.markdown("**ICT Login**")
                ict_user = st.text_input("Username", value=creds.get('ict_user', ''), key=f"ict_u_{job_id}")
                ict_pass = st.text_input("Password", value=creds.get('ict_pass', ''), key=f"ict_p_{job_id}")
            
            with c2:
                st.markdown("**DW Spectrum Login**")
                dw_user = st.text_input("Username", value=creds.get('dw_user', ''), key=f"dw_u_{job_id}")
                dw_pass = st.text_input("Password", value=creds.get('dw_pass', ''), key=f"dw_p_{job_id}")
                
                st.markdown("**Network / IPs**")
                ips = st.text_area("IP Addresses & Notes", value=creds.get('ips', ''), height=145, key=f"ips_{job_id}")

            if st.form_submit_button("Save Credentials"):
                st.session_state.jobs[job_index]['credentials'] = {
                    'windows_user': win_user,
                    'windows_pass': win_pass,
                    'dw_user': dw_user,
                    'dw_pass': dw_pass,
                    'ict_user': ict_user,
                    'ict_pass': ict_pass,
                    'ips': ips
                }
                save_state(invalidate_briefing=False)
                st.success("Credentials saved!")
                st.rerun()

    with tab_history:
        st.markdown(f"**Description:** {job['description']}")
        st.divider()
        st.write("#### 📜 History")
        if not job['reports']:
            st.info("No reports filed yet.")
        for r in reversed(job['reports']):
            r_tech = get_tech(r['techId'])
            with st.container(border=True):
                st.markdown(f"**{r_tech['name'] if r_tech else 'Unknown'}** - {r['timestamp'][:16]}")
                
                # Check if it's a "Daily Report" (has hours/techs) or "In-Progress" (just content/photos)
                is_daily_report = r.get('hoursWorked') or r.get('techsOnSite')
                
                if is_daily_report:
                    h1, h2, h3 = st.columns(3)
                    h1.caption(f"🕒 Hours: {r.get('hoursWorked')}")
                    h2.caption(f"⏰ In: {r.get('timeArrived')}")
                    h3.caption(f"⏰ Out: {r.get('timeDeparted')}")
                
                if r.get('content'):
                    st.write(r['content'])
                    
                if r.get('partsUsed'):
                    st.caption(f"🔩 Parts: {r['partsUsed']}")

                if r['photos']:
                    cols = st.columns(4)
                    for i, photo_source in enumerate(r['photos']):
                        with cols[i % 4]:
                            # st.image handles both Base64 and File Paths automatically
                            st.image(resolve_image_source(photo_source), use_container_width=True)

    with tab_progress:
        st.write("#### 📸 Quick Update")
        st.caption("Add photos and notes while working. These save to history immediately.")
        
        with st.form(key=f"progress_form_{job_id}"):
            prog_note = st.text_area("Note", placeholder="Quick update (e.g. 'Arrived on site', 'Found the issue')...")
            
            st.write("**Attach Photos**")
            c_cam, c_upl = st.columns(2)
            with c_cam:
                cam_pic = st.camera_input("Take Photo")
            with c_upl:
                upl_pics = st.file_uploader("Upload Images", accept_multiple_files=True, type=['png', 'jpg'])
                
            if st.form_submit_button("Post Update"):
                photos_list = []
                if cam_pic:
                    path = save_image_locally(cam_pic)
                    if path: photos_list.append(path)
                if upl_pics:
                    for up_file in upl_pics:
                        path = save_image_locally(up_file)
                        if path: photos_list.append(path)
                
                if prog_note or photos_list:
                    # Construct Simple Report Data
                    report_payload = {
                        'id': f"r{datetime.datetime.now().timestamp()}",
                        'techId': job['techId'] or 'unknown',
                        'timestamp': datetime.datetime.now().isoformat(),
                        'content': prog_note,
                        'photos': photos_list,
                        # Empty structured fields
                        'techsOnSite': "", 'timeArrived': "", 'timeDeparted': "", 
                        'hoursWorked': "", 'partsUsed': "", 'billableItems': ""
                    }
                    st.session_state.jobs[job_index]['reports'].append(report_payload)
                    
                    # Auto-set status to In Progress if Pending
                    if job['status'] == 'Pending':
                        st.session_state.jobs[job_index]['status'] = 'In Progress'
                        st.session_state.briefing = "Data required to generate briefing."
                    
                    save_state()
                    st.success("Update Posted!")
                    st.rerun()
                else:
                    st.warning("Please add a note or photo.")

    with tab_daily:
        # Check for confirmation state for emailing report
        confirm_key = f"confirm_daily_send_{job['id']}"
        if confirm_key in st.session_state:
            payload = st.session_state[confirm_key]
            
            st.warning("⚠️ **Review & Confirm Daily Report**")
            st.info("Please double-check your times, photos, and notes below before sending to Admins.")
            
            with st.container(border=True):
                st.markdown(f"**Time:** {payload['timeArrived']} - {payload['timeDeparted']} ({payload['hoursWorked']} hrs)")
                st.markdown(f"**Techs:** {payload['techsOnSite']}")
                st.markdown(f"**Notes:** {payload['content']}")
                if payload.get('photos'):
                    st.markdown(f"**Photos:** {len(payload['photos'])} attached")
            
            c_yes, c_no = st.columns(2)
            if c_yes.button("✅ Yes, Send Email", key="conf_yes", type="primary"):
                # Send Email
                send_daily_report_email(job, tech, loc, payload)
                
                # Also save to history if not already there (optional, but good practice)
                # We'll append it as a report so there's a record
                st.session_state.jobs[job_index]['reports'].append(payload)
                save_state()
                
                del st.session_state[confirm_key]
                st.success("Report Sent & Saved!")
                st.rerun()
                
            if c_no.button("❌ Cancel", key="conf_no"):
                del st.session_state[confirm_key]
                st.rerun()
            
            st.divider()

        st.write("#### 📝 Daily Field Report")
        st.caption("End of day reporting. Submit labor hours, parts, and finalize status.")
        
        with st.form(key=f"daily_form_{job_id}"):
            new_status = st.selectbox("Job Status", ["Pending", "In Progress", "Completed"], 
                                      index=["Pending", "In Progress", "Completed"].index(job['status']))

            r_col1, r_col2 = st.columns(2)
            with r_col1:
                # Techs on Site: Multiselect
                available_techs = [t['name'] for t in st.session_state.techs]
                default_techs = [tech['name']] if tech and tech['name'] in available_techs else []
                
                techs_on_site_list = st.multiselect("Techs On Site", options=available_techs, default=default_techs)
                time_arrived = st.time_input("Time Arrived", value=datetime.time(8, 0))
                parts_used = st.text_area("Parts/Materials Used")
            with r_col2:
                hours_worked = st.number_input("Hours Worked", min_value=0.0, step=0.5)
                time_departed = st.time_input("Time Finished", value=datetime.time(17, 0))
                billable_items = st.text_area("Billable Items / Extras")

            content = st.text_area("General Notes / Summary", placeholder="Detailed summary of work performed today...")
            
            # Logic to gather photos from "In-Progress" updates today
            current_date_str = datetime.datetime.now().strftime('%Y-%m-%d')
            todays_photos = []
            for r in job['reports']:
                # Check timestamp match
                if r['timestamp'].startswith(current_date_str) and r.get('photos'):
                    # Only grab from "In-Progress" updates (which don't have structured data like hoursWorked)
                    # to avoid duplicating photos if a Daily Report was already submitted.
                    is_full_report = r.get('hoursWorked') or r.get('techsOnSite')
                    if not is_full_report:
                        todays_photos.extend(r['photos'])
            
            if todays_photos:
                st.info(f"📸 {len(todays_photos)} photos taken today via 'In-Progress' updates will be automatically attached.")
            
            # Allow adding more photos directly here
            daily_photos = st.file_uploader("Attach Additional Photos (Optional)", accept_multiple_files=True, type=['png', 'jpg'], key=f"daily_up_{job_id}")

            f_c1, f_c2 = st.columns(2)
            submit_btn = f_c1.form_submit_button("Submit Daily Report")
            email_btn = f_c2.form_submit_button("📧 Email Report to Admins")

            if submit_btn or email_btn:
                # Process any new photos uploaded directly in this form
                if daily_photos:
                    for up_file in daily_photos:
                        path = save_image_locally(up_file)
                        if path:
                            todays_photos.append(path)

                # Construct Report Data
                report_payload = {
                    'id': f"r{datetime.datetime.now().timestamp()}",
                    'techId': job['techId'] or 'unknown',
                    'timestamp': datetime.datetime.now().isoformat(),
                    'content': content,
                    'techsOnSite': ", ".join(techs_on_site_list),
                    'timeArrived': str(time_arrived),
                    'timeDeparted': str(time_departed),
                    'hoursWorked': str(hours_worked),
                    'partsUsed': parts_used,
                    'billableItems': billable_items,
                    'photos': todays_photos # Photos handled in other tab
                }

                if email_btn:
                    # Trigger confirmation flow
                    st.session_state[f"confirm_daily_send_{job['id']}"] = report_payload
                    st.rerun()

                if submit_btn:
                    if new_status == "Completed":
                        # Set pending state and rerun to show confirmation UI
                        st.session_state[f"completion_pending_{job['id']}"] = report_payload
                        st.rerun()
                    else:
                        st.session_state.jobs[job_index]['reports'].append(report_payload)
                        
                        # Update Status
                        if new_status != job['status']:
                            st.session_state.jobs[job_index]['status'] = new_status
                            st.session_state.briefing = "Data required to generate briefing."
                        
                        save_state()
                        st.success("Daily Report Submitted!")
                        st.rerun()

# --- UI COMPONENTS ---


def render_job_card(job, compact=False, key_suffix="", allow_delete=False):
    tech = get_tech(job['techId'])
    loc = get_location(job['locationId'])
    loc_name = loc['name'] if loc else "Unknown"
    tech_name = tech['name'] if tech else "Unassigned"
    
    priority_class = f"priority-{job['priority']}"
    
    with st.container():
        st.markdown(f"""
        <div class="job-card {priority_class}">
            <div style="display:flex; justify-content:space-between;">
                <span style="font-weight:bold; font-size:1.1em;">{job['title']}</span>
                <span style="font-size:0.8em; background:#3f3f46; padding:2px 6px; border-radius:4px;">{job['priority']}</span>
            </div>
            <div style="color:#a1a1aa; font-size:0.9em; margin-top:5px;">{loc_name}</div>
            <div style="display:flex; justify-content:space-between; margin-top:10px; font-size:0.8em; color:#71717a;">
                 <span>👤 {tech_name}</span>
                 <span>📅 {job['date'][:10]}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        # Unique key using job ID AND suffix to prevent Streamlit duplicates
        if allow_delete:
            c1, c2, c3 = st.columns([6, 1, 1])
            with c1:
                if st.button("View Details", key=f"btn_{job['id']}_{key_suffix}", use_container_width=True):
                    job_details_dialog(job['id'])
            with c2:
                if st.button("✏️", key=f"edit_{job['id']}_{key_suffix}", help="Edit Job"):
                    edit_job_dialog(job['id'])
            with c3:
                if st.button("🗑️", key=f"del_{job['id']}_{key_suffix}", help="Delete from Archive"):
                    if job in st.session_state.jobs:
                        st.session_state.jobs.remove(job)
                        save_state()
                        st.rerun()
        else:
            if st.button("View Details", key=f"btn_{job['id']}_{key_suffix}", use_container_width=True):
                job_details_dialog(job['id'])

def render_admin_panel():
    st.subheader("Database Management")
    st.info(f"📁 **Data File Location:** `{DB_FILE}`")
    
    c_db1, c_db2 = st.columns(2)
    with c_db1:
        if st.button("🔄 Reload Data from Disk"):
            st.session_state.force_reload = True
            st.rerun()
    with c_db2:
        if st.button("💾 Force Save State"):
            _sync_session_to_db()
force_overwrite_from_session(invalidate_briefing=False)
st.success("Data restored successfully (DB overwritten).")
st.rerun()
st.toast("State saved to disk.", icon="💾")
            
st.divider()

# Backup / Restore
st.subheader("Backup & Restore")
c_bk1, c_bk2 = st.columns(2)

with c_bk1:
    # CSV Export
    csv_data = download_data_as_csv()
    if csv_data:
        st.download_button(
            label="📥 Download Jobs CSV",
            data=csv_data,
            file_name=f"jobs_export_{datetime.datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )
    else:
        st.button("📥 Download Jobs CSV", disabled=True)

    # JSON Export
    json_data = download_data_as_json()
    st.download_button(
        label="📦 Download Full Backup (JSON)",
        data=json_data,
        file_name=f"backup_{datetime.datetime.now().strftime('%Y%m%d')}.json",
        mime="application/json",
    )

with c_bk2:
    # Restore
    uploaded_file = st.file_uploader("Restore Backup (JSON)", type=["json"])
    if uploaded_file is not None:
        if st.button("⚠️ Restore from Backup"):
            try:
                data = json.load(uploaded_file)
                required_keys = ["jobs", "techs", "locations"]

                if all(k in data for k in required_keys):
                    st.session_state.jobs = data["jobs"]
                    st.session_state.techs = data["techs"]
                    st.session_state.locations = data["locations"]
                    st.session_state.briefing = data.get("briefing", "Data required to generate briefing.")
                    st.session_state.adminEmails = data.get("adminEmails", [])
                    st.session_state.last_reminder_date = data.get("last_reminder_date")

                    ensure_loaded_into_session()  # ✅ REQUIRED
                    _sync_session_to_db()
                    force_overwrite_from_session(invalidate_briefing=False)

                    st.success("Data restored successfully (DB overwritten).")
                    st.rerun()
                else:
                    st.error("Invalid backup file format.")
            except Exception as e:
                st.error(f"Error restoring file: {e}")

st.divider()
    
# --- ANALYTICS SECTION ---
with st.expander("📊 View Analytics Dashboard", expanded=False):
    render_analytics_dashboard()

st.divider()

# --- SYSTEM LOGS ---
st.subheader("📝 System Logs")
with st.expander("View Background Activity", expanded=False):
    st.caption("Recent keep-awake pings and system events.")

    c_log1, c_log2 = st.columns([3, 1])

    with c_log2:
        if st.button("⚡ Test Ping Now"):
            endpoints = [
                "http://localhost:8501/_stcore/health",
                "http://127.0.0.1:8501/_stcore/health"
            ]

            success = False

            for url in endpoints:
                try:
                    requests.get(url, timeout=2)
                    get_logger().log(f"Manual ping successful to {url}")
                    st.toast(f"Ping successful to {url}!", icon="✅")
                    success = True
                    break
                except Exception:
                    pass

            if not success:
                get_logger().log("Manual ping failed on all endpoints.")
                st.error("Ping failed on all endpoints.")

            st.rerun()

    logger = get_logger()
    logs = logger.get_logs()

    if logs:
        st.code("\n".join(logs), language="text")
        if st.button("Refresh Logs"):
            st.rerun()
    else:
        st.info("No logs recorded yet.")

st.divider()

    # --- ADMIN ACCESS MANAGEMENT ---
def render_admin_panel():
    st.subheader("Database Management")

    c_db1, c_db2 = st.columns(2)

    with c_db1:
        if st.button("🔄 Reload Data from DB"):
            state, ver = load_state()
            st.session_state.db = state
            st.session_state._db_version = ver
            st.session_state.jobs = state["jobs"]
            st.session_state.techs = state["techs"]
            st.session_state.locations = state["locations"]
            st.session_state.briefing = state["briefing"]
            st.session_state.adminEmails = state["adminEmails"]
            st.session_state.last_reminder_date = state.get("last_reminder_date")
            st.toast("Reloaded from DB.", icon="🔄")
            st.rerun()

    with c_db2:
        if st.button("💾 Save to DB"):
            _sync_session_to_db()
            commit_from_session(invalidate_briefing=False)
            st.toast("Saved to DB.", icon="💾")

    st.divider()

    st.subheader("Backup & Restore")
    c_bk1, c_bk2 = st.columns(2)

    with c_bk1:
        csv_data = download_data_as_csv()
        if csv_data:
            st.download_button(
                label="📥 Download Jobs CSV",
                data=csv_data,
                file_name=f"jobs_export_{datetime.datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )
        else:
            st.button("📥 Download Jobs CSV", disabled=True)

        json_data = download_data_as_json()
        st.download_button(
            label="📦 Download Full Backup (JSON)",
            data=json_data,
            file_name=f"backup_{datetime.datetime.now().strftime('%Y%m%d')}.json",
            mime="application/json",
        )

    with c_bk2:
        uploaded_file = st.file_uploader("Restore Backup (JSON)", type=["json"])
        if uploaded_file is not None:
            if st.button("⚠️ Restore from Backup"):
                try:
                    data = json.load(uploaded_file)
                    required_keys = ["jobs", "techs", "locations"]

                    if all(k in data for k in required_keys):
                        st.session_state.jobs = data["jobs"]
                        st.session_state.techs = data["techs"]
                        st.session_state.locations = data["locations"]
                        st.session_state.briefing = data.get("briefing", "Data required to generate briefing.")
                        st.session_state.adminEmails = data.get("adminEmails", [])
                        st.session_state.last_reminder_date = data.get("last_reminder_date")

                        _sync_session_to_db()
                        force_overwrite_from_session(invalidate_briefing=False)

                        st.success("Data restored successfully (DB overwritten).")
                        st.rerun()
                    else:
                        st.error("Invalid backup file format.")
                except Exception as e:
                    st.error(f"Error restoring file: {e}")

    st.divider()

    with st.expander("📊 View Analytics Dashboard", expanded=False):
        render_analytics_dashboard()

    st.divider()

    st.subheader("🛡️ Admin Access")
    st.caption("Users listed here have full access to settings and can create jobs.")

    with st.form("add_admin_form"):
        col_ad1, col_ad2 = st.columns([3, 1])
        new_admin = col_ad1.text_input("New Admin Email", placeholder="user@company.com")
        if col_ad2.form_submit_button("Add Admin"):
            if new_admin and new_admin not in st.session_state.adminEmails:
                st.session_state.adminEmails.append(new_admin)
                save_state()
                st.success(f"Added {new_admin}")
                st.rerun()
            elif new_admin in st.session_state.adminEmails:
                st.warning("Email already exists.")

    for email in st.session_state.adminEmails:
        c_e1, c_e2 = st.columns([4, 1])
        c_e1.write(f"• {email}")
        if c_e2.button("Remove", key=f"rm_admin_{email}"):
            if len(st.session_state.adminEmails) > 1:
                st.session_state.adminEmails.remove(email)
                save_state()
                st.rerun()
            else:
                st.error("Cannot remove the last admin.")

    st.divider()

    st.subheader("📧 Email Configuration")
    with st.expander("Configure SMTP Settings", expanded=False):
        st.info("Settings entered here apply to the current session only. For permanent setup, add to `.streamlit/secrets.toml`.")

        session_config = st.session_state.get("smtp_settings", {})

        def get_val(key, default=""):
            if session_config.get(key):
                return session_config.get(key)
            if key in st.secrets:
                return st.secrets[key]
            return os.getenv(key) or default

        with st.form("smtp_config_form"):
            c_smtp1, c_smtp2 = st.columns(2)
            new_server = c_smtp1.text_input("SMTP Server", value=get_val("SMTP_SERVER"))
            new_port = c_smtp2.text_input("SMTP Port", value=get_val("SMTP_PORT", "587"))
            new_email = st.text_input("Sender Email", value=get_val("SMTP_EMAIL"))
            new_pass = st.text_input("Sender Password", type="password", value=get_val("SMTP_PASSWORD"))

            if st.form_submit_button("Save Email Settings"):
                st.session_state.smtp_settings = {
                    "SMTP_SERVER": new_server,
                    "SMTP_PORT": new_port,
                    "SMTP_EMAIL": new_email,
                    "SMTP_PASSWORD": new_pass,
                }
                st.success("Email settings updated for this session!")

    st.divider()

    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Manage Technicians")
        with st.form("add_tech"):
            t_name = st.text_input("Name")
            t_email = st.text_input("Email")
            if st.form_submit_button("Add Technician"):
                if t_name and t_email:
                    initials = "".join([n[0] for n in t_name.split()]).upper()[:2]
                    color = TECH_COLORS[len(st.session_state.techs) % len(TECH_COLORS)]
                    st.session_state.techs.append(
                        {
                            "id": f"t{datetime.datetime.now().timestamp()}",
                            "name": t_name,
                            "email": t_email,
                            "initials": initials,
                            "color": color,
                        }
                    )
                    save_state()
                    st.rerun()

        st.markdown("---")
        for t in st.session_state.techs:
            st.markdown(f"**{t['name']}** ({t['email']})")
            if st.button("Remove", key=f"rm_t_{t['id']}"):
                st.session_state.techs.remove(t)
                save_state()
                st.rerun()

    with c2:
        st.subheader("Manage Locations")

        if "new_loc_name" not in st.session_state:
            st.session_state.new_loc_name = ""
        if "new_loc_addr" not in st.session_state:
            st.session_state.new_loc_addr = ""

        st.text_input("Location Name", key="new_loc_name")

        col_addr, col_btn = st.columns([3, 1])
        with col_addr:
            st.text_input("Address", key="new_loc_addr")
        with col_btn:
            st.write("")
            st.write("")

            def auto_complete_callback():
                if st.session_state.new_loc_addr:
                    suggestion = suggest_address_with_gemini(st.session_state.new_loc_addr)
                    if suggestion:
                        st.session_state.new_loc_addr = suggestion

            st.button("✨ Auto-Complete", help="Use AI to complete address", on_click=auto_complete_callback)

        def add_location_callback():
            l_name = st.session_state.new_loc_name
            l_addr = st.session_state.new_loc_addr

            if l_name and l_addr:
                map_url = get_google_maps_url(l_addr)
                new_loc = {
                    "id": f"l{datetime.datetime.now().timestamp()}",
                    "name": l_name,
                    "address": l_addr,
                    "mapsUrl": map_url,
                }

                st.session_state.locations.append(new_loc)
                st.session_state.new_loc_name = ""
                st.session_state.new_loc_addr = ""
                save_state()
                st.toast(f"Added location: {l_name}", icon="✅")
            else:
                st.toast("Please enter both Name and Address.", icon="⚠️")

        st.button("Add Location", type="primary", on_click=add_location_callback)

        st.markdown("---")
        for l in st.session_state.locations:
            if l.get("mapsUrl"):
                st.markdown(f"**[{l['name']}]({l['mapsUrl']})** - {l['address']}")
            else:
                st.markdown(f"**{l['name']}** - {l['address']}")

            if st.button("Remove", key=f"rm_l_{l['id']}"):
                st.session_state.locations.remove(l)
                save_state()
                st.rerun()

def render_analytics_dashboard():
    st.subheader("📊 Operational Analytics")
    
    if not st.session_state.jobs:
        st.info("No job data available.")
        return

    df = pd.DataFrame(st.session_state.jobs)
    
    # Metrics
    total = len(df)
    completed = len(df[df['status'] == 'Completed'])
    active = total - completed
    critical = len(df[df['priority'] == 'Critical'])
    
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Jobs", total)
    m2.metric("Active", active)
    m3.metric("Completed", completed)
    m4.metric("Critical", critical)
    
    st.divider()
    
    c1, c2 = st.columns(2)
    
    with c1:
        st.markdown("#### Jobs by Status")
        if not df.empty:
            status_counts = df['status'].value_counts()
            st.bar_chart(status_counts, color="#2563eb")
            
    with c2:
        st.markdown("#### Jobs by Priority")
        if not df.empty:
            prio_counts = df['priority'].value_counts()
            # Custom color mapping if possible, otherwise default
            st.bar_chart(prio_counts, color="#dc2626")
            
    st.divider()
    
    c3, c4 = st.columns(2)
    
    with c3:
        st.markdown("#### Tech Workload (Active)")
        active_jobs = df[df['status'] != 'Completed']
        if not active_jobs.empty:
            tech_map = {t['id']: t['name'] for t in st.session_state.techs}
            tech_map[None] = "Unassigned"
            # Map techId to Name
            workload = active_jobs['techId'].map(tech_map).fillna("Unassigned").value_counts()
            st.bar_chart(workload, horizontal=True, color="#4b5563")
        else:
            st.info("No active jobs.")
            
    with c4:
        st.markdown("#### Jobs by Type")
        if not df.empty:
            type_counts = df['type'].value_counts()
            st.bar_chart(type_counts, color="#16a34a")

def render_chatbot():
    st.sidebar.title("🤖 Tech Assistant")
    st.sidebar.markdown("Ask about jobs, history, or locations.")
    
    # Display History
    for msg in st.session_state.chat_history:
        with st.sidebar.chat_message(msg["role"]):
            st.write(msg["parts"][0])
    
    # Chat Input
    prompt = st.sidebar.chat_input("How can I help?")
    if prompt:
        api_key = get_api_key()
        if not api_key:
            st.sidebar.error("API Key missing.")
            return

        # Use dynamic model selector
        model = get_available_model(api_key)
        
        # Add user message
        st.session_state.chat_history.append({"role": "user", "parts": [prompt]})
        with st.sidebar.chat_message("user"):
            st.write(prompt)
        
        # Contextualize Data (remove heavy base64 strings before sending to LLM)
        simple_jobs = []
        for j in st.session_state.jobs:
            clean_job = {k:v for k,v in j.items() if k != 'reports'}
            
            # Include text content of reports, but strip out photos to save tokens/bandwidth
            clean_reports = []
            for r in j.get('reports', []):
                clean_reports.append({
                    'timestamp': r.get('timestamp'),
                    'techId': r.get('techId'),
                    'content': r.get('content'),
                    'photo_count': len(r.get('photos', []))
                })
            
            clean_job['reports'] = clean_reports
            simple_jobs.append(clean_job)
        
        system_context = f"""
       You are a 5G Security Assistant.
       Current Time: {datetime.datetime.now()}
       Techs: {json.dumps(st.session_state.techs)}
       Locations: {json.dumps(st.session_state.locations)}
       Jobs: {json.dumps(simple_jobs)}
       
       Answer based strictly on this data. If searching for history, note that detailed reports are not in this context, only summaries.
       """
        
        full_prompt = f"{system_context}\n\nUser Question: {prompt}"
        
        try:
            with st.sidebar.chat_message("model"):
                with st.spinner("Thinking..."):
                    response = model.generate_content(full_prompt)
                    bot_reply = response.text
                    st.write(bot_reply)
                    
            st.session_state.chat_history.append({"role": "model", "parts": [bot_reply]})
        except Exception as e:
            st.sidebar.error(f"AI Error: {str(e)}")

# --- MAIN APP FLOW ---

def main():
    # Start Keep Awake Thread
    keep_awake()

    # 1. Authenticate User
    user = authenticate()
    if not user:
        return  # Stop rendering if not logged in
        
    user_email = user.get("email")
    user_name = user.get("name")
    
    # 2. Determine Role (Admin or Tech)
    # Bootstrapping: If no admins exist in DB, first login becomes Admin
    if not st.session_state.adminEmails:
        st.session_state.adminEmails.append(user_email)
        save_state()
        st.toast(f"First login detected. {user_email} is now Super Admin.", icon="🛡️")
    
    is_admin = user_email in st.session_state.adminEmails
    
    # --- DAILY REMINDERS ---
    send_daily_reminders()
    
    # Sidebar Info
    with st.sidebar:
        st.markdown("---")
        st.write(f"Logged in as: **{user_name}**")
        if is_admin:
            st.success("🛡️ Admin Access")
        else:
            st.info("👷 Technician View")
            
        if st.button("Logout", key="logout_btn"):
            logout()

    # Top Bar
    c1, c2, c3 = st.columns([4, 4, 2])
    with c1:
        st.title("5G Security Job Board")
    with c2:
        search = st.text_input("Search Jobs...", label_visibility="collapsed", placeholder="🔍 Search jobs...")
    with c3:
        # Restricted Access: Only Admins can create jobs
        if is_admin:
            if st.button("➕ New Job", use_container_width=True):
                add_job_dialog()

    # Filter Jobs based on search
    filtered_jobs = st.session_state.jobs
    if search:
        filtered_jobs = [j for j in filtered_jobs if search.lower() in j['title'].lower() or search.lower() in j['description'].lower()]

    # Navigation Tabs
    tabs_list = ["🌅 Morning Briefing", "👷 Tech Board", "📅 Calendar", "🧰 Service Calls", "🏗️ Projects", "📦 Archive"]
    if is_admin:
        tabs_list.append("🛡️ Admin")
    
    tabs = st.tabs(tabs_list)
    
    # 1. Morning Briefing
    with tabs[0]:
        col_main, col_feed = st.columns([2, 1])
        with col_main:
            st.subheader("Daily Operational Briefing")
            
            # Automatically generate briefing if it matches default placeholder AND we have jobs
            if st.session_state.briefing == "Data required to generate briefing." and st.session_state.jobs:
                with st.spinner("🤖 AI is preparing your morning briefing..."):
                    st.session_state.briefing = generate_morning_briefing()
                    save_state(invalidate_briefing=False)
                 # NO st.rerun() here
            
            st.container(border=True).markdown(st.session_state.briefing)
            
            # Stats
            s1, s2, s3 = st.columns(3)
            active = len([j for j in st.session_state.jobs if j['status'] != 'Completed'])
            crit = len([j for j in st.session_state.jobs if j['priority'] == 'Critical'])
            s1.metric("Active Jobs", active)
            s2.metric("Critical", crit)
            s3.metric("Techs", len(st.session_state.techs))

        with col_feed:
            st.subheader("Priority Feed")
            crit_jobs = [j for j in filtered_jobs if j['priority'] in ['Critical', 'High'] and j['status'] != 'Completed']
            if not crit_jobs:
                st.caption("No critical jobs.")
            for job in crit_jobs:
                render_job_card(job, compact=True, key_suffix="feed_crit")

            st.divider()

            st.subheader("Standard Feed")
            std_jobs = [j for j in filtered_jobs if j['priority'] in ['Medium', 'Low'] and j['status'] != 'Completed']
            if not std_jobs:
                st.caption("No standard jobs.")
            for job in std_jobs:
                render_job_card(job, compact=True, key_suffix="feed_std")

    # 2. Tech Board
    with tabs[1]:
        if not st.session_state.techs:
            st.info("No technicians added. Go to Admin tab.")
        else:
            cols = st.columns(len(st.session_state.techs) + 1)
            # Tech Columns
            for i, tech in enumerate(st.session_state.techs):
                with cols[i]:
                    st.markdown(f"**{tech['initials']}** - {tech['name']}")
                    tech_jobs = [j for j in filtered_jobs if j['techId'] == tech['id'] and j['status'] != 'Completed']
                    for job in tech_jobs:
                        render_job_card(job, compact=True, key_suffix=f"tech_{tech['id']}", allow_delete=is_admin)
            # Unassigned
            with cols[-1]:
                st.markdown("**Unassigned**")
                unassigned = [j for j in filtered_jobs if not j['techId'] and j['status'] != 'Completed']
                for job in unassigned:
                    render_job_card(job, compact=True, key_suffix="unassigned", allow_delete=is_admin)

    # 3. Calendar View
    with tabs[2]:
        st.subheader("📅 Job Schedule")
        
        # Calendar Controls
        col_cal1, col_cal2 = st.columns([1, 4])
        with col_cal1:
            # Simple month navigation could be added here, defaulting to current month for now
            current_date = datetime.datetime.now()
            cal_year = st.number_input("Year", value=current_date.year, min_value=2024, max_value=2030)
            cal_month = st.selectbox("Month", list(calendar.month_name)[1:], index=current_date.month - 1)
            month_num = list(calendar.month_name).index(cal_month)
            
        with col_cal2:
            st.write("") # Spacer
        
        # Generate Calendar Grid
        cal = calendar.monthcalendar(cal_year, month_num)
        
        # Weekday Headers
        cols = st.columns(7)
        days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        for i, day in enumerate(days):
            cols[i].markdown(f"**{day}**")
            
        # Calendar Rows
        for week in cal:
            cols = st.columns(7)
            for i, day in enumerate(week):
                with cols[i]:
                    if day == 0:
                        st.write("") # Empty day from other month
                    else:
                        # Render Day Cell
                        st.markdown(f"**{day}**")
                        
                        # Find jobs for this date
                        # Note: Job 'date' is ISO format string. We compare YYYY-MM-DD.
                        target_date_str = f"{cal_year}-{month_num:02d}-{day:02d}"
                        
                        day_jobs = [j for j in filtered_jobs if j['date'].startswith(target_date_str) and j['status'] != 'Completed']
                        
                        for job in day_jobs:
                            tech = get_tech(job['techId'])
                            tech_initials = tech['initials'] if tech else "Un"
                            color = tech['color'] if tech else "#52525b"
                            
                            # Small pill for the job
                            st.markdown(f"""
                            <div style="
                                background-color: {color}; 
                                color: white; 
                                padding: 2px 4px; 
                                border-radius: 4px; 
                                font-size: 0.7em; 
                                margin-bottom: 2px;
                                white-space: nowrap;
                                overflow: hidden;
                                text-overflow: ellipsis;
                                cursor: help;"
                                title="{job['title']} ({tech['name'] if tech else 'Unassigned'})">
                                {tech_initials} - {job['title'][:10]}..
                            </div>
                            """, unsafe_allow_html=True)
                            
                        if not day_jobs:
                            st.markdown("<div style='height: 20px;'></div>", unsafe_allow_html=True)

    # 4. Service Calls
    with tabs[3]:
        service_jobs = [j for j in filtered_jobs if j['type'] == 'Service' and j['status'] != 'Completed']
        if not service_jobs: st.info("No active service calls.")
        for job in service_jobs:
            render_job_card(job, key_suffix="service", allow_delete=is_admin)

    # 5. Projects
    with tabs[4]:
        proj_jobs = [j for j in filtered_jobs if j['type'] == 'Project' and j['status'] != 'Completed']
        if not proj_jobs: st.info("No active projects.")
        for job in proj_jobs:
            render_job_card(job, key_suffix="project", allow_delete=is_admin)

    # 6. Archive
    with tabs[5]:
        archived = [j for j in filtered_jobs if j['status'] == 'Completed']
        if not archived: st.info("No archived jobs.")
        for job in archived:
            render_job_card(job, key_suffix="archive", allow_delete=is_admin)

    # 7. Admin (Only if Admin)
    if is_admin:
        with tabs[6]:
            render_admin_panel()

    # Sidebar Chatbot
    render_chatbot()

if __name__ == "__main__":
    main()
