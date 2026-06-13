import streamlit as st
from google import genai
from google.genai import types
import datetime
import base64
import os
import re
import json
import hmac
import hashlib
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
from PIL import Image, ImageOps
from io import BytesIO
from persistence_pg import (
    ensure_loaded_into_session,
    commit_from_session,
    force_overwrite_from_session,
    load_state,
    get_db_version,
    StaleStateError,
)
from object_store import upload_streamlit_file, upload_bytes, get_view_url

import io
from reportlab.lib.utils import ImageReader

# Try importing ReportLab for PDF generation
try:
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        Image as RLImage, KeepTogether, PageBreak,
    )
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False

# Try importing Streamlit Drawable Canvas for signatures
try:
    from streamlit_drawable_canvas import st_canvas
    HAS_CANVAS = True
except ImportError:
    HAS_CANVAS = False

# Try importing Folium for the interactive Map view
try:
    import folium
    from streamlit_folium import st_folium
    HAS_MAP = True
except ImportError:
    HAS_MAP = False

# Try importing Cookie Controller for persistent login
try:
    from streamlit_cookies_controller import CookieController
    HAS_COOKIES = True
except ImportError:
    HAS_COOKIES = False

# --- TIMEZONE ---
# Cloud hosts run on UTC, so naive datetime.now() stamps the wrong hours.
# All timestamps go through now_local() pinned to the company timezone.
# Override with APP_TIMEZONE (IANA name, e.g. "America/Chicago") in secrets or env.
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

def _resolve_app_timezone():
    tz_name = os.getenv("APP_TIMEZONE")
    if not tz_name:
        try:
            tz_name = st.secrets.get("APP_TIMEZONE")
        except Exception:
            tz_name = None
    tz_name = tz_name or "America/Chicago"
    if ZoneInfo:
        try:
            return ZoneInfo(tz_name)
        except Exception:
            pass
    return None

APP_TZ = _resolve_app_timezone()

def now_local():
    """Wall-clock 'now' in the app's timezone, returned naive to match stored data."""
    if APP_TZ:
        return datetime.datetime.now(APP_TZ).replace(tzinfo=None)
    return datetime.datetime.now()

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
       min-height: 2.5rem;
       width: 100%;
       padding: 0.5rem 1rem !important;
   }
   .stButton > button:hover {
       background-color: #991b1b;
       color: white;
       border-color: #7f1d1d;
   }
   /* Fix for button text centering */
   .stButton > button div {
       display: flex;
       align-items: center;
       justify-content: center;
   }
   .stButton > button p {
       margin: 0 !important;
       line-height: 1.2 !important;
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

# --- PERSISTENCE LAYER (Neon Postgres) ---
def load_data():
    try:
        ensure_loaded_into_session()
        return dict(st.session_state.db)
    except Exception as e:
        st.error(f"Failed to load data from DB: {e}")
        return {
            "jobs": [],
            "techs": [],
            "locations": [],
            "briefing": "Data required to generate briefing.",
            "adminEmails": [],
            "smtp_settings": {},
            "last_reminder_date": None
        }

def _sync_session_to_db():
    ensure_loaded_into_session()
    st.session_state.db["jobs"] = st.session_state.jobs
    st.session_state.db["techs"] = st.session_state.techs
    st.session_state.db["locations"] = st.session_state.locations
    st.session_state.db["briefing"] = st.session_state.briefing
    st.session_state.db["adminEmails"] = st.session_state.adminEmails
    st.session_state.db["smtp_settings"] = st.session_state.get("smtp_settings", {})
    st.session_state.db["last_reminder_date"] = st.session_state.get("last_reminder_date")

def refresh_session_from_db():
    """Reloads the DB row and replaces this session's working data with fresh state."""
    data, version = load_state()
    st.session_state.db = data
    st.session_state._db_version = version
    st.session_state.jobs = data.get("jobs", [])
    st.session_state.techs = data.get("techs", [])
    st.session_state.locations = data.get("locations", [])
    st.session_state.briefing = data.get("briefing", "Data required to generate briefing.")
    st.session_state.adminEmails = data.get("adminEmails", [])
    st.session_state.smtp_settings = data.get("smtp_settings", {})
    st.session_state.last_reminder_date = data.get("last_reminder_date")

def save_state(invalidate_briefing=False):
    if invalidate_briefing:
        st.session_state.briefing = "Data required to generate briefing."
    _sync_session_to_db()
    try:
        commit_from_session(invalidate_briefing=invalidate_briefing)
    except StaleStateError:
        # Someone else saved while this session held old data. Don't clobber their
        # changes - reload fresh state and ask the user to re-apply theirs.
        refresh_session_from_db()
        st.warning(
            "⚠️ Someone else saved changes at the same time. The app has refreshed "
            "with the latest data — please re-apply your last change."
        )

def update_job_status_callback(job_id, widget_key):
    """Callback to update job status and save state."""
    new_status = st.session_state.get(widget_key)
    if not new_status:
        return
        
    job_idx = next((i for i, j in enumerate(st.session_state.jobs) if j['id'] == job_id), -1)
    if job_idx != -1:
        if st.session_state.jobs[job_idx]['status'] != new_status:
            st.session_state.jobs[job_idx]['status'] = new_status
            save_state()

def update_part_status_callback(job_id, part_id, widget_key):
    """Callback to update a single part's status inline and save state."""
    new_status = st.session_state.get(widget_key)
    if not new_status:
        return
    job_idx = next((i for i, j in enumerate(st.session_state.jobs) if j['id'] == job_id), -1)
    if job_idx == -1:
        return
    for p in st.session_state.jobs[job_idx].get('parts', []):
        if p['id'] == part_id and p.get('status') != new_status:
            p['status'] = new_status
            p['updated_at'] = now_local().isoformat()
            p['added_by'] = st.session_state.user_info.get('email', p.get('added_by', 'unknown')) if "user_info" in st.session_state else p.get('added_by', 'unknown')
            save_state(invalidate_briefing=False)
            break

# --- DB SESSION INITIALIZER (safe) ---
def init_db_session():
    try:
        ensure_loaded_into_session()
    except Exception as e:
        pass

init_db_session()

# --- SESSION STATE INITIALIZATION ---
if "jobs" not in st.session_state:
    db_data = load_data()
    st.session_state.jobs = db_data.get("jobs", [])
    st.session_state.techs = db_data.get("techs", [])
    st.session_state.locations = db_data.get("locations", [])
    st.session_state.briefing = db_data.get("briefing", "Data required to generate briefing.")
    st.session_state.adminEmails = db_data.get("adminEmails", [])
    st.session_state.smtp_settings = db_data.get("smtp_settings", {})
    st.session_state.last_reminder_date = db_data.get("last_reminder_date")

if "chat_history" not in st.session_state:
    st.session_state.chat_history = [
        {"role": "model", "parts": ["Hello! I have access to your database. Ask me about active jobs, tech locations, or history."]}
    ]
# Tech Colors for UI
def get_status_color(status):
    colors = {
        "Not Started": "#71717a",
        "Pending": "#71717a",
        "In Progress": "#3b82f6",
        "Customer on Hold": "#f97316",
        "Waiting on Parts": "#ef4444",
        "Parts not ordered": "#991b1b",
        "Parts Staged": "#10b981",
        "Completed": "#059669"
    }
    return colors.get(status, "#71717a")

TECH_COLORS = ['#7f1d1d', '#3f3f46', '#b91c1c', '#52525b', '#991b1b', '#7c2d12', '#292524']

# Tech Skills Options
SKILL_OPTIONS = [
    "Cabling (Cat6/Fiber)",
    "Access Control",
    "CCTV / Cameras",
    "Alarm Systems",
    "Networking / IT",
    "Conduit / Pipe",
    "Sound Masking",
    "Locksmithing"
]

# Common system types for site credentials
SYSTEM_PRESETS = [
    "DW Spectrum",
    "ICT",
    "Windows PC / Server",
    "NVR / DVR",
    "Camera",
    "Access Control Panel",
    "Alarm Panel",
    "Switch / Router",
    "Other"
]

def location_has_system_info(loc):
    """True if the location has any systems or legacy credentials recorded."""
    if not loc:
        return False
    if loc.get('systems'):
        return True
    return any(v for v in (loc.get('credentials') or {}).values())

# Priority colors (hot -> cold), shared by the calendar and other UI
PRIORITY_COLORS = {
    "Critical": "#ef4444",
    "High": "#dc2626",
    "Medium": "#b45309",
    "Low": "#52525b",
}

# Parts pipeline: items flow left to right toward being staged for the job
PART_STATUSES = ["Needed", "Ordered", "Received", "Staged"]
PART_STATUS_COLORS = {
    "Needed": "#991b1b",
    "Ordered": "#b45309",
    "Received": "#3b82f6",
    "Staged": "#10b981",
}

def parts_summary(job):
    """Returns (staged_count, total_count) for a job's parts list."""
    parts = job.get('parts', [])
    staged = sum(1 for p in parts if p.get('status') == 'Staged')
    return staged, len(parts)

# --- AUTHENTICATION ---

# Persistent login: a signed cookie keeps techs logged in across refreshes.
SESSION_COOKIE_NAME = "fivegsec_session"
SESSION_COOKIE_DAYS = 30

def _make_cookie_controller():
    """Creates the cookie controller for this script run (must re-render every run)."""
    if not HAS_COOKIES:
        return None
    try:
        ctrl = CookieController(key="auth_cookies")
        st.session_state["_cookie_controller"] = ctrl
        return ctrl
    except Exception:
        return None

def _get_cookie_secret():
    """Secret used to sign session cookies. Set COOKIE_SECRET, or the OAuth client secret is used."""
    secret = st.secrets.get("COOKIE_SECRET") if "COOKIE_SECRET" in st.secrets else os.getenv("COOKIE_SECRET")
    if not secret:
        secret = st.secrets.get("GOOGLE_CLIENT_SECRET") or os.getenv("GOOGLE_CLIENT_SECRET")
    return secret

def _sign_session_token(user_info):
    """Builds a tamper-proof session token: base64(payload).hmac_sha256(payload)."""
    secret = _get_cookie_secret()
    if not (secret and user_info.get("email")):
        return None
    payload = {
        "email": user_info.get("email"),
        "name": user_info.get("name"),
        "picture": user_info.get("picture"),
        "exp": (now_local() + datetime.timedelta(days=SESSION_COOKIE_DAYS)).timestamp(),
    }
    raw = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    sig = hmac.new(secret.encode(), raw.encode(), hashlib.sha256).hexdigest()
    return f"{raw}.{sig}"

def _verify_session_token(token):
    """Returns the user_info payload if the token is validly signed and unexpired, else None."""
    secret = _get_cookie_secret()
    if not (secret and token and isinstance(token, str) and "." in token):
        return None
    try:
        raw, sig = token.rsplit(".", 1)
        expected = hmac.new(secret.encode(), raw.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return None
        payload = json.loads(base64.urlsafe_b64decode(raw.encode()).decode())
        if payload.get("exp", 0) < now_local().timestamp():
            return None
        if not payload.get("email"):
            return None
        return payload
    except Exception:
        return None

def authenticate():
    """Handles Google OAuth2 Flow. Returns user_info dict if logged in, else None."""

    cookie_ctrl = _make_cookie_controller()

    # 1) If already logged in, return user info
    if "user_info" in st.session_state:
        # Persist the session in a signed cookie (set on the stable run after OAuth,
        # so an immediate rerun can't swallow the cookie write)
        if cookie_ctrl and not st.session_state.get("_session_cookie_set"):
            token = _sign_session_token(st.session_state.user_info)
            if token:
                try:
                    cookie_ctrl.set(SESSION_COOKIE_NAME, token, max_age=SESSION_COOKIE_DAYS * 24 * 3600)
                    st.session_state["_session_cookie_set"] = True
                except TypeError:
                    try:
                        cookie_ctrl.set(SESSION_COOKIE_NAME, token)
                        st.session_state["_session_cookie_set"] = True
                    except Exception:
                        pass
                except Exception:
                    pass
        return st.session_state.user_info

    # 1.5) Try restoring a previous session from the signed browser cookie
    if cookie_ctrl and not st.session_state.get("_skip_cookie_restore"):
        try:
            restored = _verify_session_token(cookie_ctrl.get(SESSION_COOKIE_NAME))
        except Exception:
            restored = None
        if restored:
            st.session_state.user_info = restored
            st.session_state["_session_cookie_set"] = True
            return restored

        # The cookie component may not have delivered the browser's cookies on the
        # first run(s), so we can't yet tell a returning user from a new one.
        # Show a brief branded splash instead of flashing the login screen.
        # After a couple of retries with no valid session (new login, or the
        # 30-day token expired), fall through to the login button.
        # Skip the wait entirely when returning from the Google OAuth redirect.
        oauth_redirect = False
        try:
            oauth_redirect = "code" in st.query_params
        except Exception:
            pass

        if not oauth_redirect:
            attempts = st.session_state.get("_cookie_wait_attempts", 0)
            if attempts < 2:
                st.session_state["_cookie_wait_attempts"] = attempts + 1
                st.markdown(
                    """
                    <div class="login-container">
                        <div class="login-box">
                            <h1 style="color:white; margin-bottom: 10px;">5G Security Job Board</h1>
                            <p style="color:#a1a1aa;">Checking your session…</p>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                time.sleep(0.8)
                st.rerun()

    # 2) Setup OAuth Config
    client_id = st.secrets.get("GOOGLE_CLIENT_ID") or os.getenv("GOOGLE_CLIENT_ID")
    client_secret = st.secrets.get("GOOGLE_CLIENT_SECRET") or os.getenv("GOOGLE_CLIENT_SECRET")
    
    # Use APP_URL as fallback for redirect_uri
    app_url = os.getenv("APP_URL", "").rstrip("/")
    default_redirect = f"{app_url}/" if app_url else None
    redirect_uri = st.secrets.get("GOOGLE_REDIRECT_URI") or os.getenv("GOOGLE_REDIRECT_URI") or default_redirect

    if not (client_id and client_secret and redirect_uri):
        st.error(
            "🔒 Google OAuth is not configured. Please add `GOOGLE_CLIENT_ID`, "
            "`GOOGLE_CLIENT_SECRET`, and `GOOGLE_REDIRECT_URI` to Streamlit secrets."
        )
        return None

    # 3) Check for Auth Code from Google Redirect
    code = None
    try:
        if "code" in st.query_params:
            code = st.query_params["code"]
    except Exception:
        try:
            query_params = st.experimental_get_query_params()
            code = query_params.get("code", [None])[0]
        except Exception:
            code = None

    # Prevent infinite loops if the URL keeps the same code param
    if code and st.session_state.get("_oauth_last_code") == code:
        # Code already processed, clear it and continue without rerun
        try:
            st.query_params.clear()
        except:
            pass
        return None
    elif code:
        st.session_state["_oauth_last_code"] = code

    # If we have a code, try to exchange it for a token and fetch user info
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

            r = requests.post(token_url, data=data, timeout=15)
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

            # Clear query params so refresh doesn't keep re-processing the code
            try:
                st.query_params.clear()
            except Exception:
                try:
                    st.experimental_set_query_params()
                except Exception:
                    pass
            
            # Small delay to ensure session state propagates
            time.sleep(0.1)
            st.rerun()

        except Exception as e:
            st.error(f"Authentication Failed: {e}")

            # Clear query params so we can show login again
            try:
                st.query_params.clear()
            except Exception:
                try:
                    st.experimental_set_query_params()
                except Exception:
                    pass

            # Allow the function to continue to the login button UI (no rerun)
            code = None

    # 4) Show Login Button
    auth_url = "https://accounts.google.com/o/oauth2/v2/auth"
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "openid email profile",
        "access_type": "online",
        "prompt": "select_account",
    }
    login_url = f"{auth_url}?{urllib.parse.urlencode(params)}"

    st.markdown(
        f"""
        <div class="login-container">
            <div class="login-box">
                <h1 style="color:white; margin-bottom: 10px;">5G Security Job Board</h1>
                <p style="color:#a1a1aa; margin-bottom: 30px;">Operational Dashboard</p>
                <a href="{login_url}" style="
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
                <p style="font-size: 0.9em; color: #a1a1aa; margin-top: 20px;">
                    Please login with your 5G Security email.
                </p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    return None


def logout():
    # Clear the persistent session cookie so a refresh doesn't log the user back in
    ctrl = st.session_state.get("_cookie_controller")
    if ctrl:
        try:
            ctrl.remove(SESSION_COOKIE_NAME)
        except Exception:
            pass
    st.session_state.pop("_session_cookie_set", None)
    # Belt-and-braces: skip cookie restore for the rest of this browser session
    st.session_state["_skip_cookie_restore"] = True
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
            ts = now_local().strftime("%Y-%m-%d %H:%M:%S")
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
    Background thread to keep the app awake on Streamlit Community Cloud.

    The platform sleeps apps that receive no EXTERNAL traffic, so pinging
    localhost does nothing - the ping must go through the public URL
    (APP_URL) to count as real viewer traffic. Localhost is only used as
    a fallback health check so the logs show the process is alive.
    Note: this only helps while the app is awake; the GitHub Actions
    keepalive workflow is the backstop that pings from outside.
    """
    app_url = os.getenv("APP_URL", "").rstrip("/")
    if not app_url:
        try:
            if "APP_URL" in st.secrets:
                app_url = str(st.secrets["APP_URL"]).rstrip("/")
        except Exception:
            pass

    def run():
        logger = get_logger()
        # Wait a bit for server to fully start
        time.sleep(10)

        public_url = f"{app_url}/_stcore/health" if app_url else None
        local_url = "http://localhost:8501/_stcore/health"

        while True:
            target = public_url or local_url
            try:
                requests.get(target, timeout=10)
                logger.log(f"Keep-awake ping successful to {target}")
            except Exception as e:
                # Fallback: confirm the local server is at least alive
                try:
                    requests.get(local_url, timeout=5)
                    logger.log(f"Public ping failed ({e}), local health OK")
                except Exception:
                    logger.log(f"Keep-awake ping failed entirely: {e}")

            # Sleep thresholds are measured in hours - every 10 min is plenty
            time.sleep(600)

    # v3: public-URL pinger (old versions hit localhost:3000, which Streamlit
    # Cloud ignores). New thread name so old zombie threads are left behind.
    thread_name = "keep_awake_v3"
    for t in threading.enumerate():
        if t.name == thread_name:
            return

    thread = threading.Thread(target=run, name=thread_name, daemon=True)
    thread.start()

def start_background_scheduler():
    """Background thread to send daily reminders at 7 AM."""
    try:
        secrets_dict = dict(st.secrets)
    except Exception:
        secrets_dict = {}
        
    def run():
        time.sleep(15)
        while True:
            try:
                now = now_local()
                # Run at 7 AM Mon-Fri
                if now.weekday() <= 4 and now.hour == 7:
                    today_str = now.strftime("%Y-%m-%d")
                    
                    from persistence_pg import load_state, save_state_to_db
                    state, version = load_state()
                    
                    if state.get("last_reminder_date") != today_str:
                        smtp_server = secrets_dict.get("SMTP_SERVER") or os.getenv("SMTP_SERVER")
                        smtp_port = secrets_dict.get("SMTP_PORT") or os.getenv("SMTP_PORT", 587)
                        sender_email = secrets_dict.get("SMTP_EMAIL") or os.getenv("SMTP_EMAIL")
                        sender_password = secrets_dict.get("SMTP_PASSWORD") or os.getenv("SMTP_PASSWORD")
                        
                        if smtp_server and sender_email and sender_password:
                            import smtplib
                            from email.mime.text import MIMEText
                            from email.mime.multipart import MIMEMultipart
                            
                            if int(smtp_port) == 465:
                                server = smtplib.SMTP_SSL(smtp_server, int(smtp_port))
                                server.ehlo()
                            else:
                                server = smtplib.SMTP(smtp_server, int(smtp_port))
                                server.ehlo()
                                server.starttls()
                                server.ehlo()
                            
                            server.login(sender_email, sender_password)
                            
                            techs = state.get("techs", [])
                            jobs = state.get("jobs", [])
                            locations = state.get("locations", [])
                            
                            def get_loc(loc_id):
                                return next((l for l in locations if l['id'] == loc_id), None)
                            
                            for tech in techs:
                                active_jobs = [j for j in jobs if j['techId'] == tech['id'] and j['status'] != 'Completed']
                                if not active_jobs:
                                    continue
                                    
                                subject = f"📅 Daily Assignment Reminder - {today_str}"
                                job_list_text = ""
                                for job in active_jobs:
                                    loc = get_loc(job['locationId'])
                                    loc_name = loc['name'] if loc else "Unknown Location"
                                    loc_addr = loc['address'] if loc else ""
                                    
                                    job_list_text += f"\n- {job['title']} ({job['priority']})\n  Location: {loc_name} - {loc_addr}\n  Status: {job['status']}\n"
                                    
                                body = f"Hello {tech['name']},\n\nHere is your summary of active assignments for today ({today_str}):\n{job_list_text}\n\nPlease check the 5G Security Job Board for full details and to log your work.\n"
                                
                                msg = MIMEMultipart("alternative")
                                msg['From'] = sender_email
                                msg['To'] = tech['email']
                                msg['Subject'] = subject
                                msg.attach(MIMEText(body, 'plain'))
                                try:
                                    jobs_with_locs = [(j, get_loc(j['locationId'])) for j in active_jobs]
                                    msg.attach(MIMEText(build_reminder_email_html(tech, jobs_with_locs, today_str), 'html'))
                                except Exception:
                                    pass  # plain-text version still sends

                                server.send_message(msg)
                                
                            server.quit()
                            
                        state["last_reminder_date"] = today_str
                        # Version-guarded so the scheduler can't clobber a save that
                        # happened between its load and this write (retries next loop)
                        save_state_to_db(state, expected_version=version)
                        get_logger().log(f"Sent 7 AM background reminders for {today_str}")

                # Friday 4 PM: weekly hours digest to admins (CSV attached)
                if now.weekday() == 4 and now.hour == 16:
                    from persistence_pg import load_state, save_state_to_db
                    state, version = load_state()
                    today_str = now.strftime("%Y-%m-%d")

                    if state.get("last_hours_digest_date") != today_str:
                        smtp_server = secrets_dict.get("SMTP_SERVER") or os.getenv("SMTP_SERVER")
                        smtp_port = secrets_dict.get("SMTP_PORT") or os.getenv("SMTP_PORT", 587)
                        sender_email = secrets_dict.get("SMTP_EMAIL") or os.getenv("SMTP_EMAIL")
                        sender_password = secrets_dict.get("SMTP_PASSWORD") or os.getenv("SMTP_PASSWORD")
                        recipients = state.get("adminEmails", [])

                        end_d = now.date()
                        start_d = end_d - datetime.timedelta(days=6)
                        rows = compute_hours_rows(state.get("jobs", []), state.get("techs", []),
                                                  state.get("locations", []), start_d, end_d)

                        if rows and recipients and smtp_server and sender_email and sender_password:
                            totals = {}
                            for row in rows:
                                totals[row["Tech"]] = totals.get(row["Tech"], 0) + row["Hours"]
                            detail_rows = [(tn, f"{round(th, 2)} hrs") for tn, th in sorted(totals.items(), key=lambda x: -x[1])]
                            detail_rows.append(("Total", f"{round(sum(totals.values()), 2)} hrs"))

                            subject = f"🕒 Weekly Hours Digest — {start_d} to {end_d}"
                            plain_body = (
                                f"Hours logged {start_d} to {end_d}:\n\n"
                                + "\n".join(f"{a}: {b}" for a, b in detail_rows)
                                + "\n\nFull entry list attached as CSV."
                            )
                            try:
                                html_body = build_admin_email_html(
                                    "Weekly Hours Digest",
                                    f"Hours logged {start_d} to {end_d}:",
                                    detail_rows,
                                    "The full entry list is attached as a CSV for payroll/invoicing.",
                                )
                            except Exception:
                                html_body = None

                            csv_str = pd.DataFrame(rows).sort_values(["Date", "Tech"]).to_csv(index=False)

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
                                alt = MIMEMultipart("alternative")
                                alt.attach(MIMEText(plain_body, 'plain'))
                                if html_body:
                                    alt.attach(MIMEText(html_body, 'html'))
                                msg = MIMEMultipart("mixed")
                                msg['From'] = sender_email
                                msg['To'] = recipient
                                msg['Subject'] = subject
                                msg.attach(alt)
                                attachment = MIMEApplication(csv_str.encode('utf-8'), _subtype="csv")
                                attachment.add_header('Content-Disposition', 'attachment', filename=f"hours_{start_d}_{end_d}.csv")
                                msg.attach(attachment)
                                server.send_message(msg)
                            server.quit()
                            get_logger().log(f"Sent weekly hours digest for {start_d} to {end_d}")

                        state["last_hours_digest_date"] = today_str
                        save_state_to_db(state, expected_version=version)
            except Exception as e:
                get_logger().log(f"Background reminder error: {e}")
            
            # Check every 10 minutes
            time.sleep(600)
            
    thread_name = "reminder_cron_thread"
    for t in threading.enumerate():
        if t.name == thread_name:
            return
    thread = threading.Thread(target=run, name=thread_name, daemon=True)
    thread.start()

def get_tech(tech_id):
    return next((t for t in st.session_state.techs if t['id'] == tech_id), None)

def get_location(loc_id):
    return next((l for l in st.session_state.locations if l['id'] == loc_id), None)

# Jobs with no history entry for this many days get flagged as stale
STALE_JOB_DAYS = 5

def get_job_stale_days(job):
    """Days since the last history entry on an active job.
    Returns None for completed jobs, future-scheduled jobs, or unparseable dates."""
    if job.get('status') == 'Completed':
        return None
    last_ts = None
    for r in job.get('reports', []):
        ts = r.get('timestamp', '')
        if ts and (last_ts is None or ts > last_ts):
            last_ts = ts
    base = last_ts or job.get('date', '')
    try:
        base_dt = datetime.datetime.fromisoformat(base[:19])
    except (ValueError, TypeError):
        return None
    if base_dt > now_local():
        return None
    return (now_local() - base_dt).days

def compute_hours_rows(jobs, techs, locations, start_date, end_date):
    """Flattens logged hours from job reports into rows for the Hours Report / weekly digest.
    Pure function (no Streamlit) so the background scheduler thread can use it too.
    Hours are credited to every tech listed On Site (or the report author if none listed)."""
    name_by_id = {t['id']: t['name'] for t in techs}
    loc_by_id = {l['id']: l for l in locations}
    rows = []
    for j in jobs:
        j_loc = loc_by_id.get(j.get('locationId'))
        for r in j.get('reports', []):
            try:
                hrs = float(r.get('hoursWorked') or 0)
            except (ValueError, TypeError):
                hrs = 0.0
            if hrs <= 0:
                continue
            ts = r.get('timestamp', '')[:10]
            try:
                r_date = datetime.datetime.strptime(ts, "%Y-%m-%d").date()
            except ValueError:
                continue
            if not (start_date <= r_date <= end_date):
                continue
            tech_names = [t.strip() for t in (r.get('techsOnSite') or '').split(',') if t.strip()]
            if not tech_names:
                tech_names = [name_by_id.get(r.get('techId'), 'Unknown')]
            for tn in tech_names:
                rows.append({
                    "Date": ts,
                    "Tech": tn,
                    "Job": j['title'],
                    "Location": j_loc['name'] if j_loc else "Unknown",
                    "Hours": hrs,
                    "Warranty": "Yes" if r.get('isWarranty') else "No",
                })
    return rows

@st.cache_data(ttl=1800)
def resolve_image_source(photo_source: str):
    """
    Supports:
    - R2 object keys like 'photos/...', 'signatures/...', 'jobs/...'
    - legacy local paths (if any remain)
    """
    if not photo_source or not isinstance(photo_source, str):
        return photo_source

    # Clean the path
    clean_path = photo_source.lstrip("/")

    # If it looks like an R2 key, turn into a signed URL
    prefixes = ("photos/", "signatures/", "docs/", "jobs/")
    if clean_path.startswith(prefixes):
        return get_view_url(clean_path)

    # fallback: local paths or base64 (legacy)
    return photo_source


def save_image_locally(uploaded_file):
    """Uploads an uploaded file/camera input to R2 and returns the object key.
    Images are compressed first (max 1600px, JPEG q80) so uploads are fast on cell data.
    PDFs and other non-image files pass through unchanged."""
    if uploaded_file is None:
        return None

    file_type = getattr(uploaded_file, 'type', '') or ''
    file_name = getattr(uploaded_file, 'name', 'photo.jpg') or 'photo.jpg'

    if not file_type.startswith('image/'):
        return upload_streamlit_file(uploaded_file, folder="photos")

    try:
        img = Image.open(uploaded_file)
        # Apply EXIF rotation so phone photos don't end up sideways after re-encoding
        img = ImageOps.exif_transpose(img)
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")

        max_size = 1600
        if img.width > max_size or img.height > max_size:
            img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)

        buf = io.BytesIO()
        img.save(buf, format='JPEG', quality=80, optimize=True)

        timestamp = now_local().strftime("%Y%m%d_%H%M%S")
        base_name = file_name.rsplit('.', 1)[0] or 'photo'
        key = f"photos/{timestamp}_{base_name}.jpg"
        return upload_bytes(buf.getvalue(), key, content_type="image/jpeg")
    except Exception:
        # Compression failed (corrupt/unsupported image) - upload the original instead
        try:
            uploaded_file.seek(0)
        except Exception:
            pass
        return upload_streamlit_file(uploaded_file, folder="photos")

def save_document_locally(uploaded_file):
    """Uploads an uploaded file (PDF/etc) to R2 and returns the object key."""
    return upload_streamlit_file(uploaded_file, folder="docs")

def get_google_maps_url(address):
    """Generates a Google Maps Search URL based on address."""
    if not address: return None
    return f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote(address)}"

def get_api_key():
    # Try getting from Streamlit secrets, then Env, then return None
    if "GEMINI_API_KEY" in st.secrets:
        return st.secrets["GEMINI_API_KEY"]
    return os.getenv("GEMINI_API_KEY") or os.getenv("API_KEY")

@st.cache_resource
def get_available_model(api_key):
    """
    Dynamically lists models available to the API key and returns the client and best model name.
    Prefers current stable Flash models. (Google retired the Gemini 1.x family -
    the old hardcoded 1.5 names now 404.)
    """
    client = genai.Client(api_key=api_key)
    logger = get_logger()

    def _gen_actions(m):
        # google-genai SDK exposes 'supported_actions'; the legacy SDK used
        # 'supported_generation_methods'. Check both so the filter actually works.
        return getattr(m, 'supported_actions', None) or getattr(m, 'supported_generation_methods', None)

    try:
        all_models = list(client.models.list())
        logger.log(f"Discovery: Found {len(all_models)} available models.")

        # Text-capable Gemini models only - specialty variants (TTS, image,
        # live audio, embeddings) reject plain generate_content calls.
        EXCLUDE = ('tts', 'image', 'audio', 'live', 'embed', 'veo', 'imagen', 'aqa')
        candidates = []
        for m in all_models:
            lname = m.name.lower()
            if 'gemini' not in lname:
                continue
            if any(x in lname for x in EXCLUDE):
                continue
            actions = _gen_actions(m)
            if actions and not ('generateContent' in actions or 'generate_content' in actions):
                continue
            candidates.append(m)

        # Preference order: newest stable Flash -> rolling alias -> 2.0 Flash -> Pro -> any Flash
        preferences = ['gemini-2.5-flash', 'gemini-flash-latest', 'gemini-2.0-flash', 'gemini-2.5-pro', 'flash']

        # Pass 1: exact model names
        for pref in preferences:
            best = next((m for m in candidates if m.name.lower().split('/')[-1] == pref), None)
            if best:
                logger.log(f"Using Gemini model: {best.name}")
                return client, best.name

        # Pass 2: substring match, preferring stable over preview/experimental builds
        for pref in preferences:
            best = next((m for m in candidates if pref in m.name.lower() and 'preview' not in m.name.lower() and 'exp' not in m.name.lower()), None)
            if not best:
                best = next((m for m in candidates if pref in m.name.lower()), None)
            if best:
                logger.log(f"Using Gemini model: {best.name}")
                return client, best.name

        if candidates:
            logger.log(f"Using first available Gemini model: {candidates[0].name}")
            return client, candidates[0].name

        logger.log("No usable Gemini models found via listing. Defaulting to gemini-flash-latest.")
        return client, 'gemini-flash-latest'

    except Exception as e:
        logger.log(f"Error listing models: {e}. Defaulting to gemini-flash-latest.")
        return client, 'gemini-flash-latest'

def generate_technician_summary(notes, job_title):
    """Uses Gemini to summarize the daily work for the PDF Report."""
    api_key = get_api_key()
    if not api_key: return None
    client, model_name = get_available_model(api_key)
    prompt = f"Summarize the following technician notes for job '{job_title}' into a concise, professional paragraph (approx 50 words) suitable for a client report:\n\n{notes}"
    try:
        response = client.models.generate_content(model=model_name, contents=prompt)
        return response.text
    except:
        return None

def transcribe_audio(audio_file):
    """Transcribes audio using Gemini 1.5 Flash."""
    api_key = get_api_key()
    if not api_key: return None
    
    client, model_name = get_available_model(api_key)
    
    try:
        audio_bytes = audio_file.read()
        response = client.models.generate_content(
            model=model_name,
            contents=[
                types.Content(
                    parts=[
                        types.Part.from_text(text="Transcribe this audio note exactly as spoken. Do not add any commentary."),
                        types.Part.from_bytes(data=audio_bytes, mime_type="audio/wav")
                    ]
                )
            ]
        )
        return response.text
    except Exception as e:
        return None

def create_mailto_link(job, tech, location):
    """Generates a mailto link for client-side email sending."""
    subject = f"Assignment: {job['title']}"
    
    contact_info = ""
    if location.get('contact_name') or location.get('contact_phone'):
        contact_info = f"\nContact: {location.get('contact_name', 'N/A')} ({location.get('contact_phone', 'N/A')})"
        
    body = f"""Hello {tech['name']},

New Assignment:
{job['title']} ({job['priority']})

Location:
{location['name']}
{location['address']}{contact_info}

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
    client, model_name = get_available_model(api_key)
    prompt = f"You are an address autocomplete tool. The user typed: '{partial_address}'. Return the most likely full address. If ambiguous, return the best guess. Return ONLY the address text, no other words."
    try:
        response = client.models.generate_content(model=model_name, contents=prompt)
        return response.text.strip()
    except:
        return partial_address

@st.cache_data(ttl=3600) # Cache for 1 hour
def get_lat_lon_from_address(address):
    """Uses Open-Meteo Geocoding API to geocode an address to Lat/Lon.
       Falls back to city search if full address fails.
    """
    try:
        # Helper to query Open-Meteo
        def query_open_meteo(query):
            if not query or not query.strip(): return {}
            encoded_query = urllib.parse.quote(query.strip())
            url = f"https://geocoding-api.open-meteo.com/v1/search?name={encoded_query}&count=1&language=en&format=json"
            headers = {'User-Agent': '5GSecurityJobBoard/1.0'}
            try:
                response = requests.get(url, headers=headers, timeout=5)
                return response.json()
            except:
                return {}

        # 1. Try full address (unlikely to work for streets, but good for "City, State")
        data = query_open_meteo(address)
        
        if 'results' in data and data['results']:
            result = data['results'][0]
            return result.get('latitude'), result.get('longitude')
        
        # 2. Fallback: Try to extract City from "Street, City, State" format
        parts = [p.strip() for p in address.split(',')]
        if len(parts) >= 2:
            # Heuristic:
            # If 3+ parts (e.g. "Street, City, State, Country"), City is likely index 1.
            # If 2 parts (e.g. "City, State"), City is likely index 0.
            potential_city = parts[1] if len(parts) >= 3 else parts[0]
            
            # Avoid searching for things that look like states or zip codes if possible, 
            # but Open-Meteo is robust.
            data = query_open_meteo(potential_city)
            if 'results' in data and data['results']:
                result = data['results'][0]
                return result.get('latitude'), result.get('longitude')

        get_logger().log(f"Geocoding failed for '{address}': No results found from Open-Meteo")
        return None, None
            
    except Exception as e:
        get_logger().log(f"Geocoding failed for '{address}': {e}")
        return None, None

@st.cache_data(ttl=1800) # Cache for 30 mins (weather barely moves, and it's just informational)
def get_weather(lat, lon):
    """Fetches current weather from Open-Meteo (Free, No Key)."""
    try:
        # Ensure floats
        lat = float(lat)
        lon = float(lon)

        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m,weather_code&temperature_unit=fahrenheit&timezone=auto"
        headers = {'User-Agent': '5GSecurityJobBoard/1.0'}
        r = requests.get(url, headers=headers, timeout=3)
        
        if r.status_code != 200:
            return None
            
        data = r.json()
        
        if 'error' in data:
            return None

        current = data.get('current', {})
        temp = current.get('temperature_2m')
        code = current.get('weather_code')
        
        if temp is None:
            return None
            
        # Ensure temp is a valid number
        try:
            float(temp)
        except (ValueError, TypeError):
            return None
        
        # Simple WMO code map
        condition = "Unknown"
        if code is not None:
            try:
                code = int(code)
                if code == 0: condition = "☀️ Clear"
                elif code in [1, 2, 3]: condition = "⛅ Partly Cloudy"
                elif code in [45, 48]: condition = "🌫️ Foggy"
                elif code in [51, 53, 55]: condition = "🌧️ Drizzle"
                elif code in [61, 63, 65]: condition = "🌧️ Rain"
                elif code in [71, 73, 75]: condition = "❄️ Snow"
                elif code in [95, 96, 99]: condition = "⛈️ Thunderstorm"
            except (ValueError, TypeError):
                pass
            
        return f"{condition} {temp}°F"
    except Exception as e:
        return None

def create_ics_file(job, location):
    """Generates an iCalendar (.ics) file content for the job."""
    try:
        # Parse job date
        if 'T' in job['date']:
            dt_start = datetime.datetime.fromisoformat(job['date'])
        else:
            dt_start = datetime.datetime.strptime(job['date'][:10], "%Y-%m-%d")
            # Default to 9 AM if no time
            dt_start = dt_start.replace(hour=9, minute=0)
            
        # Assume 2 hour duration default
        dt_end = dt_start + datetime.timedelta(hours=2)
        
        # Format dates for ICS (YYYYMMDDTHHMMSSZ)
        # We'll use floating time (no Z) to respect local time of the user/device
        fmt = "%Y%m%dT%H%M%S"
        start_str = dt_start.strftime(fmt)
        end_str = dt_end.strftime(fmt)
        now_str = now_local().strftime(fmt)
        
        loc_str = f"{location['name']} - {location['address']}" if location else "Unknown Location"
        desc = f"Priority: {job['priority']}\\nType: {job['type']}\\n\\n{job['description']}"
        
        ics_content = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//5G Security//Job Board//EN
BEGIN:VEVENT
UID:{job['id']}@5gsecurity.app
DTSTAMP:{now_str}
DTSTART:{start_str}
DTEND:{end_str}
SUMMARY:🛡️ {job['title']}
DESCRIPTION:{desc}
LOCATION:{loc_str}
END:VEVENT
END:VCALENDAR"""
        return ics_content
    except Exception as e:
        return None

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
@st.cache_data(ttl=3600, show_spinner=False)
def get_image_bytes(url):
    """Fetches image bytes from a URL and caches them."""
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            return response.content
    except Exception:
        pass
    return None

@st.cache_data(show_spinner="Generating PDF...")
def generate_job_pdf(job, tech, location, report):
    """Generates a styled PDF report for a job (completion or daily field report)."""
    if not HAS_REPORTLAB:
        return None

    is_completion = 'completion_checklist' in report
    report_type = "Job Completion Report" if is_completion else "Daily Field Report"
    generated_str = now_local().strftime('%B %d, %Y at %I:%M %p')

    # Brand palette (mirrors the app theme)
    BRAND_RED = colors.HexColor("#b91c1c")
    BRAND_DARK = colors.HexColor("#18181b")
    INK = colors.HexColor("#27272a")
    MUTED = colors.HexColor("#71717a")
    LIGHT = colors.HexColor("#f4f4f5")
    BORDER = colors.HexColor("#e4e4e7")

    def esc(s):
        """Escape text for ReportLab Paragraph markup."""
        return str(s if s is not None else "").replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    def header_footer(canv, doc):
        w, h = letter
        canv.saveState()
        # Header band
        canv.setFillColor(BRAND_DARK)
        canv.rect(0, h - 80, w, 80, fill=1, stroke=0)
        canv.setFillColor(BRAND_RED)
        canv.rect(0, h - 84, w, 4, fill=1, stroke=0)
        canv.setFillColor(colors.white)
        canv.setFont("Helvetica-Bold", 20)
        canv.drawString(46, h - 48, "5G SECURITY")
        canv.setFillColor(colors.HexColor("#d4d4d8"))
        canv.setFont("Helvetica", 10)
        canv.drawString(46, h - 64, report_type)
        canv.setFont("Helvetica", 8)
        canv.drawRightString(w - 46, h - 48, f"Generated {generated_str}")
        canv.drawRightString(w - 46, h - 64, f"Job ID: {job.get('id', '')}")
        # Footer
        canv.setStrokeColor(BORDER)
        canv.setLineWidth(0.5)
        canv.line(46, 46, w - 46, 46)
        canv.setFont("Helvetica", 8)
        canv.setFillColor(MUTED)
        canv.drawString(46, 34, "5G Security  |  Cameras - Access Control - Alarm Systems - Cabling")
        canv.drawRightString(w - 46, 34, f"Page {canv.getPageNumber()}")
        canv.restoreState()

    styles = getSampleStyleSheet()
    s_title = ParagraphStyle('JobTitle', parent=styles['Heading1'], fontName="Helvetica-Bold",
                             fontSize=16, textColor=INK, spaceAfter=2)
    s_sub = ParagraphStyle('Sub', parent=styles['Normal'], fontSize=10, textColor=MUTED, spaceAfter=4)
    s_section = ParagraphStyle('Section', parent=styles['Heading2'], fontName="Helvetica-Bold",
                               fontSize=11, textColor=BRAND_RED, spaceBefore=16, spaceAfter=6)
    s_body = ParagraphStyle('Body', parent=styles['Normal'], fontName="Helvetica",
                            fontSize=9.5, leading=14, textColor=INK)
    s_label = ParagraphStyle('Label', parent=s_body, textColor=MUTED, fontSize=8)
    s_value = ParagraphStyle('Value', parent=s_body, fontName="Helvetica-Bold")
    s_italic = ParagraphStyle('Ital', parent=s_body, fontName="Helvetica-Oblique")
    s_caption = ParagraphStyle('Caption', parent=s_label, fontSize=7.5, spaceBefore=2)

    avail = letter[0] - 92  # usable width inside margins

    def info_table(rows):
        """rows: list of (label, value, label, value) tuples rendered as a styled grid."""
        data = []
        for r in rows:
            cells = []
            for i, cell in enumerate(r):
                if i % 2 == 0:
                    cells.append(Paragraph(esc(cell).upper(), s_label))
                else:
                    cells.append(Paragraph(esc(cell), s_value))
            data.append(cells)
        t = Table(data, colWidths=[avail * 0.16, avail * 0.40, avail * 0.16, avail * 0.28])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), LIGHT),
            ('BACKGROUND', (2, 0), (2, -1), LIGHT),
            ('GRID', (0, 0), (-1, -1), 0.5, BORDER),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
        ]))
        return t

    loc_name = location['name'] if location else 'Unknown'
    loc_addr = location['address'] if location else ''
    tech_name = tech['name'] if tech else 'Unassigned'

    story = []

    # Title block
    story.append(Paragraph(esc(job['title']), s_title))
    story.append(Paragraph(f"{esc(loc_name)} &mdash; {esc(loc_addr)}", s_sub))

    # Job details
    story.append(Paragraph("JOB DETAILS", s_section))
    story.append(info_table([
        ("Technician", tech_name, "Status", job.get('status', 'N/A')),
        ("Job Type", job.get('type', 'N/A'), "Priority", job.get('priority', 'N/A')),
        ("Scheduled", str(job.get('date', ''))[:10], "Warranty Work", "Yes" if report.get('isWarranty') else "No"),
    ]))

    # Field report data
    story.append(Paragraph("FIELD REPORT", s_section))
    story.append(info_table([
        ("Techs On Site", report.get('techsOnSite') or 'N/A', "Hours Worked", report.get('hoursWorked') or 'N/A'),
        ("Time Arrived", report.get('timeArrived') or 'N/A', "Time Finished", report.get('timeDeparted') or 'N/A'),
        ("Parts Used", report.get('partsUsed') or 'None', "Billable Items", report.get('billableItems') or 'None'),
    ]))

    # AI work summary (accent-boxed)
    ai_summary = report.get("ai_summary")
    if ai_summary:
        story.append(Paragraph("WORK SUMMARY", s_section))
        box = Table([[Paragraph(esc(ai_summary), s_italic)]], colWidths=[avail])
        box.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), LIGHT),
            ('LINEBEFORE', (0, 0), (0, -1), 2, BRAND_RED),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('LEFTPADDING', (0, 0), (-1, -1), 10),
            ('RIGHTPADDING', (0, 0), (-1, -1), 10),
        ]))
        story.append(box)
        story.append(Paragraph("Summary generated by AI from technician notes.", s_caption))

    # Completion checklist
    checklist = report.get("completion_checklist")
    if checklist:
        items = [Paragraph("COMPLETION CHECKLIST", s_section)]
        for item in checklist:
            items.append(Paragraph(
                f'<font name="ZapfDingbats" color="#15803d">4</font>&nbsp;&nbsp;{esc(item)}', s_body))
        story.append(KeepTogether(items))

    # Technician notes
    notes = report.get("content", "")
    if notes:
        story.append(Paragraph("TECHNICIAN NOTES", s_section))
        for line in notes.split('\n'):
            if line.strip():
                story.append(Paragraph(esc(line), s_body))
            else:
                story.append(Spacer(1, 6))

    # Customer signature
    signature_key = report.get("signature_key")
    if signature_key:
        try:
            sig_url = get_view_url(signature_key, expires_seconds=3600)
            sig_bytes = get_image_bytes(sig_url)
            if sig_bytes:
                story.append(KeepTogether([
                    Paragraph("CUSTOMER SIGN-OFF", s_section),
                    RLImage(BytesIO(sig_bytes), width=180, height=60),
                    Paragraph("Customer Digital Signature", s_caption),
                ]))
        except Exception:
            pass

    # Site photos (own page, two per row)
    photos = report.get("photos", [])
    if photos:
        photo_flowables = []
        seen_keys = set()
        for photo_key in photos:
            if photo_key in seen_keys:
                continue
            seen_keys.add(photo_key)
            try:
                photo_url = get_view_url(photo_key, expires_seconds=3600)
                img_bytes = get_image_bytes(photo_url)
                if not img_bytes:
                    continue
                img = Image.open(BytesIO(img_bytes))
                if img.mode in ("RGBA", "P"):
                    img = img.convert("RGB")
                if img.width > 1024 or img.height > 1024:
                    img.thumbnail((1024, 1024), Image.Resampling.LANCZOS)
                jb = io.BytesIO()
                img.save(jb, format='JPEG', quality=75, optimize=True)
                jb.seek(0)
                # Fit each photo into its half-page cell, preserving aspect ratio
                cell_w, cell_h = (avail / 2) - 16, 190
                ratio = min(cell_w / img.width, cell_h / img.height)
                photo_flowables.append(RLImage(jb, width=img.width * ratio, height=img.height * ratio))
            except Exception:
                continue

        if photo_flowables:
            story.append(PageBreak())
            story.append(Paragraph("SITE PHOTOS", s_section))
            rows = []
            for i in range(0, len(photo_flowables), 2):
                pair = photo_flowables[i:i + 2]
                if len(pair) == 1:
                    pair.append("")
                rows.append(pair)
            pt = Table(rows, colWidths=[avail / 2, avail / 2])
            pt.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('TOPPADDING', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ]))
            story.append(pt)

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer, pagesize=letter,
        leftMargin=46, rightMargin=46, topMargin=104, bottomMargin=64,
        title=f"5G Security - {report_type}",
    )
    try:
        doc.build(story, onFirstPage=header_footer, onLaterPages=header_footer)
    except Exception:
        return None

    buffer.seek(0)
    return buffer.getvalue()

def build_assignment_email_html(job, tech, location):
    """Branded HTML body for the new-assignment email (plain text is attached as fallback)."""
    def esc(s):
        return str(s if s is not None else "").replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    priority_colors = {"Critical": "#ef4444", "High": "#dc2626", "Medium": "#b45309", "Low": "#52525b"}
    p_color = priority_colors.get(job.get('priority'), "#52525b")

    first_name = (tech.get('name') or 'there').split()[0]
    loc_name = location.get('name', 'Unknown') if location else 'Unknown'
    loc_addr = location.get('address', '') if location else ''
    map_url = get_google_maps_url(loc_addr) if loc_addr else None
    app_url = os.getenv("APP_URL", "").rstrip("/")

    detail_rows = [
        ("Location", esc(loc_name)),
        ("Address", esc(loc_addr)),
        ("Type", esc(job.get('type', 'N/A'))),
        ("Scheduled", esc(str(job.get('date', ''))[:10])),
    ]
    if location and (location.get('contact_name') or location.get('contact_phone')):
        detail_rows.append(("Contact", f"{esc(location.get('contact_name', 'N/A'))} ({esc(location.get('contact_phone', 'N/A'))})"))

    rows_html = ""
    for label, value in detail_rows:
        rows_html += f"""
            <tr>
                <td style="padding:8px 12px;background-color:#f4f4f5;color:#71717a;font-size:11px;font-weight:bold;text-transform:uppercase;border-bottom:1px solid #e4e4e7;width:110px;">{label}</td>
                <td style="padding:8px 12px;color:#27272a;font-size:14px;border-bottom:1px solid #e4e4e7;">{value}</td>
            </tr>"""

    buttons_html = ""
    if map_url:
        buttons_html += f"""<a href="{map_url}" style="display:inline-block;background-color:#b91c1c;color:#ffffff;padding:11px 22px;border-radius:6px;text-decoration:none;font-weight:bold;font-size:14px;margin-right:10px;">&#128205; Get Directions</a>"""
    if app_url:
        buttons_html += f"""<a href="{app_url}" style="display:inline-block;background-color:#18181b;color:#ffffff;padding:11px 22px;border-radius:6px;text-decoration:none;font-weight:bold;font-size:14px;">Open Job Board</a>"""

    description_html = esc(job.get('description', '')).replace('\n', '<br>')

    return f"""
<html>
<body style="margin:0;padding:0;background-color:#f4f4f5;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f4f5;">
<tr><td align="center" style="padding:24px 12px;">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background-color:#ffffff;border-radius:8px;overflow:hidden;font-family:Arial,Helvetica,sans-serif;border:1px solid #e4e4e7;">
    <tr>
        <td style="background-color:#18181b;padding:22px 32px;border-bottom:4px solid #b91c1c;">
            <span style="color:#ffffff;font-size:22px;font-weight:bold;letter-spacing:1px;">5G SECURITY</span><br>
            <span style="color:#a1a1aa;font-size:13px;">New Job Assignment</span>
        </td>
    </tr>
    <tr>
        <td style="padding:28px 32px;">
            <p style="color:#27272a;font-size:14px;margin:0 0 18px 0;">Hello {esc(first_name)}, you've been assigned a new job:</p>
            <h2 style="color:#18181b;font-size:19px;margin:0 0 10px 0;">{esc(job.get('title', ''))}</h2>
            <span style="display:inline-block;background-color:{p_color};color:#ffffff;padding:3px 12px;border-radius:12px;font-size:12px;font-weight:bold;">{esc(job.get('priority', 'N/A'))} Priority</span>
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin:20px 0;border:1px solid #e4e4e7;border-radius:6px;border-collapse:separate;overflow:hidden;">
                {rows_html}
            </table>
            <p style="color:#71717a;font-size:11px;font-weight:bold;text-transform:uppercase;margin:0 0 6px 0;">Description</p>
            <p style="color:#27272a;font-size:14px;line-height:1.6;margin:0 0 24px 0;border-left:3px solid #b91c1c;padding-left:12px;">{description_html}</p>
            {buttons_html}
        </td>
    </tr>
    <tr>
        <td style="background-color:#f4f4f5;padding:14px 32px;color:#71717a;font-size:11px;border-top:1px solid #e4e4e7;">
            5G Security &nbsp;|&nbsp; Cameras &middot; Access Control &middot; Alarm Systems &middot; Cabling
        </td>
    </tr>
</table>
</td></tr>
</table>
</body>
</html>"""

def build_reminder_email_html(tech, jobs_with_locs, today_str):
    """Branded HTML body for the daily reminder email.
    jobs_with_locs: list of (job, location) tuples. Pure string-building, safe in threads."""
    def esc(s):
        return str(s if s is not None else "").replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    priority_colors = {"Critical": "#ef4444", "High": "#dc2626", "Medium": "#b45309", "Low": "#52525b"}
    first_name = (tech.get('name') or 'there').split()[0]
    app_url = os.getenv("APP_URL", "").rstrip("/")

    cards_html = ""
    for job, loc in jobs_with_locs:
        loc_name = loc['name'] if loc else "Unknown Location"
        loc_addr = loc['address'] if loc else ""
        p_color = priority_colors.get(job.get('priority'), "#52525b")
        map_url = get_google_maps_url(loc_addr) if loc_addr else None
        addr_html = esc(loc_addr)
        if map_url:
            addr_html = f'<a href="{map_url}" style="color:#b91c1c;text-decoration:none;">{esc(loc_addr)}</a>'
        cards_html += f"""
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e4e4e7;border-left:4px solid {p_color};border-radius:6px;border-collapse:separate;margin:0 0 12px 0;">
                <tr><td style="padding:14px 16px;">
                    <span style="color:#18181b;font-size:15px;font-weight:bold;">{esc(job.get('title', ''))}</span>
                    <span style="display:inline-block;background-color:{p_color};color:#ffffff;padding:2px 10px;border-radius:10px;font-size:11px;font-weight:bold;margin-left:8px;">{esc(job.get('priority', ''))}</span><br>
                    <span style="color:#71717a;font-size:12px;">Status: {esc(job.get('status', ''))}</span><br>
                    <span style="color:#27272a;font-size:13px;font-weight:bold;">&#128205; {esc(loc_name)}</span><br>
                    <span style="font-size:12px;">{addr_html}</span>
                </td></tr>
            </table>"""

    button_html = ""
    if app_url:
        button_html = f"""<a href="{app_url}" style="display:inline-block;background-color:#b91c1c;color:#ffffff;padding:11px 22px;border-radius:6px;text-decoration:none;font-weight:bold;font-size:14px;">Open Job Board</a>"""

    plural = "s" if len(jobs_with_locs) != 1 else ""
    return f"""
<html>
<body style="margin:0;padding:0;background-color:#f4f4f5;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f4f5;">
<tr><td align="center" style="padding:24px 12px;">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background-color:#ffffff;border-radius:8px;overflow:hidden;font-family:Arial,Helvetica,sans-serif;border:1px solid #e4e4e7;">
    <tr>
        <td style="background-color:#18181b;padding:22px 32px;border-bottom:4px solid #b91c1c;">
            <span style="color:#ffffff;font-size:22px;font-weight:bold;letter-spacing:1px;">5G SECURITY</span><br>
            <span style="color:#a1a1aa;font-size:13px;">Daily Assignment Reminder &mdash; {esc(today_str)}</span>
        </td>
    </tr>
    <tr>
        <td style="padding:28px 32px;">
            <p style="color:#27272a;font-size:14px;margin:0 0 18px 0;">Good morning {esc(first_name)} &mdash; you have <span style="font-weight:bold;">{len(jobs_with_locs)} active assignment{plural}</span> today:</p>
            {cards_html}
            <p style="color:#71717a;font-size:12px;margin:18px 0 18px 0;">Check the job board for full details and to log your work.</p>
            {button_html}
        </td>
    </tr>
    <tr>
        <td style="background-color:#f4f4f5;padding:14px 32px;color:#71717a;font-size:11px;border-top:1px solid #e4e4e7;">
            5G Security &nbsp;|&nbsp; Cameras &middot; Access Control &middot; Alarm Systems &middot; Cabling
        </td>
    </tr>
</table>
</td></tr>
</table>
</body>
</html>"""

def build_admin_email_html(header_label, intro, detail_rows, footer_note):
    """Branded HTML wrapper for short admin notification emails (the PDF attachment is the payload)."""
    def esc(s):
        return str(s if s is not None else "").replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    rows_html = ""
    for label, value in detail_rows:
        rows_html += f"""
            <tr>
                <td style="padding:8px 12px;background-color:#f4f4f5;color:#71717a;font-size:11px;font-weight:bold;text-transform:uppercase;border-bottom:1px solid #e4e4e7;width:120px;">{esc(label)}</td>
                <td style="padding:8px 12px;color:#27272a;font-size:14px;border-bottom:1px solid #e4e4e7;">{esc(value)}</td>
            </tr>"""

    return f"""
<html>
<body style="margin:0;padding:0;background-color:#f4f4f5;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f4f5;">
<tr><td align="center" style="padding:24px 12px;">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background-color:#ffffff;border-radius:8px;overflow:hidden;font-family:Arial,Helvetica,sans-serif;border:1px solid #e4e4e7;">
    <tr>
        <td style="background-color:#18181b;padding:22px 32px;border-bottom:4px solid #b91c1c;">
            <span style="color:#ffffff;font-size:22px;font-weight:bold;letter-spacing:1px;">5G SECURITY</span><br>
            <span style="color:#a1a1aa;font-size:13px;">{esc(header_label)}</span>
        </td>
    </tr>
    <tr>
        <td style="padding:28px 32px;">
            <p style="color:#27272a;font-size:14px;margin:0 0 18px 0;">{esc(intro)}</p>
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin:0 0 18px 0;border:1px solid #e4e4e7;border-radius:6px;border-collapse:separate;overflow:hidden;">
                {rows_html}
            </table>
            <p style="color:#71717a;font-size:13px;margin:0;">&#128206; {esc(footer_note)}</p>
        </td>
    </tr>
    <tr>
        <td style="background-color:#f4f4f5;padding:14px 32px;color:#71717a;font-size:11px;border-top:1px solid #e4e4e7;">
            5G Security &nbsp;|&nbsp; Cameras &middot; Access Control &middot; Alarm Systems &middot; Cabling
        </td>
    </tr>
</table>
</td></tr>
</table>
</body>
</html>"""

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
    
    contact_line = ""
    if location.get('contact_name') or location.get('contact_phone'):
        contact_line = f"   Contact: {location.get('contact_name', 'N/A')} ({location.get('contact_phone', 'N/A')})"
        
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
{contact_line}

   DESCRIPTION
   --------------------------------------------------
   {job['description']}

   Please check the 5G Security Job Board for full details.
   """

    # If no credentials, we return False to trigger fallback UI
    if not (smtp_server and sender_email and sender_password):
        # 

        return False

    # multipart/alternative: clients render the HTML version, plain text is the fallback
    msg = MIMEMultipart("alternative")
    msg['From'] = sender_email
    msg['To'] = tech['email']
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    try:
        msg.attach(MIMEText(build_assignment_email_html(job, tech, location), 'html'))
    except Exception:
        pass  # plain-text version still sends

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
        st.warning("No admin emails configured to receive completion notification.")
        return

    # Generate PDF
    try:
        pdf_bytes = generate_job_pdf(job, tech, location, report_data)
        if pdf_bytes:
            pdf_size_mb = len(pdf_bytes) / (1024 * 1024)
            get_logger().log(f"Generated PDF for job {job['id']}: {pdf_size_mb:.2f} MB")
            if pdf_size_mb > 20:
                st.warning(f"⚠️ PDF report is very large ({pdf_size_mb:.2f} MB). It may be rejected by some email servers.")
    except Exception as e:
        st.error(f"Failed to generate PDF report: {e}")
        pdf_bytes = None

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
        st.warning("SMTP not configured. Completion email could not be sent.")
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
        
        # Styled HTML body (plain text rides along as the fallback)
        try:
            html_body = build_admin_email_html(
                "Job Completed",
                f"“{job['title']}” has been marked as Completed.",
                [
                    ("Job", job['title']),
                    ("Technician", tech['name'] if tech else 'Unknown'),
                    ("Location", location['name'] if location else 'Unknown'),
                    ("Hours Worked", report_data.get('hoursWorked') or 'N/A'),
                ],
                "The full completion report is attached as a PDF.",
            )
        except Exception:
            html_body = None

        for recipient in recipients:
            # Create fresh message for each recipient to avoid header issues.
            # mixed( alternative(plain, html), pdf ) so the attachment shows in all clients.
            alt = MIMEMultipart("alternative")
            alt.attach(MIMEText(body, 'plain'))
            if html_body:
                alt.attach(MIMEText(html_body, 'html'))

            msg = MIMEMultipart("mixed")
            msg['From'] = sender_email
            msg['To'] = recipient
            msg['Subject'] = subject
            msg.attach(alt)

            if pdf_bytes:
                attachment = MIMEApplication(pdf_bytes, _subtype="pdf")
                attachment.add_header('Content-Disposition', 'attachment', filename=f"Report_{job['id']}.pdf")
                msg.attach(attachment)
            
            server.send_message(msg)
            
        server.quit()
        st.toast("📧 Completion notification sent to Admins", icon="✅")
    except Exception as e:
        st.error(f"Failed to send completion email: {str(e)}")

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
    try:
        pdf_bytes = generate_job_pdf(job, tech, location, report_data)
        if pdf_bytes:
            pdf_size_mb = len(pdf_bytes) / (1024 * 1024)
            get_logger().log(f"Generated Daily PDF for job {job['id']}: {pdf_size_mb:.2f} MB")
            if pdf_size_mb > 20:
                st.warning(f"⚠️ PDF report is very large ({pdf_size_mb:.2f} MB). It may be rejected by some email servers.")
    except Exception as e:
        st.error(f"Failed to generate PDF report: {e}")
        pdf_bytes = None

    # Prepare email content
    subject = f"📝 Daily Report: {job['title']}"
    body = f"""
    DAILY FIELD REPORT
    
    Job:      {job['title']}
    Tech:     {tech['name'] if tech else 'Unknown'}
    Location: {location['name'] if location else 'Unknown'}
    Date:     {now_local().strftime('%Y-%m-%d')}
    
    Please see the attached PDF report for today's details.
    """

    if not (smtp_server and sender_email and sender_password):
        st.error("SMTP not configured. Daily report email could not be sent.")
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
        
        # Styled HTML body (plain text rides along as the fallback)
        try:
            html_body = build_admin_email_html(
                "Daily Field Report",
                f"A daily field report was submitted for “{job['title']}”.",
                [
                    ("Job", job['title']),
                    ("Technician", tech['name'] if tech else 'Unknown'),
                    ("Location", location['name'] if location else 'Unknown'),
                    ("Date", now_local().strftime('%Y-%m-%d')),
                    ("Hours Worked", report_data.get('hoursWorked') or 'N/A'),
                ],
                "Today's full report is attached as a PDF.",
            )
        except Exception:
            html_body = None

        for recipient in recipients:
            # Create fresh message for each recipient.
            # mixed( alternative(plain, html), pdf ) so the attachment shows in all clients.
            alt = MIMEMultipart("alternative")
            alt.attach(MIMEText(body, 'plain'))
            if html_body:
                alt.attach(MIMEText(html_body, 'html'))

            msg = MIMEMultipart("mixed")
            msg['From'] = sender_email
            msg['To'] = recipient
            msg['Subject'] = subject
            msg.attach(alt)

            if pdf_bytes:
                attachment = MIMEApplication(pdf_bytes, _subtype="pdf")
                attachment.add_header('Content-Disposition', 'attachment', filename=f"DailyReport_{job['id']}_{now_local().strftime('%Y%m%d')}.pdf")
                msg.attach(attachment)
            
            server.send_message(msg)
            
        server.quit()
        st.toast("📧 Daily Report sent to Admins", icon="✅")
    except Exception as e:
        st.error(f"Failed to send daily report email: {str(e)}")

def send_daily_reminders():
    """Sends daily reminder emails to techs with active assignments (Mon-Fri only)."""
    
    # 1. Check Date & Time
    now = now_local()
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
            
            msg = MIMEMultipart("alternative")
            msg['From'] = sender_email
            msg['To'] = tech['email']
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            try:
                jobs_with_locs = [(j, get_location(j['locationId'])) for j in active_jobs]
                msg.attach(MIMEText(build_reminder_email_html(tech, jobs_with_locs, today_str), 'html'))
            except Exception:
                pass  # plain-text version still sends

            server.send_message(msg)
            techs_emailed += 1
            
        server.quit()
        
        # Update State
        st.session_state.last_reminder_date = today_str
        save_state(invalidate_briefing=False)
        
        if techs_emailed > 0:
            st.toast(f"📧 Sent daily reminders to {techs_emailed} technicians.", icon="✅")
            
    except Exception as e:
        pass

def generate_morning_briefing():
    """Generates the morning briefing using Gemini."""
    api_key = get_api_key()
    if not api_key:
        return "⚠️ API Key missing. Please set GEMINI_API_KEY in secrets.toml or environment."
    
    if not st.session_state.jobs:
        return "No active jobs to analyze. Please add jobs via the 'New Job' button."

    # Use dynamic model selector
    client, model_name = get_available_model(api_key)

    active_jobs = [j for j in st.session_state.jobs if j['status'] != 'Completed']
    critical_jobs = [j for j in active_jobs if j['priority'] in ['Critical', 'High']]

    stale_lines = []
    for j in active_jobs:
        d = get_job_stale_days(j)
        if d is not None and d >= STALE_JOB_DAYS:
            stale_lines.append(f"- {j['title']} ({d} days without an update)")

    current_date = now_local().strftime("%B %d, %Y")

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

     Stale Jobs (no updates in {STALE_JOB_DAYS}+ days):
     {chr(10).join(stale_lines) if stale_lines else "None"}

     Format:
     Start with the header: **Morning Briefing: 5G Security - {current_date}**

     Then:
     1. Security Focus (Motivation)
     2. Critical Focus (Briefly summarize the active jobs list, highlighting critical ones if any. If there are stale jobs, call them out and ask for a status update on them.)
     3. Safety Tip.

     Max 150 words. No markdown headers (#), use Bold instead.
   """
    
    try:
        response = client.models.generate_content(model=model_name, contents=prompt)
        return response.text
    except Exception as e:
        err_msg = str(e)
        if "429" in err_msg or "RESOURCE_EXHAUSTED" in err_msg:
            return "⏳ **System is currently busy (Rate Limit or Quota Reached).** \n\nPlease wait a minute and click 'Refresh Briefing' below to try again. If you just upgraded to 'Paid 1', it may take a few minutes to fully activate across all regions."
        
        # Help text for Paid 1 users or other errors
        help_tip = ""
        if "API_KEY_INVALID" in err_msg:
            help_tip = "\n\n💡 **Tip:** Your API Key appears to be invalid. Check AI Studio settings."
        elif "PERMISSION_DENIED" in err_msg:
            help_tip = "\n\n💡 **Tip:** Permission denied. If you just upgraded to 'Paid 1', it may take a few minutes to activate."
            
        return f"Error generating briefing: {err_msg}{help_tip}"

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
        job_type = c1.selectbox("Type", ["Service", "Project", "Leads"])
        priority = c2.selectbox("Priority", ["Medium", "Low", "High", "Critical"])
        
        # Date Selection
        job_date = st.date_input("Scheduled Date", value=now_local())
        
        # Location Selection
        loc_map = {l['name']: l['id'] for l in st.session_state.locations}
        loc_options = list(loc_map.keys()) + ["➕ New Location"]
        loc_selection = st.selectbox("Location", loc_options)
        
        # New Location Fields (will be used if "➕ New Location" selected)
        st.write("---")
        with st.expander("New Location Details", expanded=(loc_selection == "➕ New Location")):
            new_loc_name = st.text_input("New Location Name")
            new_loc_address = st.text_input("New Location Address")
            new_loc_maps = st.text_input("Google Maps Link (Optional)")
        
        # Multiple Site Contacts
        st.write("---")
        st.write("###### 👥 Site Contacts")
        c1, c2 = st.columns(2)
        contact1_name = c1.text_input("Primary Contact Name")
        contact1_phone = c1.text_input("Primary Contact Phone")
        
        contact2_name = c2.text_input("Secondary Contact Name")
        contact2_phone = c2.text_input("Secondary Contact Phone")
        
        contact3_name = st.text_input("Additional Contact / Notes")

        # Tech Selection
        tech_map = {t['name']: t['id'] for t in st.session_state.techs}
        
        # Create display labels with skills
        tech_display_map = {}
        for t in st.session_state.techs:
            skills_str = f" ({', '.join(t.get('skills', [])[:2])}..)" if t.get('skills') else ""
            label = f"{t['name']}{skills_str}"
            tech_display_map[label] = t['id']
            
        tech_display_map["Unassigned"] = None
        
        tech_label = st.selectbox("Assign Tech", list(tech_display_map.keys()))
        selected_tech_id = tech_display_map[tech_label]

        # Document Upload
        st.write("---")
        st.write("###### 📄 Job Documents")
        uploaded_docs = st.file_uploader("Upload Floorplans, Maps, or Docs (PDF, JPG, PNG)", accept_multiple_files=True, type=['pdf', 'jpg', 'png', 'jpeg'])

        submitted = st.form_submit_button("Save Job")
        if submitted and title:
            # Handle Inline Location Creation
            final_loc_id = None
            if loc_selection == "➕ New Location":
                if new_loc_name and new_loc_address:
                    existing_ids = [int(l['id'][1:]) for l in st.session_state.locations if l['id'].startswith('l') and l['id'][1:].isdigit()]
                    next_id = (max(existing_ids) if existing_ids else 0) + 1
                    final_loc_id = f"l{next_id}"
                    
                    new_loc = {
                        "id": final_loc_id,
                        "name": new_loc_name,
                        "address": new_loc_address,
                        "mapsUrl": new_loc_maps,
                        "contact_name": contact1_name,
                        "contact_phone": contact1_phone
                    }
                    st.session_state.locations.append(new_loc)
                else:
                    st.error("New Location Name and Address are required.")
                    return
            else:
                final_loc_id = loc_map[loc_selection]

            # Save Documents
            doc_keys = []
            if uploaded_docs:
                for up_doc in uploaded_docs:
                    dk = save_document_locally(up_doc)
                    if dk: doc_keys.append({'name': up_doc.name, 'key': dk})

            # Contacts List
            contacts = []
            if contact1_name or contact1_phone:
                contacts.append({'name': contact1_name, 'phone': contact1_phone, 'label': 'Primary'})
            if contact2_name or contact2_phone:
                contacts.append({'name': contact2_name, 'phone': contact2_phone, 'label': 'Secondary'})
            if contact3_name:
                contacts.append({'name': contact3_name, 'phone': '', 'label': 'Note'})

            # Combine date with current time for ISO format
            full_date = datetime.datetime.combine(job_date, now_local().time())
            
            new_job = {
                'id': f"j{len(st.session_state.jobs) + 100}_{now_local().timestamp()}",
                'title': title,
                'description': desc,
                'type': job_type,
                'priority': priority,
                'status': 'Not Started',
                'locationId': final_loc_id,
                'techId': selected_tech_id,
                'date': full_date.isoformat(),
                'contacts': contacts,
                'reports': [],
                'documents': doc_keys
            }
            st.session_state.jobs.insert(0, new_job)
            
            # Send Email Notification
            email_status_msg = ""
            
            if selected_tech_id:
                tech = get_tech(selected_tech_id)
                loc = get_location(final_loc_id)
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
        type_opts = ["Service", "Project", "Leads"]
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
                existing_time = now_local().time()
        except:
            existing_date = now_local().date()
            existing_time = now_local().time()
            
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
        
        # Create display labels with skills
        tech_display_map = {}
        for t in st.session_state.techs:
            skills_str = f" ({', '.join(t.get('skills', [])[:2])}..)" if t.get('skills') else ""
            label = f"{t['name']}{skills_str}"
            tech_display_map[label] = t['id']
            
        tech_display_map["Unassigned"] = None
        tech_options = list(tech_display_map.keys())
        
        current_tech_id = job.get('techId')
        # Find label for current ID
        current_tech_label = next((k for k, v in tech_display_map.items() if v == current_tech_id), "Unassigned")
        
        tech_index = 0
        if current_tech_label in tech_options:
            tech_index = tech_options.index(current_tech_label)
            
        tech_label = st.selectbox("Assign Tech", tech_options, index=tech_index)
        selected_tech_id = tech_display_map[tech_label]
        
        # Site Contacts
        st.write("---")
        st.write("###### 👥 Site Contacts")
        job_contacts = job.get('contacts', [])
        c1, c2 = st.columns(2)
        
        # Extract existing contact values
        c1_n = job_contacts[0]['name'] if len(job_contacts) > 0 else ""
        c1_p = job_contacts[0]['phone'] if len(job_contacts) > 0 else ""
        c2_n = job_contacts[1]['name'] if len(job_contacts) > 1 else ""
        c2_p = job_contacts[1]['phone'] if len(job_contacts) > 1 else ""
        c3_note = job_contacts[2]['name'] if len(job_contacts) > 2 else ""

        contact1_name = c1.text_input("Primary Contact Name", value=c1_n)
        contact1_phone = c1.text_input("Primary Contact Phone", value=c1_p)
        
        contact2_name = c2.text_input("Secondary Contact Name", value=c2_n)
        contact2_phone = c2.text_input("Secondary Contact Phone", value=c2_p)
        
        contact3_name = st.text_input("Additional Contact / Notes", value=c3_note)

        # Document Upload
        st.write("---")
        st.write("###### 📄 Job Documents")
        existing_docs = job.get('documents', [])
        if existing_docs:
            for i, d in enumerate(existing_docs):
                c_d1, c_d2 = st.columns([4, 1])
                c_d1.write(f"📎 {d['name']}")
                if c_d2.button("🗑️", key=f"del_doc_{job_id}_{i}"):
                    existing_docs.pop(i)
                    st.session_state.jobs[job_index]['documents'] = existing_docs
                    save_state(invalidate_briefing=False)
                    st.rerun()
        
        uploaded_docs = st.file_uploader("Attach More Documents", accept_multiple_files=True, type=['pdf', 'jpg', 'png', 'jpeg'], key=f"edit_docs_{job_id}")

        if st.form_submit_button("Update Job"):
            if title:
                # Save New Documents
                doc_keys = existing_docs.copy()
                if uploaded_docs:
                    for up_doc in uploaded_docs:
                        dk = save_document_locally(up_doc)
                        if dk: doc_keys.append({'name': up_doc.name, 'key': dk})

                # Update Contacts
                new_contacts = []
                if contact1_name or contact1_phone: 
                    new_contacts.append({'name': contact1_name, 'phone': contact1_phone, 'label': 'Primary'})
                if contact2_name or contact2_phone: 
                    new_contacts.append({'name': contact2_name, 'phone': contact2_phone, 'label': 'Secondary'})
                if contact3_name: 
                    new_contacts.append({'name': contact3_name, 'phone': '', 'label': 'Note'})
                
                st.session_state.jobs[job_index]['contacts'] = new_contacts
                st.session_state.jobs[job_index]['title'] = title
                st.session_state.jobs[job_index]['description'] = desc
                st.session_state.jobs[job_index]['type'] = job_type
                st.session_state.jobs[job_index]['priority'] = priority
                st.session_state.jobs[job_index]['documents'] = doc_keys
                
                # Update Date (preserve time if possible, or use current time)
                full_date = datetime.datetime.combine(job_date, existing_time)
                st.session_state.jobs[job_index]['date'] = full_date.isoformat()
                
                if loc_name:
                    st.session_state.jobs[job_index]['locationId'] = loc_map[loc_name]
                
                st.session_state.jobs[job_index]['techId'] = selected_tech_id
                
                # Invalidate briefing so it regenerates with new data
                st.session_state.briefing = "Data required to generate briefing."
                save_state()  # Save changes
                
                st.toast("Job updated successfully!", icon="✅")
                st.rerun()
            else:
                st.error("Title is required.")

@st.dialog("Edit Location")
def edit_location_dialog(loc_id):
    # Find location
    loc_index = next((i for i, l in enumerate(st.session_state.locations) if l['id'] == loc_id), -1)
    if loc_index == -1:
        st.error("Location not found")
        return

    loc = st.session_state.locations[loc_index]

    with st.form(key=f"edit_loc_form_{loc_id}"):
        l_name = st.text_input("Location Name", value=loc['name'])
        l_addr = st.text_input("Address", value=loc['address'])
        l_maps = st.text_input("Google Maps Link (Optional)", value=loc.get('mapsUrl', ''))
        
        c_l1, c_l2 = st.columns(2)
        l_contact_name = c_l1.text_input("Site Contact Name", value=loc.get('contact_name', ''))
        l_contact_phone = c_l2.text_input("Site Contact Phone", value=loc.get('contact_phone', ''))
        
        if st.form_submit_button("Update Location"):
            if l_name and l_addr:
                # Update session state
                st.session_state.locations[loc_index]['name'] = l_name
                st.session_state.locations[loc_index]['address'] = l_addr
                st.session_state.locations[loc_index]['mapsUrl'] = l_maps
                st.session_state.locations[loc_index]['contact_name'] = l_contact_name
                st.session_state.locations[loc_index]['contact_phone'] = l_contact_phone
                
                save_state(invalidate_briefing=False)
                st.success("Location updated!")
                st.rerun()
            else:
                st.error("Name and Address required.")


def render_completion_confirmation(job_index, report_payload):
    job = st.session_state.jobs[job_index]
    st.write(f"**Job:** {job['title']}")
    st.warning("You are marking this job as **Completed**. This will archive the job and notify admins.")
    st.caption("Your daily report is attached to this sign-off and will be saved when you confirm. Cancelling discards it.")

    completion_loc = get_location(job['locationId'])
    if completion_loc and not location_has_system_info(completion_loc):
        st.error("🔐 No system info (logins / IPs) has been recorded for this site. Please fill out the **IPs & Passwords** tab before closing the job.")

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

    if c_cancel.button("❌ Cancel & Discard Report"):
        if f"completion_pending_{job['id']}" in st.session_state:
            del st.session_state[f"completion_pending_{job['id']}"]
        st.rerun(scope="fragment")
        
def render_edit_report_view(job_id, report_id):
    # Find job
    job_index = next((i for i, j in enumerate(st.session_state.jobs) if j['id'] == job_id), -1)
    if job_index == -1:
        st.error("Job not found")
        return
    job = st.session_state.jobs[job_index]
    
    # Find report
    report_index = next((i for i, r in enumerate(job['reports']) if r['id'] == report_id), -1)
    if report_index == -1:
        st.error("Report not found")
        return
    report = job['reports'][report_index]

    with st.form(key=f"edit_report_form_{report_id}"):
        st.write(f"### ✏️ Editing Daily Report")
        st.caption(f"Report from {report['timestamp'][:16]}")
        
        r_col1, r_col2 = st.columns(2)
        with r_col1:
            available_techs = [t['name'] for t in st.session_state.techs]
            current_techs_str = report.get('techsOnSite', '')
            current_techs = [t.strip() for t in current_techs_str.split(',')] if current_techs_str else []
            current_techs = [t for t in current_techs if t in available_techs]
            
            techs_on_site_list = st.multiselect("Techs On Site", options=available_techs, default=current_techs)
            
            try:
                t_arr_str = report.get('timeArrived', '08:00:00')
                if len(t_arr_str) == 5: t_arr_str += ":00"
                t_arr = datetime.datetime.strptime(t_arr_str, '%H:%M:%S').time()
            except:
                t_arr = datetime.time(8, 0)
            time_arrived = st.time_input("Time Arrived", value=t_arr)
            
            parts_used = st.text_area("Parts/Materials Used", value=report.get('partsUsed', ''))
            
        with r_col2:
            try:
                h_worked = float(report.get('hoursWorked', 0.0))
            except:
                h_worked = 0.0
            hours_worked = st.number_input("Hours Worked", min_value=0.0, step=0.5, value=h_worked)
            
            try:
                t_dep_str = report.get('timeDeparted', '17:00:00')
                if len(t_dep_str) == 5: t_dep_str += ":00"
                t_dep = datetime.datetime.strptime(t_dep_str, '%H:%M:%S').time()
            except:
                t_dep = datetime.time(17, 0)
            time_departed = st.time_input("Time Finished", value=t_dep)
            
            billable_items = st.text_area("Billable Items / Extras", value=report.get('billableItems', ''))

        content = st.text_area("General Notes / Summary", value=report.get('content', ''))
        
        if st.form_submit_button("Update Report"):
            # Auto-calculate hours from arrival/finish times when left at 0
            if not hours_worked:
                arr_dt = datetime.datetime.combine(datetime.date.today(), time_arrived)
                dep_dt = datetime.datetime.combine(datetime.date.today(), time_departed)
                if dep_dt > arr_dt:
                    hours_worked = round((dep_dt - arr_dt).total_seconds() / 3600 * 4) / 4

            # Update report in session state
            st.session_state.jobs[job_index]['reports'][report_index].update({
                'content': content,
                'techsOnSite': ", ".join(techs_on_site_list),
                'timeArrived': str(time_arrived),
                'timeDeparted': str(time_departed),
                'hoursWorked': str(hours_worked),
                'partsUsed': parts_used,
                'billableItems': billable_items
            })
            
            # Log the action
            user_email = st.session_state.user_info.get("email", "Unknown Admin")
            get_logger().log(f"Admin {user_email} updated daily report {report_id} for job {job_id}")
            
            save_state(invalidate_briefing=False)
            if f"editing_report_{job_id}" in st.session_state:
                del st.session_state[f"editing_report_{job_id}"]
            st.success("Report updated!")
            st.rerun(scope="fragment")

    if st.button("Cancel Edit"):
        if f"editing_report_{job_id}" in st.session_state:
            del st.session_state[f"editing_report_{job_id}"]
        st.rerun(scope="fragment")

@st.dialog("Job Details & Report", width="large")
def job_details_dialog(job_id):
    # Find job directly from session state
    job_index = next((i for i, j in enumerate(st.session_state.jobs) if j['id'] == job_id), -1)
    if job_index == -1:
        st.error("Job not found")
        return
    
    # Check for active report editing
    edit_key = f"editing_report_{job_id}"
    if edit_key in st.session_state:
        render_edit_report_view(job_id, st.session_state[edit_key])
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
    weather_ph = None  # backfilled with weather at the end of the dialog (see below)
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

            # Paint the address immediately; the weather network call is deferred to
            # the end of the dialog so it doesn't block the tabs from rendering.
            weather_ph = st.empty()
            weather_ph.caption(loc.get('address', ''))
        else:
             st.caption(f"📍 Unknown | 👤 {tech['name'] if tech else 'Unassigned'}")
        
        # MAILTO LINK BUTTON: Provides manual alternative if SMTP is missing
        if tech and loc:
            mailto_url = create_mailto_link(job, tech, loc)
            st.link_button("📧 Email Assignment to Tech", mailto_url)
            
        # Resolve Contact Info (Job override > Location default)
        job_contacts = job.get('contacts', [])
        
        # Contact Info Logic
        contact_name = None
        contact_phone = None

        if job_contacts:
            st.write("###### 👥 Site Contacts")
            for c in job_contacts:
                col_c1, col_c2 = st.columns([2, 1])
                col_c1.write(f"**{c['label']}:** {c['name']}")
                if c.get('phone'):
                    clean_phone = re.sub(r'\D', '', c['phone'])
                    col_c2.link_button(f"📞 Call", f"tel:{clean_phone}", use_container_width=True)
                else:
                    col_c2.write("")
            
            # For the copy block below, use the first contact as a default if available
            contact_name = job_contacts[0].get('name')
            contact_phone = job_contacts[0].get('phone')
        else:
            # Fallback to old single contact logic if no list exists
            contact_name = job.get('contact_name') or (loc.get('contact_name') if loc else None)
            contact_phone = job.get('contact_phone') or (loc.get('contact_phone') if loc else None)

            # CONTACT CALL BUTTON
            if contact_phone:
                clean_phone = re.sub(r'\D', '', contact_phone)
                st.link_button(f"📞 Call {contact_name or 'Contact'}", f"tel:{clean_phone}")
            elif contact_name:
                st.write(f"👤 {contact_name}")

        # COPY JOB INFO BLOCK
        copy_text = f"""Job: {job['title']}
Address: {loc['address'] if loc else 'Unknown'}
Contact: {contact_name or 'N/A'} ({contact_phone or 'N/A'})
Desc: {job['description']}"""
        st.code(copy_text, language="text")

        # CALENDAR INVITE (.ics)
        ics_data = create_ics_file(job, loc)
        if ics_data:
            st.download_button(
                label="📅 Add to Calendar",
                data=ics_data,
                file_name=f"job_{job['id']}.ics",
                mime="text/calendar",
            )

        # PDF DOWNLOAD
        # Find the most relevant report (Completion > Latest Daily)
        relevant_report = None
        if job['status'] == 'Completed':
            relevant_report = next((r for r in reversed(job.get('reports', [])) if 'completion_checklist' in r), None)
        
        if not relevant_report and job.get('reports'):
            relevant_report = job['reports'][-1]

        if relevant_report:
            # Use a button to trigger PDF generation to avoid slow renders
            if st.button("📄 Prepare Report PDF"):
                with st.spinner("Generating PDF..."):
                    pdf_data = generate_job_pdf(job, tech, loc, relevant_report)
                    if pdf_data:
                        st.download_button(
                            label="⬇️ Download PDF Now",
                            data=pdf_data,
                            file_name=f"JobReport_{job['id']}.pdf",
                            mime="application/pdf",
                        )
                    else:
                        st.error("Failed to generate PDF.")

    with c2:
        st.markdown("**Current Status:**")
        status_color = {
            "Not Started": "gray",
            "Pending": "gray",
            "In Progress": "orange", 
            "Customer on Hold": "red",
            "Waiting on Parts": "blue",
            "Parts not ordered": "red",
            "Parts Staged": "violet",
            "Completed": "green"
        }.get(job['status'], "gray")
        st.markdown(f":{status_color}-background[{job['status']}]")

    # Flag the credentials tab when nothing is recorded yet so it doesn't get forgotten
    has_sys_info = location_has_system_info(loc)
    creds_tab_label = "🔐 IPs & Passwords" if has_sys_info else "⚠️ IPs & Passwords"
    # Parts tab label shows progress at a glance (e.g. "🔩 Parts 2/5")
    _staged, _total = parts_summary(job)
    parts_tab_label = f"🔩 Parts {_staged}/{_total}" if _total else "🔩 Parts"
    tab_history, tab_photos, tab_docs, tab_parts, tab_progress, tab_daily, tab_creds = st.tabs(["📋 Details & History", "🖼️ Photos", "📄 Documents", parts_tab_label, "📸 In-Progress", "📝 Daily Report", creds_tab_label])

    with tab_docs:
        st.write("#### 📄 Documents")
        st.caption("Floorplans, maps, and reference documents. Site documents follow the location across every job.")

        with st.expander("➕ Upload New Document"):
            dest_options = []
            if loc:
                dest_options.append(f"🏢 Site — {loc['name']} (shared across all its jobs)")
            dest_options.append("📋 This job only")
            dest_choice = st.radio("Save to", dest_options, key=f"doc_dest_{job_id}")

            new_uploaded_docs = st.file_uploader("Select files (PDF, JPG, PNG)", accept_multiple_files=True, type=['pdf', 'jpg', 'png', 'jpeg'], key=f"tab_docs_upload_{job_id}")
            if st.button("Save Uploaded Documents", key=f"btn_save_tab_docs_{job_id}"):
                if new_uploaded_docs:
                    with st.spinner("Uploading..."):
                        to_site = bool(loc) and dest_choice.startswith("🏢")
                        folder = f"locations/{loc['id']}/docs" if to_site else f"jobs/{job_id}/docs"
                        new_keys = []
                        for f in new_uploaded_docs:
                            k = upload_streamlit_file(f, folder=folder)
                            if k:
                                new_keys.append({"name": f.name, "key": k})

                        if new_keys:
                            if to_site:
                                loc.setdefault('documents', []).extend(new_keys)
                            else:
                                job.setdefault('documents', []).extend(new_keys)
                            save_state(invalidate_briefing=False)
                            st.success(f"Uploaded {len(new_keys)} document(s)!")
                            st.rerun(scope="fragment")
                else:
                    st.warning("Please select files first.")

        def render_doc_row(d, key_suffix, allow_move_to_site=False):
            with st.container(border=True):
                d_col1, d_col2 = st.columns([3, 1])
                d_col1.write(f"**{d['name']}**")
                url = resolve_image_source(d['key'])

                # If it's an image, we can show a small preview
                ext = d['name'].lower().split('.')[-1]
                if ext in ['jpg', 'jpeg', 'png']:
                    st.image(url, width=200)

                d_col2.link_button("👁️ View / Download", url, use_container_width=True)
                if allow_move_to_site and loc:
                    if d_col2.button("🏢 Move to Site", key=f"mv_doc_{key_suffix}", use_container_width=True, help="Share this document across every job at this location"):
                        loc.setdefault('documents', []).append(d)
                        job['documents'] = [x for x in job.get('documents', []) if x['key'] != d['key']]
                        save_state(invalidate_briefing=False)
                        st.toast(f"'{d['name']}' moved to site documents", icon="🏢")
                        st.rerun(scope="fragment")

        if loc:
            st.write(f"##### 🏢 Site Documents — {loc['name']}")
            site_docs = loc.get('documents', [])
            if not site_docs:
                st.caption("No site documents yet. Floorplans and as-builts belong here.")
            for i, d in enumerate(site_docs):
                render_doc_row(d, f"site_{i}")
            st.divider()

        st.write("##### 📋 This Job's Documents")
        docs = job.get('documents', [])
        if not docs:
            st.caption("No documents on this job.")
        for i, d in enumerate(docs):
            render_doc_row(d, f"job_{i}", allow_move_to_site=True)

    with tab_photos:
        st.write("#### 🖼️ All Job Photos")
        # Gather every photo/PDF across all history entries, newest first
        photo_entries = []
        seen_photo_keys = set()
        for r in job.get('reports', []):
            for p_key in (r.get('photos') or []):
                if p_key in seen_photo_keys:
                    continue
                seen_photo_keys.add(p_key)
                photo_entries.append({'key': p_key, 'timestamp': r.get('timestamp', ''), 'techId': r.get('techId')})
        photo_entries.sort(key=lambda x: x['timestamp'], reverse=True)

        if not photo_entries:
            st.info("No photos posted for this job yet.")
        else:
            st.caption(f"{len(photo_entries)} photo(s) across all reports, newest first.")
            show_all_photos = True
            if len(photo_entries) > 12:
                show_all_photos = st.checkbox(f"Show all {len(photo_entries)} photos", key=f"show_all_photos_{job_id}")
                if not show_all_photos:
                    st.caption("Showing the 12 most recent.")
            visible_entries = photo_entries if show_all_photos else photo_entries[:12]

            p_cols = st.columns(3)
            for i, pe in enumerate(visible_entries):
                with p_cols[i % 3]:
                    url = resolve_image_source(pe['key'])
                    p_tech = get_tech(pe['techId'])
                    cap = f"{pe['timestamp'][:10]} · {p_tech['name'] if p_tech else 'Unknown'}"
                    if isinstance(pe['key'], str) and pe['key'].lower().endswith('.pdf'):
                        st.link_button(f"📄 PDF — {cap}", url, use_container_width=True)
                    else:
                        st.image(url, caption=cap, use_container_width=True)

    with tab_parts:
        st.write("#### 🔩 Parts & Materials")
        st.caption("Track what this job needs, from request through staging. Anyone can add or update items.")

        parts = job.get('parts', [])
        current_user_email = st.session_state.user_info.get('email', 'unknown') if "user_info" in st.session_state else 'unknown'

        # Progress summary
        if parts:
            counts = {s: sum(1 for p in parts if p.get('status') == s) for s in PART_STATUSES}
            staged, total = parts_summary(job)
            st.progress(staged / total if total else 0, text=f"{staged} of {total} staged")
            chip_html = " ".join(
                f'<span style="background:{PART_STATUS_COLORS[s]};color:white;padding:2px 10px;border-radius:10px;font-size:0.75em;margin-right:4px;">{counts[s]} {s}</span>'
                for s in PART_STATUSES if counts[s]
            )
            st.markdown(chip_html, unsafe_allow_html=True)

            # Offer to sync the job's overall status to match the parts pipeline
            if total and staged == total and job['status'] != 'Parts Staged':
                if st.button("✅ All parts staged — mark job 'Parts Staged'", key=f"sync_staged_{job_id}", use_container_width=True):
                    st.session_state.jobs[job_index]['status'] = 'Parts Staged'
                    save_state()
                    st.rerun(scope="fragment")
            elif any(p.get('status') in ('Needed', 'Ordered') for p in parts) and job['status'] not in ('Waiting on Parts', 'Parts not ordered'):
                only_needed = all(p.get('status') == 'Needed' for p in parts)
                suggested = 'Parts not ordered' if only_needed else 'Waiting on Parts'
                if st.button(f"📦 Mark job '{suggested}'", key=f"sync_waiting_{job_id}", use_container_width=True):
                    st.session_state.jobs[job_index]['status'] = suggested
                    save_state()
                    st.rerun(scope="fragment")

        # Add a part
        with st.expander("➕ Add Part / Material", expanded=not parts):
            with st.form(key=f"add_part_form_{job_id}", clear_on_submit=True):
                ap1, ap2 = st.columns([3, 1])
                new_name = ap1.text_input("Item", placeholder="e.g. 16ch NVR, Cat6 box, PoE switch")
                new_qty = ap2.number_input("Qty", min_value=1, step=1, value=1)
                ap3, ap4, ap5 = st.columns(3)
                new_status = ap3.selectbox("Status", PART_STATUSES, index=0)
                new_vendor = ap4.text_input("Vendor (optional)")
                new_cost = ap5.text_input("Est. Cost (optional)", placeholder="$")
                new_notes = st.text_input("Notes (optional)", placeholder="PO #, part number, where it's stored...")

                if st.form_submit_button("💾 Add Part", use_container_width=True):
                    if not new_name.strip():
                        st.warning("Please enter an item name.")
                    else:
                        st.session_state.jobs[job_index].setdefault('parts', []).append({
                            'id': f"p{datetime.datetime.now().timestamp()}",
                            'name': new_name.strip(),
                            'qty': int(new_qty),
                            'status': new_status,
                            'vendor': new_vendor.strip(),
                            'cost': new_cost.strip(),
                            'notes': new_notes.strip(),
                            'added_by': current_user_email,
                            'updated_at': now_local().isoformat(),
                        })
                        save_state(invalidate_briefing=False)
                        st.success(f"Added {new_name.strip()}.")
                        st.rerun(scope="fragment")

        if not parts:
            st.info("No parts listed yet. Add what this job needs above.")

        # Part list - status is editable inline via on_change callback
        for p in parts:
            with st.container(border=True):
                pc1, pc2, pc3 = st.columns([3, 2, 1])
                qty_str = f"{p.get('qty', 1)}× " if p.get('qty') else ""
                pc1.markdown(f"**{qty_str}{p.get('name', 'Item')}**")
                meta = []
                if p.get('vendor'):
                    meta.append(f"🏬 {p['vendor']}")
                if p.get('cost'):
                    meta.append(f"💲 {p['cost']}")
                if meta:
                    pc1.caption(" · ".join(meta))
                if p.get('notes'):
                    pc1.caption(p['notes'])

                status_key = f"part_status_{p['id']}"
                pc2.selectbox(
                    "Status", PART_STATUSES, index=PART_STATUSES.index(p['status']) if p.get('status') in PART_STATUSES else 0,
                    key=status_key, label_visibility="collapsed",
                    on_change=update_part_status_callback, args=(job_id, p['id'], status_key),
                )
                if pc3.button("🗑️", key=f"del_part_{p['id']}", help="Remove this part", use_container_width=True):
                    st.session_state.jobs[job_index]['parts'] = [x for x in st.session_state.jobs[job_index].get('parts', []) if x['id'] != p['id']]
                    save_state(invalidate_briefing=False)
                    st.rerun(scope="fragment")

                if p.get('updated_at'):
                    pc1.caption(f"Updated {p['updated_at'][:16]} by {p.get('added_by', 'unknown')}")


    with tab_creds:
        st.write("#### 🔐 Site Systems & Network Info")
        st.caption("Logins, IPs, and notes for the systems at this location. Saved to the location, shared across all its jobs.")

        if not loc:
            st.warning("No location assigned to this job. System info cannot be saved.")
        else:
            # One-time migration: convert legacy fixed-field credentials to the flexible systems list
            if 'systems' not in loc:
                legacy = loc.get('credentials') or {}
                migrated = []
                legacy_logins = [
                    ("Windows PC / Server", 'windows_user', 'windows_pass'),
                    ("ICT", 'ict_user', 'ict_pass'),
                    ("DW Spectrum", 'dw_user', 'dw_pass'),
                ]
                for sys_name, u_key, p_key in legacy_logins:
                    if legacy.get(u_key) or legacy.get(p_key):
                        migrated.append({
                            'id': f"s{now_local().timestamp()}_{len(migrated)}",
                            'name': sys_name,
                            'username': legacy.get(u_key, ''),
                            'password': legacy.get(p_key, ''),
                            'ip': '',
                            'notes': ''
                        })
                if legacy.get('ips'):
                    migrated.append({
                        'id': f"s{now_local().timestamp()}_{len(migrated)}",
                        'name': "Network / IPs",
                        'username': '',
                        'password': '',
                        'ip': '',
                        'notes': legacy['ips']
                    })
                loc['systems'] = migrated
                if migrated:
                    save_state(invalidate_briefing=False)

            systems = loc.get('systems', [])
            current_user_email = st.session_state.user_info.get('email', 'unknown') if "user_info" in st.session_state else 'unknown'

            with st.expander("➕ Add a System", expanded=not systems):
                with st.form(key=f"add_system_form_{job_id}", clear_on_submit=True):
                    sys_type = st.selectbox("System Type", SYSTEM_PRESETS)
                    custom_name = st.text_input("Custom Name (optional)", placeholder="e.g. Front Desk NVR")
                    a1, a2 = st.columns(2)
                    with a1:
                        new_user = st.text_input("Username")
                        new_ip = st.text_input("IP Address(es)", placeholder="192.168.1.100")
                    with a2:
                        new_pass = st.text_input("Password")
                        new_notes = st.text_input("Notes", placeholder="Port, VLAN, where it lives...")

                    if st.form_submit_button("💾 Save System", use_container_width=True):
                        if not (new_user or new_pass or new_ip or new_notes):
                            st.warning("Please fill in at least one field.")
                        else:
                            sys_name = custom_name.strip() or sys_type
                            loc.setdefault('systems', []).append({
                                'id': f"s{now_local().timestamp()}",
                                'name': sys_name,
                                'username': new_user,
                                'password': new_pass,
                                'ip': new_ip,
                                'notes': new_notes,
                                'updated_by': current_user_email,
                                'updated_at': now_local().isoformat()
                            })
                            save_state(invalidate_briefing=False)
                            st.success(f"'{sys_name}' saved!")
                            st.rerun(scope="fragment")

            if not systems:
                st.info("No system info recorded for this site yet. Add the first one above while you're on site.")

            for s in systems:
                with st.container(border=True):
                    st.markdown(f"**🖥️ {s.get('name', 'System')}**")
                    d1, d2 = st.columns(2)
                    with d1:
                        if s.get('username'):
                            st.caption("Username")
                            st.code(s['username'], language=None)
                        if s.get('password'):
                            st.caption("Password")
                            st.code(s['password'], language=None)
                    with d2:
                        if s.get('ip'):
                            st.caption("IP Address(es)")
                            st.code(s['ip'], language=None)
                        if s.get('notes'):
                            st.caption("Notes")
                            st.write(s['notes'])

                    if s.get('updated_at'):
                        st.caption(f"Last updated {s['updated_at'][:16]} by {s.get('updated_by', 'unknown')}")

                    with st.expander("✏️ Edit / Delete"):
                        with st.form(key=f"edit_sys_form_{s['id']}"):
                            e_name = st.text_input("System Name", value=s.get('name', ''))
                            e1, e2 = st.columns(2)
                            with e1:
                                e_user = st.text_input("Username", value=s.get('username', ''))
                                e_ip = st.text_input("IP Address(es)", value=s.get('ip', ''))
                            with e2:
                                e_pass = st.text_input("Password", value=s.get('password', ''))
                                e_notes = st.text_input("Notes", value=s.get('notes', ''))

                            ec1, ec2 = st.columns(2)
                            if ec1.form_submit_button("💾 Update"):
                                s.update({
                                    'name': e_name,
                                    'username': e_user,
                                    'password': e_pass,
                                    'ip': e_ip,
                                    'notes': e_notes,
                                    'updated_by': current_user_email,
                                    'updated_at': now_local().isoformat()
                                })
                                save_state(invalidate_briefing=False)
                                st.success("System updated!")
                                st.rerun(scope="fragment")

                            if ec2.form_submit_button("🗑️ Delete System"):
                                loc['systems'] = [x for x in loc['systems'] if x['id'] != s['id']]
                                get_logger().log(f"{current_user_email} deleted system '{s.get('name')}' from location {loc['id']}")
                                save_state(invalidate_briefing=False)
                                st.toast(f"'{s.get('name')}' deleted", icon="🗑️")
                                st.rerun(scope="fragment")

    with tab_history:
        st.markdown(f"**Description:** {job['description']}")

        # Site History: what else have we done at this location?
        if loc:
            site_jobs = [sj for sj in st.session_state.jobs if sj['locationId'] == loc['id'] and sj['id'] != job_id]
            if site_jobs:
                site_jobs.sort(key=lambda x: x.get('date', ''), reverse=True)
                with st.expander(f"🏢 Site History — {len(site_jobs)} other job(s) at {loc['name']}"):
                    for sj in site_jobs:
                        sj_tech = get_tech(sj['techId'])
                        status_icon = "✅" if sj['status'] == 'Completed' else "🔧"
                        sh_c1, sh_c2 = st.columns([4, 1])
                        sh_c1.markdown(f"{status_icon} **{sj['title']}** ({sj['status']}) — {sj.get('date', '')[:10]} · 👤 {sj_tech['name'] if sj_tech else 'Unassigned'}")
                        last_note = next((r.get('content') for r in reversed(sj.get('reports', [])) if r.get('content')), None)
                        if last_note:
                            sh_c1.caption(f"Last note: {last_note[:120]}{'…' if len(last_note) > 120 else ''}")
                        if sh_c2.button("Open", key=f"site_hist_open_{sj['id']}", use_container_width=True):
                            # Can't open a dialog from inside a dialog - hand off to main()
                            st.session_state["_open_job_after_rerun"] = sj['id']
                            st.rerun()

        st.divider()
        st.write("#### 📜 History")
        if not job['reports']:
            st.info("No reports filed yet.")
        
        # Limit history display to avoid performance issues with many images
        reports_to_show = reversed(job['reports'])
        total_reports = len(job['reports'])
        
        show_all_key = f"show_all_history_{job_id}"
        show_all = st.checkbox("Show Full History", key=show_all_key) if total_reports > 5 else True
        
        if not show_all:
            reports_to_show = list(reversed(job['reports']))[:5]
            st.caption(f"Showing latest 5 of {total_reports} reports.")

        # Admin check
        user_email = st.session_state.user_info.get("email") if "user_info" in st.session_state else None
        is_admin = user_email in st.session_state.adminEmails if user_email else False
        # Current user's tech profile (techs may manage their own entries)
        viewer_tech = next((t for t in st.session_state.techs if user_email and t['email'].lower() == user_email.lower()), None)

        for r in reports_to_show:
            r_tech = get_tech(r['techId'])

            # Check if it's a "Daily Report" (has hours/techs) or "In-Progress" (just content/photos)
            is_daily_report = r.get('hoursWorked') or r.get('techsOnSite')
            is_completion = 'completion_checklist' in r

            # Admins can manage any entry; techs can manage their own (except completion reports)
            can_manage = is_admin or (viewer_tech and r.get('techId') == viewer_tech['id'] and not is_completion)

            with st.container(border=True):
                hdr_main, hdr_move, hdr_del = st.columns([4, 1, 1])
                hdr_main.markdown(f"**{r_tech['name'] if r_tech else 'Unknown'}** - {r['timestamp'][:16]}")

                if can_manage:
                    with hdr_move.popover("↪️ Move"):
                        st.caption("Filed under the wrong job? Move this entry (notes & photos) to the correct one.")
                        other_jobs = {j['id']: j for j in st.session_state.jobs if j['id'] != job_id}
                        if not other_jobs:
                            st.caption("No other jobs to move to.")
                        else:
                            def _fmt_job_option(jid):
                                j = other_jobs[jid]
                                j_loc = get_location(j['locationId'])
                                return f"{j['title']} — {j_loc['name'] if j_loc else 'No location'}"

                            target_id = st.selectbox("Move to job:", list(other_jobs.keys()), format_func=_fmt_job_option, key=f"move_target_{r['id']}")
                            if st.button("Confirm Move", key=f"move_btn_{r['id']}", type="primary", use_container_width=True):
                                target_idx = next((i for i, j in enumerate(st.session_state.jobs) if j['id'] == target_id), -1)
                                if target_idx != -1:
                                    st.session_state.jobs[target_idx].setdefault('reports', []).append(r)
                                    st.session_state.jobs[job_index]['reports'] = [x for x in st.session_state.jobs[job_index]['reports'] if x['id'] != r['id']]
                                    get_logger().log(f"{user_email} moved report {r['id']} from job {job_id} to job {target_id}")
                                    save_state(invalidate_briefing=False)
                                    st.toast(f"Entry moved to '{other_jobs[target_id]['title']}'", icon="↪️")
                                    st.rerun(scope="fragment")

                    del_confirm_key = f"confirm_del_report_{r['id']}"
                    if hdr_del.button("🗑️", key=f"del_rep_{r['id']}", help="Delete this entry"):
                        st.session_state[del_confirm_key] = True
                        st.rerun(scope="fragment")

                    if st.session_state.get(del_confirm_key):
                        st.warning("Permanently delete this entry? Its notes and photos will be removed from the job history.")
                        dc1, dc2 = st.columns(2)
                        if dc1.button("✅ Yes, Delete", key=f"del_yes_{r['id']}", type="primary", use_container_width=True):
                            st.session_state.jobs[job_index]['reports'] = [x for x in st.session_state.jobs[job_index]['reports'] if x['id'] != r['id']]
                            get_logger().log(f"{user_email} deleted report {r['id']} from job {job_id}")
                            del st.session_state[del_confirm_key]
                            save_state(invalidate_briefing=False)
                            st.toast("Entry deleted", icon="🗑️")
                            st.rerun(scope="fragment")
                        if dc2.button("❌ Cancel", key=f"del_no_{r['id']}", use_container_width=True):
                            del st.session_state[del_confirm_key]
                            st.rerun(scope="fragment")
                
                if is_daily_report:
                    h1, h2, h3 = st.columns(3)
                    h1.caption(f"🕒 Hours: {r.get('hoursWorked')}")
                    h2.caption(f"⏰ In: {r.get('timeArrived')}")
                    h3.caption(f"⏰ Out: {r.get('timeDeparted')}")
                    
                    if is_admin and not is_completion:
                        if st.button("✏️ Edit Report", key=f"edit_rep_{r['id']}"):
                            st.session_state[f"editing_report_{job_id}"] = r['id']
                            st.rerun(scope="fragment")
                
                if r.get('content'):
                    st.write(r['content'])
                    
                if r.get('partsUsed'):
                    st.caption(f"🔩 Parts: {r['partsUsed']}")

                if r['photos']:
                    cols = st.columns(4)
                    for i, photo_source in enumerate(r['photos']):
                        with cols[i % 4]:
                            url = resolve_image_source(photo_source)
                            # Check if it's an image or a PDF
                            is_pdf = False
                            if isinstance(photo_source, str) and photo_source.lower().endswith('.pdf'):
                                is_pdf = True
                            
                            if is_pdf:
                                st.link_button("📄 View PDF", url, use_container_width=True)
                            else:
                                st.image(url, use_container_width=True)

    with tab_progress:
        st.write("#### 📸 Quick Update")
        st.caption("Add photos and notes while working. These save to history immediately.")
        
        # Quick Status Buttons
        qs_cols = st.columns(4)
        status_opts = [("🚗 En Route", "En Route to Site"), ("📍 Arrived", "Arrived on Site"), ("🥪 Lunch", "On Lunch Break"), ("✅ Done for Day", "Finished for the day")]
        
        for i, (label, note_text) in enumerate(status_opts):
            if qs_cols[i].button(label, key=f"qs_{i}_{job_id}"):
                # Post update immediately
                report_payload = {
                    'id': f"r{now_local().timestamp()}",
                    'techId': job['techId'] or 'unknown',
                    'timestamp': now_local().isoformat(),
                    'content': f"[{label}] {note_text}",
                    'photos': [],
                    'techsOnSite': "", 'timeArrived': "", 'timeDeparted': "", 
                    'hoursWorked': "", 'partsUsed': "", 'billableItems': ""
                }
                st.session_state.jobs[job_index]['reports'].append(report_payload)
                
                # Auto-update status for Arrived
                if label == "📍 Arrived" and job['status'] in ['Pending', 'Not Started']:
                    st.session_state.jobs[job_index]['status'] = 'In Progress'
                
                save_state()
                st.toast(f"Status updated: {label}", icon="✅")
                st.rerun(scope="fragment")

        # Voice Note Feature
        audio_val = st.audio_input("🎙️ Record Voice Note", key=f"audio_prog_{job_id}")
        transcribed_text = ""
        if audio_val:
            with st.spinner("Transcribing..."):
                transcribed_text = transcribe_audio(audio_val)
                if transcribed_text:
                    st.success("Audio Transcribed!")
        
        with st.form(key=f"progress_form_{job_id}"):
            # If we have a transcription, use it as the default value, otherwise empty
            default_note = transcribed_text if transcribed_text else ""
            prog_note = st.text_area("Note", value=default_note, placeholder="Quick update (e.g. 'Arrived on site', 'Found the issue')...")
            
            st.write("**Attach Photos & Docs**")
            c_cam, c_upl = st.columns(2)
            with c_cam:
                cam_pic = st.camera_input("Take Photo")
            with c_upl:
                upl_pics = st.file_uploader("Upload Images/PDFs", accept_multiple_files=True, type=['png', 'jpg', 'jpeg', 'pdf'])
                
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
                        'id': f"r{now_local().timestamp()}",
                        'techId': job['techId'] or 'unknown',
                        'timestamp': now_local().isoformat(),
                        'content': prog_note,
                        'photos': photos_list,
                        # Empty structured fields
                        'techsOnSite': "", 'timeArrived': "", 'timeDeparted': "", 
                        'hoursWorked': "", 'partsUsed': "", 'billableItems': ""
                    }
                    st.session_state.jobs[job_index]['reports'].append(report_payload)
                    
                    # Auto-set status to In Progress if Pending
                    if job['status'] in ['Pending', 'Not Started']:
                        st.session_state.jobs[job_index]['status'] = 'In Progress'
                        st.session_state.briefing = "Data required to generate briefing."
                    
                    save_state()
                    st.success("Update Posted!")
                    st.rerun(scope="fragment")
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
                st.markdown(f"**Warranty:** {'Yes' if payload.get('isWarranty') else 'No'}")
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
                st.rerun(scope="fragment")
                
            if c_no.button("❌ Cancel", key="conf_no"):
                del st.session_state[confirm_key]
                st.rerun(scope="fragment")
            
            st.divider()

        st.write("#### 📝 Daily Field Report")
        st.caption("End of day reporting. Submit labor hours, parts, and finalize status.")

        if loc and not has_sys_info:
            st.warning("🔐 No system info (logins / IPs) is saved for this site yet. Take a minute to fill out the **IPs & Passwords** tab while you're on site.")
        
        # Voice Note Feature for Daily Report
        audio_daily = st.audio_input("🎙️ Record Summary", key=f"audio_daily_{job_id}")
        daily_transcribed = ""
        if audio_daily:
            with st.spinner("Transcribing..."):
                daily_transcribed = transcribe_audio(audio_daily)
                if daily_transcribed:
                    st.success("Audio Transcribed!")

        # Prefill arrival/finish times from today's quick-status taps ("📍 Arrived" / "✅ Done for Day")
        today_prefix = now_local().strftime('%Y-%m-%d')
        default_arrived = datetime.time(8, 0)
        default_departed = datetime.time(17, 0)
        times_prefilled = False
        for qr in job['reports']:
            if qr.get('timestamp', '').startswith(today_prefix) and qr.get('content', '').startswith('[📍 Arrived]'):
                try:
                    default_arrived = datetime.datetime.fromisoformat(qr['timestamp']).time().replace(second=0, microsecond=0)
                    times_prefilled = True
                except Exception:
                    pass
                break
        for qr in reversed(job['reports']):
            if qr.get('timestamp', '').startswith(today_prefix) and qr.get('content', '').startswith('[✅ Done for Day]'):
                try:
                    default_departed = datetime.datetime.fromisoformat(qr['timestamp']).time().replace(second=0, microsecond=0)
                    times_prefilled = True
                except Exception:
                    pass
                break

        if times_prefilled:
            st.caption("⏱️ Times below were prefilled from your quick-status taps today — adjust if needed.")

        with st.form(key=f"daily_form_{job_id}"):
            status_options = ["Not Started", "In Progress", "Customer on Hold", "Waiting on Parts", "Parts not ordered", "Parts Staged", "Completed"]
            current_status = job['status']
            if current_status == "Pending": current_status = "Not Started"
            try:
                status_idx = status_options.index(current_status)
            except ValueError:
                status_idx = 0
            
            new_status = st.selectbox("Job Status", status_options, index=status_idx)
            is_warranty = st.checkbox("Warranty Work?", value=job.get('isWarranty', False))

            r_col1, r_col2 = st.columns(2)
            with r_col1:
                # Techs on Site: Multiselect
                available_techs = [t['name'] for t in st.session_state.techs]
                default_techs = [tech['name']] if tech and tech['name'] in available_techs else []
                
                techs_on_site_list = st.multiselect("Techs On Site", options=available_techs, default=default_techs)
                time_arrived = st.time_input("Time Arrived", value=default_arrived)
                parts_used = st.text_area("Parts/Materials Used")
            with r_col2:
                hours_worked = st.number_input("Hours Worked", min_value=0.0, step=0.5, help="Leave at 0 to auto-calculate from arrival/finish times.")
                time_departed = st.time_input("Time Finished", value=default_departed)
                billable_items = st.text_area("Billable Items / Extras")

            # Use transcribed text if available
            default_content = daily_transcribed if daily_transcribed else ""
            content = st.text_area("General Notes / Summary", value=default_content, placeholder="Detailed summary of work performed today...")
            
            # Logic to gather photos from "In-Progress" updates today
            current_date_str = now_local().strftime('%Y-%m-%d')
            todays_photos_set = set()
            for r in job['reports']:
                # Check timestamp match
                if r['timestamp'].startswith(current_date_str) and r.get('photos'):
                    # Only grab from "In-Progress" updates (which don't have structured data like hoursWorked)
                    # to avoid duplicating photos if a Daily Report was already submitted.
                    is_full_report = r.get('hoursWorked') or r.get('techsOnSite')
                    if not is_full_report:
                        for p_key in r['photos']:
                            todays_photos_set.add(p_key)
            
            todays_photos = list(todays_photos_set)
            
            if todays_photos:
                st.info(f"📸 {len(todays_photos)} photos taken today via 'In-Progress' updates will be automatically attached.")
            
            # Allow adding more photos directly here
            daily_photos = st.file_uploader("Attach Additional Photos/Docs (Optional)", accept_multiple_files=True, type=['png', 'jpg', 'jpeg', 'pdf'], key=f"daily_up_{job_id}")

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

                # Auto-calculate hours from arrival/finish times when left at 0
                if not hours_worked:
                    arr_dt = datetime.datetime.combine(datetime.date.today(), time_arrived)
                    dep_dt = datetime.datetime.combine(datetime.date.today(), time_departed)
                    if dep_dt > arr_dt:
                        hours_worked = round((dep_dt - arr_dt).total_seconds() / 3600 * 4) / 4

                # Construct Report Data
                report_payload = {
                    'id': f"r{now_local().timestamp()}",
                    'techId': job['techId'] or 'unknown',
                    'timestamp': now_local().isoformat(),
                    'content': content,
                    'techsOnSite': ", ".join(techs_on_site_list),
                    'timeArrived': str(time_arrived),
                    'timeDeparted': str(time_departed),
                    'hoursWorked': str(hours_worked),
                    'partsUsed': parts_used,
                    'billableItems': billable_items,
                    'isWarranty': is_warranty,
                    'photos': todays_photos # Photos handled in other tab
                }

                if email_btn:
                    # Trigger confirmation flow (fragment scope keeps the dialog open)
                    st.session_state[f"confirm_daily_send_{job['id']}"] = report_payload
                    st.rerun(scope="fragment")

                if submit_btn:
                    if new_status == "Completed":
                        # Set pending state and rerun to show confirmation UI.
                        # Fragment scope keeps the dialog open so the confirmation
                        # appears immediately instead of the window closing.
                        st.session_state[f"completion_pending_{job['id']}"] = report_payload
                        st.rerun(scope="fragment")
                    else:
                        # Automatically send email to admins for in-progress daily reports
                        with st.spinner("Sending Daily Report to Admins..."):
                            send_daily_report_email(job, tech, loc, report_payload)
                            
                        st.session_state.jobs[job_index]['reports'].append(report_payload)
                        
                        # Update Status
                        if new_status != job['status']:
                            st.session_state.jobs[job_index]['status'] = new_status
                            st.session_state.briefing = "Data required to generate briefing."
                        
                        save_state()
                        st.success("Daily Report Submitted & Emailed to Admins!")
                        st.rerun(scope="fragment")

    # --- DEFERRED WEATHER ---
    # The dialog body has now rendered, so the network call below backfills the
    # weather into the address line without having delayed any of the tabs.
    if loc and weather_ph is not None and loc.get('address'):
        try:
            lat, lon = loc.get('lat'), loc.get('lon')
            try:
                lat = float(lat) if lat is not None else None
                lon = float(lon) if lon is not None else None
            except (ValueError, TypeError):
                lat = lon = None

            # Geocode once and persist on the location (skipped on every later view)
            if not lat or not lon:
                lat, lon = get_lat_lon_from_address(loc['address'])
                if lat and lon:
                    loc['lat'], loc['lon'] = lat, lon
                    save_state(invalidate_briefing=False)

            if lat and lon:
                weather = get_weather(lat, lon)
                if weather:
                    weather_ph.caption(f"{loc['address']} | {weather}")
        except Exception:
            pass

# --- UI COMPONENTS ---


def render_job_card(job, compact=False, key_suffix="", allow_delete=False):
    tech = get_tech(job['techId'])
    loc = get_location(job['locationId'])
    loc_name = loc['name'] if loc else "Unknown"
    tech_name = tech['name'] if tech else "Unassigned"
    
    priority_class = f"priority-{job['priority']}"
    status_bg = get_status_color(job['status'])
    
    map_url = loc.get('mapsUrl') or get_google_maps_url(loc['address']) if loc else None
    loc_html = f'<a href="{map_url}" target="_blank" style="color:#a1a1aa; text-decoration:none;">📍 {loc_name}</a>' if map_url else f"📍 {loc_name}"

    stale_days = get_job_stale_days(job)
    stale_html = ""
    if stale_days is not None and stale_days >= STALE_JOB_DAYS:
        stale_html = f'<div style="color:#f87171; font-size:0.8em; margin-top:6px; font-weight:bold;">🚨 No updates in {stale_days} days</div>'

    # Parts progress badge (makes the Tech Board parts columns actionable)
    staged_parts, total_parts = parts_summary(job)
    parts_html = ""
    if total_parts:
        parts_color = "#10b981" if staged_parts == total_parts else "#a1a1aa"
        parts_html = f'<div style="color:{parts_color}; font-size:0.8em; margin-top:6px;">🔩 Parts: {staged_parts}/{total_parts} staged</div>'

    with st.container():
        st.markdown(f"""
        <div class="job-card {priority_class}" style="position:relative; overflow:hidden; border-top: 4px solid {status_bg};">
            <div style="position:absolute; top:0; right:0; padding:2px 8px; background:{status_bg}; color:white; font-size:0.65em; font-weight:bold; border-bottom-left-radius:8px;">
                {job['status'].upper()}
            </div>
            <div style="display:flex; justify-content:space-between; margin-top:10px;">
                <span style="font-weight:bold; font-size:1.1em; max-width:70%;">{job['title']}</span>
                <span style="font-size:0.8em; background:#3f3f46; padding:2px 6px; border-radius:4px; height:fit-content;">{job['priority']}</span>
            </div>
            <div style="color:#a1a1aa; font-size:0.9em; margin-top:5px;">{loc_html}</div>
            <div style="display:flex; justify-content:space-between; margin-top:10px; font-size:0.8em; color:#71717a;">
                 <span>👤 {tech_name}</span>
                 <span>📅 {'🗓️ ' + job['date'][:10]}</span>
            </div>{stale_html}{parts_html}
        </div>
        """, unsafe_allow_html=True)
        # Status Dropdown
        status_options = ["Not Started", "In Progress", "Customer on Hold", "Waiting on Parts", "Parts not ordered", "Parts Staged", "Completed"]
        current_status = job['status']
        if current_status == "Pending": current_status = "Not Started"
        
        try:
            status_idx = status_options.index(current_status)
        except ValueError:
            status_idx = 0
            
        widget_key = f"status_change_{job['id']}_{key_suffix}"
        st.selectbox(
            "Change Status",
            status_options,
            index=status_idx,
            key=widget_key,
            on_change=update_job_status_callback,
            args=(job['id'], widget_key),
            label_visibility="collapsed"
        )

        # Unique key using job ID AND suffix to prevent Streamlit duplicates
        if allow_delete:
            c1, c2, c3 = st.columns([4, 1.2, 1.2])
            with c1:
                if st.button("View Details", key=f"btn_{job['id']}_{key_suffix}", use_container_width=True):
                    job_details_dialog(job['id'])
            with c2:
                if st.button("✏️", key=f"edit_{job['id']}_{key_suffix}", help="Edit Job", use_container_width=True):
                    edit_job_dialog(job['id'])
            with c3:
                if st.button("🗑️", key=f"del_{job['id']}_{key_suffix}", help="Delete Job", use_container_width=True):
                    if job in st.session_state.jobs:
                        st.session_state.jobs.remove(job)
                        save_state()
                        st.rerun()
        else:
            if st.button("View Details", key=f"btn_{job['id']}_{key_suffix}", use_container_width=True):
                job_details_dialog(job['id'])
            


def render_map_view(jobs):
    """Interactive Folium map: one dot per job at its location, colored by status
    (same palette as the Tech Board). Click a dot for a detail card + Navigate link."""
    if not HAS_MAP:
        st.info("🗺️ Map view needs the `folium` and `streamlit-folium` packages. "
                "Add them to requirements.txt and redeploy.")
        return

    def _m_esc(s):
        return (str(s if s is not None else "").replace('&', '&amp;')
                .replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;'))

    # Status legend (matches the Tech Board columns)
    legend_statuses = ["Not Started", "In Progress", "Customer on Hold",
                       "Waiting on Parts", "Parts not ordered", "Parts Staged"]
    legend = '<div style="display:flex;gap:14px;flex-wrap:wrap;margin-bottom:8px;font-size:0.8em;color:#a1a1aa;">'
    for sname in legend_statuses:
        legend += (f'<span style="display:inline-flex;align-items:center;gap:5px;">'
                   f'<span style="width:11px;height:11px;border-radius:50%;background:{get_status_color(sname)};'
                   f'display:inline-block;"></span>{sname}</span>')
    legend += '</div>'
    st.markdown(legend, unsafe_allow_html=True)

    # Resolve a lat/lon for each job, geocoding any location that lacks one (then persist)
    points = []
    skipped = 0
    geocoded_any = False
    with st.spinner("Locating jobs..."):
        for job in jobs:
            loc = get_location(job['locationId'])
            if not loc or not loc.get('address'):
                skipped += 1
                continue
            lat, lon = loc.get('lat'), loc.get('lon')
            try:
                lat = float(lat) if lat is not None else None
                lon = float(lon) if lon is not None else None
            except (ValueError, TypeError):
                lat = lon = None
            if not lat or not lon:
                lat, lon = get_lat_lon_from_address(loc['address'])
                if lat and lon:
                    loc['lat'], loc['lon'] = lat, lon
                    geocoded_any = True
            if lat and lon:
                points.append((job, loc, lat, lon))
            else:
                skipped += 1
    if geocoded_any:
        save_state(invalidate_briefing=False)

    if not points:
        st.info("No mappable jobs yet — none of the active jobs have a geocodable address.")
        return

    avg_lat = sum(p[2] for p in points) / len(points)
    avg_lon = sum(p[3] for p in points) / len(points)
    fmap = folium.Map(location=[avg_lat, avg_lon], zoom_start=8, tiles="CartoDB positron")

    # Nudge markers that share exact coordinates so they don't fully overlap
    coord_seen = {}
    for job, loc, lat, lon in points:
        key = (round(lat, 5), round(lon, 5))
        n = coord_seen.get(key, 0)
        coord_seen[key] = n + 1
        if n:
            lat += 0.0005 * n
            lon += 0.0005 * n

        color = get_status_color(job['status'])
        jtech = get_tech(job['techId'])
        nav_url = f"https://www.google.com/maps/dir/?api=1&destination={urllib.parse.quote(loc['address'])}"

        popup_html = (
            f'<div style="font-family:Arial,sans-serif;width:230px;">'
            f'<div style="font-weight:bold;font-size:14px;color:#18181b;margin-bottom:5px;">{_m_esc(job["title"])}</div>'
            f'<span style="background:{color};color:white;padding:1px 8px;border-radius:8px;font-size:11px;">{_m_esc(job["status"])}</span> '
            f'<span style="background:#3f3f46;color:white;padding:1px 8px;border-radius:8px;font-size:11px;">{_m_esc(job.get("priority", "N/A"))}</span>'
            f'<div style="font-size:12px;color:#333;margin-top:7px;">📍 <b>{_m_esc(loc["name"])}</b><br>{_m_esc(loc["address"])}</div>'
            f'<div style="font-size:12px;color:#333;margin-top:4px;">👤 {_m_esc(jtech["name"] if jtech else "Unassigned")}</div>'
            f'<a href="{nav_url}" target="_blank" style="display:inline-block;margin-top:9px;background:#b91c1c;'
            f'color:white;padding:6px 14px;border-radius:6px;text-decoration:none;font-size:12px;font-weight:bold;">🧭 Navigate</a>'
            f'</div>'
        )

        folium.CircleMarker(
            location=[lat, lon],
            radius=9, color="#27272a", weight=1.5,
            fill=True, fill_color=color, fill_opacity=0.9,
            tooltip=job['title'],
            popup=folium.Popup(popup_html, max_width=260),
        ).add_to(fmap)

    # Frame all markers
    lats = [p[2] for p in points]
    lons = [p[3] for p in points]
    if len(points) > 1:
        fmap.fit_bounds([[min(lats), min(lons)], [max(lats), max(lons)]])

    # returned_objects=[] keeps panning/clicking from triggering heavy app reruns
    st_folium(fmap, use_container_width=True, height=600, returned_objects=[], key="jobs_map")

    if skipped:
        st.caption(f"⚠️ {skipped} job(s) not shown — address missing or could not be geocoded.")


def render_analytics_dashboard():
    st.subheader("📊 Operational Analytics")

    if not st.session_state.jobs:
        st.info("No job data available.")
        return

    df = pd.DataFrame(st.session_state.jobs)

    total = len(df)
    completed = len(df[df["status"] == "Completed"])
    active = total - completed
    critical = len(df[df["priority"] == "Critical"])

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Jobs", total)
    m2.metric("Active", active)
    m3.metric("Completed", completed)
    m4.metric("Critical", critical)

    st.divider()

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Jobs by Status")
        status_counts = df["status"].value_counts()
        st.bar_chart(status_counts)  # remove color param if it errors

    with c2:
        st.markdown("#### Jobs by Priority")
        prio_counts = df["priority"].value_counts()
        st.bar_chart(prio_counts)  # remove color param if it errors

    st.divider()

    c3, c4 = st.columns(2)
    with c3:
        st.markdown("#### Tech Workload (Active)")
        active_jobs = df[df["status"] != "Completed"]
        if not active_jobs.empty:
            tech_map = {t["id"]: t["name"] for t in st.session_state.techs}
            tech_map[None] = "Unassigned"
            workload = active_jobs["techId"].map(tech_map).fillna("Unassigned").value_counts()
            st.bar_chart(workload)

    with c4:
        st.markdown("#### Jobs by Type")
        type_counts = df["type"].value_counts()
        st.bar_chart(type_counts)

    st.divider()
    
    # --- AI PARTS ANALYSIS ---
    st.markdown("#### 🔩 AI Parts Usage Tracker")
    st.caption("Uses Gemini to extract and aggregate parts data from unstructured technician notes.")
    
    if st.button("🤖 Analyze Parts Usage"):
        with st.spinner("Analyzing all job reports..."):
            # 1. Gather all "Parts Used" text
            all_parts_text = []
            for j in st.session_state.jobs:
                for r in j.get('reports', []):
                    if r.get('partsUsed'):
                        all_parts_text.append(f"- {r['partsUsed']}")
            
            if not all_parts_text:
                st.warning("No parts usage recorded in reports yet.")
            else:
                # 2. Send to Gemini
                api_key = get_api_key()
                if api_key:
                    client, model_name = get_available_model(api_key)
                    prompt = f"""
                    Analyze the following list of "Parts Used" entries from technician reports.
                    Consolidate them into a single JSON object where keys are the standardized part names (e.g., "Cat6 Cable", "NVR Power Supply") and values are the total estimated quantity used (integer).
                    Ignore vague entries like "none" or "N/A".
                    
                    Input List:
                    {chr(10).join(all_parts_text)}
                    
                    Return ONLY valid JSON. Example: {{"Cat6 Cable (ft)": 500, "RJ45 Jacks": 10}}
                    """
                    try:
                        response = client.models.generate_content(model=model_name, contents=prompt)
                        # Clean response to ensure just JSON
                        json_str = response.text.strip()
                        if "```json" in json_str:
                            json_str = json_str.split("```json")[1].split("```")[0]
                        elif "```" in json_str:
                            json_str = json_str.split("```")[1].split("```")[0]
                            
                        parts_data = json.loads(json_str)
                        
                        if parts_data:
                            st.bar_chart(parts_data, horizontal=True)
                        else:
                            st.info("AI found no quantifiable parts data.")
                            
                    except Exception as e:
                        st.error(f"Analysis failed: {e}")
                else:
                    st.error("API Key missing.")

    st.divider()
    
    st.markdown("#### 🏆 Technician Leaderboard (Completed Jobs)")
    completed_jobs = df[df["status"] == "Completed"]
    if not completed_jobs.empty:
        tech_map = {t["id"]: t["name"] for t in st.session_state.techs}
        tech_map[None] = "Unassigned"
        
        # Count completed jobs per tech
        leaderboard = completed_jobs["techId"].map(tech_map).fillna("Unassigned").value_counts()
        
        # Display as horizontal bar chart
        st.bar_chart(leaderboard, horizontal=True, color="#b91c1c")
    else:
        st.info("No completed jobs yet.")



    # --- ADMIN ACCESS MANAGEMENT ---
def render_hours_report():
    st.caption("Summed from daily report 'Hours Worked'. Hours are credited to every tech listed 'On Site' for a report (or the report author if none were listed).")

    today = now_local().date()
    hc1, hc2 = st.columns(2)
    start_date = hc1.date_input("From", value=today - datetime.timedelta(days=13), key="hours_from")
    end_date = hc2.date_input("To", value=today, key="hours_to")

    rows = compute_hours_rows(st.session_state.jobs, st.session_state.techs, st.session_state.locations, start_date, end_date)

    if not rows:
        st.info("No logged hours in this date range.")
        return

    df = pd.DataFrame(rows)

    st.write("##### Total Hours by Tech")
    totals = df.groupby("Tech", as_index=False)["Hours"].sum().sort_values("Hours", ascending=False)
    st.dataframe(totals, use_container_width=True, hide_index=True)

    st.write("##### Hours by Tech & Job")
    by_job = df.groupby(["Tech", "Job", "Location"], as_index=False)["Hours"].sum().sort_values(["Tech", "Hours"], ascending=[True, False])
    st.dataframe(by_job, use_container_width=True, hide_index=True)

    with st.expander("📄 All Entries"):
        st.dataframe(df.sort_values("Date", ascending=False), use_container_width=True, hide_index=True)

    st.download_button(
        "⬇️ Download CSV (all entries)",
        df.sort_values(["Date", "Tech"]).to_csv(index=False).encode("utf-8"),
        file_name=f"hours_{start_date}_{end_date}.csv",
        mime="text/csv",
    )

def render_admin_panel():
    # --- DEDUPLICATE IDs (Fix for existing corrupted state) ---
    if st.session_state.techs:
        seen_t_ids = set()
        # We must iterate over a copy or indices if we were modifying the list structure, 
        # but here we modify objects inside the list, which is safe.
        # However, to check for global uniqueness, we need to be careful.
        # Simple approach: Re-assign ALL IDs if duplicates found? No, that breaks references.
        # Only re-assign duplicates.
        # First pass: collect all IDs.
        all_ids = [t['id'] for t in st.session_state.techs]
        if len(all_ids) != len(set(all_ids)):
            # Duplicates exist.
            seen = set()
            for t in st.session_state.techs:
                if t['id'] in seen:
                    # This is a duplicate. Assign new ID.
                    # Find max ID number.
                    existing_nums = [int(x['id'][1:]) for x in st.session_state.techs if x['id'].startswith('t') and x['id'][1:].isdigit()]
                    next_num = (max(existing_nums) if existing_nums else 0) + 1
                    t['id'] = f"t{next_num}"
                seen.add(t['id'])
            save_state(invalidate_briefing=False)

    if st.session_state.locations:
        all_l_ids = [l['id'] for l in st.session_state.locations]
        if len(all_l_ids) != len(set(all_l_ids)):
            seen = set()
            for l in st.session_state.locations:
                if l['id'] in seen:
                    existing_nums = [int(x['id'][1:]) for x in st.session_state.locations if x['id'].startswith('l') and x['id'][1:].isdigit()]
                    next_num = (max(existing_nums) if existing_nums else 0) + 1
                    l['id'] = f"l{next_num}"
                seen.add(l['id'])
            save_state(invalidate_briefing=False)

    # --- ADMIN ACCESS MANAGEMENT ---
    st.subheader("🔑 Admin Access Management")
    with st.expander("Manage Admin Emails", expanded=False):
        st.write("Add emails that are allowed to access this Admin Panel.")
        
        # Add Admin
        with st.form("add_admin_form"):
            new_admin_email = st.text_input("New Admin Email")
            if st.form_submit_button("Add Admin"):
                if new_admin_email and "@" in new_admin_email:
                    if new_admin_email not in st.session_state.adminEmails:
                        st.session_state.adminEmails.append(new_admin_email)
                        save_state(invalidate_briefing=False)
                        st.success(f"Added {new_admin_email}")
                        st.rerun()
                    else:
                        st.warning("Email already exists.")
                else:
                    st.error("Invalid email.")

        # List / Remove Admins
        if st.session_state.adminEmails:
            st.write("###### Current Admins")
            for email in st.session_state.adminEmails:
                c1, c2 = st.columns([4, 1])
                c1.write(email)
                if c2.button("🗑️", key=f"del_admin_{email}"):
                    st.session_state.adminEmails.remove(email)
                    save_state(invalidate_briefing=False)
                    st.rerun()

    st.divider()

    # --- SMTP CONFIG ---
    st.subheader("📧 SMTP Configuration")
    with st.expander("Configure Email Settings", expanded=False):
        with st.form("smtp_config_form"):
            # Load existing from session or secrets
            current_smtp = st.session_state.get('smtp_settings', {})
            
            # Fallback to secrets if not in session
            if not current_smtp:
                current_smtp = {
                    "SMTP_SERVER": st.secrets.get("SMTP_SERVER", ""),
                    "SMTP_PORT": st.secrets.get("SMTP_PORT", 587),
                    "SMTP_EMAIL": st.secrets.get("SMTP_EMAIL", ""),
                    "SMTP_PASSWORD": st.secrets.get("SMTP_PASSWORD", "")
                }

            s_server = st.text_input("SMTP Server", value=current_smtp.get("SMTP_SERVER", ""))
            s_port = st.number_input("SMTP Port", value=int(current_smtp.get("SMTP_PORT", 587)))
            s_email = st.text_input("Sender Email", value=current_smtp.get("SMTP_EMAIL", ""))
            s_pass = st.text_input("Sender Password", value=current_smtp.get("SMTP_PASSWORD", ""), type="password")
            
            if st.form_submit_button("Save SMTP Settings"):
                st.session_state.smtp_settings = {
                    "SMTP_SERVER": s_server,
                    "SMTP_PORT": s_port,
                    "SMTP_EMAIL": s_email,
                    "SMTP_PASSWORD": s_pass
                }
                save_state(invalidate_briefing=False)
                st.success("SMTP Settings Saved to Database!")
                st.rerun()
    
    st.divider()

    # --- TECH MANAGEMENT ---
    st.subheader("👷 Manage Technicians")
    with st.expander("Add / Remove Technicians", expanded=False):
        # Add Tech
        with st.form("add_tech_form"):
            c1, c2, c3 = st.columns([2, 2, 1])
            new_tech_name = c1.text_input("Name")
            new_tech_email = c2.text_input("Email")
            new_tech_initials = c3.text_input("Initials (2 chars)", max_chars=2)
            
            new_tech_skills = st.multiselect("Skills", options=SKILL_OPTIONS)
            
            if st.form_submit_button("Add Technician"):
                if new_tech_name and new_tech_email and new_tech_initials:
                    existing_ids = [int(t['id'][1:]) for t in st.session_state.techs if t['id'].startswith('t') and t['id'][1:].isdigit()]
                    next_id = (max(existing_ids) if existing_ids else 0) + 1
                    new_id = f"t{next_id}"
                    import random
                    color = random.choice(TECH_COLORS)
                    
                    st.session_state.techs.append({
                        "id": new_id,
                        "name": new_tech_name,
                        "email": new_tech_email,
                        "initials": new_tech_initials.upper(),
                        "color": color,
                        "skills": new_tech_skills
                    })
                    save_state(invalidate_briefing=False)
                    st.success(f"Added {new_tech_name}")
                    # Removed st.rerun() to prevent thread error
                else:
                    st.error("All fields required.")

        # List / Remove Techs
        if st.session_state.techs:
            st.write("###### Current Technicians")
            for t in st.session_state.techs:
                c1, c2, c3, c4 = st.columns([1, 3, 4, 1])
                c1.markdown(f"**{t['initials']}**")
                
                skills_display = ""
                if t.get('skills'):
                    skills_display = f" | 🛠️ {', '.join(t['skills'])}"
                    
                c2.write(f"{t['name']}{skills_display}")
                c3.write(t['email'])
                if c4.button("🗑️", key=f"del_tech_{t['id']}"):
                    st.session_state.techs.remove(t)
                    save_state(invalidate_briefing=False)
                    st.rerun()

    st.divider()

    # --- LOCATION MANAGEMENT ---
    st.subheader("📍 Manage Locations")
    with st.expander("Add / Remove Locations", expanded=False):
        # Add Location
        with st.form("add_loc_form"):
            l_name = st.text_input("Location Name")
            l_addr = st.text_input("Address")
            l_maps = st.text_input("Google Maps Link (Optional)")
            
            c_l1, c_l2 = st.columns(2)
            l_contact_name = c_l1.text_input("Site Contact Name")
            l_contact_phone = c_l2.text_input("Site Contact Phone")
            
            if st.form_submit_button("Add Location"):
                if l_name and l_addr:
                    # Auto-suggest address if API key exists
                    final_addr = suggest_address_with_gemini(l_addr)
                    
                    existing_ids = [int(l['id'][1:]) for l in st.session_state.locations if l['id'].startswith('l') and l['id'][1:].isdigit()]
                    next_id = (max(existing_ids) if existing_ids else 0) + 1
                    
                    new_loc = {
                        "id": f"l{next_id}",
                        "name": l_name,
                        "address": final_addr,
                        "mapsUrl": l_maps,
                        "contact_name": l_contact_name,
                        "contact_phone": l_contact_phone
                    }
                    st.session_state.locations.append(new_loc)
                    save_state(invalidate_briefing=False)
                    st.success(f"Added {l_name}")
                    st.rerun()
                else:
                    st.error("Name and Address required.")

        # List / Remove Locations
        if st.session_state.locations:
            st.write("###### Current Locations")
            for l in st.session_state.locations:
                c1, c2, c3, c4 = st.columns([3, 4, 1, 1])
                c1.write(l['name'])
                
                contact_info = ""
                if l.get('contact_name') or l.get('contact_phone'):
                    contact_info = f" | 📞 {l.get('contact_name','')} {l.get('contact_phone','')}"
                    
                c2.caption(f"{l['address']}{contact_info}")
                
                if c3.button("✏️", key=f"edit_loc_{l['id']}"):
                    edit_location_dialog(l['id'])
                    
                if c4.button("🗑️", key=f"del_loc_{l['id']}"):
                    st.session_state.locations.remove(l)
                    save_state(invalidate_briefing=False)
                    st.rerun()

    st.divider()

    st.subheader("System Maintenance")
    c_m1, c_m2 = st.columns(2)
    with c_m1:
        if st.button("🧹 Clear App Cache"):
            st.cache_resource.clear()
            st.cache_data.clear()
            st.toast("Cache cleared!", icon="🧹")
            st.rerun()
    
    st.divider()

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

    # --- BACKUP / RESTORE ---
    st.subheader("Backup & Restore")
    c_bk1, c_bk2 = st.columns(2)

    with c_bk1:
        csv_data = download_data_as_csv()
        if csv_data:
            st.download_button(
                label="📥 Download Jobs CSV",
                data=csv_data,
                file_name=f"jobs_export_{now_local().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )
        else:
            st.button("📥 Download Jobs CSV", disabled=True)

        json_data = download_data_as_json()
        st.download_button(
            label="📦 Download Full Backup (JSON)",
            data=json_data,
            file_name=f"backup_{now_local().strftime('%Y%m%d')}.json",
            mime="application/json",
        )

    with c_bk2:
        uploaded_file = st.file_uploader("Restore Backup (JSON)", type=["json"], key="restore_json")
        if uploaded_file is not None:
            if st.button("⚠️ Restore from Backup", key="restore_btn"):
                try:
                    data = json.load(uploaded_file)
                    required_keys = ["jobs", "techs", "locations"]

                    if not all(k in data for k in required_keys):
                        st.error("Invalid backup file format.")
                    else:
                        st.session_state.jobs = data["jobs"]
                        st.session_state.techs = data["techs"]
                        st.session_state.locations = data["locations"]
                        st.session_state.briefing = data.get("briefing", "Data required to generate briefing.")
                        st.session_state.adminEmails = data.get("adminEmails", [])
                        st.session_state.last_reminder_date = data.get("last_reminder_date")

                        ensure_loaded_into_session()
                        _sync_session_to_db()
                        force_overwrite_from_session(invalidate_briefing=False)

                        st.success("Data restored successfully (DB overwritten).")
                        st.rerun()
                except Exception as e:
                    st.error(f"Error restoring file: {e}")

    st.divider()

    # --- STORAGE DEBUGGER ---
    st.subheader("☁️ Storage Debugger (R2/S3)")
    with st.expander("Test Storage Connection", expanded=False):
        st.caption("Use this to troubleshoot photo upload issues.")
        
        from object_store import get_r2_client, get_bucket_name, HAS_BOTO3
        
        if not HAS_BOTO3:
            st.error("❌ `boto3` library is missing. Cannot connect to storage.")
        else:
            if st.button("Test Connection"):
                try:
                    s3 = get_r2_client()
                    bucket = get_bucket_name()
                    
                    if not s3:
                        st.error("❌ Failed to initialize S3 client. Check credentials (R2_ACCESS_KEY_ID, etc).")
                    elif not bucket:
                        st.error("❌ Bucket name is missing (R2_BUCKET_NAME).")
                    else:
                        # Display configuration details (masked)
                        endpoint = s3.meta.endpoint_url
                        region = s3.meta.region_name
                        
                        st.info(f"**Endpoint:** `{endpoint}`")
                        st.info(f"**Bucket:** `{bucket}`")
                        st.info(f"**Region:** `{region}`")
                        
                        if endpoint and bucket in endpoint:
                            st.warning("⚠️ **Potential Configuration Issue:** The Bucket Name appears to be part of the Endpoint URL. R2 Endpoint URLs should usually end with `.r2.cloudflarestorage.com` and NOT include the bucket name.")

                        # Try listing objects (lightweight check)
                        s3.list_objects_v2(Bucket=bucket, MaxKeys=1)
                        st.success(f"✅ Successfully connected to bucket: `{bucket}`")
                        st.toast("Storage connection verified!", icon="✅")
                except Exception as e:
                    st.error(f"❌ Connection failed: {e}")
                    # Check for common errors
                    if "InvalidAccessKeyId" in str(e):
                        st.warning("💡 **Tip:** Double-check your Access Key ID. Ensure no leading/trailing spaces.")
                    elif "SignatureDoesNotMatch" in str(e):
                        st.warning("💡 **Tip:** Double-check your Secret Access Key. Ensure no leading/trailing spaces.")
                    elif "NoSuchBucket" in str(e):
                        st.warning(f"💡 **Tip:** The bucket `{bucket}` does not exist or is not accessible with these credentials.")
                    elif "EndpointConnectionError" in str(e):
                         st.warning("💡 **Tip:** Could not connect to the endpoint URL. Check for typos.")

    # --- SYSTEM LOGS ---
    st.subheader("📋 System Event Logs")
    with st.expander("View Background Logs", expanded=False):
        logs = get_logger().get_logs()
        if not logs:
            st.info("No system events logged yet.")
        else:
            for log_entry in logs:
                st.code(log_entry, language="text")

    st.divider()

    # --- ANALYTICS ---
    st.subheader("🤖 AI Service Diagnostics")
    with st.expander("Test Gemini API Connection", expanded=False):
        st.caption("Check your API key status and model accessibility.")
        
        api_key = get_api_key()
        if not api_key:
            st.error("❌ No API Key found. Set `GEMINI_API_KEY` in Streamlit Secrets.")
        else:
            st.code(f"Key Found: {'*' * (len(api_key)-4)}{api_key[-4:]}")
            
            if st.button("Run AI Diagnostics"):
                try:
                    # 1. Test Client Initialization
                    client = genai.Client(api_key=api_key)
                    st.success("✅ Gemini Client Initialized.")
                    
                    # 2. List Models
                    with st.spinner("Fetching available models..."):
                        all_models = list(client.models.list())
                        model_names = [m.name for m in all_models]
                        st.write(f"**Available Models ({len(model_names)}):**")
                        st.json(model_names[:10]) # Show first 10
                    
                    # 3. Test simple generation
                    with st.spinner("Testing generation..."):
                        # Get best model
                        _, model_name = get_available_model(api_key)
                        st.info(f"Targeting Model: `{model_name}`")
                        
                        test_resp = client.models.generate_content(
                            model=model_name, 
                            contents="Say 'Connection Successful' if you can read this."
                        )
                        st.success(f"✅ AI Response: {test_resp.text}")
                        st.toast("AI System is fully operational!", icon="🤖")
                
                except Exception as e:
                    err_str = str(e)
                    st.error(f"❌ Diagnostic Failed: {err_str}")
                    
                    if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                        st.warning("⚠️ **Rate Limit / Quota Exhausted:** If you are on the **Paid 1** tier, this usually indicates that the account has reached its burst limit or the billing upgrade is still propagating (can take 10-15 mins). On the **Free Tier**, this means you've hit the monthly or daily limit.")
                    elif "API_KEY_INVALID" in err_str:
                        st.warning("⚠️ **Invalid Key:** Ensure the key is copied exactly from AI Studio.")
                    elif "billing" in err_str.lower() or "quota" in err_str.lower():
                        st.warning("⚠️ **Quota/Billing:** Your account might have run out of free credits or billing isn't fully active yet.")

    st.divider()
    st.subheader("🕒 Hours Report")
    with st.expander("View Hours (Payroll / Invoicing)", expanded=False):
        render_hours_report()

    st.divider()
    with st.expander("📊 View Analytics Dashboard", expanded=False):
        render_analytics_dashboard()

    st.divider()

    # --- SYSTEM LOGS (optional) ---
    st.subheader("📝 System Logs")
    with st.expander("View Background Activity", expanded=False):
        st.caption("Recent keep-awake pings and system events.")

        c_log1, c_log2 = st.columns([3, 1])
        with c_log2:
            if st.button("⚡ Test Ping Now"):
                endpoints = [
                    "http://localhost:8501/_stcore/health",
                    "http://127.0.0.1:8501/_stcore/health",
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
        client, model_name = get_available_model(api_key)
        
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
        
        # SECURITY: strip site credentials/systems (logins, passwords, IPs)
        # before sending location data to the external LLM API
        safe_locations = [
            {k: v for k, v in l.items() if k not in ('credentials', 'systems')}
            for l in st.session_state.locations
        ]

        system_context = f"""
       You are a 5G Security Assistant.
       Current Time: {now_local()}
       Techs: {json.dumps(st.session_state.techs)}
       Locations: {json.dumps(safe_locations)}
       Jobs: {json.dumps(simple_jobs)}
       
       Answer based strictly on this data. If searching for history, note that detailed reports are not in this context, only summaries.
       """
        
        full_prompt = f"{system_context}\n\nUser Question: {prompt}"
        
        try:
            with st.sidebar.chat_message("model"):
                with st.spinner("Thinking..."):
                    response = client.models.generate_content(model=model_name, contents=full_prompt)
                    bot_reply = response.text
                    st.write(bot_reply)
                    
            st.session_state.chat_history.append({"role": "model", "parts": [bot_reply]})
        except Exception as e:
            st.sidebar.error(f"AI Error: {str(e)}")
            try:
                # Debug: List available models to help diagnose
                all_models = list(client.models.list())
                model_names = [m.name for m in all_models]
                st.sidebar.warning(f"Available models: {model_names}")
            except Exception as debug_e:
                st.sidebar.error(f"Could not list models: {str(debug_e)}")

# --- LIVE UPDATE WATCHER ---

@st.fragment(run_every="15s")
def live_update_watcher():
    """Keeps idle sessions in sync. Polls the DB version every 15s; when another
    user saves, quietly refreshes this session's data and shows a refresh banner.
    Deliberately does NOT force a full rerun - that would close any open dialog
    (e.g. a tech mid-report). Any interaction redraws with fresh data anyway."""
    try:
        db_ver = get_db_version()
    except Exception:
        return
    if db_ver is None or st.session_state.get('_db_version') is None:
        return

    if db_ver != st.session_state._db_version:
        refresh_session_from_db()
        st.session_state['_pending_board_update'] = True

    if st.session_state.get('_pending_board_update'):
        c1, c2 = st.columns([4, 1])
        c1.info("🔄 The board was updated by another user. Refresh to see the latest.")
        if c2.button("Refresh now", key="live_refresh_btn", use_container_width=True):
            st.session_state.pop('_pending_board_update', None)
            st.rerun(scope="app")

# --- MAIN APP FLOW ---

def main():
    # Start Keep Awake Thread
    keep_awake()
    start_background_scheduler()

    # 1. Authenticate User
    user = authenticate()
    if not user:
        return  # Stop rendering if not logged in

    # Pick up other users' saves: if the DB moved on since this session loaded,
    # refresh so we render current data (and so our next save doesn't conflict).
    try:
        db_ver = get_db_version()
        if db_ver is not None and st.session_state.get('_db_version') is not None and db_ver != st.session_state._db_version:
            refresh_session_from_db()
    except Exception:
        pass
    # A full run means the page is being redrawn with fresh data - clear any pending banner
    st.session_state.pop('_pending_board_update', None)

    # Deep-link: open a job dialog requested from elsewhere (e.g. Site History)
    open_target = st.session_state.pop("_open_job_after_rerun", None)
    if open_target:
        job_details_dialog(open_target)

    user_email = user.get("email")
    user_name = user.get("name")
    
    # 2. Determine Role (Admin or Tech)
    # Bootstrapping: If no admins exist in DB, first login becomes Admin
    if not st.session_state.adminEmails:
        st.session_state.adminEmails.append(user_email)
        save_state()
        st.toast(f"First login detected. {user_email} is now Super Admin.", icon="🛡️")
    
    is_admin = user_email in st.session_state.adminEmails

    # 2.5 ACCESS CONTROL: only admins, registered techs, or allowed-domain emails get in.
    # Anyone else with a Google account sees a denial screen instead of company data.
    is_known_tech = any(t.get('email', '').lower() == (user_email or '').lower() for t in st.session_state.techs)
    allowed_domain = (st.secrets.get("ALLOWED_EMAIL_DOMAIN") if "ALLOWED_EMAIL_DOMAIN" in st.secrets else None) or os.getenv("ALLOWED_EMAIL_DOMAIN", "")
    domain_ok = bool(allowed_domain) and (user_email or '').lower().endswith("@" + allowed_domain.lower().lstrip("@"))

    if not (is_admin or is_known_tech or domain_ok):
        get_logger().log(f"ACCESS DENIED: {user_email} attempted to log in")
        st.markdown(
            f"""
            <div class="login-container">
                <div class="login-box">
                    <h1 style="color:white; margin-bottom: 10px;">🚫 Access Not Approved</h1>
                    <p style="color:#a1a1aa; margin-bottom: 10px;">
                        <b>{user_email}</b> is not registered on the 5G Security Job Board.
                    </p>
                    <p style="color:#a1a1aa; font-size: 0.9em;">
                        If you believe this is a mistake, ask an administrator to add you
                        as a technician or admin, then sign in again.
                    </p>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button("Sign in with a different account"):
            logout()
        return

    # Live update watcher: keeps this session in sync while idle
    live_update_watcher()

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
        search = st.text_input("Search Jobs...", label_visibility="collapsed", placeholder="🔍 Search jobs, sites, techs...")
    with c3:
        # Restricted Access: Only Admins can create jobs
        if is_admin:
            if st.button("➕ New Job", use_container_width=True):
                add_job_dialog()

    # Filter Jobs based on search (matches title, description, location name/address, tech name)
    filtered_jobs = st.session_state.jobs
    if search:
        q = search.lower()

        def job_matches(j):
            if q in j['title'].lower() or q in j['description'].lower():
                return True
            j_loc = get_location(j['locationId'])
            if j_loc and (q in j_loc.get('name', '').lower() or q in j_loc.get('address', '').lower()):
                return True
            j_tech = get_tech(j['techId'])
            if j_tech and q in j_tech.get('name', '').lower():
                return True
            return False

        filtered_jobs = [j for j in filtered_jobs if job_matches(j)]

    # Determine if current user is a tech
    current_tech = next((t for t in st.session_state.techs if t['email'].lower() == user_email.lower()), None)

    # Navigation Tabs
    tabs_list = ["🌅 Morning Briefing", "👷 Tech Board", "📅 Calendar", "🗺️ Map", "🧰 Service Calls", "🏗️ Projects", "🤝 Leads", "📦 Archive"]
    
    if current_tech:
        tabs_list.insert(0, "🙋‍♂️ My Assignments")
        
    if is_admin:
        tabs_list.append("🛡️ Admin")
    
    tabs = st.tabs(tabs_list)
    tab_map = {name: tab for name, tab in zip(tabs_list, tabs)}
    
    # 0. My Assignments (Conditional)
    if current_tech:
        with tab_map["🙋‍♂️ My Assignments"]:
            st.subheader(f"Hello, {current_tech['name'].split()[0]}!")
            
            my_jobs = [j for j in filtered_jobs if j['techId'] == current_tech['id'] and j['status'] != 'Completed']
            # Most urgent first: Critical > High > Medium > Low, then soonest date
            priority_rank = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}
            my_jobs.sort(key=lambda j: (priority_rank.get(j.get('priority'), 4), str(j.get('date', ''))))
            
            if not my_jobs:
                st.info("🎉 No active assignments! Enjoy your day.")
            else:
                st.success(f"You have {len(my_jobs)} active jobs today.")
                for job in my_jobs:
                    render_job_card(job, compact=False, key_suffix="my_assign")
    
    # 1. Morning Briefing
    with tab_map["🌅 Morning Briefing"]:
        col_main, col_feed = st.columns([2, 1])
        with col_main:
            st.subheader("Daily Operational Briefing")
            
            # Briefing display box
            st.container(border=True).markdown(st.session_state.briefing)
            
            # Controls for briefing
            c1, c2 = st.columns([1, 2])
            if c1.button("🔄 Refresh Briefing", use_container_width=True):
                with st.spinner("🤖 AI is updating your briefing..."):
                    st.session_state.briefing = generate_morning_briefing()
                    save_state(invalidate_briefing=False)
                    st.rerun()

            # Automatically generate briefing ONLY if it's the default first-time text
            if st.session_state.briefing == "Data required to generate briefing." and st.session_state.jobs:
                with st.spinner("🤖 AI is preparing your initial morning briefing..."):
                    st.session_state.briefing = generate_morning_briefing()
                    save_state(invalidate_briefing=False)
                    st.rerun()
            
            # Stats
            s1, s2, s3 = st.columns(3)
            active = len([j for j in st.session_state.jobs if j['status'] != 'Completed'])
            crit = len([j for j in st.session_state.jobs if j['priority'] == 'Critical'])
            s1.metric("Active Jobs", active)
            s2.metric("Critical", crit)
            s3.metric("Techs", len(st.session_state.techs))

            # Stale job alerts: active jobs with no history entry in a while
            stale_list = []
            for sj in st.session_state.jobs:
                sd = get_job_stale_days(sj)
                if sd is not None and sd >= STALE_JOB_DAYS:
                    stale_list.append((sj, sd))
            if stale_list:
                stale_list.sort(key=lambda x: -x[1])
                st.markdown(f"##### 🚨 Stale Jobs — no updates in {STALE_JOB_DAYS}+ days")
                with st.container(border=True):
                    for sj, sd in stale_list:
                        s_tech = get_tech(sj['techId'])
                        st.markdown(f"- **{sj['title']}** ({sj['status']}) — **{sd} days** since last update · 👤 {s_tech['name'] if s_tech else 'Unassigned'}")

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
    with tab_map["👷 Tech Board"]:
        if not st.session_state.techs:
            st.info("No technicians added. Go to Admin tab.")
        else:
            board_statuses = ["Not Started", "Parts not ordered", "Waiting on Parts", "Parts Staged", "Customer on Hold", "In Progress"]
            cols = st.columns(len(board_statuses))
            for i, status in enumerate(board_statuses):
                with cols[i]:
                    st.markdown(f"<h4 style='color:{get_status_color(status)}; border-bottom: 2px solid {get_status_color(status)}; padding-bottom: 5px; margin-bottom: 15px;'>{status}</h4>", unsafe_allow_html=True)
                    if status == "Not Started":
                        status_jobs = [j for j in filtered_jobs if j['status'] in ["Not Started", "Pending"]]
                    else:
                        status_jobs = [j for j in filtered_jobs if j['status'] == status]
                    
                    if not status_jobs:
                        st.caption("No jobs.")
                    for job in status_jobs:
                        render_job_card(job, compact=True, key_suffix="board", allow_delete=is_admin)

    # 3. Calendar View
    with tab_map["📅 Calendar"]:
        st.subheader("📅 Job Schedule")
        
        # Month navigation (persisted in session so prev/next survive reruns)
        if "cal_view" not in st.session_state:
            _now = now_local()
            st.session_state.cal_view = [_now.year, _now.month]
        cal_year, month_num = st.session_state.cal_view

        nav_prev, nav_title, nav_next, nav_today, nav_mine = st.columns([1, 3, 1, 1, 2])
        if nav_prev.button("◀", key="cal_prev", use_container_width=True):
            month_num -= 1
            if month_num < 1:
                month_num, cal_year = 12, cal_year - 1
            st.session_state.cal_view = [cal_year, month_num]
            st.rerun()
        if nav_next.button("▶", key="cal_next", use_container_width=True):
            month_num += 1
            if month_num > 12:
                month_num, cal_year = 1, cal_year + 1
            st.session_state.cal_view = [cal_year, month_num]
            st.rerun()
        if nav_today.button("Today", key="cal_today", use_container_width=True):
            _now = now_local()
            st.session_state.cal_view = [_now.year, _now.month]
            st.rerun()
        nav_title.markdown(
            f"<h3 style='text-align:center; margin:0; color:#e4e4e7;'>{calendar.month_name[month_num]} {cal_year}</h3>",
            unsafe_allow_html=True,
        )

        only_my_jobs = False
        if current_tech:
            only_my_jobs = nav_mine.toggle("👷 Only my jobs", key="cal_only_mine")

        cal_jobs = filtered_jobs
        if only_my_jobs and current_tech:
            cal_jobs = [j for j in cal_jobs if j['techId'] == current_tech['id']]

        # Build the whole month as one styled HTML grid (uniform cells, today
        # highlighted, weekends shaded). Pills are hover-only, as before.
        def _cal_esc(s):
            return (str(s).replace('&', '&amp;').replace('<', '&lt;')
                    .replace('>', '&gt;').replace('"', '&quot;'))

        today = now_local().date()
        cal = calendar.monthcalendar(cal_year, month_num)

        cal_css = (
            "<style>"
            ".cal-grid{display:grid;grid-template-columns:repeat(7,1fr);gap:6px;margin-top:10px;}"
            ".cal-hdr{text-align:center;font-weight:bold;color:#a1a1aa;font-size:0.75em;"
            "padding:4px 0;text-transform:uppercase;letter-spacing:0.5px;}"
            ".cal-cell{background:#18181b;border:1px solid #27272a;border-radius:8px;"
            "min-height:104px;padding:6px;overflow:hidden;}"
            ".cal-empty{background:transparent;border:1px solid transparent;}"
            ".cal-weekend{background:#141417;}"
            ".cal-today{border:2px solid #b91c1c;background:#201416;}"
            ".cal-daynum{font-size:0.8em;font-weight:bold;color:#d4d4d8;margin-bottom:4px;}"
            ".cal-today .cal-daynum{color:#ef4444;}"
            ".cal-pill{color:white;padding:2px 6px;border-radius:4px;font-size:0.7em;"
            "margin-bottom:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;cursor:help;}"
            ".cal-more{font-size:0.65em;color:#a1a1aa;padding-left:2px;}"
            "</style>"
        )

        cal_html = cal_css + '<div class="cal-grid">'
        for d in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]:
            cal_html += f'<div class="cal-hdr">{d}</div>'

        for week in cal:
            for i, day in enumerate(week):
                if day == 0:
                    cal_html += '<div class="cal-cell cal-empty"></div>'
                    continue
                is_today = (cal_year == today.year and month_num == today.month and day == today.day)
                cls = "cal-cell"
                if is_today:
                    cls += " cal-today"
                elif i >= 5:
                    cls += " cal-weekend"

                target_date_str = f"{cal_year}-{month_num:02d}-{day:02d}"
                day_jobs = [j for j in cal_jobs if j['date'].startswith(target_date_str) and j['status'] != 'Completed']

                cell = f'<div class="{cls}"><div class="cal-daynum">{day}</div>'
                for job in day_jobs[:4]:
                    jtech = get_tech(job['techId'])
                    color = PRIORITY_COLORS.get(job.get('priority'), "#52525b")
                    initials = jtech['initials'] if jtech else "Un"
                    tip = _cal_esc(f"{job['title']} — {jtech['name'] if jtech else 'Unassigned'} [{job.get('priority', 'N/A')} · {job['status']}]")
                    label = _cal_esc(f"{initials} {job['title'][:12]}")
                    cell += f'<div class="cal-pill" style="background:{color};" title="{tip}">{label}</div>'
                if len(day_jobs) > 4:
                    cell += f'<div class="cal-more">+{len(day_jobs) - 4} more</div>'
                cell += '</div>'
                cal_html += cell

        cal_html += '</div>'

        # Priority legend (pills are colored by priority)
        legend = '<div style="display:flex; gap:14px; flex-wrap:wrap; margin-top:4px; font-size:0.75em; color:#a1a1aa;">'
        for p_name, p_color in PRIORITY_COLORS.items():
            legend += (f'<span style="display:inline-flex; align-items:center; gap:5px;">'
                       f'<span style="width:11px; height:11px; border-radius:3px; background:{p_color}; display:inline-block;"></span>{p_name}</span>')
        legend += '</div>'
        cal_html += legend

        st.markdown(cal_html, unsafe_allow_html=True)

    # 3.5 Map View
    with tab_map["🗺️ Map"]:
        st.subheader("🗺️ Job Map")
        map_only_mine = False
        if current_tech:
            map_only_mine = st.toggle("👷 Only my jobs", key="map_only_mine")
        map_jobs = [j for j in filtered_jobs if j['status'] != 'Completed']
        if map_only_mine and current_tech:
            map_jobs = [j for j in map_jobs if j['techId'] == current_tech['id']]
        render_map_view(map_jobs)

    # 4. Service Calls
    with tab_map["🧰 Service Calls"]:
        service_jobs = [j for j in filtered_jobs if j['type'] == 'Service' and j['status'] != 'Completed']
        if not service_jobs: st.info("No active service calls.")
        for job in service_jobs:
            render_job_card(job, key_suffix="service", allow_delete=is_admin)

    # 5. Projects
    with tab_map["🏗️ Projects"]:
        proj_jobs = [j for j in filtered_jobs if j['type'] == 'Project' and j['status'] != 'Completed']
        if not proj_jobs: st.info("No active projects.")
        for job in proj_jobs:
            render_job_card(job, key_suffix="project", allow_delete=is_admin)

    # 🤝 Leads
    with tab_map["🤝 Leads"]:
        lead_jobs = [j for j in filtered_jobs if j['type'] == 'Leads' and j['status'] != 'Completed']
        if not lead_jobs: st.info("No active leads.")
        for job in lead_jobs:
            render_job_card(job, key_suffix="leads", allow_delete=is_admin)

    # 6. Archive
    with tab_map["📦 Archive"]:
        archived = [j for j in filtered_jobs if j['status'] == 'Completed']
        if not archived: st.info("No archived jobs.")
        for job in archived:
            render_job_card(job, key_suffix="archive", allow_delete=is_admin)

    # 7. Admin (Only if Admin)
    if is_admin:
        with tab_map["🛡️ Admin"]:
            render_admin_panel()

    # Sidebar Chatbot
    render_chatbot()

if __name__ == "__main__":
    main()
