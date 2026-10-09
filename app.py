"""
5G Security Job Board - Streamlit entry point.

The app was previously a single ~7,500-line file. It is now split into focused
modules that this file imports and orchestrates:

- core.py               constants, state save/load, domain rules, formatting
- services_ai.py        Gemini client, model selection, AI summaries/briefing
- services_geo.py       Open-Meteo geocoding + weather
- services_push.py      ntfy push notifications
- services_pdf.py       ReportLab PDF report generation
- services_email.py     SMTP email builders/senders
- services_scheduler.py keep-awake pinger + background reminder scheduler
- ui_widgets.py         small reusable input widgets
- ui_dialogs.py         @st.dialog modals (jobs, details, completion, assets)
- ui_cards.py           job cards/grids + the interactive map view
- ui_tv.py              kiosk / TV wall display
- ui_views.py           SOPs, data browser, invoicing, analytics, hours, chatbot
- ui_admin.py           admin panel tiles (techs, locations, backup, diagnostics)
"""

import streamlit as st
import datetime
import base64
import os
import json
import hmac
import hashlib
import urllib.parse
import time
import calendar

import requests

from core import (
    now_local, esc_html, get_logger, save_state, load_data,
    refresh_session_from_db, init_db_session, get_db_version,
    ensure_loaded_into_session,
    _square_icon, get_logo_data_uri, LOGO_PATH, ICON_PATH,
    get_job_stale_days, STALE_JOB_DAYS,
    get_tech, get_location, agreement_days_left, AGREEMENT_RENEWAL_DAYS,
    PRIORITY_COLORS, find_asset, get_status_color,
)
from services_ai import generate_morning_briefing
from services_scheduler import keep_awake, start_background_scheduler
from ui_widgets import sub_nav
from ui_dialogs import job_details_dialog, asset_dialog, add_job_dialog
from ui_cards import render_job_card, render_job_grid, render_map_view
from ui_tv import render_tv_display
from ui_views import render_sops_view, render_invoicing_view
from ui_admin import render_admin_panel


def _secret(name, default=None):
    """Read a config value from Streamlit secrets with an env-var fallback.
    Returns `default` when the secrets file is missing entirely (local dev)."""
    try:
        return st.secrets[name]
    except Exception:
        return os.getenv(name) or default


# --- AUTHENTICATION ---

# Persistent login: a signed cookie keeps techs logged in across refreshes.
SESSION_COOKIE_NAME = "fivegsec_session"
SESSION_COOKIE_DAYS = 30

# Try importing Cookie Controller for persistent login
try:
    from streamlit_cookies_controller import CookieController
    HAS_COOKIES = True
except ImportError:
    HAS_COOKIES = False

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
    return _secret("COOKIE_SECRET") or _secret("GOOGLE_CLIENT_SECRET")

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
    client_id = _secret("GOOGLE_CLIENT_ID")
    client_secret = _secret("GOOGLE_CLIENT_SECRET")

    # Use APP_URL as fallback for redirect_uri
    app_url = os.getenv("APP_URL", "").rstrip("/")
    default_redirect = f"{app_url}/" if app_url else None
    redirect_uri = _secret("GOOGLE_REDIRECT_URI") or default_redirect

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

    _logo_uri = get_logo_data_uri()
    login_logo_html = f'<img src="{_logo_uri}" style="max-width:220px; margin-bottom:18px;">' if _logo_uri else ''

    st.markdown(
        f"""
        <div class="login-container">
            <div class="login-box">
                {login_logo_html}
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

# --- CONFIGURATION & STYLING ---
# Use the brand icon for the browser tab if present, else fall back to the shield emoji.

_page_icon = "🛡️"
try:
    _icon_src = ICON_PATH if os.path.exists(ICON_PATH) else (LOGO_PATH if os.path.exists(LOGO_PATH) else None)
    if _icon_src:
        _page_icon = _square_icon(_icon_src)
except Exception:
    _page_icon = "🛡️"

st.set_page_config(
    page_title="5G Security Job Board",
    page_icon=_page_icon,
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS — "Midnight Ops" theme: blacks, greys, red. Same layout as always;
# this block only controls color depth, shadows, corner radius, and typography.
st.markdown("""
   <style>
   /* Font: Inter gives the console a tighter, more modern voice. Icons keep
      their own icon fonts — this only changes inherited text. */
   @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
   html, body, .stApp {
       font-family: 'Inter', 'Source Sans Pro', sans-serif;
   }
   h1, h2, h3 { letter-spacing: -0.01em; }

   /* Main Background: near-black with a whisper of red glow */
   .stApp {
       background: radial-gradient(1100px 600px at 88% -12%, rgba(220, 38, 38, 0.055), transparent 62%),
                   radial-gradient(900px 500px at -8% 112%, rgba(220, 38, 38, 0.035), transparent 60%),
                   #070708;
       color: #ececee;
   }

   /* Inputs */
   .stTextInput > div > div > input, .stTextArea > div > div > textarea, .stSelectbox > div > div > div, .stNumberInput > div > div > input, .stMultiSelect > div > div > div {
       background-color: #000000;
       color: #ececee;
       border-color: #2a2a30;
       border-radius: 10px;
   }
   .stTextInput > div > div > input:focus, .stTextArea > div > div > textarea:focus,
   .stNumberInput > div > div > input:focus {
       border-color: #7f1d1d !important;
       box-shadow: 0 0 0 1px rgba(185, 28, 28, 0.35);
   }

   /* Sidebar */
   [data-testid="stSidebar"] {
       background-color: #0c0c0e;
       border-right: 1px solid #1c1c21;
   }

   /* Tighter page headroom (Streamlit's fixed header bar is ~3.75rem tall and
      floats over content, so padding must clear it) */
   .block-container {
       padding-top: 4.2rem !important;
   }

   /* Buttons: red gradient with an under-glow */
   .stButton > button {
       background: linear-gradient(180deg, #dc2626 0%, #b91c1c 100%);
       color: white;
       border: none;
       border-radius: 10px;
       font-weight: 700;
       min-height: 2rem;
       width: 100%;
       padding: 0.3rem 0.8rem !important;
       letter-spacing: 0.01em;
       box-shadow: 0 6px 18px rgba(185, 28, 28, 0.28), inset 0 1px 0 rgba(255, 255, 255, 0.10);
       transition: background 0.15s, box-shadow 0.15s;
   }
   .stButton > button:hover {
       background: linear-gradient(180deg, #991b1b 0%, #7f1d1d 100%);
       color: white;
       border-color: #7f1d1d;
       box-shadow: 0 4px 12px rgba(127, 29, 29, 0.35);
   }
   .stButton > button:active {
       transform: translateY(1px);
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
       white-space: nowrap;
   }

   /* Custom Job Card Style: layered panels that lift on hover */
   .job-card {
       background: linear-gradient(180deg, #17171b 0%, #121215 100%);
       border: 1px solid #232329;
       padding: 15px;
       border-radius: 14px;
       border-left: 5px solid #52525b;
       margin-bottom: 10px;
       box-shadow: 0 3px 12px rgba(0, 0, 0, 0.35);
       transition: transform 0.15s, border-color 0.15s, box-shadow 0.15s;
   }
   .job-card:hover {
       transform: translateY(-2px);
       border-color: #3a3a42;
       box-shadow: 0 12px 26px rgba(0, 0, 0, 0.55), 0 0 0 1px rgba(220, 38, 38, 0.05);
   }
   .priority-Critical { border-left-color: #ef4444 !important; }
   .priority-High { border-left-color: #dc2626 !important; }
   .priority-Medium { border-left-color: #7f1d1d !important; }
   .priority-Low { border-left-color: #52525b !important; }

   /* Tabs: underline style (active = white text + red underline) */
   .stTabs [data-baseweb="tab-list"] {
       gap: 2px;
       border-bottom: 1px solid #1f1f24;
   }
   .stTabs [data-baseweb="tab"] {
       background-color: transparent;
       border-radius: 0;
       color: #8b8b95;
       padding: 4px 10px;
   }
   .stTabs [data-baseweb="tab"]:hover {
       color: #d4d4d8;
   }
   .stTabs [aria-selected="true"] {
       background-color: transparent !important;
       color: white !important;
       font-weight: bold;
   }
   .stTabs [data-baseweb="tab-highlight"] {
       background-color: #dc2626 !important;
       height: 3px !important;
   }
   .stTabs [data-baseweb="tab-border"] {
       background-color: #1f1f24 !important;
   }
   /* Mobile fix: only the ACTIVE tab's panel should show. Streamlit keeps every
      tab panel in the DOM and hides inactive ones with the `hidden` attribute;
      on mobile that hiding can fail to stick after a rerun (e.g. opening a job),
      leaving every tab's content stacked on one page. Force it. */
   .stTabs [data-baseweb="tab-panel"][hidden],
   .stTabs [role="tabpanel"][hidden] {
       display: none !important;
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
       background-color: #101013;
       border: 1px solid #26262c;
       padding: 40px;
       border-radius: 16px;
       max-width: 400px;
       width: 100%;
       box-shadow: 0 0 0 1px #26262c, 0 24px 60px rgba(0, 0, 0, 0.6), 0 0 50px rgba(185, 28, 28, 0.10);
   }

   /* Scrollbars: dark, red on hover */
   ::-webkit-scrollbar { width: 10px; height: 10px; }
   ::-webkit-scrollbar-track { background: transparent; }
   ::-webkit-scrollbar-thumb { background: #26262c; border-radius: 8px; border: 2px solid #070708; }
   ::-webkit-scrollbar-thumb:hover { background: #b91c1c; }
   </style>
""", unsafe_allow_html=True)

# Brand logo in the sidebar (no-op until assets/logo.png is committed to the repo)
try:
    if os.path.exists(LOGO_PATH):
        st.logo(LOGO_PATH, icon_image=(ICON_PATH if os.path.exists(ICON_PATH) else LOGO_PATH))
except Exception:
    pass

# --- DB SESSION INITIALIZER (safe) ---
init_db_session()

# --- SESSION STATE INITIALIZATION ---
if "jobs" not in st.session_state:
    try:
        ensure_loaded_into_session()
        db_data = dict(st.session_state.db)
        st.session_state._db_load_error = None
    except Exception as e:
        # If the DB didn't load, we still populate defaults so the UI can render,
        # but we mark the session unsafe-to-save. This prevents a transient load
        # failure from being written back as a real empty state.
        st.session_state._db_load_error = str(e)
        st.error(f"Failed to load data from DB: {e}")
        db_data = {
            "jobs": [],
            "techs": [],
            "locations": [],
            "briefing": "Data required to generate briefing.",
            "adminEmails": [],
            "agreements": [],
            "sops": [],
            "settings": {},
            "smtp_settings": {},
            "last_reminder_date": None,
        }
    st.session_state.jobs = db_data.get("jobs", [])
    st.session_state.techs = db_data.get("techs", [])
    st.session_state.locations = db_data.get("locations", [])
    st.session_state.briefing = db_data.get("briefing", "Data required to generate briefing.")
    st.session_state.adminEmails = db_data.get("adminEmails", [])
    st.session_state.agreements = db_data.get("agreements", [])
    st.session_state.sops = db_data.get("sops", [])
    st.session_state.settings = db_data.get("settings", {})
    st.session_state.smtp_settings = db_data.get("smtp_settings", {})
    st.session_state.last_reminder_date = db_data.get("last_reminder_date")

# Back-compat: sessions created before a new key was added won't have it
# (the block above is skipped because 'jobs' already exists), so initialize here.
if "agreements" not in st.session_state:
    try:
        st.session_state.agreements = load_data().get("agreements", [])
    except Exception:
        st.session_state.agreements = []

if "sops" not in st.session_state:
    try:
        st.session_state.sops = load_data().get("sops", [])
    except Exception:
        st.session_state.sops = []

if "settings" not in st.session_state:
    try:
        st.session_state.settings = load_data().get("settings", {})
    except Exception:
        st.session_state.settings = {}

if "chat_history" not in st.session_state:
    st.session_state.chat_history = [
        {"role": "model", "parts": ["Hello! I have access to your database. Ask me about active jobs, tech locations, or history."]}
    ]

# --- LIVE UPDATE WATCHER ---

@st.fragment(run_every="15s")
def live_update_watcher():
    """Keeps idle sessions in sync. Polls the DB version every script run; when
    another user saves, show a refresh banner. We do NOT auto-refresh here — it
    can put sessions into a loop when the version cache and session state get
    out of step, and it would close open dialogs (e.g. a tech mid-report).
    The user can click Refresh, or any save will re-check the live version."""
    try:
        db_ver = get_db_version()
    except Exception:
        return
    if db_ver is None or st.session_state.get('_db_version') is None:
        return

    if db_ver != st.session_state._db_version:
        st.session_state['_pending_board_update'] = True

    if st.session_state.get('_pending_board_update'):
        c1, c2 = st.columns([4, 1])
        c1.info("🔄 The board was updated by another user. Refresh to see the latest.")
        if c2.button("Refresh now", key="live_refresh_btn", use_container_width=True):
            refresh_session_from_db()
            st.session_state.pop('_pending_board_update', None)
            st.rerun(scope="app")

# --- MAIN APP FLOW ---

def main():
    # Start Keep Awake Thread
    keep_awake()
    start_background_scheduler()

    # 0. KIOSK / TV DISPLAY (headless): a wall display can land directly on a
    # read-only board via ?kiosk=<KIOSK_TOKEN>, bypassing login. Shows only
    # high-level job info — no credentials, contracts, or editing.
    try:
        kiosk_param = st.query_params.get("kiosk")
    except Exception:
        kiosk_param = None
    kiosk_token = _secret("KIOSK_TOKEN")
    if kiosk_param and kiosk_token and kiosk_param == kiosk_token:
        render_tv_display(exitable=False)
        return

    # 1. Authenticate User
    user = authenticate()
    if not user:
        return  # Stop rendering if not logged in

    # Pick up other users' saves: if the DB moved on since this session loaded,
    # flag a pending refresh. We no longer auto-refresh here because it can loop
    # when the cached DB version and _db_version drift; the banner lets the user
    # refresh when ready, and save_state still detects true conflicts via the DB.
    try:
        db_ver = get_db_version()
        if db_ver is not None and st.session_state.get('_db_version') is not None and db_ver != st.session_state._db_version:
            st.session_state['_pending_board_update'] = True
    except Exception:
        pass

    # Deep-link: open a job dialog requested from elsewhere (e.g. Site History)
    open_target = st.session_state.pop("_open_job_after_rerun", None)
    if open_target:
        job_details_dialog(open_target)

    # Scanned asset label: the QR points at ?asset=TAG. Consume the param so the
    # dialog doesn't reopen on every later rerun.
    _scanned = st.session_state.pop("_open_asset_after_rerun", None)
    if not _scanned:
        try:
            _scanned = st.query_params.get("asset")
        except Exception:
            _scanned = None
        if _scanned:
            try:
                del st.query_params["asset"]
            except Exception:
                pass
    if _scanned:
        asset_dialog(_scanned)

    user_email = user.get("email")
    user_name = user.get("name")

    # 2. Determine Role (Admin or Tech)
    # Bootstrapping: If no admins exist in DB, first login becomes Admin.
    # Only bootstrap when we successfully loaded from the DB (db is in session
    # state). If the DB failed to load we must NOT save empty defaults, or
    # we'd wipe the real data.
    if not st.session_state.adminEmails and 'db' in st.session_state:
        st.session_state.adminEmails.append(user_email)
        save_state()
        st.toast(f"First login detected. {user_email} is now Super Admin.", icon="🛡️")

    is_admin = user_email in st.session_state.adminEmails

    # 2.5 ACCESS CONTROL: only admins, registered techs, or allowed-domain emails
    # get in. Anyone else with a Google account sees a denial screen.
    is_known_tech = any((t.get('email') or '').lower() == (user_email or '').lower()
                        for t in st.session_state.techs)
    allowed_domain = _secret("ALLOWED_EMAIL_DOMAIN", "")
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

    # Display Mode (kiosk) entered via the sidebar button — render the TV board and stop
    if st.session_state.get('kiosk_mode'):
        render_tv_display(exitable=True)
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

        if st.button("📺 Display Mode", key="kiosk_btn", use_container_width=True):
            st.session_state.kiosk_mode = True
            st.rerun()

        if st.button("Logout", key="logout_btn"):
            logout()

    # Top Bar (compact brand band: logo + wordmark | search | New Job)
    c1, c2, c3 = st.columns([3, 5, 2], vertical_alignment="center")
    with c1:
        _logo_uri = get_logo_data_uri()
        _mark = (f'<img src="{_logo_uri}" style="height:38px;">' if _logo_uri else
                 '<div style="width:34px;height:34px;background:#b91c1c;border-radius:7px;display:flex;'
                 'align-items:center;justify-content:center;color:#fff;font-weight:bold;font-size:13px;">5G</div>')
        _today_lbl = now_local().strftime('%a, %b %d').replace(' 0', ' ')
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:10px;">{_mark}'
            f'<div><div style="color:#fff;font-size:17px;font-weight:bold;letter-spacing:0.5px;line-height:1.1;">5G SECURITY</div>'
            f'<div style="color:#71717a;font-size:10.5px;letter-spacing:1.5px;">JOB BOARD &nbsp;·&nbsp; {_today_lbl}</div></div></div>',
            unsafe_allow_html=True)
    with c2:
        search = st.text_input("Search Jobs...", label_visibility="collapsed", placeholder="🔍 Search jobs, sites, techs...")
    with c3:
        # Restricted Access: Only Admins can create jobs
        if is_admin:
            if st.button("➕ New Job", use_container_width=True):
                add_job_dialog()
    st.markdown('<div style="border-bottom:3px solid #b91c1c;margin:2px 0 8px 0;"></div>', unsafe_allow_html=True)

    # Filter Jobs based on search (matches title, description, location name/address, tech name)
    filtered_jobs = list(st.session_state.jobs)
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

    # Navigation tabs. Invoicing is top-level (not buried in Admin) so the
    # office manager can reach it in one click.
    tabs_list = ["🌅 Today", "👷 Board", "🧰 Jobs", "📅 Schedule", "📚 SOPs"]
    if is_admin:
        tabs_list.append("💵 Invoicing")
        tabs_list.append("🛡️ Admin")

    tabs = st.tabs(tabs_list)
    tab_map = {name: tab for name, tab in zip(tabs_list, tabs)}

    # SOP reference library — everyone reads, admins write
    with tab_map["📚 SOPs"]:
        render_sops_view(is_admin)

    # 0. Today — your assignments first (if you're a tech), then the briefing
    with tab_map["🌅 Today"]:
        if current_tech:
            _first = current_tech['name'].split()[0]

            my_jobs = [j for j in filtered_jobs if j['techId'] == current_tech['id'] and j['status'] != 'Completed']
            # Most urgent first: Critical > High > Medium > Low, then soonest date
            priority_rank = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}
            my_jobs.sort(key=lambda j: (priority_rank.get(j.get('priority'), 4), str(j.get('date', ''))))

            # Slim greeting strip (replaces subheader + tall banner)
            if not my_jobs:
                _greet = f'👋 <b>Hello, {_first}</b> — no active assignments. Enjoy your day! 🎉'
            else:
                _greet = (f'👋 <b>Hello, {_first}</b> — you have '
                          f'<b style="color:#6ee7b7;">{len(my_jobs)} active job{"s" if len(my_jobs) != 1 else ""}</b> today.')
            st.markdown(
                f'<div style="background:#18181b;border:1px solid #27272a;border-left:4px solid #10b981;'
                f'border-radius:8px;padding:9px 14px;margin-bottom:12px;color:#e4e4e7;font-size:0.95em;">{_greet}</div>',
                unsafe_allow_html=True)

            render_job_grid(my_jobs, key_suffix="my_assign")
            st.divider()

        col_main, col_feed = st.columns([2, 1])
        with col_main:
            st.subheader("Daily Operational Briefing")

            # Stats + stale list computed up front so the tiles show live counts
            sec_jobs = list(st.session_state.jobs)
            active = len([j for j in sec_jobs if j['status'] != 'Completed'])
            crit = len([j for j in sec_jobs if j['priority'] == 'Critical'])

            stale_list = []
            for sj in sec_jobs:
                sd = get_job_stale_days(sj)
                if sd is not None and sd >= STALE_JOB_DAYS:
                    stale_list.append((sj, sd))
            stale_list.sort(key=lambda x: -x[1])

            # Stat tiles (matches the TV board design language)
            _tiles = [("ACTIVE", active, "#e4e4e7"), ("CRITICAL", crit, "#ef4444"),
                      ("TECHS", len(st.session_state.techs), "#e4e4e7"), ("STALE", len(stale_list), "#f87171")]
            _tiles_html = "".join(
                f'<div style="flex:1;background:#18181b;border:1px solid #27272a;border-radius:10px;padding:12px;text-align:center;">'
                f'<div style="font-size:30px;font-weight:bold;color:{c};line-height:1;">{v}</div>'
                f'<div style="font-size:10.5px;color:#a1a1aa;margin-top:5px;letter-spacing:1px;">{lbl}</div></div>'
                for lbl, v, c in _tiles)
            st.markdown(f'<div style="display:flex;gap:10px;margin-bottom:12px;">{_tiles_html}</div>', unsafe_allow_html=True)

            # Briefing display box
            st.container(border=True).markdown(st.session_state.briefing)

            # Controls for briefing
            c1, c2 = st.columns([1, 2])
            if c1.button("🔄 Refresh Briefing", use_container_width=True):
                with st.spinner("🤖 AI is updating your briefing..."):
                    st.session_state.briefing = generate_morning_briefing()
                    save_state(invalidate_briefing=False)
                    st.rerun()

            # Stale job alerts: badged rows (red = ancient, amber = recent)
            if stale_list:
                def _b_esc(s):
                    return (str(s if s is not None else "").replace('&', '&amp;')
                            .replace('<', '&lt;').replace('>', '&gt;'))
                _rows = ""
                for sj, sd in stale_list:
                    s_tech = get_tech(sj['techId'])
                    _bg, _fg = ("#7f1d1d", "#fecaca") if sd >= 30 else ("#b45309", "#fde68a")
                    _rows += (f'<div style="display:flex;align-items:center;gap:9px;padding:5px 0;border-bottom:1px solid #27272a;">'
                              f'<span style="background:{_bg};color:{_fg};font-size:11px;font-weight:bold;padding:2px 8px;'
                              f'border-radius:10px;min-width:46px;text-align:center;flex-shrink:0;">{sd}d</span>'
                              f'<span style="color:#e4e4e7;font-size:13.5px;font-weight:bold;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{_b_esc(sj["title"])}</span>'
                              f'<span style="color:#71717a;font-size:12px;white-space:nowrap;">{_b_esc(sj["status"])} · {_b_esc(s_tech["name"] if s_tech else "Unassigned")}</span>'
                              f'</div>')
                st.markdown(
                    f'<div style="background:#18181b;border:1px solid #27272a;border-radius:8px;padding:10px 14px;margin-top:12px;">'
                    f'<div style="color:#f87171;font-size:13px;font-weight:bold;margin-bottom:6px;">🚨 Stale Jobs — no updates in {STALE_JOB_DAYS}+ days</div>'
                    f'{_rows}</div>',
                    unsafe_allow_html=True)

            # Upcoming contract renewals — admins only (contract values are sensitive)
            if is_admin:
                loc_by_id = {l['id']: l for l in st.session_state.locations}
                renewals = []
                for a in st.session_state.get('agreements', []):
                    d = agreement_days_left(a)
                    if d is not None and d <= AGREEMENT_RENEWAL_DAYS:
                        renewals.append((a, d))
                if renewals:
                    renewals.sort(key=lambda x: x[1])
                    st.markdown(f"##### 🔔 Upcoming Renewals — within {AGREEMENT_RENEWAL_DAYS} days")
                    with st.container(border=True):
                        for a, d in renewals:
                            loc = loc_by_id.get(a.get('locationId'))
                            when = f"in {d} days" if d >= 0 else f"**{abs(d)} days ago**"
                            st.markdown(f"- **{a.get('title', 'Agreement')}** ({a.get('type', '')}) — renews {when} · 📍 {loc['name'] if loc else 'Unknown'}")

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

    # 2. Board
    with tab_map["👷 Board"]:
        # Manual tag lookup — the fallback for a scuffed label, or for desktop
        with st.expander("🏷️ Look up an equipment tag", expanded=False):
            _lc1, _lc2 = st.columns([3, 1])
            _tag_q = _lc1.text_input("Tag", key="asset_lookup", label_visibility="collapsed",
                                     placeholder="e.g. 5GS-000042")
            if _lc2.button("Find", use_container_width=True, key="asset_lookup_btn") and _tag_q.strip():
                _l, _a = find_asset(_tag_q)
                if _a:
                    st.session_state["_open_asset_after_rerun"] = _a['tag']
                    st.rerun()
                else:
                    st.warning(f"No equipment tagged '{_tag_q.strip()}'.")

        if not st.session_state.techs:
            st.info("No technicians added. Go to Admin tab.")
        else:
            board_statuses = ["Not Started", "Parts not ordered", "Waiting on Parts", "Parts Staged", "Customer on Hold", "In Progress"]
            cols = st.columns(len(board_statuses))
            for i, status in enumerate(board_statuses):
                with cols[i]:
                    if status == "Not Started":
                        status_jobs = [j for j in filtered_jobs if j['status'] in ["Not Started", "Pending"]]
                    else:
                        status_jobs = [j for j in filtered_jobs if j['status'] == status]

                    _s_color = get_status_color(status)
                    st.markdown(
                        f"<h4 style='color:{_s_color}; border-bottom: 3px solid {_s_color}; padding-bottom: 5px; margin-bottom: 15px; font-size:1.0em;'>"
                        f"{status} <span style='color:#52525b; font-weight:normal;'>({len(status_jobs)})</span></h4>",
                        unsafe_allow_html=True)

                    if not status_jobs:
                        st.caption("No jobs.")
                    for job in status_jobs:
                        render_job_card(job, compact=True, key_suffix="board", allow_delete=is_admin)

    # 3. Schedule — calendar and map are two views of the same "where/when" question
    with tab_map["📅 Schedule"]:
      _sched_view = sub_nav(["📅 Calendar", "🗺️ Map"], "sched_view")
      if _sched_view == "📅 Calendar":
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
            ".cal-hdr{text-align:center;font-weight:bold;color:#8b8b95;font-size:0.75em;"
            "padding:4px 0;text-transform:uppercase;letter-spacing:0.5px;}"
            ".cal-cell{background:#121215;border:1px solid #232329;border-radius:10px;"
            "min-height:104px;padding:6px;overflow:hidden;}"
            ".cal-empty{background:transparent;border:1px solid transparent;}"
            ".cal-weekend{background:#0e0e10;}"
            ".cal-today{border:2px solid #dc2626;background:#1a1214;}"
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

      if _sched_view == "🗺️ Map":
        st.subheader("🗺️ Job Map")
        map_only_mine = False
        if current_tech:
            map_only_mine = st.toggle("👷 Only my jobs", key="map_only_mine")
        map_jobs = [j for j in filtered_jobs if j['status'] != 'Completed']
        if map_only_mine and current_tech:
            map_jobs = [j for j in map_jobs if j['techId'] == current_tech['id']]
        render_map_view(map_jobs)

    # 4. Jobs — the four job lists were near-identical tabs; they're one view with
    # a filter now. Counts sit in the labels so you can see where the work is.
    with tab_map["🧰 Jobs"]:
        _active = [j for j in filtered_jobs if j['status'] != 'Completed']
        _buckets = [
            ("service", "🧰 Service",  "service calls",
             [j for j in _active if j['type'] == 'Service']),
            ("project", "🏗️ Projects", "projects",
             [j for j in _active if j['type'] == 'Project']),
            ("leads",   "🤝 Leads",    "leads",
             [j for j in _active if j['type'] == 'Leads']),
            ("archive", "📦 Archive",  "archived jobs",
             [j for j in filtered_jobs if j['status'] == 'Completed']),
        ]
        _labels = [f"{label} ({len(rows)})" for _, label, _, rows in _buckets]
        _by_label = {lbl: b for lbl, b in zip(_labels, _buckets)}
        _slug, _, _empty_word, _rows = _by_label.get(sub_nav(_labels, "jobs_view"), _buckets[0])
        if not _rows:
            st.info(f"No {_empty_word} to show.")
        render_job_grid(_rows, key_suffix=f"jobs_{_slug}", allow_delete=is_admin)

    # 6. Invoicing (admin / office manager)
    if is_admin:
        with tab_map["💵 Invoicing"]:
            render_invoicing_view(user_email)

    # 7. Admin (Only if Admin)
    if is_admin:
        with tab_map["🛡️ Admin"]:
            render_admin_panel()

if __name__ == "__main__":
    main()
