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
import html as _html
from reportlab.lib.utils import ImageReader

def esc_html(v):
    """Escape a value for embedding in the raw HTML blocks we build for cards,
    tiles and the calendar. Job titles, site names and the quote field are all
    free text typed by users — an unescaped '&' or '<' breaks the markup, and
    worse can inject script into every other user's view."""
    return _html.escape(str(v if v is not None else ""), quote=True)

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

# QR generation ships inside reportlab, so asset labels need no extra dependency.
try:
    from reportlab.graphics.barcode import qr as _rl_qr
    from reportlab.graphics.shapes import Drawing as _RLDrawing
    from reportlab.graphics import renderPDF as _rl_renderPDF
    HAS_QR = True
except ImportError:
    HAS_QR = False

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
# Use the brand icon for the browser tab if present, else fall back to the shield emoji.
LOGO_PATH = "assets/logo.png"
ICON_PATH = "assets/icon.png"

def _square_icon(path):
    """Pad a (possibly non-square) logo onto a transparent square canvas so the
    browser-tab icon isn't squished — works with a horizontal/wordmark logo."""
    img = Image.open(path).convert("RGBA")
    side = max(img.width, img.height)
    canvas = Image.new("RGBA", (side, side), (0, 0, 0, 0))
    canvas.paste(img, ((side - img.width) // 2, (side - img.height) // 2), img)
    return canvas

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

   /* Sidebar */
   [data-testid="stSidebar"] {
       background-color: #18181b;
       border-right: 1px solid #27272a;
   }

   /* Tighter page headroom (Streamlit's fixed header bar is ~3.75rem tall and
      floats over content, so padding must clear it) */
   .block-container {
       padding-top: 4.2rem !important;
   }

   /* Buttons */
   .stButton > button {
       background-color: #b91c1c;
       color: white;
       border: none;
       border-radius: 8px;
       font-weight: bold;
       min-height: 2rem;
       width: 100%;
       padding: 0.3rem 0.8rem !important;
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
       white-space: nowrap;
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

   /* Tabs: underline style (active = white text + red underline) */
   .stTabs [data-baseweb="tab-list"] {
       gap: 2px;
       border-bottom: 1px solid #27272a;
   }
   .stTabs [data-baseweb="tab"] {
       background-color: transparent;
       border-radius: 0;
       color: #a1a1aa;
       padding: 4px 10px;
   }
   .stTabs [data-baseweb="tab"]:hover {
       color: #e4e4e7;
   }
   .stTabs [aria-selected="true"] {
       background-color: transparent !important;
       color: white !important;
       font-weight: bold;
   }
   .stTabs [data-baseweb="tab-highlight"] {
       background-color: #b91c1c !important;
       height: 3px !important;
   }
   .stTabs [data-baseweb="tab-border"] {
       background-color: #27272a !important;
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

# Brand logo in the sidebar (no-op until assets/logo.png is committed to the repo)
try:
    if os.path.exists(LOGO_PATH):
        st.logo(LOGO_PATH, icon_image=(ICON_PATH if os.path.exists(ICON_PATH) else LOGO_PATH))
except Exception:
    pass

@st.cache_data(show_spinner=False)
def get_logo_data_uri():
    """Returns the brand logo as a base64 data URI for embedding in raw HTML
    (login box, etc.). None if no logo file is present."""
    try:
        if os.path.exists(LOGO_PATH):
            with open(LOGO_PATH, "rb") as f:
                b64 = base64.b64encode(f.read()).decode()
            return f"data:image/png;base64,{b64}"
    except Exception:
        pass
    return None

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
            "agreements": [],
            "sops": [],
            "settings": {},
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
    st.session_state.db["agreements"] = st.session_state.get("agreements", [])
    st.session_state.db["sops"] = st.session_state.get("sops", [])
    st.session_state.db["settings"] = st.session_state.get("settings", {})
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
    st.session_state.agreements = data.get("agreements", [])
    st.session_state.sops = data.get("sops", [])
    st.session_state.settings = data.get("settings", {})
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
        actor = st.session_state.user_info.get('email') if "user_info" in st.session_state else None
        if apply_job_status(st.session_state.jobs[job_idx], new_status, actor):
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
# Tech Colors for UI
def get_status_color(status):
    # Reads as a left-to-right gradient on the board:
    # grey (not started) -> red (we're the blocker) -> amber (someone else is)
    # -> blue (moving) -> green (done). The column label says which flavour.
    colors = {
        "Not Started":       SEMANTIC["neutral"],
        "Pending":           SEMANTIC["neutral"],
        "Parts not ordered": SEMANTIC["act"],      # WE haven't ordered yet
        "Waiting on Parts":  SEMANTIC["waiting"],  # ordered; vendor has it
        "Customer on Hold":  SEMANTIC["waiting"],
        "In Progress":       SEMANTIC["moving"],
        "Parts Staged":      SEMANTIC["done"],
        "Completed":         SEMANTIC["done"],
    }
    return colors.get(status, SEMANTIC["neutral"])

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

# ── ONE COLOR LANGUAGE ────────────────────────────────────────────────────────
# Every colored chip in the app resolves to one of these five meanings, so a
# colour means the SAME thing wherever you see it. Before this, green meant
# "Parts Staged" and "Staged" and "Paid"; blue meant "In Progress" and
# "Received" and "Invoiced" — nothing could be read at a glance.
SEMANTIC = {
    "act":     "#ef4444",   # we have to do something, now
    "waiting": "#d97706",   # blocked on an outside party
    "moving":  "#3b82f6",   # actively in motion
    "done":    "#10b981",   # finished / good
    "neutral": "#52525b",   # not started / not applicable
}

# Priority is a separate channel — a single red intensity ramp meaning "urgency",
# so it never competes with the status colours above.
PRIORITY_COLORS = {
    "Critical": "#ef4444",
    "High": "#dc2626",
    "Medium": "#7f1d1d",
    "Low": SEMANTIC["neutral"],
}

# Parts pipeline: items flow left to right toward being staged for the job
PART_STATUSES = ["Needed", "Ordered", "Received", "Staged"]
PART_STATUS_COLORS = {
    "Needed":   SEMANTIC["act"],      # nobody has ordered it yet
    "Ordered":  SEMANTIC["waiting"],  # waiting on the vendor
    "Received": SEMANTIC["moving"],
    "Staged":   SEMANTIC["done"],
}

def parts_summary(job):
    """Returns (staged_count, total_count) for a job's parts list."""
    parts = job.get('parts', [])
    staged = sum(1 for p in parts if p.get('status') == 'Staged')
    return staged, len(parts)

# Invoicing pipeline (admin/office-manager facing). Only applies once a job is
# Completed — invoicing isn't meaningful while work is still running.
INVOICE_STATUSES = ["Ready to Invoice", "Invoiced", "Paid", "No Charge"]
INVOICE_STATUS_COLORS = {
    "Ready to Invoice": SEMANTIC["act"],      # the office manager has to send it
    "Invoiced":         SEMANTIC["waiting"],  # sent; waiting on the customer
    "Paid":             SEMANTIC["done"],
    "No Charge":        SEMANTIC["neutral"],
}
INVOICE_STATUS_ICONS = {
    "Ready to Invoice": "🧾",
    "Invoiced": "📤",
    "Paid": "✅",
    "No Charge": "🛡️",
}

def job_is_warranty(job):
    """True if the job or any of its reports was flagged as warranty work."""
    if job.get('isWarranty'):
        return True
    return any(r.get('isWarranty') for r in (job.get('reports') or []))

def invoice_status(job):
    """Current invoice status, or None if the job isn't Completed yet.

    Derived rather than stamped on completion: a Completed job with no explicit
    invoice record defaults to 'No Charge' when it's warranty work, otherwise
    'Ready to Invoice'. That means the queue self-populates (including for jobs
    completed before this feature existed) with no migration."""
    if job.get('status') != 'Completed':
        return None
    stored = (job.get('invoice') or {}).get('status')
    if stored in INVOICE_STATUSES:
        return stored
    return "No Charge" if job_is_warranty(job) else "Ready to Invoice"

# --- ASSET REGISTRY (tagged equipment installed at a site) --------------------
# Assets live on the LOCATION, not the job: an NVR stays at the site across every
# future job. We record which job installed it so the history survives. Locations
# already persist, so this needs no new top-level state key.
ASSET_TYPES = ["NVR", "DVR", "Camera", "Switch", "Access Panel", "Reader",
               "Alarm Panel", "Keypad", "Router", "UPS", "Gate Operator", "Other"]
ASSET_TAG_PREFIX = "5GS"

def all_assets():
    """Every registered asset across all sites, each with its location attached."""
    out = []
    for l in st.session_state.locations:
        for a in (l.get('assets') or []):
            out.append((l, a))
    return out

def next_asset_tag():
    """Next sequential tag. Derived from the highest existing tag rather than a
    stored counter, so it can't drift out of sync with the actual data."""
    top = 0
    for _, a in all_assets():
        tag = str(a.get('tag', ''))
        if tag.startswith(ASSET_TAG_PREFIX + "-"):
            try:
                top = max(top, int(tag.split("-", 1)[1]))
            except (ValueError, IndexError):
                pass
    return f"{ASSET_TAG_PREFIX}-{top + 1:06d}"

def find_asset(tag):
    """(location, asset) for a tag, or (None, None). Case/space tolerant so a
    typed-in code works as well as a scanned one."""
    q = str(tag or "").strip().upper()
    if not q:
        return None, None
    for l, a in all_assets():
        if str(a.get('tag', '')).upper() == q:
            return l, a
    return None, None

def asset_warranty_left(asset):
    """(months_remaining, expiry_date) or (None, None) when it can't be worked out.
    Negative months mean it has already expired."""
    months = asset.get('warranty_months')
    installed = asset.get('installed_date')
    if not months or not installed:
        return None, None
    try:
        d0 = datetime.datetime.fromisoformat(str(installed)[:19]).date()
        m = int(months)
    except (ValueError, TypeError):
        return None, None
    total = d0.month - 1 + m
    expiry = d0.replace(year=d0.year + total // 12, month=total % 12 + 1,
                        day=min(d0.day, 28))
    today = now_local().date()
    return (expiry.year - today.year) * 12 + (expiry.month - today.month), expiry

def asset_label_lines(location, asset):
    """The four text lines printed on a label."""
    kind = asset.get('type', 'Asset')
    model = asset.get('make_model', '')
    line2 = f"{kind} — {model}" if model else kind
    place = " · ".join(x for x in [(location or {}).get('name', ''), asset.get('position', '')] if x)
    return "5G SECURITY", line2, place, asset.get('tag', '')


def asset_scan_url(tag):
    """URL encoded into the QR. Any phone camera opens this and the app deep-links
    to the asset — which is why no in-app QR scanner (and no fragile system
    library) is needed. Falls back to the bare tag if APP_URL isn't configured."""
    base = (os.getenv("APP_URL", "") or "").rstrip("/")
    if not base:
        # Streamlit Cloud secrets don't always surface as environment variables,
        # so check there too (same fallback the keep-awake pinger uses).
        try:
            if "APP_URL" in st.secrets:
                base = str(st.secrets["APP_URL"]).rstrip("/")
        except Exception:
            pass
    return f"{base}/?asset={tag}" if base else str(tag)

def build_asset_labels_pdf(pairs):
    """Avery 5160/8160 sheet (letter, 3 x 10 = 30 labels). `pairs` is a list of
    (location, asset). Returns PDF bytes, or None if reportlab is unavailable."""
    if not (HAS_REPORTLAB and pairs):
        return None

    INCH = 72.0
    PAGE_W, PAGE_H = letter
    COLS, ROWS = 3, 10
    LBL_W, LBL_H = 2.625 * INCH, 1.0 * INCH
    MARGIN_L, MARGIN_T = 0.21875 * INCH, 0.5 * INCH
    PITCH_X, PITCH_Y = 2.75 * INCH, 1.0 * INCH
    PAD = 0.09 * INCH

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=letter)

    for i, (loc, asset) in enumerate(pairs):
        slot = i % (COLS * ROWS)
        if i and slot == 0:
            c.showPage()
        col, row = slot % COLS, slot // COLS
        x = MARGIN_L + col * PITCH_X
        y = PAGE_H - MARGIN_T - (row + 1) * PITCH_Y   # reportlab origin is bottom-left

        brand, line2, place, tag = asset_label_lines(loc, asset)

        # QR square on the left, sized to the label height
        qr_side = LBL_H - 2 * PAD
        if HAS_QR:
            try:
                widget = _rl_qr.QrCodeWidget(asset_scan_url(tag))
                bx0, by0, bx1, by1 = widget.getBounds()
                bw, bh = (bx1 - bx0) or 1, (by1 - by0) or 1
                d = _RLDrawing(qr_side, qr_side,
                               transform=[qr_side / bw, 0, 0, qr_side / bh, 0, 0])
                d.add(widget)
                _rl_renderPDF.draw(d, c, x + PAD, y + PAD)
            except Exception:
                pass   # a label without a QR still carries the printed tag

        tx = x + PAD + qr_side + 0.07 * INCH
        avail = (x + LBL_W - PAD) - tx

        def _fit(text, font, size):
            """Trim to the label width so long model names can't bleed into the
            neighbouring label."""
            t = str(text or "")
            while t and c.stringWidth(t, font, size) > avail:
                t = t[:-1]
            return t

        c.setFillColor(colors.HexColor("#b91c1c"))
        c.setFont("Helvetica-Bold", 5.5)
        c.drawString(tx, y + LBL_H - PAD - 5, _fit(brand, "Helvetica-Bold", 5.5))

        c.setFillColor(colors.HexColor("#18181b"))
        c.setFont("Helvetica-Bold", 8)
        c.drawString(tx, y + LBL_H - PAD - 15, _fit(line2, "Helvetica-Bold", 8))

        c.setFillColor(colors.HexColor("#52525b"))
        c.setFont("Helvetica", 6)
        c.drawString(tx, y + LBL_H - PAD - 24, _fit(place, "Helvetica", 6))

        c.setFillColor(colors.HexColor("#18181b"))
        c.setFont("Courier-Bold", 10)
        c.drawString(tx, y + PAD + 3, _fit(tag, "Courier-Bold", 10))

    c.save()
    return buf.getvalue()


def get_setting(key, default=None):
    return (st.session_state.get('settings') or {}).get(key, default)

def set_setting(key, value):
    st.session_state.setdefault('settings', {})[key] = value
    save_state(invalidate_briefing=False)

def money_to_float(v):
    """'$1,450.00' / '1450' -> 1450.0. None for blanks or free text like 'TBD',
    so callers can tell 'no number' apart from 'zero'."""
    txt = str(v or "").replace("$", "").replace(",", "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except ValueError:
        return None

def job_man_hours(job):
    """Total MAN-hours on a job.

    A report's hours are counted once per tech listed on site — three techs for
    eight hours is 24 man-hours of effort, not 8. This matches how the Hours
    Report already credits time, so the two screens agree."""
    total = 0.0
    for r in (job.get('reports') or []):
        try:
            hrs = float(r.get('hoursWorked') or 0)
        except (TypeError, ValueError):
            continue
        if hrs <= 0:
            continue
        crew = [t.strip() for t in (r.get('techsOnSite') or '').split(',') if t.strip()]
        total += hrs * (len(crew) if crew else 1)
    return total

def job_parts_cost(job):
    """Summed cost of parts on a job — what was paid to a supplier. Blank or
    free-text costs are ignored."""
    total = 0.0
    for p in (job.get('parts') or []):
        c = money_to_float(p.get('cost'))
        if c:
            total += c * (p.get('qty') or 1)
    return total

def job_value_summary(job):
    """What a job was worth against the effort it consumed.

    Deliberately contains NO labour cost and no hourly rates — pay is not recorded
    anywhere in this app. Effort is expressed in man-hours, and 'revenue per
    man-hour' is the comparable figure: it ranks jobs against each other without
    anyone's wage being involved. Returns None when the job has no price at all."""
    quoted = money_to_float(job.get('quoteValue'))
    billed = money_to_float((job.get('invoice') or {}).get('amount'))
    revenue = billed if billed is not None else quoted
    if revenue is None:
        return None
    hours = job_man_hours(job)
    parts = job_parts_cost(job)
    return {
        'quoted': quoted, 'billed': billed, 'revenue': revenue,
        'man_hours': hours, 'parts': parts,
        'rev_per_hour': (revenue / hours) if hours else None,
        'variance': (billed - quoted) if (billed is not None and quoted is not None) else None,
    }


def format_money(v):
    """Display helper for the free-text money fields. A plain number gets a $ and
    thousands separators; anything else (e.g. "TBD", "2 visits @ 500") is shown
    exactly as typed rather than mangled."""
    s = str(v or "").strip()
    if not s:
        return ""
    try:
        n = float(s.replace("$", "").replace(",", "").strip())
    except ValueError:
        return s
    return f"${n:,.0f}" if n == int(n) else f"${n:,.2f}"


def job_invoice(job):
    """The job's invoice record with every field defaulted."""
    inv = job.get('invoice') or {}
    return {
        'status': invoice_status(job),
        'number': inv.get('number', ''),
        'amount': inv.get('amount', ''),
        'date': inv.get('date', ''),
        'notes': inv.get('notes', ''),
        'updated_by': inv.get('updated_by', ''),
        'updated_at': inv.get('updated_at', ''),
    }

def set_job_invoice(job_id, **fields):
    """Update a job's invoice record in session state and persist."""
    j = next((x for x in st.session_state.jobs if x['id'] == job_id), None)
    if not j:
        return False
    inv = dict(j.get('invoice') or {})
    for k, v in fields.items():
        if v is not None:
            inv[k] = v
    user_email = st.session_state.user_info.get('email', 'Unknown') if "user_info" in st.session_state else 'Unknown'
    inv['updated_by'] = user_email
    inv['updated_at'] = now_local().isoformat()
    j['invoice'] = inv
    save_state(invalidate_briefing=False)
    return True

# --- TIME CLOCK ---
def _fmt_duration(hours):
    """0h 0m formatting from a float hours value."""
    mins = int(round((hours or 0) * 60))
    return f"{mins // 60}h {mins % 60}m"

def clocked_hours(entries, user_email=None, on_date=None, include_open=True):
    """Total labor hours from time-clock entries. Optionally filter to one user
    and/or one date. Open (not-yet-clocked-out) entries count up to 'now'."""
    total = 0.0
    now = now_local()
    for e in (entries or []):
        if user_email and (e.get('userEmail', '').lower() != user_email.lower()):
            continue
        ci = e.get('clock_in')
        if not ci:
            continue
        try:
            ci_dt = datetime.datetime.fromisoformat(ci)
        except (ValueError, TypeError):
            continue
        if on_date and ci_dt.date() != on_date:
            continue
        co = e.get('clock_out')
        if co:
            try:
                co_dt = datetime.datetime.fromisoformat(co)
            except (ValueError, TypeError):
                continue
        else:
            if not include_open:
                continue
            co_dt = now
            # Safety: a forgotten open timer shouldn't accrue endlessly (cap at 12h)
            if (co_dt - ci_dt).total_seconds() > 12 * 3600:
                co_dt = ci_dt + datetime.timedelta(hours=12)
        if co_dt > ci_dt:
            total += (co_dt - ci_dt).total_seconds() / 3600
    return total

def open_time_entry(entries, user_email):
    """The user's currently-running (not clocked-out) entry, if any."""
    return next((e for e in (entries or [])
                 if e.get('userEmail', '').lower() == (user_email or '').lower()
                 and not e.get('clock_out')), None)

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

class StepTimer:
    """Times each stage of a slow operation so we can see where the seconds go
    instead of guessing. Passed explicitly rather than kept in a global, because
    Streamlit runs each session in its own thread and a global would collide
    between concurrent users.

    Usage:
        t = StepTimer("daily submit")
        ...work...
        t.mark("photos")
        ...work...
        t.mark("email")
        t.finish(photos=3)
    """
    def __init__(self, label):
        self.label = label
        self._t0 = time.perf_counter()
        self._last = self._t0
        self.steps = []

    def mark(self, name):
        now = time.perf_counter()
        self.steps.append((name, now - self._last))
        self._last = now

    def total(self):
        return time.perf_counter() - self._t0

    def summary(self, **context):
        parts = " | ".join(f"{n} {d:.2f}s" for n, d in self.steps)
        ctx = " ".join(f"{k}={v}" for k, v in context.items() if v not in (None, ""))
        return f"⏱️ {self.label}: TOTAL {self.total():.2f}s = {parts}" + (f"  [{ctx}]" if ctx else "")

    def finish(self, **context):
        """Write the breakdown to the system log (Admin → Diagnostics → Event Logs)."""
        line = self.summary(**context)
        try:
            get_logger().log(line)
        except Exception:
            pass
        return line


def get_config_val(key, default=None):
    """SMTP/config lookup, in priority order: saved settings > secrets > env.
    Was duplicated verbatim inside four different email functions."""
    if 'smtp_settings' in st.session_state and st.session_state.smtp_settings.get(key):
        return st.session_state.smtp_settings[key]
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return os.getenv(key) or default


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
                            recipients = daily_summary_recipients(techs, state.get("adminEmails", []))
                            active_exists = any(j.get('status') != 'Completed' for j in jobs)

                            if recipients and active_exists:
                                subject, plain_body, html_body = build_ops_summary_email(jobs, techs, locations, today_str)
                                for recipient in recipients:
                                    try:
                                        msg = MIMEMultipart("alternative")
                                        msg['From'] = sender_email
                                        msg['To'] = recipient
                                        msg['Subject'] = subject
                                        msg.attach(MIMEText(plain_body, 'plain'))
                                        msg.attach(MIMEText(html_body, 'html'))
                                        server.send_message(msg)
                                    except Exception:
                                        continue  # one bad address shouldn't stop the rest

                            server.quit()

                            # Morning push to techs' phones (ntfy) — generic payload,
                            # topics are only read here (never generated in the thread)
                            for t in techs:
                                topic = t.get('notify_topic')
                                if not topic:
                                    continue
                                n_active = len([j for j in jobs
                                                if j.get('techId') == t.get('id') and j.get('status') != 'Completed'])
                                if n_active:
                                    send_push(topic, "Good Morning",
                                              f"You have {n_active} active job(s) today — check the board for your day.",
                                              tags=["sunrise"])
                            
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

                        end_d = now.date()
                        start_d = end_d - datetime.timedelta(days=6)
                        admin_emails = state.get("adminEmails", [])
                        jobs = state.get("jobs", [])
                        techs = state.get("techs", [])
                        locations = state.get("locations", [])

                        # Weekly hours digest -> admins only
                        _send_hours_digest_email('Hours', admin_emails,
                                                 smtp_server, smtp_port, sender_email, sender_password,
                                                 jobs, techs, locations, start_d, end_d)

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

# --- SERVICE AGREEMENTS / CONTRACTS ---
AGREEMENT_TYPES = ["Monitoring", "Service / Maintenance", "Inspection", "Warranty", "Other"]
BILLING_CYCLES = ["Monthly", "Quarterly", "Annual", "One-time"]
# Agreements renewing within this many days are flagged
AGREEMENT_RENEWAL_DAYS = 60

def agreement_days_left(agr):
    """Days until an agreement's renewal/end date. Negative = expired.
    None if no/invalid date or the agreement is cancelled."""
    if not agr or agr.get('status') == 'Cancelled':
        return None
    rd = agr.get('renewal_date')
    if not rd:
        return None
    try:
        rd_dt = datetime.datetime.strptime(str(rd)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None
    return (rd_dt - now_local().date()).days

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

# --- FOLLOW-UPS (jobs parked waiting on someone else) -------------------------
# Statuses that mean "we're blocked on an outside party". Each maps to how many
# days it may sit before it needs chasing, and who to chase.
FOLLOWUP_RULES = {
    "Customer on Hold":  (7, "Chase the customer"),
    "Waiting on Parts":  (5, "Chase the vendor"),
    "Parts not ordered": (3, "Order the parts"),
}

def apply_job_status(job, new_status, actor=None):
    """Set a job's status and stamp when it changed. Returns True if it changed.

    Use this everywhere instead of assigning job['status'] directly — the stamp is
    what lets us say 'on hold for 12 days' rather than just 'quiet for 12 days'."""
    if not job or not new_status or job.get('status') == new_status:
        return False
    job['status'] = new_status
    job['status_changed_at'] = now_local().isoformat()
    if actor:
        job['status_changed_by'] = actor
    return True

def job_status_since(job):
    """Date the job entered its current status. Falls back to last activity for
    jobs that predate status stamping, so this works on existing data too."""
    stamped = job.get('status_changed_at')
    if stamped:
        try:
            return datetime.datetime.fromisoformat(str(stamped)[:19]).date()
        except (ValueError, TypeError):
            pass
    last_ts = None
    for r in (job.get('reports') or []):
        ts = r.get('timestamp', '')
        if ts and (last_ts is None or ts > last_ts):
            last_ts = ts
    try:
        return datetime.datetime.fromisoformat(str(last_ts or job.get('date', ''))[:19]).date()
    except (ValueError, TypeError):
        return None

def days_in_status(job):
    """Whole days the job has sat in its current status (None if undeterminable)."""
    d = job_status_since(job)
    if not d:
        return None
    return max((now_local().date() - d).days, 0)

def job_followup(job):
    """(days, threshold, action) when a job is overdue for a nudge, else None."""
    if job.get('status') == 'Completed':
        return None
    rule = FOLLOWUP_RULES.get(job.get('status'))
    if not rule:
        return None
    threshold, action = rule
    days = days_in_status(job)
    if days is None or days < threshold:
        return None
    return days, threshold, action

def followup_jobs(jobs):
    """Overdue jobs, most-overdue first, as (job, days, threshold, action)."""
    out = []
    for j in jobs:
        fu = job_followup(j)
        if fu:
            out.append((j, fu[0], fu[1], fu[2]))
    return sorted(out, key=lambda x: -x[1])

def last_daily_report(job):
    """The most recent FULL daily report on a job, or None.

    Quick-status pings ("📍 Arrived", photo-only updates) are skipped — a report
    only counts if it carries structured data, which is the same test the photo
    roll-up uses. Used to prefill the next day's report: on a multi-day install
    the same crew arrives at the same time every day and shouldn't have to retype
    it on a phone."""
    fulls = [r for r in (job.get('reports') or [])
             if r.get('hoursWorked') or r.get('techsOnSite')]
    if not fulls:
        return None
    fulls.sort(key=lambda r: str(r.get('timestamp', '')), reverse=True)
    return fulls[0]

def _parse_report_time(value, fallback):
    """'08:30:00' / '08:30' -> datetime.time, else the fallback."""
    txt = str(value or "").strip()
    if not txt:
        return fallback
    if len(txt) == 5:
        txt += ":00"
    try:
        return datetime.datetime.strptime(txt, '%H:%M:%S').time()
    except (ValueError, TypeError):
        return fallback

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
        data = buf.getvalue()
        stored = upload_bytes(data, key, content_type="image/jpeg")
        # We already hold the exact bytes — hand them to the PDF builder for free
        remember_photo_bytes(stored, data)
        return stored
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
        "agreements": st.session_state.get("agreements", []),
        "sops": st.session_state.get("sops", []),
        "settings": st.session_state.get("settings", {}),
        "last_reminder_date": st.session_state.get("last_reminder_date")
    }
    return json.dumps(data, indent=2)

# --- PDF GENERATION ---
def remember_photo_bytes(key, data):
    """Keep just-uploaded photo bytes in the session so the PDF builder doesn't
    have to download them straight back out of R2. Photos used to go UP on upload
    and immediately back DOWN to build the email attachment — paid for twice."""
    if not key or not data:
        return
    cache = st.session_state.setdefault('_photo_bytes', {})
    cache[key] = data
    if len(cache) > 40:                      # bound a long session
        for k in list(cache)[:-40]:
            cache.pop(k, None)

def photo_bytes_for_key(photo_key):
    """Bytes for an R2 photo key: the in-session copy if we just uploaded it,
    otherwise fetched. Keyed on the STABLE R2 key — get_image_bytes is cached on
    the URL, and presigned URLs change every call, so that cache rarely hits."""
    if not photo_key:
        return None
    cached = (st.session_state.get('_photo_bytes') or {}).get(photo_key)
    if cached:
        return cached
    url = get_view_url(photo_key, expires_seconds=3600)
    data = get_image_bytes(url) if url else None
    if data:
        remember_photo_bytes(photo_key, data)
    return data

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
        # Brand logo if present, otherwise the text wordmark
        _logo_drawn = False
        try:
            if os.path.exists(LOGO_PATH):
                canv.drawImage(ImageReader(LOGO_PATH), 46, h - 64, width=150, height=34,
                               preserveAspectRatio=True, anchor='sw', mask='auto')
                _logo_drawn = True
        except Exception:
            _logo_drawn = False
        if not _logo_drawn:
            canv.setFillColor(colors.white)
            canv.setFont("Helvetica-Bold", 20)
            canv.drawString(46, h - 48, "5G SECURITY")
        canv.setFillColor(colors.HexColor("#d4d4d8"))
        canv.setFont("Helvetica", 10)
        canv.drawString(46, h - 76, report_type)
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
                img_bytes = photo_bytes_for_key(photo_key)
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

# --- PUSH NOTIFICATIONS (ntfy) ---
def get_ntfy_server():
    """ntfy server to publish to. Defaults to the public ntfy.sh; override with
    NTFY_SERVER (secret/env) if we ever self-host or move to ntfy Pro."""
    server = os.getenv("NTFY_SERVER")
    if not server:
        try:
            server = st.secrets.get("NTFY_SERVER")
        except Exception:
            server = None
    return (server or "https://ntfy.sh").rstrip("/")

def send_push(topic, title, message, tags=None, priority=3):
    """Sends a push notification via ntfy. Pure/thread-safe (safe in the scheduler).
    Keep payloads generic — job titles only, never addresses or credentials.
    Returns True on success."""
    if not topic:
        return False
    try:
        r = requests.post(
            get_ntfy_server() + "/",
            json={
                "topic": topic,
                "title": title,
                "message": message,
                "tags": tags or ["hammer_and_wrench"],
                "priority": priority,
            },
            timeout=8,
        )
        return r.status_code == 200
    except Exception:
        return False

def get_or_create_notify_topic(tech):
    """Returns the tech's personal push topic, generating and persisting an
    unguessable one on first use (random suffix = the 'password')."""
    if not tech:
        return None
    if not tech.get('notify_topic'):
        slug = re.sub(r'[^a-z0-9]', '', (tech.get('name') or 'tech').lower())[:10] or 'tech'
        tech['notify_topic'] = f"5gsec-{slug}-{os.urandom(4).hex()}"
        save_state(invalidate_briefing=False)
    return tech['notify_topic']

def push_assignment(job, tech):
    """New-assignment push to the tech's phone. Generic payload by design.
    Generates the tech's topic on first use so pushes work out of the box
    (they just won't be received until the tech subscribes in the ntfy app)."""
    if not tech:
        return False
    topic = get_or_create_notify_topic(tech)
    return send_push(
        topic,
        "New Job Assignment",
        f"{job.get('title', 'A job')} ({job.get('priority', 'N/A')}) — open the job board for details.",
        tags=["clipboard"],
    )

def email_brand_mark():
    """Email header brand: an <img> if LOGO_URL is configured (emails need a public
    URL — they can't read a repo file), otherwise the text wordmark."""
    url = os.getenv("LOGO_URL")
    if not url:
        try:
            url = st.secrets.get("LOGO_URL")
        except Exception:
            url = None
    if url:
        return f'<img src="{url}" alt="5G Security" style="height:34px; display:inline-block;">'
    return '<span style="color:#ffffff;font-size:22px;font-weight:bold;letter-spacing:1px;">5G SECURITY</span>'

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
            {email_brand_mark()}<br>
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
            {email_brand_mark()}<br>
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

def daily_summary_recipients(techs, admin_emails):
    """Unique, case-insensitive list of tech + admin emails for the daily summary."""
    seen, out = set(), []
    sec_techs = list(techs or [])
    for e in [t.get('email') for t in sec_techs] + list(admin_emails or []):
        if e and e.strip() and e.lower() not in seen:
            seen.add(e.lower())
            out.append(e.strip())
    return out

def build_ops_summary_email(jobs, techs, locations, today_str):
    """Company-wide summary of all active jobs, grouped by tech. Pure/thread-safe.
    Returns (subject, plain_text, html)."""
    def esc(s):
        return str(s if s is not None else "").replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    loc_by_id = {l['id']: l for l in locations}
    tech_by_id = {t['id']: t for t in techs}
    active = [j for j in jobs if j.get('status') != 'Completed']
    crit = [j for j in active if j.get('priority') in ('Critical', 'High')]

    def loc_name(j):
        l = loc_by_id.get(j.get('locationId'))
        return l['name'] if l else 'Unknown location'

    # Group active jobs by assigned tech; unknown/missing techId -> Unassigned
    groups = {}
    for j in active:
        groups.setdefault(j.get('techId'), []).append(j)
    ordered_tids = sorted([tid for tid in groups if tid in tech_by_id],
                          key=lambda t: tech_by_id[t]['name'].lower())
    unassigned = [j for tid, js in groups.items() if tid not in tech_by_id for j in js]

    subject = f"🗓️ Daily Operations Summary - {today_str}"

    # ---- Plain text fallback ----
    lines = [f"5G Security - Daily Operations Summary ({today_str})", "",
             f"{len(active)} active job(s), {len(crit)} critical/high, {len(techs)} tech(s).", ""]
    for tid in ordered_tids:
        lines.append(f"{tech_by_id[tid]['name']} ({len(groups[tid])}):")
        for j in groups[tid]:
            lines.append(f"  - {j['title']} [{j.get('priority')}/{j.get('status')}] @ {loc_name(j)}")
        lines.append("")
    if unassigned:
        lines.append(f"Unassigned ({len(unassigned)}):")
        for j in unassigned:
            lines.append(f"  - {j['title']} [{j.get('priority')}/{j.get('status')}] @ {loc_name(j)}")
        lines.append("")
    lines.append("Open the 5G Security Job Board for full details.")
    plain = "\n".join(lines)

    # ---- HTML ----
    def job_row(j):
        p_color = PRIORITY_COLORS.get(j.get('priority'), "#52525b")
        s_color = get_status_color(j.get('status'))
        return (f'<tr>'
                f'<td style="padding:5px 8px;border-bottom:1px solid #eee;font-size:13px;color:#18181b;">'
                f'<span style="display:inline-block;width:9px;height:9px;border-radius:50%;background:{p_color};margin-right:6px;"></span>'
                f'{esc(j.get("title",""))}</td>'
                f'<td style="padding:5px 8px;border-bottom:1px solid #eee;font-size:12px;color:#555;">{esc(loc_name(j))}</td>'
                f'<td style="padding:5px 8px;border-bottom:1px solid #eee;font-size:11px;text-align:right;">'
                f'<span style="background:{s_color};color:white;padding:1px 7px;border-radius:8px;white-space:nowrap;">{esc(j.get("status",""))}</span></td>'
                f'</tr>')

    sections = ""
    for tid in ordered_tids:
        rows = "".join(job_row(j) for j in groups[tid])
        sections += (f'<div style="margin-top:16px;"><div style="font-weight:bold;font-size:14px;color:#18181b;'
                     f'border-bottom:2px solid #b91c1c;padding-bottom:3px;margin-bottom:4px;">{esc(tech_by_id[tid]["name"])} '
                     f'<span style="color:#a1a1aa;font-weight:normal;">({len(groups[tid])})</span></div>'
                     f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">{rows}</table></div>')
    if unassigned:
        rows = "".join(job_row(j) for j in unassigned)
        sections += (f'<div style="margin-top:16px;"><div style="font-weight:bold;font-size:14px;color:#991b1b;'
                     f'border-bottom:2px solid #991b1b;padding-bottom:3px;margin-bottom:4px;">⚠️ Unassigned '
                     f'<span style="font-weight:normal;">({len(unassigned)})</span></div>'
                     f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">{rows}</table></div>')
    if not active:
        sections = '<p style="color:#555;font-size:14px;">No active jobs right now. 🎉</p>'

    html = f"""<html><body style="margin:0;padding:0;background-color:#f4f4f5;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f4f5;">
<tr><td align="center" style="padding:24px 12px;">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background-color:#ffffff;border-radius:8px;overflow:hidden;font-family:Arial,Helvetica,sans-serif;border:1px solid #e4e4e7;">
    <tr><td style="background-color:#18181b;padding:22px 32px;border-bottom:4px solid #b91c1c;">
        {email_brand_mark()}<br>
        <span style="color:#a1a1aa;font-size:13px;">Daily Operations Summary &mdash; {esc(today_str)}</span>
    </td></tr>
    <tr><td style="padding:24px 32px;">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
            <td style="text-align:center;"><div style="font-size:24px;font-weight:bold;color:#18181b;">{len(active)}</div><div style="font-size:11px;color:#71717a;text-transform:uppercase;">Active</div></td>
            <td style="text-align:center;"><div style="font-size:24px;font-weight:bold;color:#b91c1c;">{len(crit)}</div><div style="font-size:11px;color:#71717a;text-transform:uppercase;">Critical / High</div></td>
            <td style="text-align:center;"><div style="font-size:24px;font-weight:bold;color:#18181b;">{len(techs)}</div><div style="font-size:11px;color:#71717a;text-transform:uppercase;">Techs</div></td>
        </tr></table>
        {sections}
        <p style="color:#71717a;font-size:12px;margin:20px 0 0 0;">Open the 5G Security Job Board for full details and to log work.</p>
    </td></tr>
    <tr><td style="background-color:#f4f4f5;padding:14px 32px;color:#71717a;font-size:11px;border-top:1px solid #e4e4e7;">
        5G Security &nbsp;|&nbsp; Cameras &middot; Access Control &middot; Alarm Systems &middot; Cabling
    </td></tr>
</table></td></tr></table></body></html>"""
    return subject, plain, html

def send_assignment_email(job, tech, location):
    """Sends an email notification via SMTP, returning True if successful."""
    # Helper to resolve config priority: Session > Secrets > Env

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

def send_completion_email(job, tech, location, report_data, timer=None):
    """Sends an email notification to Admins when a job is completed, with PDF attachment."""
    # Helper to resolve config priority: Session > Secrets > Env

    smtp_server = get_config_val("SMTP_SERVER")
    smtp_port = get_config_val("SMTP_PORT", 587)
    sender_email = get_config_val("SMTP_EMAIL")
    sender_password = get_config_val("SMTP_PASSWORD")
    
    recipients = list(st.session_state.adminEmails)
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
    if timer: timer.mark(f"pdf({round(len(pdf_bytes)/1024) if pdf_bytes else 0}KB)")

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
        if timer: timer.mark("smtp")
        st.toast("📧 Completion notification sent to Admins", icon="✅")
    except Exception as e:
        st.error(f"Failed to send completion email: {str(e)}")

def send_daily_report_email(job, tech, location, report_data, timer=None):
    """Sends a Daily Report email to Admins with PDF attachment.
    `timer` is an optional StepTimer so the caller can see the PDF vs SMTP split."""
    # Helper to resolve config priority: Session > Secrets > Env

    smtp_server = get_config_val("SMTP_SERVER")
    smtp_port = get_config_val("SMTP_PORT", 587)
    sender_email = get_config_val("SMTP_EMAIL")
    sender_password = get_config_val("SMTP_PASSWORD")
    
    recipients = list(st.session_state.adminEmails)
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
    if timer: timer.mark(f"pdf({round(len(pdf_bytes)/1024) if pdf_bytes else 0}KB)")

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
        if timer: timer.mark("smtp")
        st.toast("📧 Daily Report sent to Admins", icon="✅")
    except Exception as e:
        st.error(f"Failed to send daily report email: {str(e)}")

def send_ops_summary_email(recipients, subject_prefix=""):
    """Sends the company-wide ops summary to the given recipients immediately.
    Used by the admin 'send test' button - bypasses the Mon-Fri / once-a-day guards.
    Returns (sent_count, error_message_or_None)."""

    smtp_server = get_config_val("SMTP_SERVER")
    smtp_port = get_config_val("SMTP_PORT", 587)
    sender_email = get_config_val("SMTP_EMAIL")
    sender_password = get_config_val("SMTP_PASSWORD")

    if not (smtp_server and sender_email and sender_password):
        return 0, "SMTP is not configured (check the SMTP Configuration section)."
    if not recipients:
        return 0, "No recipient email address available."

    today_str = now_local().strftime("%Y-%m-%d")
    sent = 0
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

        subject, plain_body, html_body = build_ops_summary_email(
            st.session_state.jobs, st.session_state.techs, st.session_state.locations, today_str)
        for recipient in recipients:
            try:
                msg = MIMEMultipart("alternative")
                msg['From'] = sender_email
                msg['To'] = recipient
                msg['Subject'] = subject_prefix + subject
                msg.attach(MIMEText(plain_body, 'plain'))
                msg.attach(MIMEText(html_body, 'html'))
                server.send_message(msg)
                sent += 1
            except Exception:
                continue
        server.quit()
        return sent, None
    except Exception as e:
        return sent, str(e)

def _send_hours_digest_email(label, recipients, smtp_server, smtp_port,
                             sender_email, sender_password, jobs, techs, locations, start_d, end_d):
    """Builds and emails the weekly hours digest (CSV attached).
    Pure/thread-safe — used by the Friday scheduler. Returns rows sent (0 if nothing)."""
    recipients = list(dict.fromkeys([r for r in (recipients or []) if r]))  # dedup, keep order
    if not (recipients and smtp_server and sender_email and sender_password):
        return 0
    rows = compute_hours_rows(jobs, techs, locations, start_d, end_d)
    if not rows:
        return 0

    totals = {}
    for row in rows:
        totals[row["Tech"]] = totals.get(row["Tech"], 0) + row["Hours"]
    detail_rows = [(tn, f"{round(th, 2)} hrs") for tn, th in sorted(totals.items(), key=lambda x: -x[1])]
    detail_rows.append(("Total", f"{round(sum(totals.values()), 2)} hrs"))

    subject = f"🕒 {label} Weekly Hours — {start_d} to {end_d}"
    plain_body = (f"{label} hours logged {start_d} to {end_d}:\n\n"
                  + "\n".join(f"{a}: {b}" for a, b in detail_rows)
                  + "\n\nFull entry list attached as CSV.")
    try:
        html_body = build_admin_email_html(f"{label} Weekly Hours",
                                           f"Hours logged {start_d} to {end_d}:", detail_rows,
                                           "Full entry list attached as a CSV for payroll/invoicing.")
    except Exception:
        html_body = None

    csv_str = pd.DataFrame(rows).sort_values(["Date", "Tech"]).to_csv(index=False)
    try:
        if int(smtp_port) == 465:
            server = smtplib.SMTP_SSL(smtp_server, int(smtp_port)); server.ehlo()
        else:
            server = smtplib.SMTP(smtp_server, int(smtp_port)); server.ehlo(); server.starttls(); server.ehlo()
        server.login(sender_email, sender_password)
        for recipient in recipients:
            try:
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
                attachment.add_header('Content-Disposition', 'attachment',
                                      filename=f"hours_{start_d}_{end_d}.csv")
                msg.attach(attachment)
                server.send_message(msg)
            except Exception:
                continue
        server.quit()
    except Exception:
        return 0
    return len(rows)

def generate_morning_briefing():
    """Generates the morning briefing using Gemini."""
    api_key = get_api_key()
    if not api_key:
        return "⚠️ API Key missing. Please set GEMINI_API_KEY in secrets.toml or environment."
    
    if not st.session_state.jobs:
        return "No active jobs to analyze. Please add jobs via the 'New Job' button."

    # Use dynamic model selector
    client, model_name = get_available_model(api_key)

    sec_jobs = list(st.session_state.jobs)
    active_jobs = [j for j in sec_jobs if j['status'] != 'Completed']
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

def time_select(label, default, key, step_minutes=15):
    """Mobile-native time picker: number pad for hour, tap chips for minute/am-pm.
    No dropdowns, no popover, no keyboard-dismiss bugs inside dialogs."""
    st.caption(label)

    if isinstance(default, str):
        try:
            default = datetime.datetime.strptime(default, "%H:%M:%S").time()
        except (ValueError, TypeError):
            default = datetime.time(8, 0)
    elif not isinstance(default, datetime.time):
        default = datetime.time(8, 0)

    hour24 = default.hour
    minute = default.minute
    ampm = "AM" if hour24 < 12 else "PM"
    hour12 = hour24 % 12
    if hour12 == 0:
        hour12 = 12

    minute_opts = list(range(0, 60, step_minutes))
    rounded_min = min(minute_opts, key=lambda x: abs(x - minute))

    c1, c2, c3 = st.columns([1.2, 1.8, 1.2])
    with c1:
        h = st.number_input(
            "Hr", min_value=1, max_value=12, value=hour12,
            key=f"{key}_h", label_visibility="collapsed"
        )
    with c2:
        if hasattr(st, "segmented_control"):
            m = st.segmented_control(
                "Min", minute_opts, format_func=lambda x: f"{x:02d}",
                default=rounded_min, key=f"{key}_m", label_visibility="collapsed"
            )
        else:
            m = st.radio(
                "Min", minute_opts, format_func=lambda x: f"{x:02d}",
                index=minute_opts.index(rounded_min), horizontal=True,
                key=f"{key}_m", label_visibility="collapsed"
            )
    with c3:
        if hasattr(st, "segmented_control"):
            ap = st.segmented_control(
                "AM/PM", ["AM", "PM"], default=ampm,
                key=f"{key}_ap", label_visibility="collapsed"
            )
        else:
            ap = st.radio(
                "AM/PM", ["AM", "PM"], index=0 if ampm == "AM" else 1,
                horizontal=True, key=f"{key}_ap", label_visibility="collapsed"
            )

    h = int(h)
    m = int(m) if m is not None else rounded_min
    ap = ap or ampm

    if ap == "PM" and h != 12:
        h += 12
    elif ap == "AM" and h == 12:
        h = 0

    return datetime.time(h, m)


# --- DIALOGS (MODALS) ---

@st.dialog("Create New Job")
def add_job_dialog():
    if not st.session_state.locations:
        st.error("Please create a Location in the Admin tab first.")
        if st.button("Close"): st.rerun()
        return

    # Location picker lives OUTSIDE the form so choosing a site can immediately
    # pull in that location's saved contact. (Widgets inside an st.form don't
    # rerun on change, so this prefill can't happen from within the form.)
    loc_map = {l['name']: l['id'] for l in st.session_state.locations}
    loc_options = list(loc_map.keys()) + ["➕ New Location"]
    loc_selection = st.selectbox("Location", loc_options)

    selected_loc = get_location(loc_map[loc_selection]) if loc_selection in loc_map else None
    prefill_name = (selected_loc or {}).get('contact_name', '') or ''
    prefill_phone = (selected_loc or {}).get('contact_phone', '') or ''
    if selected_loc and (prefill_name or prefill_phone):
        st.caption(f"📇 Loaded the saved contact for **{selected_loc['name']}** — edit below if needed.")

    with st.form("new_job_form"):
        title = st.text_input("Job Title")
        desc = st.text_area("Description")

        c1, c2 = st.columns(2)
        job_type = c1.selectbox("Type", ["Service", "Project", "Leads"])
        priority = c2.selectbox("Priority", ["Medium", "Low", "High", "Critical"])

        # Date Selection
        job_date = st.date_input("Scheduled Date", value=now_local())

        # New Location Fields (used only if "➕ New Location" is selected above)
        st.write("---")
        with st.expander("New Location Details", expanded=(loc_selection == "➕ New Location")):
            new_loc_name = st.text_input("New Location Name")
            new_loc_address = st.text_input("New Location Address")
            new_loc_maps = st.text_input("Google Maps Link (Optional)")

        # Multiple Site Contacts (Primary is prefilled from the selected location)
        st.write("---")
        st.write("###### 👥 Site Contacts")
        c1, c2 = st.columns(2)
        contact1_name = c1.text_input("Primary Contact Name", value=prefill_name, key=f"njc1_name_{loc_selection}")
        contact1_phone = c1.text_input("Primary Contact Phone", value=prefill_phone, key=f"njc1_phone_{loc_selection}")

        contact2_name = c2.text_input("Secondary Contact Name")
        contact2_phone = c2.text_input("Secondary Contact Phone")

        contact3_name = st.text_input("Additional Contact / Notes")

        # Tech Selection
        company_crew = list(st.session_state.techs)
        tech_map = {t['name']: t['id'] for t in company_crew}

        # Create display labels with skills
        tech_display_map = {}
        for t in company_crew:
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
                'documents': doc_keys,
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
                    # Push notification to the tech's phone (ntfy)
                    push_assignment(new_job, tech)

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

    # Documents are managed OUTSIDE the form: Streamlit raises
    # "st.button() can't be used in an st.form()", so a job with documents used to
    # crash this dialog outright.
    existing_docs = job.get('documents', [])
    if existing_docs:
        st.write("###### 📄 Job Documents")
        for i, d in enumerate(existing_docs):
            c_d1, c_d2 = st.columns([4, 1])
            c_d1.write(f"📎 {d['name']}")
            if c_d2.button(":material/delete:", key=f"del_doc_{job_id}_{i}"):
                existing_docs.pop(i)
                st.session_state.jobs[job_index]['documents'] = existing_docs
                save_state(invalidate_briefing=False)
                st.toast(f"Removed {d['name']}", icon="🗑️")
                st.rerun()

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
        
        # Tech Selection (scoped to the job's company so crews don't cross over)
        company_crew = list(st.session_state.techs)

        # Create display labels with skills
        tech_display_map = {}
        for t in company_crew:
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

        # Only the uploader lives in the form — st.button is not allowed inside
        # st.form, so the delete controls sit above it (see the block before the form).
        st.write("---")
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
                
                # Push-notify the new tech if the job changed hands
                _prev_tech_id = st.session_state.jobs[job_index].get('techId')
                st.session_state.jobs[job_index]['techId'] = selected_tech_id
                if selected_tech_id and selected_tech_id != _prev_tech_id:
                    _new_tech = get_tech(selected_tech_id)
                    if _new_tech:
                        push_assignment(st.session_state.jobs[job_index], _new_tech)

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
                st.toast("Location updated!", icon="✅")
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

        _t = StepTimer("job completion")
        if report_payload.get("content"):
            with st.spinner("Generating AI Summary..."):
                summary = generate_technician_summary(report_payload["content"], job["title"])
                if summary:
                    report_payload["ai_summary"] = summary
        _t.mark("gemini")

        st.session_state.jobs[job_index]["reports"].append(report_payload)
        _actor = st.session_state.user_info.get('email') if "user_info" in st.session_state else None
        apply_job_status(st.session_state.jobs[job_index], "Completed", _actor)
        st.session_state.briefing = "Data required to generate briefing."

        # Persist before emailing — a slow or failing SMTP server must never be
        # the reason a completed job wasn't recorded.
        save_state()
        _t.mark("save")

        tech = get_tech(job["techId"])
        loc = get_location(job["locationId"])
        send_completion_email(job, tech, loc, report_payload, timer=_t)
        _t.finish(job=job.get('id'), photos=len(report_payload.get('photos') or []))

        if f"completion_pending_{job['id']}" in st.session_state:
            del st.session_state[f"completion_pending_{job['id']}"]

        st.toast("Job Completed & Closed!", icon="✅")
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
            time_arrived = time_select("Time Arrived", t_arr, key=f"edit_arr_sel_{report_id}")
            
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
            time_departed = time_select("Time Finished", t_dep, key=f"edit_dep_sel_{report_id}")
            
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
            st.toast("Report updated!", icon="✅")
            st.rerun(scope="fragment")

    if st.button("Cancel Edit"):
        if f"editing_report_{job_id}" in st.session_state:
            del st.session_state[f"editing_report_{job_id}"]
        st.rerun(scope="fragment")

@st.dialog("🏷️ Asset")
def asset_dialog(tag):
    loc, asset = find_asset(tag)
    if not asset:
        st.error(f"No equipment found with tag **{tag}**.")
        st.caption("Check the code on the label, or search for it in Admin → Data Browser → Assets.")
        return

    st.subheader(f"{asset.get('type', 'Asset')}"
                 + (f" — {asset['make_model']}" if asset.get('make_model') else ""))
    st.code(asset.get('tag', ''), language=None)

    rows = [
        ("Site", (loc or {}).get('name', '—')),
        ("Where", asset.get('position') or "—"),
        ("Serial", asset.get('serial') or "—"),
        ("Installed", str(asset.get('installed_date', ''))[:10] or "—"),
    ]
    months, expiry = asset_warranty_left(asset)
    if months is not None:
        rows.append(("Warranty",
                     f"{months} months left (to {expiry})" if months >= 0
                     else f"expired {abs(months)} months ago ({expiry})"))
    for k, v in rows:
        c1, c2 = st.columns([1, 2])
        c1.caption(k)
        c2.write(v)

    if asset.get('notes'):
        st.info(asset['notes'])

    src_job = next((j for j in st.session_state.jobs if j['id'] == asset.get('job_id')), None)
    if src_job:
        st.caption(f"Installed on job: **{src_job.get('title', '')}**")
        if st.button("Open that job", use_container_width=True):
            st.session_state["_open_job_after_rerun"] = src_job['id']
            st.rerun()

    if loc:
        site_jobs = [j for j in st.session_state.jobs if j.get('locationId') == loc['id']]
        st.caption(f"{len(site_jobs)} job(s) recorded at this site.")


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

    # Section nav: a single-select control + conditional rendering (only the
    # chosen section is built). st.tabs kept ALL panels in the DOM and on mobile
    # the inactive ones would "unhide" after an in-dialog interaction (e.g. picking
    # a time); rendering just one section makes that impossible. Stable IDs are
    # used so the selection survives reruns even when a label changes (e.g. Parts count).
    _sections = ["history", "photos", "docs", "parts", "progress", "daily", "assets", "creds"]
    # Invoicing is admin/office-manager only, and only once the work is done.
    _viewer_email = st.session_state.user_info.get('email', '') if "user_info" in st.session_state else ''
    _viewer_is_admin = _viewer_email in st.session_state.adminEmails if _viewer_email else False
    if _viewer_is_admin and job.get('status') == 'Completed':
        _sections.append("invoice")
    _section_labels = {
        "history": "📋 Details & History", "photos": "🖼️ Photos",
        "docs": "📄 Documents", "parts": parts_tab_label,
        "progress": "📸 In-Progress", "daily": "📝 Daily Report",
        "creds": creds_tab_label, "invoice": "💵 Invoicing",
        "assets": "🏷️ Equipment",
    }
    _fmt_section = lambda s: _section_labels.get(s, s)
    if hasattr(st, "segmented_control"):
        section = st.segmented_control(
            "Section", _sections, format_func=_fmt_section,
            default="history", key=f"jobsection_{job_id}", label_visibility="collapsed",
        )
    else:
        section = st.radio(
            "Section", _sections, format_func=_fmt_section, horizontal=True,
            key=f"jobsection_{job_id}", label_visibility="collapsed",
        )
    if section is None:
        section = "history"

    if section == "docs":
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
                            st.toast(f"Uploaded {len(new_keys)} document(s)!", icon="✅")
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

    if section == "photos":
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

    if section == "parts":
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
                    apply_job_status(st.session_state.jobs[job_index], 'Parts Staged', _viewer_email)
                    save_state()
                    st.rerun(scope="fragment")
            elif any(p.get('status') in ('Needed', 'Ordered') for p in parts) and job['status'] not in ('Waiting on Parts', 'Parts not ordered'):
                only_needed = all(p.get('status') == 'Needed' for p in parts)
                suggested = 'Parts not ordered' if only_needed else 'Waiting on Parts'
                if st.button(f"📦 Mark job '{suggested}'", key=f"sync_waiting_{job_id}", use_container_width=True):
                    apply_job_status(st.session_state.jobs[job_index], suggested, _viewer_email)
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
                        st.toast(f"Added {new_name.strip()}.", icon="✅")
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
                if pc3.button(":material/delete:", key=f"del_part_{p['id']}", help="Remove this part", use_container_width=True):
                    st.session_state.jobs[job_index]['parts'] = [x for x in st.session_state.jobs[job_index].get('parts', []) if x['id'] != p['id']]
                    save_state(invalidate_briefing=False)
                    st.rerun(scope="fragment")

                if p.get('updated_at'):
                    pc1.caption(f"Updated {p['updated_at'][:16]} by {p.get('added_by', 'unknown')}")


    if section == "creds":
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
                            st.toast(f"'{sys_name}' saved!", icon="✅")
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
                                st.toast("System updated!", icon="✅")
                                st.rerun(scope="fragment")

                            if ec2.form_submit_button("🗑️ Delete System"):
                                loc['systems'] = [x for x in loc['systems'] if x['id'] != s['id']]
                                get_logger().log(f"{current_user_email} deleted system '{s.get('name')}' from location {loc['id']}")
                                save_state(invalidate_briefing=False)
                                st.toast(f"'{s.get('name')}' deleted", icon="🗑️")
                                st.rerun(scope="fragment")

    if section == "assets":
        st.write("#### 🏷️ Equipment")
        if not loc:
            st.warning("This job has no site assigned, so equipment can't be registered "
                       "against one. Assign a location first.")
        else:
            st.caption(f"Gear installed at **{loc['name']}**. Tags stay with the site, so "
                       "scanning one later shows its full history no matter which job it came from.")

            with st.expander("➕ Register equipment", expanded=not (loc.get('assets') or [])):
                with st.form(key=f"asset_form_{job_id}"):
                    ac1, ac2 = st.columns([1, 1])
                    a_type = ac1.selectbox("Type", ASSET_TYPES)
                    a_qty = ac2.number_input("How many", min_value=1, max_value=20, value=1,
                                             help="Registers this many, each with its own tag.")
                    a_model = st.text_input("Make / model", placeholder="e.g. Hikvision DS-7616NI-K2")
                    ac3, ac4 = st.columns([1, 1])
                    a_serial = ac3.text_input("Serial", placeholder="one unit only")
                    a_pos = ac4.text_input("Where on site", placeholder="e.g. IDF 2")
                    ac5, ac6 = st.columns([1, 1])
                    a_warr = ac5.number_input("Warranty (months)", min_value=0, max_value=120, value=36)
                    a_date = ac6.text_input("Installed", value=now_local().strftime('%Y-%m-%d'),
                                            placeholder="YYYY-MM-DD")
                    a_notes = st.text_area("Notes", height=70)

                    if st.form_submit_button("Register + create tag(s)"):
                        made = []
                        for n in range(int(a_qty)):
                            tag = next_asset_tag()
                            rec = {
                                "id": f"as{now_local().timestamp()}_{n}",
                                "tag": tag,
                                "type": a_type,
                                "make_model": a_model.strip(),
                                # A serial only makes sense for a single unit
                                "serial": a_serial.strip() if int(a_qty) == 1 else "",
                                "position": a_pos.strip(),
                                "installed_date": a_date.strip() or now_local().strftime('%Y-%m-%d'),
                                "warranty_months": int(a_warr) or None,
                                "notes": a_notes.strip(),
                                "job_id": job_id,
                                "created_by": _viewer_email,
                                "created_at": now_local().isoformat(),
                            }
                            loc.setdefault('assets', []).append(rec)
                            made.append(tag)
                        save_state(invalidate_briefing=False)
                        get_logger().log(f"{_viewer_email} registered {len(made)} asset(s) at {loc['id']}: {', '.join(made)}")
                        st.toast(f"Registered {', '.join(made)}. Print labels below.", icon="✅")
                        st.rerun(scope="fragment")

            site_assets = loc.get('assets') or []
            if not site_assets:
                st.info("No equipment registered at this site yet.")
            else:
                _here = [a for a in site_assets if a.get('job_id') == job_id]
                st.caption(f"{len(site_assets)} on site"
                           + (f" · {len(_here)} from this job" if _here else ""))

                if HAS_REPORTLAB:
                    pc1, pc2 = st.columns([1, 1])
                    if _here:
                        pdf_here = build_asset_labels_pdf([(loc, a) for a in _here])
                        if pdf_here:
                            pc1.download_button(f"🏷️ Print labels — this job ({len(_here)})",
                                                pdf_here, file_name=f"labels_{job_id}.pdf",
                                                mime="application/pdf", use_container_width=True,
                                                key=f"lbl_job_{job_id}")
                    pdf_all = build_asset_labels_pdf([(loc, a) for a in site_assets])
                    if pdf_all:
                        pc2.download_button(f"🏷️ Print labels — whole site ({len(site_assets)})",
                                            pdf_all, file_name=f"labels_site_{loc['id']}.pdf",
                                            mime="application/pdf", use_container_width=True,
                                            key=f"lbl_site_{job_id}")
                    st.caption("Avery 5160 / 8160 sheets — 30 labels per page.")
                else:
                    st.caption("Label printing needs reportlab, which isn't available here.")

                for a in site_assets:
                    months, expiry = asset_warranty_left(a)
                    with st.container(border=True):
                        r1, r2 = st.columns([3, 1])
                        _model = f" — {a['make_model']}" if a.get('make_model') else ""
                        r1.markdown(f"**`{a.get('tag', '')}`** · {a.get('type', '')}{_model}")
                        _bits = [x for x in [a.get('position'), a.get('serial'),
                                             f"installed {str(a.get('installed_date', ''))[:10]}"] if x]
                        r1.caption(" · ".join(_bits))
                        if months is not None:
                            if months >= 0:
                                r2.markdown(
                                    f"<span style='background:#27272a;color:{SEMANTIC['done']};"
                                    f"font-size:0.72em;padding:2px 7px;border-radius:4px;'>"
                                    f"warranty {months} mo</span>", unsafe_allow_html=True)
                            else:
                                r2.markdown(
                                    f"<span style='background:#27272a;color:{SEMANTIC['neutral']};"
                                    f"font-size:0.72em;padding:2px 7px;border-radius:4px;'>"
                                    f"out of warranty</span>", unsafe_allow_html=True)
                        if a.get('notes'):
                            st.caption(a['notes'])
                        if _viewer_is_admin:
                            if st.button("🗑️ Remove", key=f"del_asset_{a['id']}"):
                                loc['assets'] = [x for x in loc['assets'] if x['id'] != a['id']]
                                save_state(invalidate_briefing=False)
                                st.toast(f"Removed {a.get('tag', '')}", icon="🗑️")
                                st.rerun(scope="fragment")

    if section == "invoice":
        st.write("#### 💵 Invoicing")
        inv = job_invoice(job)
        _cur = inv['status']
        st.markdown(
            f'<span style="background:{INVOICE_STATUS_COLORS.get(_cur, "#52525b")};color:white;'
            f'padding:3px 12px;border-radius:10px;font-size:0.85em;">'
            f'{INVOICE_STATUS_ICONS.get(_cur, "")} {_cur}</span>',
            unsafe_allow_html=True)
        if inv['updated_by']:
            st.caption(f"Last updated by {inv['updated_by']} on {inv['updated_at'][:16].replace('T', ' ')}")

        # What to bill: hours + billable items pulled straight off the daily reports
        _tot_hours = 0.0
        _billables = []
        for r in (job.get('reports') or []):
            try:
                _tot_hours += float(r.get('hoursWorked') or 0)
            except (TypeError, ValueError):
                pass
            if r.get('billableItems'):
                _billables.append(f"{(r.get('timestamp') or '')[:10]} — {r['billableItems']}")
        m1, m2 = st.columns(2)
        m1.metric("Hours logged", f"{_tot_hours:g}")
        m2.metric("Warranty work", "Yes" if job_is_warranty(job) else "No")
        if _billables:
            with st.expander(f"🧾 Billable items ({len(_billables)})", expanded=False):
                for b in _billables:
                    st.write(f"- {b}")

        # Seed the amount from the quote when nothing has been billed yet — saves
        # retyping a number the system already has, and the times she CHANGES it
        # are exactly the quote-vs-actual variance worth knowing about.
        _quote_raw = str(job.get('quoteValue', '') or '').strip()
        _amount_seed = inv['amount'] or _quote_raw
        _from_quote = bool(_quote_raw) and not inv['amount']

        with st.form(key=f"invoice_form_{job_id}"):
            i_status = st.selectbox("Invoice Status", INVOICE_STATUSES,
                                    index=INVOICE_STATUSES.index(_cur) if _cur in INVOICE_STATUSES else 0)
            ic1, ic2 = st.columns(2)
            i_number = ic1.text_input("Invoice #", value=inv['number'])
            i_amount = ic2.text_input("Amount", value=_amount_seed, placeholder="e.g. 1450.00")
            if _from_quote:
                ic2.caption(f"Prefilled from the quote ({format_money(_quote_raw)}) — edit if you billed something else.")
            # Plain text, not st.date_input — date pickers hit the same unclickable
            # popover problem inside dialogs on mobile. Auto-stamped on save.
            i_date = st.text_input("Invoice Date", value=inv['date'], placeholder="YYYY-MM-DD")
            i_notes = st.text_area("Invoice Notes", value=inv['notes'])
            if st.form_submit_button("💾 Save Invoicing"):
                if i_status in ("Invoiced", "Paid") and not i_date.strip():
                    i_date = now_local().strftime('%Y-%m-%d')
                set_job_invoice(job_id, status=i_status, number=i_number,
                                amount=i_amount, date=i_date, notes=i_notes)
                get_logger().log(f"{_viewer_email} set invoice status '{i_status}' on job {job_id}")
                st.toast("Invoicing updated.", icon="✅")
                st.rerun(scope="fragment")

    if section == "history":
        st.markdown(f"**Description:** {job['description']}")

        # Quote value — what we quoted the customer for this job. Free text on
        # purpose so "TBD" or a note survives; format_money() prettifies numbers.
        _qv_c1, _qv_c2 = st.columns([1, 1])
        with _qv_c1:
            with st.form(key=f"quote_form_{job_id}"):
                _qv_new = st.text_input("💲 Quote Value", value=job.get('quoteValue', ''),
                                        placeholder="e.g. 1450.00")
                if st.form_submit_button("Save Quote Value"):
                    st.session_state.jobs[job_index]['quoteValue'] = _qv_new.strip()
                    save_state(invalidate_briefing=False)
                    st.toast("Quote value saved", icon="💲")
                    st.rerun(scope="fragment")

        # Site History: what else have we done at this location?
        if loc:
            site_jobs = [sj for sj in st.session_state.jobs
                         if sj['locationId'] == loc['id'] and sj['id'] != job_id]
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
            r_tech = get_tech(r.get('techId'))

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
                        other_jobs = {j['id']: j for j in st.session_state.jobs
                                      if j['id'] != job_id}
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
                    if hdr_del.button(":material/delete:", key=f"del_rep_{r['id']}", help="Delete this entry"):
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

    if section == "progress":
        # --- TIME CLOCK ---
        st.write("#### ⏱️ Time Clock")
        viewer_email = st.session_state.user_info.get('email', '') if "user_info" in st.session_state else ''
        viewer_name = st.session_state.user_info.get('name', '') if "user_info" in st.session_state else ''
        entries = st.session_state.jobs[job_index].setdefault('time_entries', [])
        my_open = open_time_entry(entries, viewer_email)

        tc1, tc2 = st.columns([2, 1])
        if my_open:
            try:
                ci_dt = datetime.datetime.fromisoformat(my_open['clock_in'])
                since_str = ci_dt.strftime('%I:%M %p').lstrip('0')
            except (ValueError, TypeError):
                since_str = "earlier"
            elapsed = clocked_hours([my_open])
            tc1.success(f"🟢 Clocked in since {since_str} · {_fmt_duration(elapsed)}")
            if tc2.button("⏹️ Clock Out", key=f"clockout_{job_id}", use_container_width=True):
                my_open['clock_out'] = now_local().isoformat()
                save_state(invalidate_briefing=False)
                st.toast("Clocked out", icon="⏹️")
                st.rerun(scope="fragment")
        else:
            tc1.caption("Not clocked in.")
            if tc2.button("⏱️ Clock In", key=f"clockin_{job_id}", use_container_width=True):
                entries.append({
                    'id': f"tc{now_local().timestamp()}",
                    'userEmail': viewer_email,
                    'tech_name': viewer_name or viewer_email,
                    'clock_in': now_local().isoformat(),
                    'clock_out': None,
                })
                save_state(invalidate_briefing=False)
                st.toast("Clocked in", icon="⏱️")
                st.rerun(scope="fragment")

        my_today = clocked_hours(entries, viewer_email, now_local().date())
        job_total = clocked_hours(entries)
        st.caption(f"Your time today: **{_fmt_duration(my_today)}**  ·  Everyone, all-time on this job: **{_fmt_duration(job_total)}**")

        # Labor log (per person) — handy for admins
        if entries:
            with st.expander("🕒 Time Log"):
                by_person = {}
                for e in entries:
                    by_person.setdefault(e.get('tech_name', 'Unknown'), 0.0)
                    by_person[e['tech_name'] if e.get('tech_name') else 'Unknown'] += clocked_hours([e])
                for name, hrs in sorted(by_person.items(), key=lambda x: -x[1]):
                    st.write(f"**{name}** — {_fmt_duration(hrs)}")
                st.divider()
                for e in sorted(entries, key=lambda x: x.get('clock_in', ''), reverse=True):
                    try:
                        ci = datetime.datetime.fromisoformat(e['clock_in'])
                        ci_s = ci.strftime('%b %d, %I:%M %p').replace(' 0', ' ')
                    except (ValueError, TypeError):
                        ci_s = e.get('clock_in', '?')
                    if e.get('clock_out'):
                        try:
                            co = datetime.datetime.fromisoformat(e['clock_out'])
                            co_s = co.strftime('%I:%M %p').lstrip('0')
                        except (ValueError, TypeError):
                            co_s = "?"
                        st.caption(f"{e.get('tech_name', 'Unknown')}: {ci_s} → {co_s} ({_fmt_duration(clocked_hours([e]))})")
                    else:
                        st.caption(f"{e.get('tech_name', 'Unknown')}: {ci_s} → 🟢 still clocked in")

        st.divider()

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
                    apply_job_status(st.session_state.jobs[job_index], 'In Progress', _viewer_email)
                
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
                        apply_job_status(st.session_state.jobs[job_index], 'In Progress', _viewer_email)
                        st.session_state.briefing = "Data required to generate briefing."
                    
                    save_state()
                    st.toast("Update Posted!", icon="✅")
                    st.rerun(scope="fragment")
                else:
                    st.warning("Please add a note or photo.")

    if section == "daily":
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
                st.toast("Report Sent & Saved!", icon="✅")
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

        # Prefill, in increasing order of authority:
        #   1. plain defaults  2. the job's last daily report  3. today's quick-status taps
        today_prefix = now_local().strftime('%Y-%m-%d')
        default_arrived = datetime.time(8, 0)
        default_departed = datetime.time(17, 0)
        times_prefilled = False

        _prev = last_daily_report(job)
        _prev_used = False
        if _prev:
            default_arrived = _parse_report_time(_prev.get('timeArrived'), default_arrived)
            default_departed = _parse_report_time(_prev.get('timeDeparted'), default_departed)
            _prev_used = bool(_prev.get('timeArrived') or _prev.get('timeDeparted'))
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
        elif _prev_used:
            st.caption(f"↩️ Prefilled from the last report on this job "
                       f"({str(_prev.get('timestamp', ''))[:10]}) — adjust anything that changed.")

        # Prefill Hours Worked from the viewer's time clock (today), rounded to 1/4 hr
        _viewer_email = st.session_state.user_info.get('email', '') if "user_info" in st.session_state else ''
        clocked_today = clocked_hours(job.get('time_entries', []), _viewer_email, now_local().date())
        default_hours = round(clocked_today * 4) / 4 if clocked_today > 0 else 0.0
        if clocked_today > 0:
            st.caption(f"⏱️ Hours Worked is prefilled from your time clock today ({_fmt_duration(clocked_today)}) — adjust if needed.")

        with st.form(key=f"daily_form_{job_id}"):
            status_options = ["Not Started", "In Progress", "Customer on Hold", "Waiting on Parts", "Parts not ordered", "Parts Staged", "Completed"]
            current_status = job['status']
            if current_status == "Pending": current_status = "Not Started"
            try:
                status_idx = status_options.index(current_status)
            except ValueError:
                status_idx = 0
            
            new_status = st.selectbox("Job Status", status_options, index=status_idx)
            # Default from what's actually been recorded on this job. job['isWarranty']
            # is never written anywhere — warranty lives on the reports — so reading it
            # directly made this box forget every time. job_is_warranty() checks both.
            is_warranty = st.checkbox("Warranty Work?", value=job_is_warranty(job))

            r_col1, r_col2 = st.columns(2)
            with r_col1:
                # Techs on Site: prefer whoever was on site last time (same crew
                # usually returns), falling back to the assigned tech.
                available_techs = [t['name'] for t in st.session_state.techs]
                default_techs = [tech['name']] if tech and tech['name'] in available_techs else []
                if _prev and _prev.get('techsOnSite'):
                    _prev_crew = [n.strip() for n in str(_prev['techsOnSite']).split(',') if n.strip()]
                    # Drop anyone who has since left, so the multiselect can't error
                    _prev_crew = [n for n in _prev_crew if n in available_techs]
                    if _prev_crew:
                        default_techs = _prev_crew
                
                techs_on_site_list = st.multiselect("Techs On Site", options=available_techs, default=default_techs)
                time_arrived = time_select("Time Arrived", default_arrived, key=f"daily_arr_sel_{job_id}")
                parts_used = st.text_area("Parts/Materials Used")
            with r_col2:
                hours_worked = st.number_input("Hours Worked", min_value=0.0, step=0.5, value=default_hours, help="Prefilled from your time clock; leave at 0 to calc from arrival/finish times.")
                time_departed = time_select("Time Finished", default_departed, key=f"daily_dep_sel_{job_id}")
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
                _t = StepTimer("daily submit")
                # Wrapping up the day — clock the viewer out if they're still running
                _open = open_time_entry(job.get('time_entries', []), _viewer_email)
                if _open:
                    _open['clock_out'] = now_local().isoformat()

                # Process any new photos uploaded directly in this form
                _new_photo_count = len(daily_photos or [])
                if daily_photos:
                    for up_file in daily_photos:
                        path = save_image_locally(up_file)
                        if path:
                            todays_photos.append(path)
                _t.mark(f"upload({_new_photo_count})")

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
                        # Persist FIRST, then email. The tech's work is safe the moment
                        # they hit submit instead of riding on whether SMTP answers.
                        st.session_state.jobs[job_index]['reports'].append(report_payload)

                        # Update Status
                        if new_status != job['status']:
                            apply_job_status(st.session_state.jobs[job_index], new_status, _viewer_email)
                            st.session_state.briefing = "Data required to generate briefing."

                        save_state()
                        _t.mark("save")

                        with st.spinner("Sending Daily Report to Admins..."):
                            send_daily_report_email(job, tech, loc, report_payload, timer=_t)
                        _t.finish(job=job_id, photos=len(todays_photos),
                                  total_reports=len(st.session_state.jobs[job_index]['reports']))
                        st.toast("Daily Report Submitted & Emailed to Admins!", icon="✅")
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
    loc_html = (f'<a href="{esc_html(map_url)}" target="_blank" title="{esc_html(loc_name)}" '
                f'style="color:#a1a1aa; text-decoration:none;">📍 {esc_html(loc_name)}</a>'
                if map_url else f'<span title="{esc_html(loc_name)}">📍 {esc_html(loc_name)}</span>')

    # Quote value, if one has been entered (small, right of the site name)
    _qv = format_money(job.get('quoteValue'))
    quote_html = (f'<span style="color:#a1a1aa; font-size:0.8em; white-space:nowrap;">{esc_html(_qv)}</span>'
                  if _qv else "")

    # Compact chips instead of a stack of alert lines. Each feature used to add its
    # own full-width coloured line (stale / follow-up / parts / invoice), so a busy
    # job grew a four-line wall. These sit inline on the tech/date row (see below),
    # so a card with signals is the same height as one without.
    signals = []

    _fu = job_followup(job)
    if _fu:
        _days, _thr, _action = _fu
        signals.append((f'⏳ {_days}d waiting',
                        SEMANTIC["act"] if _days >= _thr * 2 else SEMANTIC["waiting"]))

    # Only when there's no follow-up chip already: a job flagged "Customer on Hold
    # 12 days" is self-evidently quiet, so showing both is noise — and two long
    # chips plus the date won't fit on one row.
    stale_days = get_job_stale_days(job)
    if not _fu and stale_days is not None and stale_days >= STALE_JOB_DAYS:
        signals.append((f'🚨 {stale_days}d quiet', SEMANTIC["act"]))

    staged_parts, total_parts = parts_summary(job)
    if total_parts:
        signals.append((f'🔩 {staged_parts}/{total_parts}',
                        SEMANTIC["done"] if staged_parts == total_parts else "#a1a1aa"))

    _inv = invoice_status(job)   # None unless the job is Completed
    if _inv:
        _short = {"Ready to Invoice": "To invoice", "No Charge": "No charge"}.get(_inv, _inv)
        signals.append((f'{INVOICE_STATUS_ICONS.get(_inv, "")} {_short}',
                        INVOICE_STATUS_COLORS.get(_inv, SEMANTIC["neutral"])))

    # Chips sit under the priority badge, inside vertical space the 2-line title
    # clamp already reserves — so a card with signals is exactly as tall as one
    # without, and the tech name keeps its own full-width row below.
    signal_chips = "".join(
        f'<span style="background:#27272a;color:{c};font-size:0.72em;'
        f'padding:1px 6px;border-radius:4px;white-space:nowrap;">{t}</span>'
        for t, c in signals)
    signal_stack = (f'<span style="display:flex; gap:4px;">{signal_chips}</span>'
                    if signal_chips else "")

    with st.container():
        st.markdown(f"""
        <div class="job-card {priority_class}" style="position:relative; overflow:hidden; border-top: 4px solid {status_bg};">
            <div style="position:absolute; top:0; right:0; padding:2px 8px; background:{status_bg}; color:white; font-size:0.65em; font-weight:bold; border-bottom-left-radius:8px;">
                {esc_html(job['status']).upper()}
            </div>
            <div style="display:flex; justify-content:space-between; align-items:flex-start; gap:6px; margin-top:10px;">
                <span title="{esc_html(job['title'])}" style="font-weight:bold; font-size:1.1em; min-width:0; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; line-height:1.3; height:2.6em;">{esc_html(job['title'])}</span>
                <span style="display:flex; flex-direction:column; align-items:flex-end; gap:4px; flex-shrink:0;"><span style="font-size:0.8em; background:#3f3f46; padding:2px 6px; border-radius:4px;">{esc_html(job['priority'])}</span>{signal_stack}</span>
            </div>
            <div style="display:flex; justify-content:space-between; align-items:baseline; gap:8px; margin-top:5px;"><span style="color:#a1a1aa; font-size:0.9em; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{loc_html}</span>{quote_html}</div>
            <div style="display:flex; justify-content:space-between; align-items:center; gap:8px; flex-wrap:nowrap; margin-top:10px; font-size:0.8em; color:#71717a;">
                 <span title="{esc_html(tech_name)}" style="min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">👤 {esc_html(tech_name)}</span>
                 <span style="white-space:nowrap; flex-shrink:0;">📅 {esc_html(str(job.get('date', ''))[:10])}</span>
            </div>
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

        def _delete_job():
            if job in st.session_state.jobs:
                st.session_state.jobs.remove(job)
                save_state()
                st.rerun()

        if compact:
            # Narrow columns (Tech Board / feeds): dropdown on its own row, then
            # the button row — tolerates 100% zoom without squishing text.
            st.selectbox(
                "Change Status", status_options, index=status_idx, key=widget_key,
                on_change=update_job_status_callback, args=(job['id'], widget_key),
                label_visibility="collapsed")
            if allow_delete:
                b1, b2, b3 = st.columns([3, 1, 1])
                with b1:
                    if st.button("Details", key=f"btn_{job['id']}_{key_suffix}", use_container_width=True):
                        job_details_dialog(job['id'])
                with b2:
                    if st.button(":material/edit:", key=f"edit_{job['id']}_{key_suffix}", help="Edit Job", use_container_width=True):
                        edit_job_dialog(job['id'])
                with b3:
                    if st.button(":material/delete:", key=f"del_{job['id']}_{key_suffix}", help="Delete Job", use_container_width=True):
                        _delete_job()
            else:
                if st.button("Details", key=f"btn_{job['id']}_{key_suffix}", use_container_width=True):
                    job_details_dialog(job['id'])
        else:
            # Wide cards (3-col grid pages): everything in one inline row
            if allow_delete:
                f1, f2, f3, f4 = st.columns([3, 2.2, 0.9, 0.9])
            else:
                f1, f2 = st.columns([3, 2.2])
                f3 = f4 = None

            with f1:
                st.selectbox(
                    "Change Status", status_options, index=status_idx, key=widget_key,
                    on_change=update_job_status_callback, args=(job['id'], widget_key),
                    label_visibility="collapsed")
            with f2:
                if st.button("Details", key=f"btn_{job['id']}_{key_suffix}", use_container_width=True):
                    job_details_dialog(job['id'])
            if f3 is not None:
                with f3:
                    if st.button(":material/edit:", key=f"edit_{job['id']}_{key_suffix}", help="Edit Job", use_container_width=True):
                        edit_job_dialog(job['id'])
                with f4:
                    if st.button(":material/delete:", key=f"del_{job['id']}_{key_suffix}", help="Delete Job", use_container_width=True):
                        _delete_job()


def render_job_grid(jobs, key_suffix="", allow_delete=False, cols=3):
    """Full job cards in a 3-up grid (Streamlit stacks columns on phones, so
    mobile keeps the familiar single-column feed)."""
    if not jobs:
        return
    columns = st.columns(cols)
    for i, job in enumerate(jobs):
        with columns[i % cols]:
            render_job_card(job, key_suffix=key_suffix, allow_delete=allow_delete)


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

    # Center on the middle of the actual jobs, but at a FIXED regional zoom so the
    # default view frames the TX / NM operating area (never zoomed to the world or
    # jammed into a single cluster). Users can still pan/zoom freely from there.
    avg_lat = sum(p[2] for p in points) / len(points)
    avg_lon = sum(p[3] for p in points) / len(points)
    fmap = folium.Map(location=[avg_lat, avg_lon], zoom_start=6, tiles="CartoDB positron")

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

    # returned_objects=[] keeps panning/clicking from triggering heavy app reruns
    st_folium(fmap, use_container_width=True, height=600, returned_objects=[], key="jobs_map")

    if skipped:
        st.caption(f"⚠️ {skipped} job(s) not shown — address missing or could not be geocoded.")


# --- SOPs (reference library: written procedures techs read in the field) -----
# Seed categories only — the picker also offers whatever categories already exist
# plus a free-text box, so this list never has to be edited to add one.
SOP_SEED_CATEGORIES = ["Cameras / NVR", "Access Control", "Alarms", "Cabling",
                       "Safety", "Company"]

def sop_categories():
    """Seed categories plus any in use, alphabetical."""
    used = {(s.get('category') or '').strip() for s in st.session_state.get('sops', [])}
    return sorted({c for c in (set(SOP_SEED_CATEGORIES) | used) if c})

def sop_matches(sop, q):
    """Case-insensitive match across title, category, steps, notes, attachment names."""
    if not q:
        return True
    hay = " ".join([
        sop.get('title', ''), sop.get('category', ''), sop.get('notes', ''),
        " ".join(sop.get('steps') or []),
        " ".join(a.get('name', '') for a in (sop.get('attachments') or [])),
    ]).lower()
    return q.lower() in hay

def _sop_form(sop, key_prefix, on_save, submit_label):
    """Shared add/edit form. `sop` seeds the fields; on_save(dict) persists."""
    cats = sop_categories()
    cur_cat = sop.get('category', '')
    with st.form(key=f"{key_prefix}_form"):
        title = st.text_input("Title", value=sop.get('title', ''),
                              placeholder="e.g. Commissioning a Hikvision NVR")
        c1, c2 = st.columns(2)
        cat_options = cats + ["➕ New category..."]
        cat_idx = cat_options.index(cur_cat) if cur_cat in cat_options else 0
        picked = c1.selectbox("Category", cat_options, index=cat_idx)
        new_cat = c2.text_input("New category name", value="" if picked != "➕ New category..." else cur_cat,
                                placeholder="Only if adding a new one")
        steps_txt = st.text_area(
            "Steps (one per line)", value="\n".join(sop.get('steps') or []), height=220,
            placeholder="Confirm the model matches the quote\nSet a static IP outside the DHCP pool\n...")
        notes = st.text_area("Notes / cautions (optional)", value=sop.get('notes', ''), height=90)
        files = st.file_uploader("Attachments (datasheets, diagrams)", accept_multiple_files=True,
                                 type=['pdf', 'jpg', 'jpeg', 'png'], key=f"{key_prefix}_files")
        if st.form_submit_button(submit_label):
            final_cat = (new_cat.strip() or (picked if picked != "➕ New category..." else "")).strip()
            if not title.strip():
                st.error("Title is required.")
                return
            atts = list(sop.get('attachments') or [])
            for f in (files or []):
                k = save_document_locally(f)
                if k:
                    atts.append({"name": f.name, "key": k})
            on_save({
                "title": title.strip(),
                "category": final_cat,
                "steps": [ln.strip() for ln in steps_txt.splitlines() if ln.strip()],
                "notes": notes.strip(),
                "attachments": atts,
            })

def render_sops_view(is_admin):
    st.subheader("📚 Standard Operating Procedures")
    st.caption("Reference procedures for the field. "
               + ("You can add and edit these." if is_admin
                  else "Read-only — ask an admin to add or change a procedure."))

    sops = st.session_state.get('sops', [])

    if is_admin:
        with st.expander("➕ New Procedure", expanded=not sops):
            def _create(vals):
                vals.update({
                    "id": f"sop{now_local().timestamp()}",
                    "updated_by": st.session_state.user_info.get('email', '') if "user_info" in st.session_state else '',
                    "updated_at": now_local().isoformat(),
                })
                st.session_state.sops.append(vals)
                save_state(invalidate_briefing=False)
                st.toast(f"Added '{vals['title']}'", icon="✅")
                st.rerun()
            _sop_form({}, "new_sop", _create, "Add Procedure")

    if not sops:
        st.info("No procedures yet." + (" Add the first one above." if is_admin else ""))
        return

    f1, f2 = st.columns([3, 2])
    q = f1.text_input("Search", key="sop_q", label_visibility="collapsed",
                      placeholder="🔍 Search procedures, steps, attachments...")
    cat_pick = f2.selectbox("Category", ["All categories"] + sop_categories(),
                            key="sop_cat", label_visibility="collapsed")

    shown = [s for s in sops
             if sop_matches(s, q)
             and (cat_pick == "All categories" or s.get('category') == cat_pick)]
    shown.sort(key=lambda s: ((s.get('category') or '~'), s.get('title', '')))

    st.caption(f"{len(shown)} of {len(sops)} procedure(s)"
               + (f' matching "{q}"' if q else ""))
    if not shown:
        st.info("Nothing matches those filters.")
        return

    for s in shown:
        steps = s.get('steps') or []
        atts = s.get('attachments') or []
        meta = f"{len(steps)} step(s) · {len(atts)} attachment(s)"
        if s.get('updated_at'):
            meta += f" · updated {str(s['updated_at'])[:10]}"
            if s.get('updated_by'):
                meta += f" by {s['updated_by']}"

        label = f"{s.get('title', 'Untitled')}"
        if s.get('category'):
            label += f"  ·  {s['category']}"
        with st.expander(label, expanded=bool(q) and len(shown) <= 3):
            st.caption(meta)
            if steps:
                for i, stp in enumerate(steps, 1):
                    st.markdown(f"**{i}.** {stp}")
            else:
                st.caption("No steps recorded.")

            if s.get('notes'):
                st.info(s['notes'])

            if atts:
                st.write("**Attachments**")
                for i, a in enumerate(atts):
                    url = resolve_image_source(a.get('key'))
                    ac1, ac2 = st.columns([3, 1])
                    ac1.write(f"📎 {a.get('name', 'file')}")
                    if url:
                        ac2.link_button("👁️ View", url, use_container_width=True)
                    ext = str(a.get('name', '')).lower().split('.')[-1]
                    if url and ext in ('jpg', 'jpeg', 'png'):
                        st.image(url, width=320)

            if is_admin:
                st.divider()
                ec1, ec2 = st.columns([1, 1])
                edit_key = f"sop_editing_{s['id']}"
                if ec1.button("✏️ Edit", key=f"sop_edit_btn_{s['id']}", use_container_width=True):
                    st.session_state[edit_key] = not st.session_state.get(edit_key, False)
                    st.rerun()
                if ec2.button("🗑️ Delete", key=f"sop_del_{s['id']}", use_container_width=True):
                    st.session_state.sops = [x for x in st.session_state.sops if x['id'] != s['id']]
                    save_state(invalidate_briefing=False)
                    st.toast(f"Deleted '{s.get('title', '')}'", icon="🗑️")
                    st.rerun()

                if st.session_state.get(edit_key):
                    def _update(vals, _sid=s['id'], _ek=edit_key):
                        for x in st.session_state.sops:
                            if x['id'] == _sid:
                                x.update(vals)
                                x['updated_by'] = st.session_state.user_info.get('email', '') if "user_info" in st.session_state else ''
                                x['updated_at'] = now_local().isoformat()
                                break
                        save_state(invalidate_briefing=False)
                        st.session_state[_ek] = False
                        st.toast("Procedure updated.", icon="✅")
                        st.rerun()
                    _sop_form(s, f"edit_sop_{s['id']}", _update, "Save Changes")


BROWSER_TABLES = ["Jobs", "Reports", "Parts", "Time", "Photos", "Invoices", "Assets", "Sites", "Techs", "SOPs"]

def _bnum(v):
    """Best-effort float (blank/garbage -> 0.0)."""
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0

def _bdate(s):
    """Best-effort date from an ISO-ish string, else None."""
    try:
        return datetime.datetime.fromisoformat(str(s)[:19]).date()
    except Exception:
        return None

def browser_rows(table):
    """Flatten the in-memory state into display rows for the data browser.

    Every row carries hidden _date/_tech/_site/_job_id keys used for filtering and
    row-click, which are stripped before display. Site systems/credentials are
    deliberately NOT exposed here — they hold customer passwords and IPs."""
    rows = []
    jobs = st.session_state.jobs

    if table == "Jobs":
        for j in jobs:
            loc, tech = get_location(j.get('locationId')), get_tech(j.get('techId'))
            staged, total = parts_summary(j)
            hrs = sum(_bnum(r.get('hoursWorked')) for r in (j.get('reports') or []))
            rows.append({
                "Date": str(j.get('date', ''))[:10],
                "Title": j.get('title', ''),
                "Site": loc['name'] if loc else '',
                "Tech": tech['name'] if tech else 'Unassigned',
                "Type": j.get('type', ''),
                "Priority": j.get('priority', ''),
                "Status": j.get('status', ''),
                "Reports": len(j.get('reports') or []),
                "Hours": round(hrs, 2),
                "Parts": f"{staged}/{total}" if total else "",
                "Quote": format_money(j.get('quoteValue')),
                "Invoice Status": invoice_status(j) or "",
                "_date": _bdate(j.get('date')), "_tech": tech['name'] if tech else '',
                "_site": loc['name'] if loc else '', "_job_id": j['id'],
            })

    elif table == "Reports":
        for j in jobs:
            loc = get_location(j.get('locationId'))
            for r in (j.get('reports') or []):
                rt = get_tech(r.get('techId'))
                who = r.get('techsOnSite') or (rt['name'] if rt else '')
                rows.append({
                    "Date": str(r.get('timestamp', ''))[:10],
                    "Job": j.get('title', ''),
                    "Site": loc['name'] if loc else '',
                    "Techs On Site": who,
                    "Arrived": r.get('timeArrived', '') or '',
                    "Departed": r.get('timeDeparted', '') or '',
                    "Hours": _bnum(r.get('hoursWorked')),
                    "Parts Used": r.get('partsUsed', '') or '',
                    "Billable": r.get('billableItems', '') or '',
                    "Warranty": "Yes" if r.get('isWarranty') else "",
                    "Photos": len(r.get('photos') or []),
                    "Author": r.get('authorEmail', '') or (rt['email'] if rt else ''),
                    "Notes": (r.get('content') or '').replace("\n", " "),
                    "_date": _bdate(r.get('timestamp')), "_tech": who,
                    "_site": loc['name'] if loc else '', "_job_id": j['id'],
                })

    elif table == "Parts":
        for j in jobs:
            loc = get_location(j.get('locationId'))
            for p in (j.get('parts') or []):
                rows.append({
                    "Job": j.get('title', ''), "Site": loc['name'] if loc else '',
                    "Part": p.get('name', ''), "Qty": p.get('qty', 1),
                    "Status": p.get('status', ''), "Vendor": p.get('vendor', '') or '',
                    "Cost": p.get('cost', '') or '', "Notes": p.get('notes', '') or '',
                    "Added By": p.get('added_by', '') or '',
                    "Updated": str(p.get('updated_at', ''))[:10],
                    "_date": _bdate(p.get('updated_at')), "_tech": '',
                    "_site": loc['name'] if loc else '', "_job_id": j['id'],
                })

    elif table == "Time":
        for j in jobs:
            loc = get_location(j.get('locationId'))
            for e in (j.get('time_entries') or []):
                ci, co = e.get('clock_in'), e.get('clock_out')
                dur = ''
                try:
                    if ci and co:
                        secs = (datetime.datetime.fromisoformat(co)
                                - datetime.datetime.fromisoformat(ci)).total_seconds()
                        dur = f"{round(secs / 3600, 2)} hrs"
                except Exception:
                    dur = ''
                who = e.get('tech_name') or e.get('userEmail', '') or ''
                rows.append({
                    "Tech": who, "Job": j.get('title', ''),
                    "Site": loc['name'] if loc else '',
                    "Clock In": str(ci or '')[:16].replace('T', ' '),
                    "Clock Out": str(co or '')[:16].replace('T', ' '),
                    "Duration": dur, "Running": "Yes" if (ci and not co) else "",
                    "_date": _bdate(ci), "_tech": who,
                    "_site": loc['name'] if loc else '', "_job_id": j['id'],
                })

    elif table == "Photos":
        for j in jobs:
            loc = get_location(j.get('locationId'))
            jt = get_tech(j.get('techId'))
            for k in (j.get('photos') or []):
                rows.append({
                    "Date": str(j.get('date', ''))[:10], "Job": j.get('title', ''),
                    "Site": loc['name'] if loc else '', "Tech": jt['name'] if jt else '',
                    "Source": "Job", "Key": k,
                    "_date": _bdate(j.get('date')), "_tech": jt['name'] if jt else '',
                    "_site": loc['name'] if loc else '', "_job_id": j['id'],
                })
            for r in (j.get('reports') or []):
                rt = get_tech(r.get('techId'))
                for k in (r.get('photos') or []):
                    rows.append({
                        "Date": str(r.get('timestamp', ''))[:10], "Job": j.get('title', ''),
                        "Site": loc['name'] if loc else '', "Tech": rt['name'] if rt else '',
                        "Source": "Report", "Key": k,
                        "_date": _bdate(r.get('timestamp')), "_tech": rt['name'] if rt else '',
                        "_site": loc['name'] if loc else '', "_job_id": j['id'],
                    })

    elif table == "Invoices":
        for j in jobs:
            if j.get('status') != 'Completed':
                continue
            loc = get_location(j.get('locationId'))
            inv = job_invoice(j)
            hrs = sum(_bnum(r.get('hoursWorked')) for r in (j.get('reports') or []))
            rows.append({
                "Job": j.get('title', ''), "Site": loc['name'] if loc else '',
                "Completed": str(j.get('date', ''))[:10], "Hours": round(hrs, 2),
                "Status": inv['status'], "Invoice #": inv['number'],
                "Amount": inv['amount'], "Invoice Date": inv['date'],
                "Updated By": inv['updated_by'],
                "_date": _bdate(j.get('date')), "_tech": '',
                "_site": loc['name'] if loc else '', "_job_id": j['id'],
            })

    elif table == "Sites":
        for l in st.session_state.locations:
            l_jobs = [j for j in jobs if j.get('locationId') == l['id']]
            last = max((str(j.get('date', ''))[:10] for j in l_jobs), default='')
            rows.append({
                "Name": l.get('name', ''), "Address": l.get('address', ''),
                "Contact": l.get('contact_name', '') or '',
                "Phone": l.get('contact_phone', '') or '',
                "Jobs": len(l_jobs), "Systems": len(l.get('systems') or []),
                "Documents": len(l.get('documents') or []), "Last Visit": last,
                "_date": None, "_tech": '', "_site": l.get('name', ''), "_job_id": None,
            })

    elif table == "Assets":
        for l, a in all_assets():
            months, expiry = asset_warranty_left(a)
            src = next((j for j in jobs if j['id'] == a.get('job_id')), None)
            rows.append({
                "Tag": a.get('tag', ''), "Type": a.get('type', ''),
                "Make / Model": a.get('make_model', '') or '',
                "Serial": a.get('serial', '') or '',
                "Site": l.get('name', ''), "Where": a.get('position', '') or '',
                "Installed": str(a.get('installed_date', ''))[:10],
                "Warranty Left (mo)": "" if months is None else months,
                "Expires": "" if expiry is None else str(expiry),
                "Installed On Job": src.get('title', '') if src else '',
                "Notes": (a.get('notes') or '').replace('\n', " "),
                "_date": _bdate(a.get('installed_date')), "_tech": '',
                "_site": l.get('name', ''), "_job_id": a.get('job_id'),
            })

    elif table == "SOPs":
        for s in st.session_state.get('sops', []):
            rows.append({
                "Title": s.get('title', ''), "Category": s.get('category', ''),
                "Steps": len(s.get('steps') or []),
                "Attachments": len(s.get('attachments') or []),
                "Notes": (s.get('notes') or '').replace("\n", " "),
                "Procedure": " | ".join(s.get('steps') or []),
                "Updated": str(s.get('updated_at', ''))[:10],
                "Updated By": s.get('updated_by', '') or '',
                "_date": _bdate(s.get('updated_at')), "_tech": '',
                "_site": '', "_job_id": None,
            })

    elif table == "Techs":
        for t in st.session_state.techs:
            act = [j for j in jobs
                   if j.get('techId') == t['id'] and j.get('status') != 'Completed']
            rows.append({
                "Name": t.get('name', ''), "Email": t.get('email', ''),
                "Initials": t.get('initials', ''),
                "Skills": ", ".join(t.get('skills') or []),
                "Active Jobs": len(act),
                "_date": None, "_tech": t.get('name', ''), "_site": '', "_job_id": None,
            })

    return rows

def browser_count(table):
    """Row count without building the rows — the picker reruns on every keystroke."""
    jobs = st.session_state.jobs
    if table == "Jobs":     return len(jobs)
    if table == "Reports":  return sum(len(j.get('reports') or []) for j in jobs)
    if table == "Parts":    return sum(len(j.get('parts') or []) for j in jobs)
    if table == "Time":     return sum(len(j.get('time_entries') or []) for j in jobs)
    if table == "Photos":
        return sum(len(j.get('photos') or [])
                   + sum(len(r.get('photos') or []) for r in (j.get('reports') or []))
                   for j in jobs)
    if table == "Invoices": return sum(1 for j in jobs if j.get('status') == 'Completed')
    if table == "Sites":    return len(st.session_state.locations)
    if table == "Techs":    return len(st.session_state.techs)
    if table == "SOPs":     return len(st.session_state.get('sops', []))
    if table == "Assets":   return len(all_assets())
    return 0

def render_data_browser():
    st.subheader("🗂️ Data Browser")
    st.caption("Read-only, flattened views of everything in the database. Site systems "
               "(IPs & passwords) are deliberately excluded.")

    counts = {t: browser_count(t) for t in BROWSER_TABLES}
    _fmt_tbl = lambda t: f"{t} {counts[t]:,}"
    if hasattr(st, "segmented_control"):
        table = st.segmented_control("Table", BROWSER_TABLES, format_func=_fmt_tbl,
                                     default="Jobs", key="browser_table",
                                     label_visibility="collapsed")
    else:
        table = st.radio("Table", BROWSER_TABLES, format_func=_fmt_tbl, horizontal=True,
                         key="browser_table", label_visibility="collapsed")
    if not table:
        table = "Jobs"

    f1, f2, f3, f4 = st.columns([3, 2, 2, 2])
    q = f1.text_input("Search", key="browser_q", label_visibility="collapsed",
                      placeholder="🔍 Search anything (including report notes)...")
    range_label = f2.selectbox("Range", ["All time", "Last 30 days", "Last 90 days",
                                         "Last 12 months"], key="browser_range",
                               label_visibility="collapsed")
    tech_names = ["All techs"] + sorted({t.get('name', '') for t in st.session_state.techs if t.get('name')})
    site_names = ["All sites"] + sorted({l.get('name', '') for l in st.session_state.locations if l.get('name')})
    tech_pick = f3.selectbox("Tech", tech_names, key="browser_tech", label_visibility="collapsed")
    site_pick = f4.selectbox("Site", site_names, key="browser_site", label_visibility="collapsed")

    days = {"Last 30 days": 30, "Last 90 days": 90, "Last 12 months": 365}.get(range_label)
    cutoff = (now_local().date() - datetime.timedelta(days=days)) if days else None

    rows = browser_rows(table)
    filtered = []
    for r in rows:
        if cutoff and r.get('_date') and r['_date'] < cutoff:
            continue
        if tech_pick != "All techs":
            if tech_pick.lower() not in (r.get('_tech') or '').lower():
                continue
        if site_pick != "All sites" and (r.get('_site') or '') != site_pick:
            continue
        if q:
            hay = " ".join(str(v) for k, v in r.items() if not k.startswith('_')).lower()
            if q.lower() not in hay:
                continue
        filtered.append(r)

    if not filtered:
        st.info("Nothing matches those filters.")
        return

    disp = [{k: v for k, v in r.items() if not k.startswith('_')} for r in filtered]

    # Thumbnails only for small result sets — signing thousands of URLs is wasteful
    col_cfg = None
    if table == "Photos":
        if len(disp) <= 50:
            for d in disp:
                d["Preview"] = resolve_image_source(d.get("Key"))
            col_cfg = {"Preview": st.column_config.ImageColumn("Preview", width="small")}
        else:
            st.caption(f"{len(disp):,} photos — narrow the filters below 50 to see thumbnails.")

    df = pd.DataFrame(disp)
    st.caption(f"{len(filtered):,} of {counts[table]:,} {table.lower()} row(s)"
               + (" · click a row to open the job" if filtered[0].get('_job_id') else ""))

    selected_row = None
    try:
        event = st.dataframe(df, use_container_width=True, hide_index=True,
                             column_config=col_cfg, on_select="rerun",
                             selection_mode="single-row")
        picked = list(getattr(getattr(event, "selection", None), "rows", []) or [])
        if picked:
            selected_row = filtered[picked[0]]
    except TypeError:
        # Older Streamlit without dataframe selection support
        st.dataframe(df, use_container_width=True, hide_index=True, column_config=col_cfg)

    st.download_button(
        f"⬇️ Download {table} CSV ({len(filtered):,} rows, current filters)",
        df.to_csv(index=False).encode("utf-8"),
        file_name=f"{table.lower()}_{now_local().strftime('%Y%m%d')}.csv",
        mime="text/csv", key=f"browser_csv_{table}",
    )

    if selected_row and selected_row.get('_job_id'):
        job_details_dialog(selected_row['_job_id'])


def render_invoicing_view(user_email):
    """Office-manager worklist: every Completed job grouped by invoice status, so
    'what still needs billing' is answerable at a glance. Admins only."""
    st.subheader("💵 Invoicing")
    st.caption("Every completed job and where it sits in billing. Jobs land in "
               "'Ready to Invoice' automatically when they're marked Completed "
               "(warranty work goes straight to 'No Charge').")

    completed = [j for j in st.session_state.jobs if j.get('status') == 'Completed']
    if not completed:
        st.info("No completed jobs yet.")
        return

    buckets = {s: [] for s in INVOICE_STATUSES}
    for j in completed:
        buckets.setdefault(invoice_status(j), []).append(j)

    # Status summary chips
    chips = " ".join(
        f'<span style="background:{INVOICE_STATUS_COLORS[s]};color:white;padding:3px 12px;'
        f'border-radius:12px;font-size:0.78em;margin-right:6px;">'
        f'{INVOICE_STATUS_ICONS[s]} {len(buckets.get(s, []))} {s}</span>'
        for s in INVOICE_STATUSES
    )
    st.markdown(chips, unsafe_allow_html=True)
    st.write("")

    view = st.radio("Show", INVOICE_STATUSES + ["All"], horizontal=True,
                    key="invoicing_filter", label_visibility="collapsed")

    rows = completed if view == "All" else buckets.get(view, [])
    # Most recently worked first
    rows = sorted(rows, key=lambda j: (j.get('date') or ''), reverse=True)

    if not rows:
        st.success(f"Nothing sitting in '{view}'. 🎉")
        return

    st.caption(f"{len(rows)} job(s)")
    for j in rows:
        loc = get_location(j['locationId'])
        tech = get_tech(j['techId'])
        cur = invoice_status(j)
        inv = job_invoice(j)

        hrs = 0.0
        for r in (j.get('reports') or []):
            try:
                hrs += float(r.get('hoursWorked') or 0)
            except (TypeError, ValueError):
                pass

        c1, c2, c3 = st.columns([5, 2, 2])
        with c1:
            meta = " · ".join(x for x in [
                loc['name'] if loc else "No site",
                tech['name'] if tech else "Unassigned",
                str(j.get('date', ''))[:10],
                f"{hrs:g} hrs" if hrs else "",
                f"#{inv['number']}" if inv['number'] else "",
                # Billed amount once known, otherwise what we quoted
                (f"billed {format_money(inv['amount'])}" if inv['amount']
                 else (f"quoted {format_money(j.get('quoteValue'))}" if j.get('quoteValue') else "")),
            ] if x)
            st.markdown(
                f"**{esc_html(j['title'])}**<br><span style='color:#71717a;font-size:0.82em;'>{esc_html(meta)}</span>",
                unsafe_allow_html=True)
        with c2:
            st.markdown(
                f'<span style="background:{INVOICE_STATUS_COLORS.get(cur, "#52525b")};color:white;'
                f'padding:2px 10px;border-radius:10px;font-size:0.75em;">'
                f'{INVOICE_STATUS_ICONS.get(cur, "")} {cur}</span>',
                unsafe_allow_html=True)
        with c3:
            # One-tap advance through the pipeline; full editing lives in the job dialog
            nxt = {"Ready to Invoice": "Invoiced", "Invoiced": "Paid"}.get(cur)
            if nxt:
                if st.button(f"Mark {nxt}", key=f"inv_adv_{j['id']}", use_container_width=True):
                    # Stamp the invoice date the first time it's marked Invoiced;
                    # leave it alone (None = unchanged) when advancing to Paid.
                    new_date = None
                    if nxt == "Invoiced" and not inv['date']:
                        new_date = now_local().strftime('%Y-%m-%d')
                    set_job_invoice(j['id'], status=nxt, date=new_date)
                    get_logger().log(f"{user_email} marked job {j['id']} '{nxt}'")
                    st.toast(f"Marked {nxt}", icon="💵")
                    st.rerun()
            if st.button("Open", key=f"inv_open_{j['id']}", use_container_width=True):
                job_details_dialog(j['id'])
        st.divider()


def render_profitability():
    st.subheader("💰 Quote vs Actual")
    st.caption("What each job was worth against the effort it took. Effort is in "
               "MAN-hours — a report's hours counted once per tech on site — so a "
               "three-man day counts as three. No pay information is used or stored "
               "anywhere in this app.")

    scope = sub_nav(["Completed only", "Include in-progress"], "profit_scope")
    include_open = scope == "Include in-progress"
    pool = [j for j in st.session_state.jobs
            if include_open or j.get('status') == 'Completed']

    rows, skipped = [], 0
    for j in pool:
        v = job_value_summary(j)
        if not v:
            skipped += 1
            continue
        loc = get_location(j.get('locationId'))
        done = j.get('status') == 'Completed'
        rows.append({
            "Done": "✓" if done else "⚠ in progress",
            "Job": j.get('title', ''),
            "Site": loc['name'] if loc else '',
            "Type": j.get('type', ''),
            "Source": "billed" if v['billed'] is not None else "quoted",
            "Quoted": v['quoted'], "Billed": v['billed'], "Revenue": v['revenue'],
            "Man-hours": round(v['man_hours'], 2),
            "Parts $": round(v['parts'], 2),
            "$ / man-hour": round(v['rev_per_hour'], 2) if v['rev_per_hour'] is not None else None,
            "Variance": v['variance'],
            "_done": done,
        })

    if not rows:
        st.info("Nothing to report yet — no job has a quote value or an invoice amount"
                + (" and is completed." if not include_open else ".")
                + " Add a Quote Value on a job to bring it into this view.")
        return

    df = pd.DataFrame(rows)
    done_df = df[df["_done"]]

    # Headline figures use COMPLETED jobs only — an unfinished job hasn't logged
    # all its hours, so its $/man-hour looks far better than it will finish.
    if done_df.empty:
        st.warning("No **completed** job has a price on it yet, so there's nothing "
                   "reliable to summarise. The rows below are still in progress — "
                   "their hours haven't all been logged.")
    else:
        rev = done_df["Revenue"].sum()
        mh = done_df["Man-hours"].sum()
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Revenue", f"${rev:,.0f}")
        m2.metric("Man-hours", f"{mh:,.1f}")
        m3.metric("$ / man-hour", f"${rev / mh:,.0f}" if mh else "—",
                  help="Revenue divided by effort. Compare jobs against each other — "
                       "a low figure means the job ate more work than it returned.")
        m4.metric("Completed jobs", len(done_df),
                  help=f"{skipped} job(s) skipped for having no price")
        st.caption("Totals cover completed jobs only.")

    if include_open and (~df["_done"]).any():
        st.warning(f"⚠️ {int((~df['_done']).sum())} row(s) below are still in progress — "
                   "their hours aren't all logged, so their $/man-hour is flattering.")

    st.write("##### Per job — lowest return per man-hour first")
    st.dataframe(df.drop(columns=["_done"]).sort_values("$ / man-hour", na_position="last"),
                 use_container_width=True, hide_index=True)

    def _per_hour(frame):
        """Revenue per man-hour, blank rather than dividing by zero."""
        return [round(r / h, 2) if h else None
                for r, h in zip(frame["Revenue"], frame["Man-hours"])]

    if not done_df.empty:
        g1, g2 = st.columns(2)
        with g1:
            st.write("##### By job type")
            by_type = done_df.groupby("Type", as_index=False).agg(
                Jobs=("Job", "count"), Revenue=("Revenue", "sum"),
                **{"Man-hours": ("Man-hours", "sum")})
            by_type["$ / man-hour"] = _per_hour(by_type)
            st.dataframe(by_type.sort_values("$ / man-hour", na_position="last"),
                         use_container_width=True, hide_index=True)
        with g2:
            st.write("##### Sites returning least per man-hour")
            by_site = done_df.groupby("Site", as_index=False).agg(
                Jobs=("Job", "count"), Revenue=("Revenue", "sum"),
                **{"Man-hours": ("Man-hours", "sum")})
            by_site["$ / man-hour"] = _per_hour(by_site)
            st.dataframe(by_site.sort_values("$ / man-hour", na_position="last").head(10),
                         use_container_width=True, hide_index=True)

        _var = done_df[done_df["Variance"].notna()]
        if not _var.empty:
            st.write("##### Quote accuracy")
            v1, v2, v3 = st.columns(3)
            v1.metric("Billed over quote", int((_var["Variance"] > 0).sum()))
            v2.metric("Billed under quote", int((_var["Variance"] < 0).sum()))
            v3.metric("Average variance", f"${_var['Variance'].mean():,.0f}",
                      help="Positive means you tend to bill more than you quote.")

    st.download_button("⬇️ Download CSV",
                       df.drop(columns=["_done"]).to_csv(index=False).encode("utf-8"),
                       file_name=f"job_value_{now_local().strftime('%Y%m%d')}.csv",
                       mime="text/csv", key="profit_csv")


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
        key="hours_csv",
    )

def _agreement_monthly_value(agr):
    """Monthly-equivalent recurring value of an agreement (0 for one-time/cancelled)."""
    if not agr or agr.get('status') == 'Cancelled':
        return 0.0
    try:
        val = float(agr.get('value') or 0)
    except (ValueError, TypeError):
        return 0.0
    cycle = agr.get('billing')
    if cycle == "Monthly":
        return val
    if cycle == "Quarterly":
        return val / 3
    if cycle == "Annual":
        return val / 12
    return 0.0  # One-time

def render_service_agreements():
    st.caption("Monitoring, service, inspection, and warranty contracts by site — with renewal alerts.")

    agreements = st.session_state.agreements
    loc_by_id = {l['id']: l for l in st.session_state.locations}

    # Summary
    active = [a for a in agreements if a.get('status') != 'Cancelled']
    expiring = [a for a in active if (agreement_days_left(a) is not None and agreement_days_left(a) <= AGREEMENT_RENEWAL_DAYS)]
    monthly_total = sum(_agreement_monthly_value(a) for a in active)
    m1, m2, m3 = st.columns(3)
    m1.metric("Active Contracts", len(active))
    m2.metric(f"Renewing ≤{AGREEMENT_RENEWAL_DAYS}d", len(expiring))
    m3.metric("Recurring / mo", f"${monthly_total:,.0f}")

    # Add agreement
    with st.expander("➕ Add Service Agreement", expanded=not agreements):
        if not st.session_state.locations:
            st.warning("Add a location first.")
        else:
            with st.form("add_agreement_form", clear_on_submit=True):
                loc_names = {l['name']: l['id'] for l in st.session_state.locations}
                a_loc = st.selectbox("Site", list(loc_names.keys()))
                ac1, ac2 = st.columns(2)
                a_type = ac1.selectbox("Type", AGREEMENT_TYPES)
                a_title = ac2.text_input("Title", placeholder="e.g. 24/7 Central Station Monitoring")
                ac3, ac4 = st.columns(2)
                a_start = ac3.date_input("Start Date", value=now_local())
                a_renew = ac4.date_input("Renewal / End Date", value=now_local() + datetime.timedelta(days=365))
                ac5, ac6 = st.columns(2)
                a_value = ac5.number_input("Value ($)", min_value=0.0, step=10.0)
                a_billing = ac6.selectbox("Billing", BILLING_CYCLES)
                a_auto = st.checkbox("Auto-renews")
                a_notes = st.text_input("Notes", placeholder="Contract #, terms, contact...")

                if st.form_submit_button("Save Agreement", use_container_width=True):
                    if not a_title.strip():
                        st.warning("Please enter a title.")
                    else:
                        st.session_state.agreements.append({
                            'id': f"a{now_local().timestamp()}",
                            'locationId': loc_names[a_loc],
                            'type': a_type,
                            'title': a_title.strip(),
                            'start_date': str(a_start),
                            'renewal_date': str(a_renew),
                            'value': a_value,
                            'billing': a_billing,
                            'auto_renew': a_auto,
                            'notes': a_notes.strip(),
                            'status': 'Active',
                        })
                        save_state(invalidate_briefing=False)
                        st.toast(f"Added '{a_title.strip()}'", icon="✅")
                        st.rerun()

    if not agreements:
        st.info("No service agreements recorded yet.")
        return

    # List, soonest renewal first
    def _sort_key(a):
        d = agreement_days_left(a)
        return (d if d is not None else 999999)
    for a in sorted(agreements, key=_sort_key):
        loc = loc_by_id.get(a.get('locationId'))
        days = agreement_days_left(a)
        with st.container(border=True):
            hc1, hc2 = st.columns([3, 1])
            hc1.markdown(f"**{a.get('title', 'Agreement')}** · {a.get('type', '')}")
            hc1.caption(f"📍 {loc['name'] if loc else 'Unknown site'}")

            if a.get('status') == 'Cancelled':
                hc2.markdown(":gray-background[Cancelled]")
            elif days is None:
                hc2.caption("No renewal date")
            elif days < 0:
                hc2.markdown(f":red-background[Expired {abs(days)}d ago]")
            elif days <= AGREEMENT_RENEWAL_DAYS:
                hc2.markdown(f":orange-background[Renews in {days}d]")
            else:
                hc2.markdown(f":green-background[Renews in {days}d]")

            meta = []
            if a.get('value'):
                meta.append(f"💲{float(a['value']):,.0f} {a.get('billing', '')}")
            if a.get('renewal_date'):
                meta.append(f"📅 {a['renewal_date']}")
            if a.get('auto_renew'):
                meta.append("🔁 Auto-renews")
            if meta:
                hc1.caption(" · ".join(meta))
            if a.get('notes'):
                hc1.caption(a['notes'])

            with st.expander("✏️ Edit / Delete"):
                with st.form(f"edit_agr_{a['id']}"):
                    e_title = st.text_input("Title", value=a.get('title', ''))
                    ec1, ec2 = st.columns(2)
                    e_type = ec1.selectbox("Type", AGREEMENT_TYPES,
                                           index=AGREEMENT_TYPES.index(a['type']) if a.get('type') in AGREEMENT_TYPES else 0)
                    e_status = ec2.selectbox("Status", ["Active", "Cancelled"],
                                             index=0 if a.get('status') != 'Cancelled' else 1)
                    ec3, ec4 = st.columns(2)
                    try:
                        _rv = datetime.datetime.strptime(str(a.get('renewal_date'))[:10], "%Y-%m-%d").date()
                    except (ValueError, TypeError):
                        _rv = now_local().date()
                    e_renew = ec3.date_input("Renewal / End Date", value=_rv, key=f"agr_renew_{a['id']}")
                    e_value = ec4.number_input("Value ($)", min_value=0.0, step=10.0, value=float(a.get('value') or 0))
                    ec5, ec6 = st.columns(2)
                    e_billing = ec5.selectbox("Billing", BILLING_CYCLES,
                                              index=BILLING_CYCLES.index(a['billing']) if a.get('billing') in BILLING_CYCLES else 0)
                    e_auto = ec6.checkbox("Auto-renews", value=bool(a.get('auto_renew')))
                    e_notes = st.text_input("Notes", value=a.get('notes', ''))

                    bc1, bc2 = st.columns(2)
                    if bc1.form_submit_button("💾 Update"):
                        a.update({'title': e_title, 'type': e_type, 'status': e_status,
                                  'renewal_date': str(e_renew), 'value': e_value,
                                  'billing': e_billing, 'auto_renew': e_auto, 'notes': e_notes})
                        save_state(invalidate_briefing=False)
                        st.toast("Updated.", icon="✅")
                        st.rerun()
                    if bc2.form_submit_button("🗑️ Delete"):
                        st.session_state.agreements = [x for x in st.session_state.agreements if x['id'] != a['id']]
                        save_state(invalidate_briefing=False)
                        st.toast("Agreement deleted", icon="🗑️")
                        st.rerun()

def _admin_access():
    st.subheader("🔑 Admin Access Management")
    with st.expander("Manage Admin Emails", expanded=True):
        st.write("Add emails that are allowed to access this Admin Panel.")

        with st.form("add_admin_form"):
            new_admin_email = st.text_input("New Admin Email")
            if st.form_submit_button("Add Admin"):
                if new_admin_email and "@" in new_admin_email:
                    if new_admin_email not in st.session_state.adminEmails:
                        st.session_state.adminEmails.append(new_admin_email)
                        save_state(invalidate_briefing=False)
                        st.toast(f"Added {new_admin_email}", icon="✅")
                        st.rerun()
                    else:
                        st.warning("Email already exists.")
                else:
                    st.error("Invalid email.")

        if st.session_state.adminEmails:
            st.write("###### Current Admins")
            for email in st.session_state.adminEmails:
                c1, c2 = st.columns([4, 1])
                c1.write(email)
                if c2.button(":material/delete:", key=f"del_admin_{email}"):
                    st.session_state.adminEmails.remove(email)
                    save_state(invalidate_briefing=False)
                    st.rerun()


def _admin_email():
    st.subheader("📧 SMTP Configuration")
    with st.expander("Configure Email Settings", expanded=True):
        with st.form("smtp_config_form"):
            current_smtp = st.session_state.get('smtp_settings', {})
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
                st.toast("SMTP Settings Saved to Database!", icon="✅")
                st.rerun()

    st.subheader("📧 Daily Summary Email")
    with st.expander("Send a test of the daily ops summary"):
        recip_count = len(daily_summary_recipients(st.session_state.techs, st.session_state.adminEmails))
        st.caption(
            f"The scheduled summary goes to all techs + admins ({recip_count} recipient(s)) at 7 AM, Mon–Fri. "
            "This test sends the very same email to **you only**, so you can preview it without notifying the team."
        )
        current_email = st.session_state.user_info.get("email") if "user_info" in st.session_state else None
        if st.button("📧 Send Me a Test Summary Now", use_container_width=True):
            if not current_email:
                st.error("Could not determine your email address.")
            else:
                with st.spinner("Sending test summary..."):
                    sent, err = send_ops_summary_email([current_email], subject_prefix="[TEST] ")
                if err:
                    st.error(f"Failed to send: {err}")
                elif sent:
                    st.success(f"✅ Test summary sent to {current_email}. Check your inbox.")
                else:
                    st.warning("Nothing was sent.")


def _admin_techs():
    st.subheader("👷 Manage Technicians")
    with st.expander("Add / Remove Technicians", expanded=True):
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

                    _slug = re.sub(r'[^a-z0-9]', '', new_tech_name.lower())[:10] or 'tech'
                    st.session_state.techs.append({
                        "id": new_id,
                        "name": new_tech_name,
                        "email": new_tech_email,
                        "initials": new_tech_initials.upper(),
                        "color": color,
                        "skills": new_tech_skills,
                        "notify_topic": f"5gsec-{_slug}-{os.urandom(4).hex()}"
                    })
                    save_state(invalidate_briefing=False)
                    st.success(f"Added {new_tech_name}")
                else:
                    st.error("All fields required.")

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
                if c4.button(":material/delete:", key=f"del_tech_{t['id']}"):
                    st.session_state.techs.remove(t)
                    save_state(invalidate_briefing=False)
                    st.rerun()

    st.subheader("📳 Push Notifications (ntfy)")
    with st.expander("Phone Push Setup & Testing", expanded=False):
        st.write("Each tech installs the free **ntfy** app (App Store / Google Play) and subscribes "
                 "to their personal topic below. After that, new job assignments buzz their phone instantly.")
        st.caption("Treat topic names like passwords — anyone who knows one can receive (and send) its notifications. "
                   "Notifications only contain the job title, never addresses or credentials.")
        for t in st.session_state.techs:
            topic = get_or_create_notify_topic(t)
            pc1, pc2, pc3 = st.columns([2, 3, 1])
            pc1.write(f"**{t['name']}**")
            pc2.code(topic, language=None)
            if pc3.button("📳 Test", key=f"push_test_{t['id']}", use_container_width=True):
                ok = send_push(topic, "Test Notification",
                               f"Hey {t['name'].split()[0]}! Push notifications from the 5G job board are working.",
                               tags=["tada"])
                if ok:
                    st.toast(f"Test push sent to {t['name']}", icon="📳")
                else:
                    st.error("Push failed — check the network or NTFY_SERVER setting.")


def _admin_locations():
    st.subheader("📍 Manage Locations")
    with st.expander("Add / Remove Locations", expanded=True):
        with st.form("add_loc_form"):
            l_name = st.text_input("Location Name")
            l_addr = st.text_input("Address")
            l_maps = st.text_input("Google Maps Link (Optional)")

            c_l1, c_l2 = st.columns(2)
            l_contact_name = c_l1.text_input("Site Contact Name")
            l_contact_phone = c_l2.text_input("Site Contact Phone")

            if st.form_submit_button("Add Location"):
                if l_name and l_addr:
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
                    st.toast(f"Added {l_name}", icon="✅")
                    st.rerun()
                else:
                    st.error("Name and Address required.")

        if st.session_state.locations:
            st.write("###### Current Locations")
            for l in st.session_state.locations:
                c1, c2, c3, c4 = st.columns([3, 4, 1, 1])
                c1.write(l['name'])
                contact_info = ""
                if l.get('contact_name') or l.get('contact_phone'):
                    contact_info = f" | 📞 {l.get('contact_name','')} {l.get('contact_phone','')}"
                c2.caption(f"{l['address']}{contact_info}")
                if c3.button(":material/edit:", key=f"edit_loc_{l['id']}"):
                    edit_location_dialog(l['id'])
                if c4.button(":material/delete:", key=f"del_loc_{l['id']}"):
                    st.session_state.locations.remove(l)
                    save_state(invalidate_briefing=False)
                    st.rerun()


def _admin_data():
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
            st.session_state.agreements = state.get("agreements", [])
            st.session_state.sops = state.get("sops", [])
            st.session_state.settings = state.get("settings", {})
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
                        st.session_state.agreements = data.get("agreements", [])
                        st.session_state.sops = data.get("sops", [])
                        st.session_state.settings = data.get("settings", {})
                        st.session_state.last_reminder_date = data.get("last_reminder_date")
                        ensure_loaded_into_session()
                        _sync_session_to_db()
                        force_overwrite_from_session(invalidate_briefing=False)
                        st.toast("Data restored successfully (DB overwritten).", icon="✅")
                        st.rerun()
                except Exception as e:
                    st.error(f"Error restoring file: {e}")


def _admin_diagnostics():
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
                        endpoint = s3.meta.endpoint_url
                        region = s3.meta.region_name
                        st.info(f"**Endpoint:** `{endpoint}`")
                        st.info(f"**Bucket:** `{bucket}`")
                        st.info(f"**Region:** `{region}`")
                        if endpoint and bucket in endpoint:
                            st.warning("⚠️ **Potential Configuration Issue:** The Bucket Name appears to be part of the Endpoint URL. R2 Endpoint URLs should usually end with `.r2.cloudflarestorage.com` and NOT include the bucket name.")
                        s3.list_objects_v2(Bucket=bucket, MaxKeys=1)
                        st.success(f"✅ Successfully connected to bucket: `{bucket}`")
                        st.toast("Storage connection verified!", icon="✅")
                except Exception as e:
                    st.error(f"❌ Connection failed: {e}")
                    if "InvalidAccessKeyId" in str(e):
                        st.warning("💡 **Tip:** Double-check your Access Key ID. Ensure no leading/trailing spaces.")
                    elif "SignatureDoesNotMatch" in str(e):
                        st.warning("💡 **Tip:** Double-check your Secret Access Key. Ensure no leading/trailing spaces.")
                    elif "NoSuchBucket" in str(e):
                        st.warning(f"💡 **Tip:** The bucket `{bucket}` does not exist or is not accessible with these credentials.")
                    elif "EndpointConnectionError" in str(e):
                        st.warning("💡 **Tip:** Could not connect to the endpoint URL. Check for typos.")

    st.divider()
    st.subheader("⏱️ Submit Timings")
    st.caption("Where the seconds actually go when a tech hits submit. Each line breaks "
               "one submit into its stages, so you can see what to fix instead of guessing.")
    with st.expander("View timings", expanded=True):
        _timings = [l for l in get_logger().get_logs() if "⏱️" in l]
        if not _timings:
            st.info("No submits recorded yet. File a daily report and it'll show up here.")
        else:
            for _line in _timings:
                st.code(_line, language="text")
            # Rough averages per stage across whatever is still in the log buffer
            import re as _re
            _agg, _totals = {}, []
            for _line in _timings:
                _m = _re.search(r"TOTAL ([\d.]+)s", _line)
                if _m:
                    _totals.append(float(_m.group(1)))
                for _name, _secs in _re.findall(r"([a-z]+)(?:\([^)]*\))? ([\d.]+)s", _line):
                    _agg.setdefault(_name, []).append(float(_secs))
            if _totals:
                st.metric("Average total", f"{sum(_totals)/len(_totals):.2f}s",
                          help=f"across {len(_totals)} recorded submit(s)")
                _rows = [{"Stage": k, "Avg (s)": round(sum(v)/len(v), 2),
                          "Worst (s)": round(max(v), 2), "Samples": len(v)}
                         for k, v in _agg.items() if k != "total"]
                if _rows:
                    _rows.sort(key=lambda r: -r["Avg (s)"])
                    st.dataframe(pd.DataFrame(_rows), use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("📋 System Event Logs")
    with st.expander("View Background Logs", expanded=False):
        logs = get_logger().get_logs()
        if not logs:
            st.info("No system events logged yet.")
        else:
            for log_entry in logs:
                st.code(log_entry, language="text")

    st.divider()
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
                    client = genai.Client(api_key=api_key)
                    st.success("✅ Gemini Client Initialized.")
                    with st.spinner("Fetching available models..."):
                        all_models = list(client.models.list())
                        model_names = [m.name for m in all_models]
                        st.write(f"**Available Models ({len(model_names)}):**")
                        st.json(model_names[:10])
                    with st.spinner("Testing generation..."):
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


def render_admin_panel():
    # --- DEDUPLICATE IDs (Fix for existing corrupted state) ---
    if st.session_state.techs:
        all_ids = [t['id'] for t in st.session_state.techs]
        if len(all_ids) != len(set(all_ids)):
            seen = set()
            for t in st.session_state.techs:
                if t['id'] in seen:
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

    # Tile-based navigation: a grid of cards instead of one long scroll
    tiles = [
        ("invoicing", "💵", "Invoicing", _admin_invoicing),
        ("techs", "👷", "Technicians", _admin_techs),
        ("locations", "📍", "Locations", _admin_locations),
        ("agreements", "📄", "Service Agreements", render_service_agreements),
        # SHELVED Aug 2026 pending a conversation with the boss + office manager.
        # The feature is complete and untouched — uncomment this single line to
        # bring the tile back. render_profitability() is still defined below.
        # ("profit", "💰", "Quote vs Actual", render_profitability),
        ("hours", "🕒", "Hours Report", render_hours_report),
        ("browser", "🗂️", "Data Browser", render_data_browser),
        ("analytics", "📊", "Analytics", render_analytics_dashboard),
        ("access", "🔑", "Access & Admins", _admin_access),
        ("email", "📧", "Email & SMTP", _admin_email),
        ("data", "💾", "Data & Backup", _admin_data),
        ("diagnostics", "🛠️", "Diagnostics & Logs", _admin_diagnostics),
    ]

    if "admin_view" not in st.session_state:
        st.session_state.admin_view = None

    view = st.session_state.admin_view

    if not view:
        st.caption("Choose a section:")
        cols = st.columns(3)
        for i, (key, icon, label, _fn) in enumerate(tiles):
            with cols[i % 3]:
                if st.button(f"{icon}  {label}", key=f"admin_tile_{key}", use_container_width=True):
                    st.session_state.admin_view = key
                    st.rerun()
        return

    sel = next((t for t in tiles if t[0] == view), None)
    bc1, bc2 = st.columns([1, 4])
    if bc1.button("← Menu", key="admin_back", use_container_width=True):
        st.session_state.admin_view = None
        st.rerun()
    if sel:
        bc2.markdown(f"### {sel[1]} {sel[2]}")
    st.divider()
    if sel:
        sel[3]()


# TV rotation: the wall display cycles through these screens
TV_VIEWS = [("board", "Operations Board"), ("schedule", "Schedule"), ("map", "Job Map")]

def _tv_esc(s):
    return str(s if s is not None else "").replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def _tv_tile(j, badge=None):
    jtech = get_tech(j.get('techId'))
    jloc = get_location(j.get('locationId'))
    p_color = PRIORITY_COLORS.get(j.get('priority'), "#52525b")
    badge_html = (f'<div style="font-size:13px;color:#f87171;font-weight:bold;margin-top:3px;">{badge}</div>'
                  if badge else '')
    return (f'<div style="background:#0f0f11;border-left:5px solid {p_color};border-radius:6px;padding:9px 11px;margin-bottom:8px;">'
            f'<div style="font-size:17px;font-weight:bold;color:#fff;">{_tv_esc(j.get("title", "")[:34])}</div>'
            f'<div style="font-size:14px;color:#a1a1aa;margin-top:3px;">📍 {_tv_esc((jloc["name"] if jloc else "—")[:26])}</div>'
            f'<div style="font-size:14px;color:#71717a;">👤 {_tv_esc(jtech["name"] if jtech else "Unassigned")}</div>{badge_html}</div>')

def _tv_columns(columns_data, cap=8):
    """columns_data: list of (label, header_color, jobs, optional badge_fn)."""
    col_html = ""
    for entry in columns_data:
        label, color, s_jobs = entry[0], entry[1], entry[2]
        badge_fn = entry[3] if len(entry) > 3 else None
        tiles = "".join(_tv_tile(j, badge_fn(j) if badge_fn else None) for j in s_jobs[:cap])
        if len(s_jobs) > cap:
            tiles += f'<div style="color:#71717a;font-size:14px;">+{len(s_jobs) - cap} more</div>'
        if not s_jobs:
            tiles = '<div style="color:#52525b;font-size:14px;">—</div>'
        col_html += (f'<div style="flex:1;min-width:0;">'
                     f'<div style="background:{color};color:#fff;font-size:15px;font-weight:bold;padding:8px 10px;border-radius:8px 8px 0 0;text-align:center;letter-spacing:0.5px;">'
                     f'{label} ({len(s_jobs)})</div>'
                     f'<div style="background:#18181b;border:1px solid #27272a;border-top:none;border-radius:0 0 8px 8px;padding:10px;min-height:120px;">{tiles}</div></div>')
    return f'<div style="display:flex;gap:12px;align-items:flex-start;">{col_html}</div>'

@st.fragment(run_every="20s")
def _tv_board():
    """Auto-refreshing, rotating wall display. Each ~20s refresh re-reads the DB
    and advances to the next screen (board -> schedule -> attention)."""
    try:
        refresh_session_from_db()
    except Exception:
        pass

    # Advance rotation on a TIME basis (not per-run) so the interactive map
    # component's postbacks can't skip a screen. ~20s dwell per screen.
    if time.time() - st.session_state.get('tv_last_rotate', 0) >= 18:
        st.session_state.tv_view_idx = (st.session_state.get('tv_view_idx', -1) + 1) % len(TV_VIEWS)
        st.session_state.tv_last_rotate = time.time()
    idx = st.session_state.get('tv_view_idx', 0)
    view_key, view_label = TV_VIEWS[idx]

    jobs = list(st.session_state.jobs)
    active = [j for j in jobs if j.get('status') != 'Completed']
    today_str = now_local().strftime('%Y-%m-%d')
    completed_today = [j for j in jobs if j.get('status') == 'Completed' and any(
        r.get('timestamp', '').startswith(today_str) and 'completion_checklist' in r for r in j.get('reports', []))]
    in_progress = [j for j in active if j.get('status') == 'In Progress']
    crit = [j for j in active if j.get('priority') in ('Critical', 'High')]

    # Header: logo/title + live clock (persistent across all screens)
    logo_uri = get_logo_data_uri()
    brand = (f'<img src="{logo_uri}" style="height:54px;">' if logo_uri
             else '<span style="font-size:38px;font-weight:bold;color:#fff;letter-spacing:2px;">5G SECURITY</span>')
    clock = now_local().strftime('%A, %b %d  ·  %I:%M %p').replace(' 0', ' ')
    st.markdown(
        f'<div style="display:flex;justify-content:space-between;align-items:center;border-bottom:4px solid #b91c1c;padding-bottom:14px;margin-bottom:18px;">'
        f'<div>{brand}<div style="color:#a1a1aa;font-size:18px;margin-top:4px;">{view_label}</div></div>'
        f'<div style="text-align:right;color:#e4e4e7;font-size:26px;font-weight:bold;">{clock}</div>'
        f'</div>', unsafe_allow_html=True)

    # Big stat tiles (persistent across all screens)
    stats = [("ACTIVE JOBS", len(active), "#e4e4e7"), ("CRITICAL / HIGH", len(crit), "#ef4444"),
             ("IN PROGRESS", len(in_progress), "#3b82f6"), ("COMPLETED TODAY", len(completed_today), "#10b981")]
    cards = "".join(
        f'<div style="flex:1;background:#18181b;border:1px solid #27272a;border-radius:12px;padding:18px;text-align:center;">'
        f'<div style="font-size:52px;font-weight:bold;color:{c};line-height:1;">{v}</div>'
        f'<div style="font-size:15px;color:#a1a1aa;margin-top:8px;letter-spacing:1px;">{lbl}</div></div>'
        for lbl, v, c in stats)
    st.markdown(f'<div style="display:flex;gap:14px;margin-bottom:22px;">{cards}</div>', unsafe_allow_html=True)

    # --- Rotating content ---
    if view_key == "board":
        # Every active status needs a column — anything missing here is invisible
        # on the wall display rather than merely out of place.
        board_statuses = ["Not Started", "In Progress", "Parts not ordered",
                          "Waiting on Parts", "Parts Staged", "Customer on Hold"]
        cols = []
        for status in board_statuses:
            if status == "Not Started":
                s_jobs = [j for j in active if j.get('status') in ("Not Started", "Pending")]
            else:
                s_jobs = [j for j in active if j.get('status') == status]
            cols.append((status.upper(), get_status_color(status), s_jobs))
        st.markdown(_tv_columns(cols), unsafe_allow_html=True)

    elif view_key == "schedule":
        today_d = now_local().date()
        def _bucket(j):
            try:
                d = datetime.datetime.fromisoformat(j['date'][:19]).date()
            except (ValueError, TypeError, KeyError):
                return "later"
            if d <= today_d:
                return "today"
            if d == today_d + datetime.timedelta(days=1):
                return "tomorrow"
            if d <= today_d + datetime.timedelta(days=7):
                return "week"
            return "later"
        buckets = {"today": [], "tomorrow": [], "week": [], "later": []}
        for j in active:
            buckets[_bucket(j)].append(j)
        for k in buckets:
            buckets[k].sort(key=lambda j: str(j.get('date', '')))
        st.markdown(_tv_columns([
            ("TODAY / OVERDUE", "#b91c1c", buckets["today"]),
            ("TOMORROW", "#3b82f6", buckets["tomorrow"]),
            ("THIS WEEK", "#52525b", buckets["week"]),
            ("LATER", "#3f3f46", buckets["later"]),
        ]), unsafe_allow_html=True)

    else:  # map — job dots across the TX/NM area
        # Only plot jobs whose location is already geocoded; never geocode or write
        # to the DB from the kiosk (it bypasses auth and runs unattended).
        map_points = []
        for j in active:
            jloc = get_location(j.get('locationId'))
            if not jloc:
                continue
            lat, lon = jloc.get('lat'), jloc.get('lon')
            try:
                lat = float(lat) if lat is not None else None
                lon = float(lon) if lon is not None else None
            except (ValueError, TypeError):
                lat = lon = None
            if lat and lon:
                map_points.append((j, lat, lon))

        if HAS_MAP and map_points:
            avg_lat = sum(p[1] for p in map_points) / len(map_points)
            avg_lon = sum(p[2] for p in map_points) / len(map_points)
            fmap = folium.Map(location=[avg_lat, avg_lon], zoom_start=6, tiles="CartoDB dark_matter")
            coord_seen = {}
            for j, lat, lon in map_points:
                ckey = (round(lat, 5), round(lon, 5))
                n = coord_seen.get(ckey, 0)
                coord_seen[ckey] = n + 1
                if n:
                    lat += 0.0005 * n
                    lon += 0.0005 * n
                folium.CircleMarker(
                    location=[lat, lon], radius=10, color="#000000", weight=1,
                    fill=True, fill_color=get_status_color(j['status']), fill_opacity=0.95,
                    tooltip=j['title'],
                ).add_to(fmap)
            st_folium(fmap, use_container_width=True, height=470, returned_objects=[], key="tv_map")
        elif not HAS_MAP:
            st.markdown('<div style="text-align:center;color:#71717a;font-size:22px;margin-top:40px;">Map unavailable (folium not installed).</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="text-align:center;color:#52525b;font-size:22px;margin-top:40px;">No mapped jobs yet.</div>', unsafe_allow_html=True)

    # Footer: rotation indicator + last-updated
    dots = "".join(
        f'<span style="color:{"#b91c1c" if i == idx else "#3f3f46"};font-size:16px;margin:0 3px;">●</span>'
        for i in range(len(TV_VIEWS)))
    st.markdown(
        f'<div style="display:flex;justify-content:space-between;align-items:center;margin-top:16px;color:#52525b;font-size:13px;">'
        f'<div>{dots}</div>'
        f'<div>Rotating every 20s · Updated {now_local().strftime("%I:%M %p").lstrip("0")}</div>'
        f'</div>', unsafe_allow_html=True)


def render_tv_display(exitable=False):
    """Full-screen, read-only wall/TV board. No sensitive data (no credentials,
    no contract values) — safe for an always-on display."""
    st.markdown("""
        <style>
        [data-testid="stToolbar"], #MainMenu, header, footer,
        [data-testid="stSidebar"], [data-testid="stSidebarCollapsedControl"] { display: none !important; }
        [data-testid="stAppViewContainer"] .block-container { padding: 1.5rem 2rem !important; max-width: 100% !important; }
        .stApp { background-color: #09090b; }
        </style>
    """, unsafe_allow_html=True)
    if exitable:
        if st.button("✕ Exit Display Mode"):
            st.session_state.kiosk_mode = False
            st.rerun()
    _tv_board()


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
        
        # Contextualize Data (remove heavy base64 strings before sending to LLM).
        simple_jobs = []
        for j in list(st.session_state.jobs):
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

def _admin_invoicing():
    """Admin tile wrapper — the tile registry calls its functions with no args."""
    _email = st.session_state.user_info.get('email', '') if "user_info" in st.session_state else ''
    render_invoicing_view(_email)


def sub_nav(options, key, default=None):
    """Selector for views grouped under one tab. Falls back to a radio on older
    Streamlit, and never returns None so callers can compare directly."""
    default = default or options[0]
    if hasattr(st, "segmented_control"):
        picked = st.segmented_control(key, options, default=default, key=key,
                                      label_visibility="collapsed")
    else:
        picked = st.radio(key, options, horizontal=True, key=key,
                          label_visibility="collapsed")
    return picked or default


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

    # 0. KIOSK / TV DISPLAY (headless): a wall display can land directly on a
    # read-only board via ?kiosk=<KIOSK_TOKEN>, bypassing login. Shows only
    # high-level job info — no credentials, contracts, or editing.
    try:
        kiosk_param = st.query_params.get("kiosk")
    except Exception:
        kiosk_param = None
    kiosk_token = (st.secrets.get("KIOSK_TOKEN") if "KIOSK_TOKEN" in st.secrets else None) or os.getenv("KIOSK_TOKEN")
    if kiosk_param and kiosk_token and kiosk_param == kiosk_token:
        render_tv_display(exitable=False)
        return

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
    # Bootstrapping: If no admins exist in DB, first login becomes Admin
    if not st.session_state.adminEmails:
        st.session_state.adminEmails.append(user_email)
        save_state()
        st.toast(f"First login detected. {user_email} is now Super Admin.", icon="🛡️")
    
    is_admin = user_email in st.session_state.adminEmails

    # 2.5 ACCESS CONTROL: only admins, registered techs, or allowed-domain emails
    # get in. Anyone else with a Google account sees a denial screen.
    is_known_tech = any((t.get('email') or '').lower() == (user_email or '').lower()
                        for t in st.session_state.techs)
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

    # Navigation: six tabs. Related views are grouped behind a sub-selector rather
    # than each claiming a top-level tab — twelve competing labels made the app
    # tiring to scan. Nothing was removed, only regrouped.
    tabs_list = ["🌅 Today", "👷 Board", "🧰 Jobs", "📅 Schedule", "📚 SOPs"]
    if is_admin:
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

            # Automatically generate briefing ONLY if it's the default first-time text
            if st.session_state.briefing == "Data required to generate briefing." and st.session_state.jobs:
                with st.spinner("🤖 AI is preparing your initial morning briefing..."):
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

            # Follow-ups: jobs parked waiting on a customer or vendor. Grouped by who
            # needs chasing, since that's how the work actually gets divided up.
            _followups = followup_jobs([j for j in filtered_jobs if j['status'] != 'Completed'])
            if _followups:
                st.markdown(f"##### ⏳ Needs Follow-Up — {len(_followups)} job(s)")
                with st.container(border=True):
                    _by_action = {}
                    for j, d, thr, action in _followups:
                        _by_action.setdefault(action, []).append((j, d, thr))
                    for action, items in _by_action.items():
                        st.markdown(f"**{action}**")
                        for j, d, thr in items:
                            _l = get_location(j.get('locationId'))
                            _t = get_tech(j.get('techId'))
                            _color = "#ef4444" if d >= thr * 2 else "#d97706"
                            st.markdown(
                                f"- <span style='color:{_color};font-weight:bold;'>{d}d</span> "
                                f"**{esc_html(j.get('title', ''))}** — {esc_html(j.get('status'))} · "
                                f"📍 {esc_html(_l['name'] if _l else 'No site')} · "
                                f"👤 {esc_html(_t['name'] if _t else 'Unassigned')}",
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

    # 7. Admin (Only if Admin)
    if is_admin:
        with tab_map["🛡️ Admin"]:
            render_admin_panel()

    # Sidebar Chatbot
    render_chatbot()

if __name__ == "__main__":
    main()
