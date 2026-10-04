import os
import re
import json
import base64
import io
import datetime
import threading
import time
import urllib.parse
import html as _html

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

import streamlit as st
import pandas as pd
import requests
from PIL import Image, ImageOps

from persistence_pg import (
    ensure_loaded_into_session,
    commit_from_session,
    force_overwrite_from_session,
    load_state,
    get_db_version,
    StaleStateError,
)
from object_store import upload_streamlit_file, upload_bytes, get_view_url

# ReportLab is imported lazily inside build_asset_labels_pdf - it costs real
# startup time and most runs never print labels.


def esc_html(v):
    """Escape a value for embedding in the raw HTML blocks we build for cards,
    tiles and the calendar. Job titles, site names and the quote field are all
    free text typed by users — an unescaped '&' or '<' breaks the markup, and
    worse can inject script into every other user's view."""
    return _html.escape(str(v if v is not None else ""), quote=True)


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
    # Never persist the SMTP password to the database. Server/port/email can
    # be saved, but the password must live in env/secrets only.
    smtp = dict(st.session_state.get("smtp_settings", {}))
    smtp.pop("SMTP_PASSWORD", None)
    st.session_state.db["smtp_settings"] = smtp
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
    if st.session_state.get('_db_load_error'):
        st.error(
            "⚠️ Cannot save: the app failed to load data from the database when it started. "
            "Please refresh the page. If the problem persists, check your DATABASE_URL/NEON_DB_URL secret."
        )
        return
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

# Assets whose warranty ends within this window (or already has) show up on the
# Warranty Radar and in the monthly warranty email.
ASSET_WARRANTY_ALERT_DAYS = 90

def expiring_assets(locations, within_days=ASSET_WARRANTY_ALERT_DAYS):
    """(location, asset, expiry_date, days_left) for every registered asset whose
    warranty expires within `within_days` - including already-expired ones
    (negative days_left). Soonest expiry first. Assets with no determinable
    warranty are skipped."""
    out = []
    today = now_local().date()
    for loc in (locations or []):
        for a in (loc.get('assets') or []):
            _months, expiry = asset_warranty_left(a)
            if not expiry:
                continue
            days_left = (expiry - today).days
            if days_left <= within_days:
                out.append((loc, a, expiry, days_left))
    return sorted(out, key=lambda x: x[3])

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
    try:
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import letter
        from reportlab.lib import colors
        from reportlab.graphics.barcode import qr as _rl_qr
        from reportlab.graphics.shapes import Drawing as _RLDrawing
        from reportlab.graphics import renderPDF as _rl_renderPDF
        has_qr = True
    except ImportError:
        return None
    if not pairs:
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
        if has_qr:
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
    Was duplicated verbatim inside four different email functions.

    The SMTP password is never read from saved settings; it must come from
    secrets or environment so it is not persisted to the database."""
    if key != "SMTP_PASSWORD":
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


def validate_state_dict(data: dict) -> tuple:
    """Validate a backup/state dict before it replaces production data.

    Returns (ok: bool, message: str). Checks required keys, basic structure,
    duplicate IDs, and referential integrity between jobs, techs, and locations.
    """
    required = ["jobs", "techs", "locations"]
    for k in required:
        if k not in data:
            return False, f"Missing required key: {k}"
        if not isinstance(data[k], list):
            return False, f"'{k}' must be a list"

    tech_ids = {t.get("id") for t in data["techs"] if t.get("id")}
    loc_ids = {l.get("id") for l in data["locations"] if l.get("id")}

    seen = {"jobs": set(), "techs": set(), "locations": set()}
    for entity_type in ("jobs", "techs", "locations"):
        for item in data[entity_type]:
            if not isinstance(item, dict):
                return False, f"Invalid item in {entity_type}: {item!r}"
            eid = item.get("id")
            if not eid:
                return False, f"Missing 'id' in {entity_type} item"
            if eid in seen[entity_type]:
                return False, f"Duplicate {entity_type} id: {eid}"
            seen[entity_type].add(eid)

    for job in data["jobs"]:
        tech_id = job.get("techId")
        loc_id = job.get("locationId")
        if tech_id and tech_id not in tech_ids:
            return False, f"Job '{job.get('id')}' references unknown tech '{tech_id}'"
        if loc_id and loc_id not in loc_ids:
            return False, f"Job '{job.get('id')}' references unknown location '{loc_id}'"

    return True, "State dict is valid"
