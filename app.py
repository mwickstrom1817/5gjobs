import streamlit as st
import google.generativeai as genai
import datetime
import base64
import os
import json
import smtplib
import urllib.parse
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from PIL import Image
from io import BytesIO

# Try importing ReportLab for PDF generation
try:
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    from reportlab.lib import colors
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False

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

# --- PERSISTENCE LAYER ---
# Use absolute path to ensure we know exactly where the file is
DB_FILE = os.path.join(os.getcwd(), "service_data.json")

def load_data():
    """Loads data from the local JSON file."""
    default_data = {
        "jobs": [],
        "techs": [],
        "locations": [],
        "briefing": "Data required to generate briefing.",
        "adminEmails": []
    }
    
    if not os.path.exists(DB_FILE):
        return default_data
        
    try:
        with open(DB_FILE, "r") as f:
            data = json.load(f)
            # Ensure all keys exist
            for k, v in default_data.items():
                if k not in data:
                    data[k] = v
            return data
    except (json.JSONDecodeError, IOError):
        return default_data

def save_state():
    """Saves the current session state (relevant parts) to JSON."""
    data = {
        "jobs": st.session_state.jobs,
        "techs": st.session_state.techs,
        "locations": st.session_state.locations,
        "briefing": st.session_state.briefing,
        "adminEmails": st.session_state.adminEmails
    }
    try:
        with open(DB_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except IOError as e:
        st.error(f"Failed to save data: {e}")

# --- SESSION STATE INITIALIZATION ---
# Load persistent data immediately on every rerun to sync state
db_data = load_data()
st.session_state.jobs = db_data['jobs']
st.session_state.techs = db_data['techs']
st.session_state.locations = db_data['locations']
st.session_state.briefing = db_data['briefing']
st.session_state.adminEmails = db_data['adminEmails']

if 'chat_history' not in st.session_state:
    st.session_state.chat_history = [
        {"role": "model", "parts": ["Hello! I have access to your database. Ask me about active jobs, tech locations, or history."]}
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
        try:
            # Exchange code for token
            token_url = "https://oauth2.googleapis.com/token"
            data = {
                "code": code,
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code"
            }
            r = requests.post(token_url, data=data)
            r.raise_for_status()
            tokens = r.json()
            access_token = tokens["access_token"]
            
            # Get User Info
            user_r = requests.get("https://www.googleapis.com/oauth2/v1/userinfo", 
                                  headers={"Authorization": f"Bearer {access_token}"})
            user_r.raise_for_status()
            user_info = user_r.json()
            
            # Set Session
            st.session_state.user_info = user_info
            
            # Clear Query Params to clean URL
            try:
                st.query_params.clear()
            except:
                st.experimental_set_query_params()
                
            st.rerun()
            
        except Exception as e:
            st.error(f"Authentication Failed: {e}")
            # Optional: Print response text for debugging 403s during token exchange
            if 'r' in locals() and r:
                print(f"Token Exchange Error: {r.text}")
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

def get_tech(tech_id):
    return next((t for t in st.session_state.techs if t['id'] == tech_id), None)

def get_location(loc_id):
    return next((l for l in st.session_state.locations if l['id'] == loc_id), None)

def image_to_base64(image):
    buffered = BytesIO()
    image.save(buffered, format="JPEG")
    return "data:image/jpeg;base64," + base64.b64encode(buffered.getvalue()).decode()

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
    p.line(50, y-5, width-50, y-5)
    
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
    p.line(50, y-5, width-50, y-5)
    
    y -= 25
    p.setFont("Helvetica", 11)
    
    data_points = [
        f"Techs On Site: {report.get('techsOnSite', 'N/A')}",
        f"Time Arrived: {report.get('timeArrived', 'N/A')}",
        f"Time Finished: {report.get('timeDeparted', 'N/A')}",
        f"Hours Worked: {report.get('hoursWorked', 'N/A')}",
        f"Parts Used: {report.get('partsUsed', 'N/A')}",
        f"Billable Items: {report.get('billableItems', 'N/A')}"
    ]
    
    for point in data_points:
        p.drawString(50, y, point)
        y -= 20
        
    # Content/Notes
    y -= 20
    p.setFont("Helvetica-Bold", 12)
    p.drawString(50, y, "TECHNICIAN NOTES")
    p.line(50, y-5, width-50, y-5)
    y -= 25
    p.setFont("Helvetica", 10)
    
    notes = report.get('content', '')
    # Basic wrapping
    text_object = p.beginText(50, y)
    text_object.setFont("Helvetica", 10)
    
    max_width = 80  # approx characters
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
        server = smtplib.SMTP(smtp_server, int(smtp_port))
        server.starttls()
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
        server = smtplib.SMTP(smtp_server, int(smtp_port))
        server.starttls()
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
    
    prompt = f"""
      You are the Operations Manager for 5G Security. Generate a concise "Morning Briefing" for the dashboard.
      
      Data:
      - Active Jobs: {len(active_jobs)}
      - Critical: {len(critical_jobs)}
      - Techs: {', '.join([t['name'] for t in st.session_state.techs])}
      
      Critical Issues:
      {chr(10).join([f"- {j['title']} ({j['priority']})" for j in critical_jobs])}

      Format: 1. Security Focus (Motivation), 2. Critical Focus, 3. Safety Tip.
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
        
        # Location Selection
        loc_map = {l['name']: l['id'] for l in st.session_state.locations}
        loc_name = st.selectbox("Location", list(loc_map.keys()))
        
        # Tech Selection
        tech_map = {t['name']: t['id'] for t in st.session_state.techs}
        tech_map["Unassigned"] = None
        tech_name = st.selectbox("Assign Tech", list(tech_map.keys()))

        submitted = st.form_submit_button("Save Job")
        if submitted and title:
            new_job = {
                'id': f"j{len(st.session_state.jobs) + 100}_{datetime.datetime.now().timestamp()}",
                'title': title,
                'description': desc,
                'type': job_type,
                'priority': priority,
                'status': 'Pending',
                'locationId': loc_map[loc_name],
                'techId': tech_map[tech_name],
                'date': datetime.datetime.now().isoformat(),
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

@st.dialog("Job Details & Report", width="large")
def job_details_dialog(job_id):
    # Find job directly from session state
    job_index = next((i for i, j in enumerate(st.session_state.jobs) if j['id'] == job_id), -1)
    if job_index == -1:
        st.error("Job not found")
        return
    
    job = st.session_state.jobs[job_index]
    loc = get_location(job['locationId'])
    tech = get_tech(job['techId'])

    # Header
    c1, c2 = st.columns([3, 1])
    with c1:
        st.subheader(f"{job['title']}")
        st.caption(f"📍 {loc['name'] if loc else 'Unknown'} | 👤 {tech['name'] if tech else 'Unassigned'}")
        
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

    tab_history, tab_progress, tab_daily = st.tabs(["📋 Details & History", "📸 In-Progress", "📝 Daily Report"])

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
                    for i, photo_b64 in enumerate(r['photos']):
                        with cols[i % 4]:
                            st.image(photo_b64, use_container_width=True)

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
                    img = Image.open(cam_pic)
                    photos_list.append(image_to_base64(img))
                if upl_pics:
                    for up_file in upl_pics:
                        img = Image.open(up_file)
                        photos_list.append(image_to_base64(img))
                
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
            
            # Note about photos
            st.info("ℹ️ Photos should be uploaded in the 'In-Progress' tab. They will be linked to the job history.")

            if st.form_submit_button("Submit Daily Report"):
                # Construct Report Data (No Photos)
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
                    'photos': [] # Photos handled in other tab
                }

                st.session_state.jobs[job_index]['reports'].append(report_payload)
                changes_made = True

                # Update Status
                if new_status != job['status']:
                    st.session_state.jobs[job_index]['status'] = new_status
                    st.session_state.briefing = "Data required to generate briefing."
                    
                    if new_status == "Completed":
                        send_completion_email(job, tech, loc, report_payload)
                
                save_state()
                st.success("Daily Report Submitted!")
                st.rerun()

# --- UI COMPONENTS ---

def render_job_card(job, compact=False, key_suffix=""):
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
        if st.button("View Details", key=f"btn_{job['id']}_{key_suffix}", use_container_width=True):
            job_details_dialog(job['id'])

def render_admin_panel():
    st.subheader("Database Management")
    st.info(f"📁 **Data File Location:** `{DB_FILE}`")
    
    c_db1, c_db2 = st.columns(2)
    with c_db1:
        if st.button("🔄 Reload Data from Disk"):
            st.rerun()
    with c_db2:
        if st.button("💾 Force Save State"):
            save_state()

    st.divider()

    # --- ADMIN ACCESS MANAGEMENT ---
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

    # --- EMAIL CONFIGURATION SECTION ---
    st.subheader("📧 Email Configuration")
    with st.expander("Configure SMTP Settings", expanded=False):
        st.info("Settings entered here apply to the current session only. For permanent setup, add to `.streamlit/secrets.toml`.")
        
        # Helper to get current value for display
        session_config = st.session_state.get('smtp_settings', {})
        def get_val(key, default=""):
            if session_config.get(key): return session_config.get(key)
            if key in st.secrets: return st.secrets[key]
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
                    "SMTP_PASSWORD": new_pass
                }
                st.success("Email settings updated for this session!")

    st.divider()

    c1, c2 = st.columns(2)
    
    # Tech Management
    with c1:
        st.subheader("Manage Technicians")
        with st.form("add_tech"):
            t_name = st.text_input("Name")
            t_email = st.text_input("Email")
            if st.form_submit_button("Add Technician"):
                if t_name and t_email:
                    initials = "".join([n[0] for n in t_name.split()]).upper()[:2]
                    color = TECH_COLORS[len(st.session_state.techs) % len(TECH_COLORS)]
                    st.session_state.techs.append({
                        'id': f"t{datetime.datetime.now().timestamp()}",
                        'name': t_name, 'email': t_email, 'initials': initials, 'color': color
                    })
                    save_state() # Save changes
                    st.rerun()
        
        st.markdown("---")
        for t in st.session_state.techs:
            st.markdown(f"**{t['name']}** ({t['email']})")
            if st.button("Remove", key=f"rm_t_{t['id']}"):
                st.session_state.techs.remove(t)
                save_state() # Save changes
                st.rerun()

    # Location Management
    with c2:
        st.subheader("Manage Locations")
        with st.form("add_loc"):
            l_name = st.text_input("Location Name")
            l_addr = st.text_input("Address")
            if st.form_submit_button("Add Location"):
                if l_name:
                    st.session_state.locations.append({
                        'id': f"l{datetime.datetime.now().timestamp()}",
                        'name': l_name, 'address': l_addr
                    })
                    save_state() # Save changes
                    st.rerun()
        
        st.markdown("---")
        for l in st.session_state.locations:
            st.markdown(f"**{l['name']}** - {l['address']}")
            if st.button("Remove", key=f"rm_l_{l['id']}"):
                st.session_state.locations.remove(l)
                save_state() # Save changes
                st.rerun()

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
    tabs_list = ["🌅 Morning Briefing", "👷 Tech Board", "🧰 Service Calls", "🏗️ Projects", "📦 Archive"]
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
                    save_state() # Save new briefing
                    st.rerun()

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
            for job in crit_jobs:
                render_job_card(job, compact=True, key_suffix="feed")

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
                        render_job_card(job, compact=True, key_suffix=f"tech_{tech['id']}")
            # Unassigned
            with cols[-1]:
                st.markdown("**Unassigned**")
                unassigned = [j for j in filtered_jobs if not j['techId'] and j['status'] != 'Completed']
                for job in unassigned:
                    render_job_card(job, compact=True, key_suffix="unassigned")

    # 3. Service Calls
    with tabs[2]:
        service_jobs = [j for j in filtered_jobs if j['type'] == 'Service' and j['status'] != 'Completed']
        if not service_jobs: st.info("No active service calls.")
        for job in service_jobs:
            render_job_card(job, key_suffix="service")

    # 4. Projects
    with tabs[3]:
        proj_jobs = [j for j in filtered_jobs if j['type'] == 'Project' and j['status'] != 'Completed']
        if not proj_jobs: st.info("No active projects.")
        for job in proj_jobs:
            render_job_card(job, key_suffix="project")

    # 5. Archive
    with tabs[4]:
        archived = [j for j in filtered_jobs if j['status'] == 'Completed']
        if not archived: st.info("No archived jobs.")
        for job in archived:
            render_job_card(job, key_suffix="archive")

    # 6. Admin (Only if Admin)
    if is_admin:
        with tabs[5]:
            render_admin_panel()

    # Sidebar Chatbot
    render_chatbot()

if __name__ == "__main__":
    main()
