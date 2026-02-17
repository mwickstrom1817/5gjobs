import streamlit as st
import google.generativeai as genai
import datetime
import base64
import os
import json
import smtplib
import urllib.parse
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from PIL import Image
from io import BytesIO

# --- CONFIGURATION & STYLING ---
st.set_page_config(
    page_title="ServiceCommand",
    page_icon="🧰",
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
    .stTextInput > div > div > input, .stTextArea > div > div > textarea, .stSelectbox > div > div > div {
        background-color: #000000;
        color: white;
        border-color: #27272a;
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
        "briefing": "Data required to generate briefing."
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
        "briefing": st.session_state.briefing
    }
    try:
        with open(DB_FILE, "w") as f:
            json.dump(data, f, indent=2)
        # Give user visual feedback that data is safe
        st.toast("💾 Data saved successfully!", icon="✅")
    except IOError as e:
        st.error(f"Failed to save data: {e}")

# --- SESSION STATE INITIALIZATION ---
# Load persistent data immediately on every rerun to sync state
db_data = load_data()
st.session_state.jobs = db_data['jobs']
st.session_state.techs = db_data['techs']
st.session_state.locations = db_data['locations']
st.session_state.briefing = db_data['briefing']

if 'chat_history' not in st.session_state:
    st.session_state.chat_history = [
        {"role": "model", "parts": ["Hello! I have access to your database. Ask me about active jobs, tech locations, or history."]}
    ]

# Tech Colors for UI
TECH_COLORS = ['#7f1d1d', '#3f3f46', '#b91c1c', '#52525b', '#991b1b', '#7c2d12', '#292524']

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

def send_assignment_email(job, tech, location):
    """Sends an email notification via SMTP, returning True if successful."""
    # Attempt to get SMTP credentials from secrets or env
    smtp_server = os.getenv("SMTP_SERVER") or (st.secrets["SMTP_SERVER"] if "SMTP_SERVER" in st.secrets else None)
    smtp_port = os.getenv("SMTP_PORT") or (st.secrets["SMTP_PORT"] if "SMTP_PORT" in st.secrets else 587)
    sender_email = os.getenv("SMTP_EMAIL") or (st.secrets["SMTP_EMAIL"] if "SMTP_EMAIL" in st.secrets else None)
    sender_password = os.getenv("SMTP_PASSWORD") or (st.secrets["SMTP_PASSWORD"] if "SMTP_PASSWORD" in st.secrets else None)

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

    Please check the ServiceCommand dashboard for full details.
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
      You are the Operations Manager. Generate a concise "Morning Briefing" for the dashboard.
      
      Data:
      - Active Jobs: {len(active_jobs)}
      - Critical: {len(critical_jobs)}
      - Techs: {', '.join([t['name'] for t in st.session_state.techs])}
      
      Critical Issues:
      {chr(10).join([f"- {j['title']} ({j['priority']})" for j in critical_jobs])}

      Format: 1. Coach's Corner (Motivation), 2. Critical Focus, 3. Safety Tip.
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
        new_status = st.selectbox("Status", ["Pending", "In Progress", "Completed"], 
                                  index=["Pending", "In Progress", "Completed"].index(job['status']),
                                  key=f"status_{job_id}")
        if new_status != job['status']:
            st.session_state.jobs[job_index]['status'] = new_status
            # Invalidate briefing on status change (e.g. active count changes)
            st.session_state.briefing = "Data required to generate briefing."
            save_state() # Save changes
            st.rerun()

    tab1, tab2 = st.tabs(["📋 Details & History", "📸 Daily Report"])

    with tab1:
        st.markdown(f"**Description:** {job['description']}")
        st.divider()
        st.write("#### 📜 History")
        if not job['reports']:
            st.info("No reports filed yet.")
        for r in reversed(job['reports']):
            r_tech = get_tech(r['techId'])
            with st.container(border=True):
                st.markdown(f"**{r_tech['name'] if r_tech else 'Unknown'}** - {r['timestamp'][:16]}")
                st.write(r['content'])
                if r['photos']:
                    cols = st.columns(4)
                    for i, photo_b64 in enumerate(r['photos']):
                        with cols[i % 4]:
                            st.image(photo_b64, use_container_width=True)

    with tab2:
        with st.form(key=f"report_form_{job_id}"):
            content = st.text_area("Report Content", placeholder="Describe progress, issues...")
            
            st.write("#### Attach Photos")
            col_cam, col_upload = st.columns(2)
            
            with col_cam:
                cam_pic = st.camera_input("Take Photo")
            with col_upload:
                upl_pics = st.file_uploader("Upload Images", accept_multiple_files=True, type=['png', 'jpg'])

            submitted = st.form_submit_button("Submit Report")
            
            if submitted and content:
                photos_list = []
                
                # Process Camera
                if cam_pic:
                    img = Image.open(cam_pic)
                    photos_list.append(image_to_base64(img))
                
                # Process Uploads
                if upl_pics:
                    for up_file in upl_pics:
                        img = Image.open(up_file)
                        photos_list.append(image_to_base64(img))

                new_report = {
                    'id': f"r{datetime.datetime.now().timestamp()}",
                    'techId': job['techId'] or 'unknown',
                    'timestamp': datetime.datetime.now().isoformat(),
                    'content': content,
                    'photos': photos_list
                }
                st.session_state.jobs[job_index]['reports'].append(new_report)
                save_state() # Save changes
                st.success("Report Submitted!")
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
        You are a ServiceCommand Assistant.
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
    # Top Bar
    c1, c2, c3 = st.columns([4, 4, 2])
    with c1:
        st.title("ServiceCommand")
    with c2:
        search = st.text_input("Search Jobs...", label_visibility="collapsed", placeholder="🔍 Search jobs...")
    with c3:
        if st.button("➕ New Job", use_container_width=True):
            add_job_dialog()

    # Filter Jobs based on search
    filtered_jobs = st.session_state.jobs
    if search:
        filtered_jobs = [j for j in filtered_jobs if search.lower() in j['title'].lower() or search.lower() in j['description'].lower()]

    # Navigation Tabs
    tabs = st.tabs(["🌅 Morning Briefing", "👷 Tech Board", "🧰 Service Calls", "🏗️ Projects", "📦 Archive", "🛡️ Admin"])

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

    # 6. Admin
    with tabs[5]:
        render_admin_panel()

    # Sidebar Chatbot
    render_chatbot()

if __name__ == "__main__":
    main()
