import streamlit as st
import google.generativeai as genai
import datetime
import base64
import os
import json
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
    }
    .stButton > button:hover {
        background-color: #991b1b;
        color: white;
    }

    /* Headers */
    h1, h2, h3 {
        color: white !important;
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
    .job-card:hover {
        border-color: #3f3f46;
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

# --- TYPES & CONSTANTS (Ported from constants.ts) ---

TECHS = [
  { 'id': 't1', 'name': 'Alex Rivera', 'initials': 'AR', 'color': '#7f1d1d' },
  { 'id': 't2', 'name': 'Sarah Chen', 'initials': 'SC', 'color': '#3f3f46' },
  { 'id': 't3', 'name': 'Mike Johnson', 'initials': 'MJ', 'color': '#b91c1c' },
  { 'id': 't4', 'name': 'Emily Davis', 'initials': 'ED', 'color': '#52525b' },
  { 'id': 't5', 'name': 'David Kim', 'initials': 'DK', 'color': '#991b1b' },
]

LOCATIONS = [
  { 'id': 'l1', 'name': 'HQ Main Office', 'address': '123 Enterprise Blvd, Tech City' },
  { 'id': 'l2', 'name': 'Westside Warehouse', 'address': '4500 Industrial Pkwy, West End' },
  { 'id': 'l3', 'name': 'Downtown Branch', 'address': '88 Market St, Downtown' },
  { 'id': 'l4', 'name': 'North Medical Center', 'address': '500 Wellness Way, North Hills' },
  { 'id': 'l5', 'name': 'Sunnydale Mall', 'address': '1200 Retail Row, Sunnydale' },
]

INITIAL_JOBS = [
  {
    'id': 'j1', 'title': 'HVAC Maintenance Unit 4', 'description': 'Routine maintenance check on rooftop unit. Replace filters.',
    'type': 'Service', 'priority': 'Medium', 'status': 'Pending', 'techId': 't1', 'locationId': 'l2',
    'date': datetime.datetime.now().isoformat(), 'reports': [],
  },
  {
    'id': 'j2', 'title': 'Server Room Cooling Failure', 'description': 'Critical high temp alarm in server room. Immediate dispatch.',
    'type': 'Service', 'priority': 'Critical', 'status': 'In Progress', 'techId': 't2', 'locationId': 'l1',
    'date': datetime.datetime.now().isoformat(),
    'reports': [{
        'id': 'r1', 'techId': 't2', 'timestamp': (datetime.datetime.now() - datetime.timedelta(hours=1)).isoformat(),
        'content': 'Arrived on site. Temperature is 85F. Investigating compressor unit.', 'photos': []
    }],
  },
  {
    'id': 'j3', 'title': 'Lobby Lighting Install', 'description': 'Install new LED fixtures in the main lobby entrance.',
    'type': 'Project', 'priority': 'Low', 'status': 'Pending', 'techId': 't3', 'locationId': 'l3',
    'date': datetime.datetime.now().isoformat(), 'reports': [],
  },
]

# --- SESSION STATE INITIALIZATION ---
if 'jobs' not in st.session_state:
    st.session_state.jobs = INITIAL_JOBS
if 'briefing' not in st.session_state:
    st.session_state.briefing = None
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = [
        {"role": "model", "parts": ["Hello! I have access to all active jobs. Ask me about history, locations, or tech schedules."]}
    ]

# --- HELPER FUNCTIONS ---

def get_tech(tech_id):
    return next((t for t in TECHS if t['id'] == tech_id), None)

def get_location(loc_id):
    return next((l for l in LOCATIONS if l['id'] == loc_id), None)

def image_to_base64(image):
    buffered = BytesIO()
    image.save(buffered, format="JPEG")
    return "data:image/jpeg;base64," + base64.b64encode(buffered.getvalue()).decode()

def generate_morning_briefing():
    """Generates the morning briefing using Gemini."""
    api_key = os.getenv("API_KEY") # Ensure this env var is set
    if not api_key:
        return "⚠️ API Key missing. Please set API_KEY environment variable."
    
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-1.5-flash') # Using stable model mapping

    active_jobs = [j for j in st.session_state.jobs if j['status'] != 'Completed']
    critical_jobs = [j for j in active_jobs if j['priority'] in ['Critical', 'High']]
    
    prompt = f"""
      You are the Operations Manager for a field service company. 
      Generate a concise, professional, and motivating "Morning Briefing" for the TV dashboard.
      
      Here is the current operational data:
      - Total Active Jobs: {len(active_jobs)}
      - Critical/High Priority Issues: {len(critical_jobs)}
      - Techs on Duty: {', '.join([t['name'] for t in TECHS])}
      
      Top Critical Issues:
      {chr(10).join([f"- {j['title']} ({j['priority']})" for j in critical_jobs])}

      Format with: 1. Coach's Corner (Motivation), 2. Critical Focus, 3. Safety Tip.
      Keep it brief (max 150 words). No markdown headers (#), use Bold instead.
    """
    
    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"Error generating briefing: {str(e)}"

# --- MODALS / DIALOGS ---

@st.dialog("Create New Job")
def add_job_modal():
    with st.form("new_job_form"):
        title = st.text_input("Job Title")
        desc = st.text_area("Description")
        
        c1, c2 = st.columns(2)
        job_type = c1.selectbox("Type", ["Service", "Project"])
        priority = c2.selectbox("Priority", ["Low", "Medium", "High", "Critical"])
        
        loc_map = {l['name']: l['id'] for l in LOCATIONS}
        loc_name = st.selectbox("Location", list(loc_map.keys()))
        
        tech_map = {t['name']: t['id'] for t in TECHS}
        tech_map["Unassigned"] = None
        tech_name = st.selectbox("Assign Tech", list(tech_map.keys()))

        submitted = st.form_submit_button("Save Job")
        if submitted:
            new_job = {
                'id': f"j{len(st.session_state.jobs) + 100}",
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
            st.rerun()

@st.dialog("Job Details & Report", width="large")
def job_details_modal(job_id):
    # Find job by ID (references session state directly)
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
        st.caption(f"📍 {loc['name']} | 👤 {tech['name'] if tech else 'Unassigned'}")
    with c2:
        new_status = st.selectbox("Status", ["Pending", "In Progress", "Completed"], 
                                  index=["Pending", "In Progress", "Completed"].index(job['status']),
                                  key=f"status_{job_id}")
        if new_status != job['status']:
            st.session_state.jobs[job_index]['status'] = new_status
            st.rerun()

    tab1, tab2 = st.tabs(["📋 Details & History", "📸 Daily Report"])

    with tab1:
        st.markdown(f"**Description:** {job['description']}")
        st.markdown("---")
        st.write("#### 📜 History")
        if not job['reports']:
            st.info("No reports filed yet.")
        for r in reversed(job['reports']):
            r_tech = get_tech(r['techId'])
            with st.container(border=True):
                st.markdown(f"**{r_tech['name'] if r_tech else 'Unknown'}** - {r['timestamp'][:10]}")
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
                    'techId': job['techId'] or 't1', # Default to t1 if unassigned for demo
                    'timestamp': datetime.datetime.now().isoformat(),
                    'content': content,
                    'photos': photos_list
                }
                st.session_state.jobs[job_index]['reports'].append(new_report)
                st.success("Report Submitted!")
                st.rerun()

# --- MAIN UI COMPONENTS ---

def render_job_card(job, compact=False):
    tech = get_tech(job['techId'])
    loc = get_location(job['locationId'])
    priority_class = f"priority-{job['priority']}"
    
    with st.container():
        st.markdown(f"""
        <div class="job-card {priority_class}">
            <div style="display:flex; justify-content:space-between;">
                <span style="font-weight:bold; font-size:1.1em;">{job['title']}</span>
                <span style="font-size:0.8em; background:#3f3f46; padding:2px 6px; border-radius:4px;">{job['priority']}</span>
            </div>
            <div style="color:#a1a1aa; font-size:0.9em; margin-top:5px;">{loc['name']}</div>
            <div style="display:flex; justify-content:space-between; margin-top:10px; font-size:0.8em; color:#71717a;">
                 <span>👤 {tech['name'] if tech else 'Unassigned'}</span>
                 <span>📅 {job['date'][:10]}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        if st.button("View Details", key=f"btn_{job['id']}", use_container_width=True):
            job_details_modal(job['id'])

def render_chatbot():
    st.sidebar.title("🤖 Tech Assistant")
    st.sidebar.markdown("Ask about jobs, history, or locations.")
    
    # Init chat engine if needed
    if "chat_model" not in st.session_state:
        api_key = os.getenv("API_KEY")
        if api_key:
            genai.configure(api_key=api_key)
            st.session_state.chat_model = genai.GenerativeModel('gemini-1.5-flash')
        else:
            st.sidebar.error("API Key missing")
            return

    # Display History
    for msg in st.session_state.chat_history:
        with st.sidebar.chat_message(msg["role"]):
            st.write(msg["parts"][0])

    # Chat Input
    prompt = st.sidebar.chat_input("How can I help?")
    if prompt:
        # Add user message
        st.session_state.chat_history.append({"role": "user", "parts": [prompt]})
        with st.sidebar.chat_message("user"):
            st.write(prompt)

        # Contextualize Data
        # We simplify the data context to save tokens/complexity for this demo
        simple_jobs = [{k:v for k,v in j.items() if k != 'reports'} for j in st.session_state.jobs]
        
        system_context = f"""
        You are a ServiceCommand Assistant.
        Current Time: {datetime.datetime.now()}
        Techs: {json.dumps(TECHS)}
        Locations: {json.dumps(LOCATIONS)}
        Jobs: {json.dumps(simple_jobs)}
        
        Answer the user's question based strictly on this data.
        """
        
        full_prompt = f"{system_context}\n\nUser Question: {prompt}"

        try:
            # Generate response
            response = st.session_state.chat_model.generate_content(full_prompt)
            bot_reply = response.text
            
            st.session_state.chat_history.append({"role": "model", "parts": [bot_reply]})
            with st.sidebar.chat_message("model"):
                st.write(bot_reply)
        except Exception as e:
            st.sidebar.error("AI Error. Check API Key.")

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
            add_job_modal()

    # Filter Jobs based on search
    filtered_jobs = st.session_state.jobs
    if search:
        filtered_jobs = [j for j in filtered_jobs if search.lower() in j['title'].lower() or search.lower() in j['description'].lower()]

    # Tabs
    tabs = st.tabs(["🌅 Morning Briefing", "👷 Tech Board", "🧰 Service Calls", "🏗️ Projects", "📦 Archive"])

    # 1. Morning Briefing
    with tabs[0]:
        col_main, col_feed = st.columns([2, 1])
        with col_main:
            st.subheader("Daily Operational Briefing")
            if not st.session_state.briefing:
                if st.button("Generate Briefing"):
                    with st.spinner("Analyzing schedule with AI..."):
                        st.session_state.briefing = generate_morning_briefing()
                        st.rerun()
            else:
                st.container(border=True).markdown(st.session_state.briefing)
                if st.button("Refresh"):
                    st.session_state.briefing = None
                    st.rerun()
                
            # Stats
            s1, s2, s3, s4 = st.columns(4)
            active = len([j for j in st.session_state.jobs if j['status'] != 'Completed'])
            crit = len([j for j in st.session_state.jobs if j['priority'] == 'Critical'])
            s1.metric("Active Jobs", active)
            s2.metric("Critical", crit, delta_color="inverse")
            s3.metric("Techs Active", len(TECHS))
            s4.metric("SLA Uptime", "98%")

        with col_feed:
            st.subheader("Priority Feed")
            crit_jobs = [j for j in filtered_jobs if j['priority'] in ['Critical', 'High'] and j['status'] != 'Completed']
            for job in crit_jobs:
                render_job_card(job, compact=True)
            if not crit_jobs:
                st.info("No critical issues.")

    # 2. Tech Board
    with tabs[1]:
        cols = st.columns(len(TECHS) + 1)
        # Tech Columns
        for i, tech in enumerate(TECHS):
            with cols[i]:
                st.markdown(f"**{tech['initials']}** - {tech['name']}")
                tech_jobs = [j for j in filtered_jobs if j['techId'] == tech['id'] and j['status'] != 'Completed']
                for job in tech_jobs:
                    render_job_card(job, compact=True)
        # Unassigned Column
        with cols[-1]:
            st.markdown("**Unassigned**")
            unassigned = [j for j in filtered_jobs if not j['techId'] and j['status'] != 'Completed']
            for job in unassigned:
                render_job_card(job, compact=True)

    # 3. Service Calls
    with tabs[2]:
        service_jobs = [j for j in filtered_jobs if j['type'] == 'Service' and j['status'] != 'Completed']
        grid = st.columns(4)
        for i, job in enumerate(service_jobs):
            with grid[i % 4]:
                render_job_card(job)
        if not service_jobs:
            st.info("No active service calls.")

    # 4. Projects
    with tabs[3]:
        proj_jobs = [j for j in filtered_jobs if j['type'] == 'Project' and j['status'] != 'Completed']
        grid = st.columns(4)
        for i, job in enumerate(proj_jobs):
            with grid[i % 4]:
                render_job_card(job)
        if not proj_jobs:
            st.info("No active projects.")

    # 5. Archive
    with tabs[4]:
        archived = [j for j in filtered_jobs if j['status'] == 'Completed']
        grid = st.columns(4)
        for i, job in enumerate(archived):
            with grid[i % 4]:
                render_job_card(job)
        if not archived:
            st.info("No archived jobs.")

    # Sidebar Chatbot
    render_chatbot()

if __name__ == "__main__":
    main()
