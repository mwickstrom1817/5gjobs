import os
import io
import datetime
import re

import streamlit as st
from PIL import Image

from core import (
    now_local, save_state, get_tech, get_location, apply_job_status,
    get_logger, StepTimer, location_has_system_info, job_is_warranty,
    parts_summary, PART_STATUSES, PART_STATUS_COLORS,
    update_part_status_callback, job_invoice, set_job_invoice,
    INVOICE_STATUSES, INVOICE_STATUS_COLORS, INVOICE_STATUS_ICONS,
    format_money, SEMANTIC, ASSET_TYPES, SYSTEM_PRESETS,
    next_asset_tag, asset_warranty_left, find_asset, build_asset_labels_pdf,
    save_image_locally, save_document_locally, resolve_image_source,
    last_daily_report, _parse_report_time,
    get_google_maps_url, create_ics_file, HAS_REPORTLAB,
)
from object_store import upload_bytes, get_view_url, upload_streamlit_file
from services_ai import generate_technician_summary, transcribe_audio
from services_geo import get_lat_lon_from_address, get_weather
from services_pdf import generate_job_pdf
from services_email import (
    create_mailto_link, send_assignment_email, send_completion_email,
    send_daily_report_email,
)
from services_push import push_assignment
from ui_widgets import time_select

try:
    from streamlit_drawable_canvas import st_canvas
    HAS_CANVAS = True
except ImportError:
    HAS_CANVAS = False


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
    signed_name = ""
    pad_ok = False

    if HAS_CANVAS:
        try:
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
            # Reading .image_data raises a RuntimeError when streamlit-drawable-canvas
            # and Streamlit versions disagree (both are unpinned, and the canvas
            # library lags Streamlit releases). A drawing pad must never be the
            # reason a tech can't close out a job — fall back to a typed name.
            signature_data = canvas_result.image_data
            pad_ok = True
        except Exception as e:
            signature_data = None
            pad_ok = False
            try:
                get_logger().log(f"Signature pad unavailable, using typed fallback: {e}")
            except Exception:
                pass

    if not pad_ok:
        st.info("✍️ The drawing pad isn't available right now — type the customer's "
                "name below to sign off instead.")
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
        if signature_data is not None:
            try:
                if signature_data.sum() > 0:
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
        elif signed_name.strip():
            checklist.append(f"Customer Signed: {signed_name.strip()}")

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
            user_email = st.session_state.user_info.get("email", "Unknown") if "user_info" in st.session_state else "Unknown"
            st.session_state.jobs[job_index]['reports'][report_index].update({
                'content': content,
                'techsOnSite': ", ".join(techs_on_site_list),
                'timeArrived': str(time_arrived),
                'timeDeparted': str(time_departed),
                'hoursWorked': str(hours_worked),
                'partsUsed': parts_used,
                'billableItems': billable_items,
                # Audit trail: an edited report shows who changed it and when,
                # right in the history view.
                'updated_by': user_email,
                'updated_at': now_local().isoformat()
            })

            # Log the action
            get_logger().log(f"{user_email} updated daily report {report_id} for job {job_id}")
            
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
        "progress": "📝 Updates", "daily": "📝 Daily Report",
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

                    # Techs may fix their own entries (same rule as delete/move);
                    # completion reports stay admin-only since they carry the
                    # customer sign-off.
                    if can_manage and not is_completion:
                        if st.button("✏️ Edit Report", key=f"edit_rep_{r['id']}"):
                            st.session_state[f"editing_report_{job_id}"] = r['id']
                            st.rerun(scope="fragment")

                    if r.get('updated_at'):
                        st.caption(f"✏️ Edited {str(r['updated_at'])[:16].replace('T', ' ')} by {r.get('updated_by', 'unknown')}")
                
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
        st.write("#### 📝 Post an Update")
        st.caption("Add photos and notes while working. These save to history immediately, "
                   "and today's photos are attached to your daily report automatically.")

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
        # Review step: every submission passes through here before it counts.
        confirm_key = f"confirm_daily_send_{job['id']}"
        if confirm_key in st.session_state:
            bundle = st.session_state[confirm_key]
            payload = bundle["payload"]
            new_status = bundle["status"]

            st.warning("⚠️ **Review & Confirm Daily Report**")
            st.info("Please double-check your times, photos, and notes below before sending to Admins.")

            with st.container(border=True):
                st.markdown(f"**Status:** {new_status}")
                st.markdown(f"**Time:** {payload['timeArrived']} - {payload['timeDeparted']} ({payload['hoursWorked']} hrs)")
                st.markdown(f"**Techs:** {payload['techsOnSite']}")
                st.markdown(f"**Warranty:** {'Yes' if payload.get('isWarranty') else 'No'}")
                st.markdown(f"**Notes:** {payload['content']}")
                if payload.get('photos'):
                    st.markdown(f"**Photos:** {len(payload['photos'])} attached")

            c_yes, c_no = st.columns(2)
            if c_yes.button("✅ Yes, Submit Report", key="conf_yes", type="primary"):
                del st.session_state[confirm_key]
                if new_status == "Completed":
                    # Completion goes through the sign-off flow (checklist,
                    # customer signature, completion email) - never just an email.
                    st.session_state[f"completion_pending_{job['id']}"] = payload
                    st.rerun(scope="fragment")
                else:
                    # Persist FIRST, then email. The tech's work is safe the moment
                    # they confirm instead of riding on whether SMTP answers.
                    st.session_state.jobs[job_index]['reports'].append(payload)
                    if new_status != job['status']:
                        apply_job_status(st.session_state.jobs[job_index], new_status, _viewer_email)
                        st.session_state.briefing = "Data required to generate briefing."
                    save_state()
                    with st.spinner("Sending Daily Report to Admins..."):
                        send_daily_report_email(job, tech, loc, payload)
                    st.toast("Daily Report Submitted & Emailed to Admins!", icon="✅")
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

        # Prefill from the job's last daily report - the same crew usually
        # returns day after day and shouldn't have to retype the same times.
        default_arrived = datetime.time(8, 0)
        default_departed = datetime.time(17, 0)

        _prev = last_daily_report(job)
        _prev_used = False
        if _prev:
            default_arrived = _parse_report_time(_prev.get('timeArrived'), default_arrived)
            default_departed = _parse_report_time(_prev.get('timeDeparted'), default_departed)
            _prev_used = bool(_prev.get('timeArrived') or _prev.get('timeDeparted'))

        if _prev_used:
            st.caption(f"↩️ Prefilled from the last report on this job "
                       f"({str(_prev.get('timestamp', ''))[:10]}) — adjust anything that changed.")

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
                hours_worked = st.number_input("Hours Worked", min_value=0.0, step=0.5, value=0.0, help="Leave at 0 to calculate from your arrival/finish times.")
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
                st.info(f"📸 {len(todays_photos)} photos taken today via Updates will be automatically attached.")
            
            # Allow adding more photos directly here
            daily_photos = st.file_uploader("Attach Additional Photos/Docs (Optional)", accept_multiple_files=True, type=['png', 'jpg', 'jpeg', 'pdf'], key=f"daily_up_{job_id}")

            submit_btn = st.form_submit_button("Submit Daily Report", use_container_width=True)

            if submit_btn:
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
                    'photos': todays_photos
                }

                # Every submission goes through the same review step; the confirm
                # button routes Completed jobs into the sign-off flow.
                st.session_state[f"confirm_daily_send_{job['id']}"] = {"payload": report_payload, "status": new_status}
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
