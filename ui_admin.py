import os
import re
import json
import datetime

import pandas as pd
import requests
import streamlit as st

from core import (
    now_local, save_state, get_logger, _sync_session_to_db,
    SKILL_OPTIONS, TECH_COLORS, download_data_as_csv, download_data_as_json,
    expiring_assets, validate_state_dict,
)
from persistence_pg import (
    load_state, ensure_loaded_into_session, commit_from_session,
    force_overwrite_from_session,
)
from services_ai import get_available_model, suggest_address_with_gemini, get_api_key
from services_email import daily_summary_recipients, send_ops_summary_email
from services_push import get_or_create_notify_topic, send_push
from ui_dialogs import edit_location_dialog, job_details_dialog
from ui_views import (
    render_service_agreements, render_hours_report, render_data_browser,
    render_analytics_dashboard, render_invoicing_view,
)

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
                    "SMTP_EMAIL": st.secrets.get("SMTP_EMAIL", "")
                }
            s_server = st.text_input("SMTP Server", value=current_smtp.get("SMTP_SERVER", ""))
            s_port = st.number_input("SMTP Port", value=int(current_smtp.get("SMTP_PORT", 587)))
            s_email = st.text_input("Sender Email", value=current_smtp.get("SMTP_EMAIL", ""))
            # Password is read from secrets/env at send time and is never persisted to the DB.
            pw_source = "Secrets" if "SMTP_PASSWORD" in st.secrets else ("Env" if os.environ.get("SMTP_PASSWORD") else "Not set")
            st.text_input("Sender Password", value="••••••••", type="password", disabled=True,
                          help=f"Password is read from {pw_source} and is not stored in the database.")
            if st.form_submit_button("Save SMTP Settings"):
                st.session_state.smtp_settings = {
                    "SMTP_SERVER": s_server,
                    "SMTP_PORT": s_port,
                    "SMTP_EMAIL": s_email
                }
                save_state(invalidate_briefing=False)
                st.toast("SMTP Settings Saved!", icon="✅")
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


def _admin_warranty():
    st.subheader("🛡️ Warranty Radar")
    st.caption("Registered equipment whose warranty expires within 90 days (or already has), "
               "soonest first. Every row is a renewal conversation — the site contact is one tap away. "
               "Admins also get this list by email on the 1st of each month.")

    rows = expiring_assets(st.session_state.locations)
    if not rows:
        st.success("Nothing expiring in the next 90 days. 🎉")
        return

    expired = sum(1 for *_x, d in rows if d < 0)
    within30 = sum(1 for *_x, d in rows if 0 <= d <= 30)
    m1, m2, m3 = st.columns(3)
    m1.metric("Expired", expired)
    m2.metric("Expiring ≤ 30 days", within30)
    m3.metric("Total on radar", len(rows))
    st.write("")

    for loc, a, expiry, days_left in rows:
        with st.container(border=True):
            wc1, wc2, wc3 = st.columns([4, 2, 1])
            _model = f" — {a['make_model']}" if a.get('make_model') else ""
            wc1.markdown(f"**`{a.get('tag', '')}`** · {a.get('type', 'Asset')}{_model}")
            _bits = [x for x in [a.get('position'), a.get('serial'),
                                 f"warranty ends {expiry}"] if x]
            wc1.caption(" · ".join(_bits))
            _site_line = f"🏢 {loc.get('name', '?')}"
            if loc.get('contact_name'):
                _site_line += f" · {loc['contact_name']}"
            wc1.caption(_site_line)
            if days_left < 0:
                wc2.markdown(f":red-background[EXPIRED {abs(days_left)}d ago]")
            else:
                wc2.markdown(f":orange-background[{days_left} days left]")
            _phone = re.sub(r'\D', '', (loc.get('contact_phone') or ''))
            if _phone:
                wc3.link_button("📞 Call", f"tel:{_phone}", use_container_width=True)

    csv_df = pd.DataFrame([
        {"Tag": a.get('tag'), "Type": a.get('type'), "Make / Model": a.get('make_model'),
         "Serial": a.get('serial'), "Site": loc.get('name'), "Warranty Ends": str(expiry),
         "Days Left": d, "Contact": loc.get('contact_name'), "Phone": loc.get('contact_phone')}
        for loc, a, expiry, d in rows
    ])
    st.download_button("⬇️ Download CSV",
                       csv_df.to_csv(index=False).encode("utf-8"),
                       file_name=f"warranty_radar_{now_local().strftime('%Y%m%d')}.csv",
                       mime="text/csv")


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
                    ok, msg = validate_state_dict(data)
                    if not ok:
                        st.error(f"Invalid backup: {msg}")
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

    st.divider()
    st.subheader("☁️ Cloud Backups (R2)")
    st.caption("A snapshot of the entire database is written to object storage every night at 1 AM "
               "(the newest 30 daily snapshots are kept). Backup files match the restore format "
               "above, so one can be pulled from the bucket and restored here if the database is lost.")
    if st.button("💾 Back Up Now", key="backup_now_btn"):
        from services_backup import run_daily_backup
        with st.spinner("Writing backup to object storage..."):
            ok, msg = run_daily_backup()
        if ok:
            st.success(f"✅ {msg}")
            st.toast(msg, icon="💾")
        else:
            st.error(f"Backup failed: {msg}")


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
                    from google import genai
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
        ("warranty", "🛡️", "Warranty Radar", _admin_warranty),
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


def _admin_invoicing():
    """Admin tile wrapper — the tile registry calls its functions with no args."""
    _email = st.session_state.user_info.get('email', '') if "user_info" in st.session_state else ''
    render_invoicing_view(_email)
