import os
import time
import datetime
import threading

import requests
import streamlit as st

from core import now_local, get_logger, compute_hours_rows
from persistence_pg import load_state, save_state_to_db
from services_email import daily_summary_recipients, build_ops_summary_email, _send_hours_digest_email
from services_push import send_push

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
