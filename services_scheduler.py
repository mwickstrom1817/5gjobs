import os
import time
import datetime
import threading

import requests
import streamlit as st

from core import now_local, get_logger, compute_hours_rows, invoice_status, job_status_since, expiring_assets
from persistence_pg import load_state, save_state_to_db, ping_db
from services_email import (daily_summary_recipients, build_ops_summary_email,
                            _send_hours_digest_email, build_warranty_expiry_email)
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
        backup_done_for = None
        while True:
            try:
                now = now_local()
                # Run at 7 AM Mon-Fri (first 10 minutes of the hour only).
                # The minute guard is a backstop so a failed persistence write
                # can't cause a re-send every 10 minutes for the whole hour.
                if now.weekday() <= 4 and now.hour == 7 and now.minute < 10:
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

                # Friday 4 PM: weekly hours digest to admins (CSV attached).
                # Pin to the first 10 minutes of the hour as a backstop.
                if now.weekday() == 4 and now.hour == 16 and now.minute < 10:
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

                        # Unbilled-invoice aging: completed jobs still sitting in
                        # 'Ready to Invoice', folded into the digest as one row.
                        ready = [j for j in jobs
                                 if j.get('status') == 'Completed' and invoice_status(j) == 'Ready to Invoice']
                        ages = [(now.date() - d).days for j in ready
                                if (d := job_status_since(j))]
                        unbilled_row = None
                        if ages:
                            unbilled_row = ("Unbilled jobs", f"{len(ages)} waiting to invoice, oldest {max(ages)}d")

                        # Weekly hours digest -> admins only
                        _send_hours_digest_email('Hours', admin_emails,
                                                 smtp_server, smtp_port, sender_email, sender_password,
                                                 jobs, techs, locations, start_d, end_d,
                                                 extra_row=unbilled_row)

                        get_logger().log(f"Sent weekly hours digest for {start_d} to {end_d}")
                        state["last_hours_digest_date"] = today_str
                        save_state_to_db(state, expected_version=version)

                # 1st of the month, 7 AM: warranty-expiration report to admins.
                # Every expiring asset is a renewal/upsell conversation the
                # office wouldn't otherwise know to start. Pin to first 10 min.
                if now.day == 1 and now.hour == 7 and now.minute < 10:
                    from persistence_pg import load_state, save_state_to_db
                    state, version = load_state()
                    month_key = now.strftime("%Y-%m")

                    if state.get("last_warranty_report_month") != month_key:
                        smtp_server = secrets_dict.get("SMTP_SERVER") or os.getenv("SMTP_SERVER")
                        smtp_port = secrets_dict.get("SMTP_PORT") or os.getenv("SMTP_PORT", 587)
                        sender_email = secrets_dict.get("SMTP_EMAIL") or os.getenv("SMTP_EMAIL")
                        sender_password = secrets_dict.get("SMTP_PASSWORD") or os.getenv("SMTP_PASSWORD")
                        admin_emails = state.get("adminEmails", [])

                        rows = expiring_assets(state.get("locations", []))
                        subject, plain, html = build_warranty_expiry_email(
                            rows, now.strftime("%B %Y"))
                        if subject and admin_emails and smtp_server and sender_email and sender_password:
                            import smtplib
                            from email.mime.text import MIMEText
                            from email.mime.multipart import MIMEMultipart
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
                                for recipient in admin_emails:
                                    try:
                                        msg = MIMEMultipart("alternative")
                                        msg['From'] = sender_email
                                        msg['To'] = recipient
                                        msg['Subject'] = subject
                                        msg.attach(MIMEText(plain, 'plain'))
                                        if html:
                                            msg.attach(MIMEText(html, 'html'))
                                        server.send_message(msg)
                                    except Exception:
                                        continue
                                server.quit()
                                get_logger().log(f"Sent warranty report for {month_key} ({len(rows)} assets)")
                            except Exception as e:
                                get_logger().log(f"Warranty report send failed: {e}")

                        # Mark the month done whether or not anything was
                        # expiring - a quiet month shouldn't re-check hourly.
                        state["last_warranty_report_month"] = month_key
                        save_state_to_db(state, expected_version=version)

                # 1 AM daily: snapshot the whole DB state to object storage.
                # The snapshot file is date-keyed, so re-running the same day
                # just overwrites it - an in-memory flag is enough to skip
                # duplicate work without another DB write. Pin to first 10 min.
                if now.hour == 1 and now.minute < 10:
                    from services_backup import run_daily_backup
                    today_str = now.strftime("%Y-%m-%d")
                    if backup_done_for != today_str:
                        ok, msg = run_daily_backup()
                        if ok:
                            backup_done_for = today_str
                        get_logger().log(f"Scheduled backup: {msg}")

                # Keep the serverless DB warm. Without periodic traffic Neon
                # dozes off, and the first real query of the morning pays a
                # cold-connection penalty.
                try:
                    ping_db()
                except Exception:
                    pass
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
