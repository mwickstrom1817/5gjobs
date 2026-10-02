import os
import datetime
import smtplib
import urllib.parse

import streamlit as st
import pandas as pd

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

from core import (
    get_config_val, get_logger, now_local, get_google_maps_url,
    compute_hours_rows, PRIORITY_COLORS, get_status_color,
)
from services_pdf import generate_job_pdf

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
