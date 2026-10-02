import os
import io
import datetime
from io import BytesIO

import streamlit as st
from PIL import Image

# ReportLab is imported lazily inside generate_job_pdf - it costs real startup
# time and most runs never build a PDF.

from object_store import get_view_url
from core import now_local, LOGO_PATH, remember_photo_bytes, photo_bytes_for_key, get_image_bytes

@st.cache_data(show_spinner="Generating PDF...")
def generate_job_pdf(job, tech, location, report):
    """Generates a styled PDF report for a job (completion or daily field report)."""
    try:
        from reportlab.lib.utils import ImageReader
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import letter
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.platypus import (
            SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
            Image as RLImage, KeepTogether, PageBreak,
        )
    except ImportError:
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
