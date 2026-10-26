import os
import io
import re
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

    attachment_names = []
    for attachment in (report.get("attachments") or report.get("documents") or []):
        name = attachment.get("name", "Unnamed file") if isinstance(attachment, dict) else str(attachment)
        extension = name.rsplit(".", 1)[-1].lower() if "." in name else ""
        if extension not in ("jpg", "jpeg", "png", "gif", "bmp", "webp", "pdf"):
            attachment_names.append(name)
    if attachment_names:
        story.append(Paragraph("REPORT ATTACHMENTS", s_section))
        for name in attachment_names:
            story.append(Paragraph(f"• {esc(name)}", s_body))

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



def _md_to_rl_html(text):
    """Lightweight markdown -> ReportLab-safe HTML for SOP procedure text."""
    text = str(text or "")
    # Escape first so user HTML doesn't break the parser
    text = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    # Inline formatting
    def repl_bold(m):
        return f"<b>{m.group(1)}</b>"

    def repl_italic(m):
        return f"<i>{m.group(1)}</i>"

    # Bold: **text** or __text__
    text = re.sub(r'\*\*(.+?)\*\*', repl_bold, text)
    text = re.sub(r'__(.+?)__', repl_bold, text)
    # Italic: *text* or _text_ (single)
    text = re.sub(r'\*(.+?)\*', repl_italic, text)
    text = re.sub(r'_(.+?)_', repl_italic, text)
    return text


def _sop_content_flowables(content, s_body, brand_red):
    """Turn free-form SOP text into ReportLab flowables.

    Supports headings (#/##/###), bullet lists (-, *, •), and blank-line
    paragraph breaks. Everything else is rendered as a body paragraph.
    """
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.platypus import Paragraph, Spacer

    s_bullet = ParagraphStyle('Bullet', parent=s_body, leftIndent=18, firstLineIndent=-10)
    s_h1 = ParagraphStyle('SOPH1', parent=s_body, fontName="Helvetica-Bold",
                          fontSize=16, textColor=brand_red, spaceBefore=14, spaceAfter=6)
    s_h2 = ParagraphStyle('SOPH2', parent=s_body, fontName="Helvetica-Bold",
                          fontSize=13, textColor=brand_red, spaceBefore=12, spaceAfter=5)
    s_h3 = ParagraphStyle('SOPH3', parent=s_body, fontName="Helvetica-Bold",
                          fontSize=11, textColor=brand_red, spaceBefore=10, spaceAfter=4)

    flowables = []
    if not content:
        return flowables

    lines = content.split('\n')
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Blank line -> small vertical gap
        if not stripped:
            flowables.append(Spacer(1, 6))
            i += 1
            continue

        # Headings
        if stripped.startswith('### '):
            flowables.append(Paragraph(_md_to_rl_html(stripped[4:]), s_h3))
            i += 1
            continue
        if stripped.startswith('## '):
            flowables.append(Paragraph(_md_to_rl_html(stripped[3:]), s_h2))
            i += 1
            continue
        if stripped.startswith('# '):
            flowables.append(Paragraph(_md_to_rl_html(stripped[2:]), s_h1))
            i += 1
            continue

        # Bullet list: gather consecutive bullet lines
        if stripped.startswith(('- ', '* ', '• ')):
            bullet_lines = []
            while i < len(lines):
                bline = lines[i].strip()
                if bline.startswith(('- ', '* ', '• ')):
                    bullet_lines.append(bline[2:])
                    i += 1
                elif not bline:
                    i += 1
                    break
                else:
                    break
            for bl in bullet_lines:
                flowables.append(Paragraph(f"• {_md_to_rl_html(bl)}", s_bullet))
            continue

        # Regular paragraph; absorb following non-blank, non-special lines
        para_lines = [line]
        i += 1
        while i < len(lines):
            nxt = lines[i]
            nxt_stripped = nxt.strip()
            if not nxt_stripped or nxt_stripped.startswith(('#', '- ', '* ', '• ')):
                break
            para_lines.append(nxt)
            i += 1
        para = ' '.join(p.strip() for p in para_lines)
        flowables.append(Paragraph(_md_to_rl_html(para), s_body))

    return flowables


@st.cache_data(show_spinner="Generating SOP PDF...")
def generate_sop_pdf(sop):
    """Generate a professional, printable PDF for a single SOP."""
    try:
        return _generate_sop_pdf(sop)
    except Exception:
        return None


def _generate_sop_pdf(sop):
    try:
        from reportlab.lib.utils import ImageReader
        from reportlab.lib.pagesizes import letter
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.platypus import (
            SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        )
    except ImportError:
        return None

    BRAND_RED = colors.HexColor("#b91c1c")
    BRAND_DARK = colors.HexColor("#18181b")
    INK = colors.HexColor("#27272a")
    MUTED = colors.HexColor("#71717a")
    LIGHT = colors.HexColor("#f4f4f5")
    BORDER = colors.HexColor("#e4e4e7")

    generated_str = now_local().strftime('%B %d, %Y at %I:%M %p')
    sop_id = sop.get('id', '')
    title = sop.get('title', 'Untitled Procedure')
    category = sop.get('category', '')
    notes = sop.get('notes', '')
    atts = sop.get('attachments') or []
    updated_at = sop.get('updated_at', '')
    updated_by = sop.get('updated_by', '')

    def esc(value):
        return str(value if value is not None else "").replace(
            '&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    def header_footer(canv, doc):
        w, h = letter
        canv.saveState()
        # Header band
        canv.setFillColor(BRAND_DARK)
        canv.rect(0, h - 80, w, 80, fill=1, stroke=0)
        canv.setFillColor(BRAND_RED)
        canv.rect(0, h - 84, w, 4, fill=1, stroke=0)
        # Logo or wordmark
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
        canv.drawString(46, h - 76, "Standard Operating Procedure")
        canv.setFont("Helvetica", 8)
        canv.drawRightString(w - 46, h - 48, f"Generated {generated_str}")
        canv.drawRightString(w - 46, h - 64, f"SOP ID: {sop_id}")
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
    s_title = ParagraphStyle('SOPTitle', parent=styles['Heading1'], fontName="Helvetica-Bold",
                             fontSize=20, textColor=INK, spaceAfter=4)
    s_category = ParagraphStyle('SOPCategory', parent=styles['Normal'], fontSize=11,
                                textColor=MUTED, spaceAfter=12)
    s_section = ParagraphStyle('SOPSection', parent=styles['Heading2'], fontName="Helvetica-Bold",
                               fontSize=12, textColor=BRAND_RED, spaceBefore=16, spaceAfter=6)
    s_body = ParagraphStyle('SOPBody', parent=styles['Normal'], fontName="Helvetica",
                            fontSize=10.5, leading=15, textColor=INK)
    s_label = ParagraphStyle('SOPLabel', parent=s_body, textColor=MUTED, fontSize=9)
    s_value = ParagraphStyle('SOPValue', parent=s_body, fontName="Helvetica-Bold")
    s_caption = ParagraphStyle('SOPCaption', parent=s_label, fontSize=8, spaceBefore=2)

    avail = letter[0] - 92
    story = []

    # Title block
    story.append(Paragraph(esc(title), s_title))
    if category:
        story.append(Paragraph(f"Category: {esc(category)}", s_category))

    # Metadata table
    meta_rows = []
    if updated_at:
        meta_rows.append([Paragraph("LAST UPDATED", s_label), Paragraph(esc(str(updated_at)[:10]), s_value)])
    if updated_by:
        meta_rows.append([Paragraph("AUTHOR", s_label), Paragraph(esc(updated_by), s_value)])
    if atts:
        meta_rows.append([Paragraph("ATTACHMENTS", s_label), Paragraph(str(len(atts)), s_value)])

    if meta_rows:
        # Build a single table with label/value pairs side by side
        data = []
        for r in meta_rows:
            data.append(r)
        t = Table(data, colWidths=[avail * 0.22, avail * 0.78])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), LIGHT),
            ('GRID', (0, 0), (-1, -1), 0.5, BORDER),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
        ]))
        story.append(Spacer(1, 8))
        story.append(t)

    # Procedure content
    story.append(Paragraph("PROCEDURE", s_section))
    content = sop.get('content', '')
    if content:
        story.extend(_sop_content_flowables(content, s_body, BRAND_RED))
    else:
        steps = sop.get('steps') or []
        if steps:
            for i, stp in enumerate(steps, 1):
                story.append(Paragraph(f"{i}. {esc(stp)}", s_body))
        else:
            story.append(Paragraph("No procedure recorded.", s_body))

    # Notes / cautions
    if notes:
        story.append(Paragraph("NOTES / CAUTIONS", s_section))
        story.extend(_sop_content_flowables(notes, s_body, BRAND_RED))

    # Attachments list
    if atts:
        story.append(Paragraph("ATTACHMENTS", s_section))
        for a in atts:
            name = a.get('name', 'Unnamed file')
            story.append(Paragraph(f"• {esc(name)}", s_body))

    # Optional footer caption
    story.append(Spacer(1, 24))
    story.append(Paragraph(
        "This Standard Operating Procedure is confidential and intended for authorized 5G Security personnel only.",
        s_caption))

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer, pagesize=letter,
        leftMargin=46, rightMargin=46, topMargin=104, bottomMargin=64,
        title=f"5G Security SOP - {title}",
    )
    doc.build(story, onFirstPage=header_footer, onLaterPages=header_footer)
    buffer.seek(0)
    return buffer.getvalue()


@st.cache_data(show_spinner="Generating job brief...")
def generate_job_brief_pdf(job, tech, location, site_jobs):
    """Create a complete job and site reference, including site credentials."""
    try:
        return _generate_job_brief_pdf(job, tech, location, site_jobs)
    except Exception:
        return None


def _generate_job_brief_pdf(job, tech, location, site_jobs):
    from reportlab.lib.utils import ImageReader
    from reportlab.lib.pagesizes import letter
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

    brand_red = colors.HexColor("#c81e1e")
    brand_dark = colors.HexColor("#15161b")
    ink = colors.HexColor("#27272a")
    muted = colors.HexColor("#71717a")
    light = colors.HexColor("#f4f4f5")
    border = colors.HexColor("#e4e4e7")
    generated = now_local().strftime('%B %d, %Y at %I:%M %p')

    def esc(value):
        return str(value if value is not None else "").replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    def header_footer(canv, doc):
        width, height = letter
        canv.saveState()
        canv.setFillColor(brand_dark)
        canv.rect(0, height - 80, width, 80, fill=1, stroke=0)
        canv.setFillColor(brand_red)
        canv.rect(0, height - 84, width, 4, fill=1, stroke=0)
        logo_drawn = False
        try:
            if os.path.exists(LOGO_PATH):
                canv.drawImage(ImageReader(LOGO_PATH), 46, height - 64, width=150, height=34,
                               preserveAspectRatio=True, anchor='sw', mask='auto')
                logo_drawn = True
        except Exception:
            pass
        if not logo_drawn:
            canv.setFillColor(colors.white)
            canv.setFont("Helvetica-Bold", 20)
            canv.drawString(46, height - 48, "5G SECURITY")
        canv.setFillColor(colors.HexColor("#d4d4d8"))
        canv.setFont("Helvetica", 10)
        canv.drawString(46, height - 76, "Job Brief & Site Reference")
        canv.setFont("Helvetica", 8)
        canv.drawRightString(width - 46, height - 48, f"Generated {generated}")
        canv.drawRightString(width - 46, height - 64, f"Job ID: {job.get('id', '')}")
        canv.setStrokeColor(border)
        canv.setLineWidth(0.5)
        canv.line(46, 46, width - 46, 46)
        canv.setFont("Helvetica", 8)
        canv.setFillColor(muted)
        canv.drawString(46, 34, "CONFIDENTIAL - includes site credentials - authorized personnel only")
        canv.drawRightString(width - 46, 34, f"Page {canv.getPageNumber()}")
        canv.restoreState()

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('BriefTitle', parent=styles['Heading1'], fontName='Helvetica-Bold',
                                 fontSize=18, textColor=ink, spaceAfter=3)
    subtitle_style = ParagraphStyle('BriefSubtitle', parent=styles['Normal'], fontSize=10,
                                    textColor=muted, spaceAfter=6)
    section_style = ParagraphStyle('BriefSection', parent=styles['Heading2'], fontName='Helvetica-Bold',
                                   fontSize=11, textColor=brand_red, spaceBefore=14, spaceAfter=6)
    body_style = ParagraphStyle('BriefBody', parent=styles['Normal'], fontName='Helvetica',
                                fontSize=9.2, leading=13, textColor=ink)
    label_style = ParagraphStyle('BriefLabel', parent=body_style, fontSize=7.8, textColor=muted)
    value_style = ParagraphStyle('BriefValue', parent=body_style, fontName='Helvetica-Bold')
    note_style = ParagraphStyle('BriefNote', parent=body_style, fontSize=8.5, leading=12)
    caption_style = ParagraphStyle('BriefCaption', parent=label_style, fontSize=7.5, spaceBefore=2)

    available = letter[0] - 92
    story = []

    def section(title):
        story.append(Paragraph(esc(title).upper(), section_style))

    def text_block(text, style=body_style):
        for line in str(text or '').split('\n'):
            if line.strip():
                story.append(Paragraph(esc(line), style))
            else:
                story.append(Spacer(1, 4))

    def info_table(rows):
        data = []
        for row in rows:
            cells = []
            for index, value in enumerate(row):
                cells.append(Paragraph(esc(value).upper() if index % 2 == 0 else esc(value),
                                       label_style if index % 2 == 0 else value_style))
            data.append(cells)
        table = Table(data, colWidths=[available * 0.16, available * 0.40, available * 0.16, available * 0.28])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), light), ('BACKGROUND', (2, 0), (2, -1), light),
            ('GRID', (0, 0), (-1, -1), 0.5, border), ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('TOPPADDING', (0, 0), (-1, -1), 5), ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('LEFTPADDING', (0, 0), (-1, -1), 7), ('RIGHTPADDING', (0, 0), (-1, -1), 7),
        ]))
        story.append(table)

    def data_table(headers, rows, widths):
        data = [[Paragraph(esc(h).upper(), label_style) for h in headers]]
        for row in rows:
            data.append([Paragraph(esc(cell), body_style) for cell in row])
        table = Table(data, colWidths=widths, repeatRows=1)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), light), ('GRID', (0, 0), (-1, -1), 0.5, border),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('TOPPADDING', (0, 0), (-1, -1), 5), ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('LEFTPADDING', (0, 0), (-1, -1), 6), ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ]))
        story.append(table)

    loc = location or {}
    assigned_tech = tech.get('name') if tech else 'Unassigned'
    story.append(Paragraph(esc(job.get('title', 'Untitled Job')), title_style))
    story.append(Paragraph(esc(f"{loc.get('name', 'Unknown site')} — {loc.get('address', '')}"), subtitle_style))

    section('Job details')
    story.append(info_table([
        ('Status', job.get('status', 'N/A'), 'Priority', job.get('priority', 'N/A')),
        ('Technician', assigned_tech, 'Job Type', job.get('type', 'N/A')),
        ('Scheduled', str(job.get('date', ''))[:16].replace('T', ' '), 'Quote', job.get('quoteValue') or 'N/A'),
    ]))

    if job.get('description'):
        section('Scope / description')
        text_block(job['description'])

    job_contacts = job.get('contacts') or []
    if job_contacts:
        contact_rows = [(c.get('label') or c.get('name', 'Contact'), c.get('name', ''),
                         c.get('phone', ''), c.get('email', '')) for c in job_contacts]
    elif any([job.get('contact_name'), job.get('contact_phone'), job.get('contact_email'),
              loc.get('contact_name'), loc.get('contact_phone'), loc.get('contact_email')]):
        contact_rows = [('Site Contact', job.get('contact_name') or loc.get('contact_name', ''),
                         job.get('contact_phone') or loc.get('contact_phone', ''),
                         job.get('contact_email') or loc.get('contact_email', ''))]
    else:
        contact_rows = []
    if contact_rows:
        section('Site contacts')
        data_table(['Role', 'Name', 'Phone', 'Email'], contact_rows,
                   [available * 0.18, available * 0.25, available * 0.22, available * 0.35])

    parts = job.get('parts') or []
    if parts:
        section('Parts & materials')
        part_rows = [(p.get('name', ''), p.get('qty', ''), p.get('status', ''),
                      p.get('vendor', ''), p.get('cost', ''), p.get('notes', '')) for p in parts]
        data_table(['Item', 'Qty', 'Status', 'Vendor', 'Cost', 'Notes'], part_rows,
                   [available * 0.27, available * 0.07, available * 0.13,
                    available * 0.16, available * 0.11, available * 0.26])

    systems = loc.get('systems') or []
    if systems:
        section('Systems, logins & network information')
        system_rows = [(s.get('name', 'System'), s.get('username', ''), s.get('password', ''),
                        s.get('ip', ''), s.get('notes', '')) for s in systems]
        data_table(['System', 'Username', 'Password', 'IP Address(es)', 'Notes'], system_rows,
                   [available * 0.20, available * 0.17, available * 0.17,
                    available * 0.20, available * 0.26])

    job_docs = job.get('documents') or []
    site_docs = loc.get('documents') or []
    if job_docs or site_docs:
        section('Documents')
        if job_docs:
            story.append(Paragraph('This job', value_style))
            for doc in job_docs:
                story.append(Paragraph(f"• {esc(doc.get('name', 'Unnamed file'))}", body_style))
        if site_docs:
            story.append(Paragraph('Site documents', value_style))
            for doc in site_docs:
                story.append(Paragraph(f"• {esc(doc.get('name', 'Unnamed file'))}", body_style))

    assets = loc.get('assets') or []
    if assets:
        section('Registered equipment')
        asset_rows = [(a.get('tag', ''), a.get('type', ''), a.get('make_model', ''),
                       a.get('position', ''), a.get('serial', '')) for a in assets]
        data_table(['Tag', 'Type', 'Make / Model', 'Location', 'Serial'], asset_rows,
                   [available * 0.16, available * 0.15, available * 0.29,
                    available * 0.22, available * 0.18])

    def note_text(report):
        return report.get('content') or report.get('ai_summary') or 'No notes recorded.'

    tech_names = {t.get('id'): t.get('name') for t in st.session_state.get('techs', [])}

    def note_author(report):
        if report.get('authorEmail'):
            return report['authorEmail']
        author_id = report.get('authorTechId') or report.get('techId')
        return tech_names.get(author_id) or author_id or assigned_tech

    reports = list(reversed(job.get('reports') or []))
    if reports:
        section('Current job work history')
        for report in reports[:5]:
            story.append(Paragraph(esc(f"{str(report.get('timestamp', ''))[:16].replace('T', ' ')} · {note_author(report)}"), value_style))
            text_block(note_text(report), note_style)
            story.append(Spacer(1, 5))

    history_jobs = sorted(site_jobs or [], key=lambda item: str(item.get('date', '')), reverse=True)
    if history_jobs:
        section('Recent work at this location')
        for previous in history_jobs[:5]:
            story.append(Paragraph(esc(f"{previous.get('title', 'Untitled Job')} · {str(previous.get('date', ''))[:10]} · {previous.get('status', '')}"), value_style))
            previous_reports = list(reversed(previous.get('reports') or []))
            if not previous_reports:
                story.append(Paragraph('No work notes recorded.', note_style))
            for report in previous_reports[:2]:
                story.append(Paragraph(esc(f"{str(report.get('timestamp', ''))[:16].replace('T', ' ')} · {note_author(report)}"), caption_style))
                text_block(note_text(report), note_style)
            story.append(Spacer(1, 6))

    story.append(Spacer(1, 18))
    story.append(Paragraph('This document contains confidential operational and login information. Store and share securely.', caption_style))

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, leftMargin=46, rightMargin=46,
                            topMargin=104, bottomMargin=64,
                            title=f"5G Security Job Brief - {job.get('title', 'Job')}")
    doc.build(story, onFirstPage=header_footer, onLaterPages=header_footer)
    buffer.seek(0)
    return buffer.getvalue()
