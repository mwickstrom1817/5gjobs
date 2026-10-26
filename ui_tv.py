import datetime

import streamlit as st

from core import (
    now_local, get_status_color, PRIORITY_COLORS, get_logo_data_uri,
    refresh_session_from_db, get_db_version,
)


TV_CSS = """
<style>
[data-testid="stToolbar"], #MainMenu, header, footer,
[data-testid="stSidebar"], [data-testid="stSidebarCollapsedControl"] { display: none !important; }
[data-testid="stAppViewContainer"] .block-container { padding: 0 !important; max-width: 100% !important; }
.stApp { background: #0b0c10; }
.tv-shell { --bg:#0b0c10; --panel:#15161b; --panel2:#1b1c22; --border:#2a2c34;
  --text:#f4f4f5; --muted:#a1a1aa; --red:#c81e1e; --green:#10b981; --blue:#3b82f6;
  background:var(--bg); color:var(--text); min-height:100vh; padding:24px 30px 18px;
  font-family:Inter,'Source Sans Pro',sans-serif; box-sizing:border-box; font-size:clamp(14px,.85vw,20px); }
.tv-shell * { box-sizing:border-box; }
.tv-header { display:flex; align-items:center; justify-content:space-between; gap:24px;
  border-bottom:2px solid var(--red); padding-bottom:16px; margin-bottom:18px; }
.tv-brand { display:flex; align-items:center; gap:18px; min-width:0; }
.tv-brand img { height:58px; max-width:240px; object-fit:contain; }
.tv-brand-name { font-size:clamp(24px,1.55vw,38px); font-weight:800; letter-spacing:1.5px; }
.tv-title { color:var(--muted); font-size:clamp(12px,.75vw,17px); letter-spacing:2px; margin-top:4px; }
.tv-clock { text-align:right; flex-shrink:0; }
.tv-clock-date { color:var(--muted); font-size:clamp(13px,.8vw,18px); }
.tv-clock-time { font-size:clamp(25px,1.65vw,40px); font-weight:700; line-height:1.15; }
.tv-kpis { display:grid; grid-template-columns:repeat(6,1fr); gap:12px; margin-bottom:18px; }
.tv-kpi { background:var(--panel); border:1px solid var(--border); border-radius:10px; padding:14px 16px; }
.tv-kpi-value { font-size:clamp(30px,2vw,48px); font-weight:750; line-height:1.05; }
.tv-kpi-label { color:var(--muted); font-size:clamp(10px,.58vw,13px); letter-spacing:1.1px; margin-top:5px; }
.tv-main { display:grid; grid-template-columns:1.2fr 1fr .82fr; gap:16px; align-items:start; }
.tv-panel { background:var(--panel); border:1px solid var(--border); border-radius:12px; padding:16px; min-width:0; }
.tv-panel-title { display:flex; justify-content:space-between; align-items:baseline; gap:12px;
  border-bottom:1px solid var(--border); padding-bottom:10px; margin-bottom:12px; }
.tv-panel-title h2 { margin:0; font-size:clamp(17px,1.05vw,25px); font-weight:700; letter-spacing:.5px; }
.tv-count { color:var(--muted); font-size:clamp(11px,.65vw,14px); white-space:nowrap; }
.tv-subhead { color:var(--muted); font-size:clamp(10px,.62vw,14px); font-weight:700;
  letter-spacing:1.4px; margin:12px 0 8px; }
.tv-subhead.alert { color:#f87171; }
.tv-job { background:var(--panel2); border:1px solid var(--border); border-left:3px solid var(--status,#52525b);
  border-radius:8px; padding:11px 12px; margin-bottom:9px; }
.tv-job-top { display:flex; align-items:flex-start; justify-content:space-between; gap:10px; }
.tv-job-title { font-size:clamp(14px,.88vw,21px); font-weight:650; line-height:1.25; }
.tv-job-date { color:var(--muted); font-size:clamp(10px,.62vw,14px); white-space:nowrap; }
.tv-job-meta { color:var(--muted); font-size:clamp(11px,.68vw,16px); margin-top:6px;
  overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.tv-badges { display:flex; gap:5px; flex-wrap:wrap; margin-top:7px; }
.tv-badge { display:inline-block; border-radius:5px; padding:2px 7px; color:#fff;
  font-size:clamp(9px,.55vw,12px); font-weight:650; }
.tv-priority { background:#30323b; color:#e4e4e7 !important; border:1px solid #454854; }
.tv-reason { background:#7f1d1d; color:#fecaca !important; }
.tv-empty { color:var(--muted); text-align:center; padding:24px 10px; font-size:clamp(12px,.75vw,17px); }
.tv-more { color:var(--muted); text-align:right; font-size:clamp(10px,.62vw,14px); margin-top:5px; }
.tv-day { display:grid; grid-template-columns:76px 1fr; gap:10px; border-bottom:1px solid var(--border); padding:9px 0; }
.tv-day:last-child { border-bottom:none; }
.tv-day-name { font-weight:700; font-size:clamp(11px,.68vw,16px); }
.tv-day-date { color:var(--muted); font-size:clamp(10px,.58vw,13px); }
.tv-day-jobs { color:#d4d4d8; font-size:clamp(11px,.68vw,16px); line-height:1.35; }
.tv-day-empty { color:#52525b; }
.tv-attention .tv-job { border-left-color:#c81e1e; }
.tv-team-row { margin-bottom:13px; }
.tv-team-head { display:flex; justify-content:space-between; gap:10px; font-size:clamp(12px,.75vw,17px); }
.tv-team-count { color:var(--muted); }
.tv-team-next { color:var(--muted); font-size:clamp(10px,.6vw,14px); margin-top:3px;
  overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.tv-bar { height:5px; background:#2a2c34; border-radius:4px; margin-top:6px; overflow:hidden; }
.tv-bar-fill { height:100%; background:#3b82f6; border-radius:4px; }
.tv-future { display:grid; grid-template-columns:repeat(3,1fr); gap:10px; }
.tv-future .tv-job { margin-bottom:0; }
.tv-footer { display:flex; justify-content:space-between; gap:16px; color:var(--muted);
  font-size:clamp(10px,.6vw,13px); margin-top:14px; }
@media (max-width:1200px) {
  .tv-kpis { grid-template-columns:repeat(3,1fr); }
  .tv-main { grid-template-columns:1fr; }
  .tv-future { grid-template-columns:1fr 1fr; }
}
</style>
"""


def _tv_esc(value):
    return str(value if value is not None else "").replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def _tv_date(job):
    raw = str(job.get('date', '') or '')
    if not raw:
        return None
    try:
        return datetime.datetime.fromisoformat(raw[:19]).date()
    except (ValueError, TypeError):
        return None


def _tv_sort_key(job):
    day = _tv_date(job) or datetime.date.max
    priority = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}.get(job.get('priority'), 4)
    return day, priority, str(job.get('date', '')), str(job.get('title', '')).lower()


def _tv_job_card(job, techs, locations, reason=None, future=False):
    tech = techs.get(job.get('techId'))
    loc = locations.get(job.get('locationId'))
    status = job.get('status', 'Unknown')
    status_color = get_status_color(status)
    priority = job.get('priority', 'Medium')
    priority_color = PRIORITY_COLORS.get(priority, '#52525b')
    day = _tv_date(job)
    date_label = day.strftime('%b %d') if day else 'Unscheduled'
    reason_html = f'<span class="tv-badge tv-reason">{_tv_esc(reason)}</span>' if reason else ''
    return f'''<div class="tv-job" style="--status:{status_color}">
      <div class="tv-job-top">
        <div class="tv-job-title">{_tv_esc(job.get('title', 'Untitled Job'))}</div>
        <div class="tv-job-date">{date_label}</div>
      </div>
      <div class="tv-job-meta">📍 {_tv_esc(loc.get('name') if loc else 'Unknown site')} &nbsp;·&nbsp; 👤 {_tv_esc(tech.get('name') if tech else 'Unassigned')}</div>
      <div class="tv-badges">
        <span class="tv-badge" style="background:{status_color}">{_tv_esc(status)}</span>
        <span class="tv-badge tv-priority" style="border-color:{priority_color}">{_tv_esc(priority)}</span>
        {reason_html}
      </div>
    </div>'''


def _tv_dashboard_html(jobs, tech_records, location_records, generated=None):
    generated = generated or now_local()
    today = generated.date()
    week_end = today + datetime.timedelta(days=6)
    techs = {t.get('id'): t for t in tech_records}
    locations = {loc.get('id'): loc for loc in location_records}

    active = [j for j in jobs if j.get('status') != 'Completed']
    overdue = sorted([j for j in active if (_tv_date(j) and _tv_date(j) < today)], key=_tv_sort_key)
    today_jobs = sorted([j for j in active if _tv_date(j) == today], key=_tv_sort_key)
    week_jobs = [j for j in active if _tv_date(j) and today <= _tv_date(j) <= week_end]
    future_jobs = sorted([j for j in active if _tv_date(j) and _tv_date(j) > week_end], key=_tv_sort_key)
    critical = [j for j in active if j.get('priority') in ('Critical', 'High')]
    unassigned = [j for j in active if not j.get('techId')]
    today_str = today.isoformat()
    completed_today = [j for j in jobs if j.get('status') == 'Completed' and any(
        str(r.get('timestamp', '')).startswith(today_str) and 'completion_checklist' in r
        for r in j.get('reports', []))]

    attention_map = {}
    for job in overdue:
        attention_map[job['id']] = (job, 'Overdue')
    for job in critical:
        attention_map.setdefault(job['id'], (job, f"{job.get('priority')} priority"))
    for job in unassigned:
        attention_map.setdefault(job['id'], (job, 'Unassigned'))
    for job in active:
        if _tv_date(job) is None:
            attention_map.setdefault(job['id'], (job, 'Unscheduled'))
    attention = sorted(attention_map.values(), key=lambda pair: _tv_sort_key(pair[0]))

    kpis = [
        ('TODAY', len(today_jobs), '#f4f4f5'),
        ('OVERDUE', len(overdue), '#f87171' if overdue else '#f4f4f5'),
        ('THIS WEEK', len(week_jobs), '#60a5fa'),
        ('ACTIVE JOBS', len(active), '#f4f4f5'),
        ('NEEDS ATTENTION', len(attention), '#f87171' if attention else '#f4f4f5'),
        ('DONE TODAY', len(completed_today), '#10b981'),
    ]
    kpi_html = ''.join(
        f'<div class="tv-kpi"><div class="tv-kpi-value" style="color:{color}">{value}</div>'
        f'<div class="tv-kpi-label">{label}</div></div>'
        for label, value, color in kpis)

    today_html = ''
    if overdue:
        today_html += '<div class="tv-subhead alert">OVERDUE</div>'
        today_html += ''.join(_tv_job_card(j, techs, locations, 'Overdue') for j in overdue[:3])
        if len(overdue) > 3:
            today_html += f'<div class="tv-more">+{len(overdue) - 3} more overdue</div>'
    today_html += '<div class="tv-subhead">TODAY’S WORK</div>'
    if today_jobs:
        today_html += ''.join(_tv_job_card(j, techs, locations) for j in today_jobs[:6])
        if len(today_jobs) > 6:
            today_html += f'<div class="tv-more">+{len(today_jobs) - 6} more today</div>'
    elif not overdue:
        today_html += '<div class="tv-empty">Nothing scheduled today. Clear runway.</div>'

    week_html = ''
    for offset in range(7):
        day = today + datetime.timedelta(days=offset)
        day_jobs = sorted([j for j in week_jobs if _tv_date(j) == day], key=_tv_sort_key)
        day_label = 'TODAY' if offset == 0 else day.strftime('%a').upper()
        if day_jobs:
            shown = ' · '.join(_tv_esc(j.get('title', 'Untitled Job')) for j in day_jobs[:2])
            if len(day_jobs) > 2:
                shown += f' <span class="tv-day-empty">+{len(day_jobs) - 2}</span>'
            count = len(day_jobs)
        else:
            shown = '<span class="tv-day-empty">—</span>'
            count = 0
        week_html += (f'<div class="tv-day"><div><div class="tv-day-name">{day_label}</div>'
                      f'<div class="tv-day-date">{day.strftime("%b %d")}</div></div>'
                      f'<div class="tv-day-jobs">{shown}<div class="tv-day-date">{count} job{"s" if count != 1 else ""}</div></div></div>')

    attention_html = ''.join(
        _tv_job_card(job, techs, locations, reason) for job, reason in attention[:4]
    ) or '<div class="tv-empty">Nothing needs immediate attention.</div>'
    if len(attention) > 4:
        attention_html += f'<div class="tv-more">+{len(attention) - 4} more to review</div>'

    team_html = ''
    for tech in tech_records:
        assigned = sorted([j for j in active if j.get('techId') == tech.get('id')], key=_tv_sort_key)
        next_title = assigned[0].get('title', '') if assigned else 'No active assignments'
        width = min(100, len(assigned) * 20)
        team_html += (f'<div class="tv-team-row"><div class="tv-team-head"><strong>{_tv_esc(tech.get("name"))}</strong>'
                      f'<span class="tv-team-count">{len(assigned)} active</span></div>'
                      f'<div class="tv-team-next">{_tv_esc(next_title)}</div>'
                      f'<div class="tv-bar"><div class="tv-bar-fill" style="width:{width}%"></div></div></div>')
    if not tech_records:
        team_html = '<div class="tv-empty">No technicians configured.</div>'

    future_html = ''.join(_tv_job_card(j, techs, locations, future=True) for j in future_jobs[:6])
    if not future_html:
        future_html = '<div class="tv-empty">No work scheduled beyond this week.</div>'
    elif len(future_jobs) > 6:
        future_html += f'<div class="tv-more">+{len(future_jobs) - 6} more in the future</div>'

    logo_uri = get_logo_data_uri()
    brand = (f'<img src="{logo_uri}" alt="5G Security">' if logo_uri
             else '<div class="tv-brand-name">5G SECURITY</div>')
    updated = generated.strftime('%I:%M %p').lstrip('0')

    return f'''{TV_CSS}
    <div class="tv-shell">
      <div class="tv-header">
        <div class="tv-brand">{brand}<div><div class="tv-title">OPERATIONS COMMAND CENTER</div></div></div>
        <div class="tv-clock"><div class="tv-clock-date">{generated.strftime('%A, %B %d, %Y')}</div><div class="tv-clock-time">{generated.strftime('%I:%M %p')}</div></div>
      </div>
      <div class="tv-kpis">{kpi_html}</div>
      <div class="tv-main">
        <section class="tv-panel"><div class="tv-panel-title"><h2>TODAY</h2><span class="tv-count">{len(today_jobs)} scheduled · {len(overdue)} overdue</span></div>{today_html}</section>
        <section class="tv-panel"><div class="tv-panel-title"><h2>THIS WEEK</h2><span class="tv-count">{len(week_jobs)} jobs · 7 days</span></div>{week_html}</section>
        <section class="tv-panel">
          <div class="tv-panel-title"><h2>NEEDS ATTENTION</h2><span class="tv-count">{len(attention)}</span></div>
          <div class="tv-attention">{attention_html}</div>
          <div class="tv-panel-title" style="margin-top:16px"><h2>TEAM LOAD</h2><span class="tv-count">{len(tech_records)} techs</span></div>
          {team_html}
        </section>
      </div>
      <section class="tv-panel" style="margin-top:16px"><div class="tv-panel-title"><h2>LOOKING AHEAD</h2><span class="tv-count">{len(future_jobs)} jobs beyond this week</span></div><div class="tv-future">{future_html}</div></section>
      <div class="tv-footer"><span>LIVE OPERATIONS · READ ONLY</span><span>Updated {updated}</span></div>
    </div>'''


@st.fragment(run_every="30s")
def _tv_dashboard():
    try:
        db_ver = get_db_version()
        if db_ver is not None and db_ver != st.session_state.get('_db_version'):
            refresh_session_from_db()
    except Exception:
        pass

    st.markdown(
        _tv_dashboard_html(
            list(st.session_state.get('jobs', [])),
            list(st.session_state.get('techs', [])),
            list(st.session_state.get('locations', [])),
        ),
        unsafe_allow_html=True,
    )


def render_tv_display(exitable=False):
    """Single-screen, read-only 4K operations dashboard for an always-on TV."""
    if exitable:
        if st.button("✕ Exit Display Mode", key="tv_exit"):
            st.session_state.kiosk_mode = False
            st.rerun()
    _tv_dashboard()
