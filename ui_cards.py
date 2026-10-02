import urllib.parse

import streamlit as st

from core import (
    esc_html, get_status_color, SEMANTIC, PRIORITY_COLORS, format_money,
    get_job_stale_days, STALE_JOB_DAYS, parts_summary, invoice_status,
    INVOICE_STATUS_ICONS, INVOICE_STATUS_COLORS, get_tech, get_location,
    save_state, update_job_status_callback, job_followup, get_google_maps_url,
)
from services_geo import get_lat_lon_from_address
from ui_dialogs import job_details_dialog, edit_job_dialog


def _load_map_libs():
    """Lazy-load folium: it and its deps cost real startup time and only pay
    for themselves when a map actually renders. Returns (folium, st_folium)
    or (None, None) when unavailable."""
    try:
        import folium
        from streamlit_folium import st_folium
        return folium, st_folium
    except ImportError:
        return None, None


def render_job_card(job, compact=False, key_suffix="", allow_delete=False):
    tech = get_tech(job['techId'])
    loc = get_location(job['locationId'])
    loc_name = loc['name'] if loc else "Unknown"
    tech_name = tech['name'] if tech else "Unassigned"
    
    priority_class = f"priority-{job['priority']}"
    status_bg = get_status_color(job['status'])
    
    map_url = loc.get('mapsUrl') or get_google_maps_url(loc['address']) if loc else None
    loc_html = (f'<a href="{esc_html(map_url)}" target="_blank" title="{esc_html(loc_name)}" '
                f'style="color:#a1a1aa; text-decoration:none;">📍 {esc_html(loc_name)}</a>'
                if map_url else f'<span title="{esc_html(loc_name)}">📍 {esc_html(loc_name)}</span>')

    # Quote value, if one has been entered (small, right of the site name)
    _qv = format_money(job.get('quoteValue'))
    quote_html = (f'<span style="color:#a1a1aa; font-size:0.8em; white-space:nowrap;">{esc_html(_qv)}</span>'
                  if _qv else "")

    # Compact chips instead of a stack of alert lines. Each feature used to add its
    # own full-width coloured line (stale / follow-up / parts / invoice), so a busy
    # job grew a four-line wall. These sit inline on the tech/date row (see below),
    # so a card with signals is the same height as one without.
    signals = []

    _fu = job_followup(job)
    if _fu:
        _days, _thr, _action = _fu
        signals.append((f'⏳ {_days}d waiting',
                        SEMANTIC["act"] if _days >= _thr * 2 else SEMANTIC["waiting"]))

    # Only when there's no follow-up chip already: a job flagged "Customer on Hold
    # 12 days" is self-evidently quiet, so showing both is noise — and two long
    # chips plus the date won't fit on one row.
    stale_days = get_job_stale_days(job)
    if not _fu and stale_days is not None and stale_days >= STALE_JOB_DAYS:
        signals.append((f'🚨 {stale_days}d quiet', SEMANTIC["act"]))

    staged_parts, total_parts = parts_summary(job)
    if total_parts:
        signals.append((f'🔩 {staged_parts}/{total_parts}',
                        SEMANTIC["done"] if staged_parts == total_parts else "#a1a1aa"))

    _inv = invoice_status(job)   # None unless the job is Completed
    if _inv:
        _short = {"Ready to Invoice": "To invoice", "No Charge": "No charge"}.get(_inv, _inv)
        signals.append((f'{INVOICE_STATUS_ICONS.get(_inv, "")} {_short}',
                        INVOICE_STATUS_COLORS.get(_inv, SEMANTIC["neutral"])))

    # Chips sit under the priority badge, inside vertical space the 2-line title
    # clamp already reserves — so a card with signals is exactly as tall as one
    # without, and the tech name keeps its own full-width row below.
    signal_chips = "".join(
        f'<span style="background:#27272a;color:{c};font-size:0.72em;'
        f'padding:1px 6px;border-radius:4px;white-space:nowrap;">{t}</span>'
        for t, c in signals)
    signal_stack = (f'<span style="display:flex; gap:4px;">{signal_chips}</span>'
                    if signal_chips else "")

    with st.container():
        st.markdown(f"""
        <div class="job-card {priority_class}" style="position:relative; overflow:hidden; border-top: 4px solid {status_bg};">
            <div style="position:absolute; top:0; right:0; padding:2px 8px; background:{status_bg}; color:white; font-size:0.65em; font-weight:bold; border-bottom-left-radius:8px;">
                {esc_html(job['status']).upper()}
            </div>
            <div style="display:flex; justify-content:space-between; align-items:flex-start; gap:6px; margin-top:10px;">
                <span title="{esc_html(job['title'])}" style="font-weight:bold; font-size:1.1em; min-width:0; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; line-height:1.3; height:2.6em;">{esc_html(job['title'])}</span>
                <span style="display:flex; flex-direction:column; align-items:flex-end; gap:4px; flex-shrink:0;"><span style="font-size:0.8em; background:#3f3f46; padding:2px 6px; border-radius:4px;">{esc_html(job['priority'])}</span>{signal_stack}</span>
            </div>
            <div style="display:flex; justify-content:space-between; align-items:baseline; gap:8px; margin-top:5px;"><span style="color:#a1a1aa; font-size:0.9em; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{loc_html}</span>{quote_html}</div>
            <div style="display:flex; justify-content:space-between; align-items:center; gap:8px; flex-wrap:nowrap; margin-top:10px; font-size:0.8em; color:#71717a;">
                 <span title="{esc_html(tech_name)}" style="min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">👤 {esc_html(tech_name)}</span>
                 <span style="white-space:nowrap; flex-shrink:0;">📅 {esc_html(str(job.get('date', ''))[:10])}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        # Status Dropdown
        status_options = ["Not Started", "In Progress", "Customer on Hold", "Waiting on Parts", "Parts not ordered", "Parts Staged", "Completed"]
        current_status = job['status']
        if current_status == "Pending": current_status = "Not Started"
        
        try:
            status_idx = status_options.index(current_status)
        except ValueError:
            status_idx = 0
            
        widget_key = f"status_change_{job['id']}_{key_suffix}"

        def _delete_job():
            if job in st.session_state.jobs:
                st.session_state.jobs.remove(job)
                save_state()
                st.rerun()

        if compact:
            # Narrow columns (Tech Board / feeds): dropdown on its own row, then
            # the button row — tolerates 100% zoom without squishing text.
            st.selectbox(
                "Change Status", status_options, index=status_idx, key=widget_key,
                on_change=update_job_status_callback, args=(job['id'], widget_key),
                label_visibility="collapsed")
            if allow_delete:
                b1, b2, b3 = st.columns([3, 1, 1])
                with b1:
                    if st.button("Details", key=f"btn_{job['id']}_{key_suffix}", use_container_width=True):
                        job_details_dialog(job['id'])
                with b2:
                    if st.button(":material/edit:", key=f"edit_{job['id']}_{key_suffix}", help="Edit Job", use_container_width=True):
                        edit_job_dialog(job['id'])
                with b3:
                    if st.button(":material/delete:", key=f"del_{job['id']}_{key_suffix}", help="Delete Job", use_container_width=True):
                        _delete_job()
            else:
                if st.button("Details", key=f"btn_{job['id']}_{key_suffix}", use_container_width=True):
                    job_details_dialog(job['id'])
        else:
            # Wide cards (3-col grid pages): everything in one inline row
            if allow_delete:
                f1, f2, f3, f4 = st.columns([3, 2.2, 0.9, 0.9])
            else:
                f1, f2 = st.columns([3, 2.2])
                f3 = f4 = None

            with f1:
                st.selectbox(
                    "Change Status", status_options, index=status_idx, key=widget_key,
                    on_change=update_job_status_callback, args=(job['id'], widget_key),
                    label_visibility="collapsed")
            with f2:
                if st.button("Details", key=f"btn_{job['id']}_{key_suffix}", use_container_width=True):
                    job_details_dialog(job['id'])
            if f3 is not None:
                with f3:
                    if st.button(":material/edit:", key=f"edit_{job['id']}_{key_suffix}", help="Edit Job", use_container_width=True):
                        edit_job_dialog(job['id'])
                with f4:
                    if st.button(":material/delete:", key=f"del_{job['id']}_{key_suffix}", help="Delete Job", use_container_width=True):
                        _delete_job()


def render_job_grid(jobs, key_suffix="", allow_delete=False, cols=3):
    """Full job cards in a 3-up grid (Streamlit stacks columns on phones, so
    mobile keeps the familiar single-column feed)."""
    if not jobs:
        return
    columns = st.columns(cols)
    for i, job in enumerate(jobs):
        with columns[i % cols]:
            render_job_card(job, key_suffix=key_suffix, allow_delete=allow_delete)


def render_map_view(jobs):
    """Interactive Folium map: one dot per job at its location, colored by status
    (same palette as the Tech Board). Click a dot for a detail card + Navigate link."""
    folium, st_folium = _load_map_libs()
    if not folium:
        st.info("🗺️ Map view needs the `folium` and `streamlit-folium` packages. "
                "Add them to requirements.txt and redeploy.")
        return

    def _m_esc(s):
        return (str(s if s is not None else "").replace('&', '&amp;')
                .replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;'))

    # Status legend (matches the Tech Board columns)
    legend_statuses = ["Not Started", "In Progress", "Customer on Hold",
                       "Waiting on Parts", "Parts not ordered", "Parts Staged"]
    legend = '<div style="display:flex;gap:14px;flex-wrap:wrap;margin-bottom:8px;font-size:0.8em;color:#a1a1aa;">'
    for sname in legend_statuses:
        legend += (f'<span style="display:inline-flex;align-items:center;gap:5px;">'
                   f'<span style="width:11px;height:11px;border-radius:50%;background:{get_status_color(sname)};'
                   f'display:inline-block;"></span>{sname}</span>')
    legend += '</div>'
    st.markdown(legend, unsafe_allow_html=True)

    # Resolve a lat/lon for each job, geocoding any location that lacks one (then persist)
    points = []
    skipped = 0
    geocoded_any = False
    with st.spinner("Locating jobs..."):
        for job in jobs:
            loc = get_location(job['locationId'])
            if not loc or not loc.get('address'):
                skipped += 1
                continue
            lat, lon = loc.get('lat'), loc.get('lon')
            try:
                lat = float(lat) if lat is not None else None
                lon = float(lon) if lon is not None else None
            except (ValueError, TypeError):
                lat = lon = None
            if not lat or not lon:
                lat, lon = get_lat_lon_from_address(loc['address'])
                if lat and lon:
                    loc['lat'], loc['lon'] = lat, lon
                    geocoded_any = True
            if lat and lon:
                points.append((job, loc, lat, lon))
            else:
                skipped += 1
    if geocoded_any:
        save_state(invalidate_briefing=False)

    if not points:
        st.info("No mappable jobs yet — none of the active jobs have a geocodable address.")
        return

    # Center on the middle of the actual jobs, but at a FIXED regional zoom so the
    # default view frames the TX / NM operating area (never zoomed to the world or
    # jammed into a single cluster). Users can still pan/zoom freely from there.
    avg_lat = sum(p[2] for p in points) / len(points)
    avg_lon = sum(p[3] for p in points) / len(points)
    fmap = folium.Map(location=[avg_lat, avg_lon], zoom_start=6, tiles="CartoDB positron")

    # Nudge markers that share exact coordinates so they don't fully overlap
    coord_seen = {}
    for job, loc, lat, lon in points:
        key = (round(lat, 5), round(lon, 5))
        n = coord_seen.get(key, 0)
        coord_seen[key] = n + 1
        if n:
            lat += 0.0005 * n
            lon += 0.0005 * n

        color = get_status_color(job['status'])
        jtech = get_tech(job['techId'])
        nav_url = f"https://www.google.com/maps/dir/?api=1&destination={urllib.parse.quote(loc['address'])}"

        popup_html = (
            f'<div style="font-family:Arial,sans-serif;width:230px;">'
            f'<div style="font-weight:bold;font-size:14px;color:#18181b;margin-bottom:5px;">{_m_esc(job["title"])}</div>'
            f'<span style="background:{color};color:white;padding:1px 8px;border-radius:8px;font-size:11px;">{_m_esc(job["status"])}</span> '
            f'<span style="background:#3f3f46;color:white;padding:1px 8px;border-radius:8px;font-size:11px;">{_m_esc(job.get("priority", "N/A"))}</span>'
            f'<div style="font-size:12px;color:#333;margin-top:7px;">📍 <b>{_m_esc(loc["name"])}</b><br>{_m_esc(loc["address"])}</div>'
            f'<div style="font-size:12px;color:#333;margin-top:4px;">👤 {_m_esc(jtech["name"] if jtech else "Unassigned")}</div>'
            f'<a href="{nav_url}" target="_blank" style="display:inline-block;margin-top:9px;background:#b91c1c;'
            f'color:white;padding:6px 14px;border-radius:6px;text-decoration:none;font-size:12px;font-weight:bold;">🧭 Navigate</a>'
            f'</div>'
        )

        folium.CircleMarker(
            location=[lat, lon],
            radius=9, color="#27272a", weight=1.5,
            fill=True, fill_color=color, fill_opacity=0.9,
            tooltip=job['title'],
            popup=folium.Popup(popup_html, max_width=260),
        ).add_to(fmap)

    # returned_objects=[] keeps panning/clicking from triggering heavy app reruns
    st_folium(fmap, use_container_width=True, height=600, returned_objects=[], key="jobs_map")

    if skipped:
        st.caption(f"⚠️ {skipped} job(s) not shown — address missing or could not be geocoded.")

