import time
import datetime

import streamlit as st

from core import (
    now_local, get_status_color, PRIORITY_COLORS, get_tech, get_location,
    get_logo_data_uri, refresh_session_from_db,
)


def _load_map_libs():
    """Lazy-load folium (heavy); returns (folium, st_folium) or (None, None)."""
    try:
        import folium
        from streamlit_folium import st_folium
        return folium, st_folium
    except ImportError:
        return None, None


# TV rotation: the wall display cycles through these screens
TV_VIEWS = [("board", "Operations Board"), ("schedule", "Schedule"), ("map", "Job Map")]

def _tv_esc(s):
    return str(s if s is not None else "").replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def _tv_tile(j, badge=None):
    jtech = get_tech(j.get('techId'))
    jloc = get_location(j.get('locationId'))
    p_color = PRIORITY_COLORS.get(j.get('priority'), "#52525b")
    badge_html = (f'<div style="font-size:13px;color:#f87171;font-weight:bold;margin-top:3px;">{badge}</div>'
                  if badge else '')
    return (f'<div style="background:#0f0f11;border-left:5px solid {p_color};border-radius:6px;padding:9px 11px;margin-bottom:8px;">'
            f'<div style="font-size:17px;font-weight:bold;color:#fff;">{_tv_esc(j.get("title", "")[:34])}</div>'
            f'<div style="font-size:14px;color:#a1a1aa;margin-top:3px;">📍 {_tv_esc((jloc["name"] if jloc else "—")[:26])}</div>'
            f'<div style="font-size:14px;color:#71717a;">👤 {_tv_esc(jtech["name"] if jtech else "Unassigned")}</div>{badge_html}</div>')

def _tv_columns(columns_data, cap=8):
    """columns_data: list of (label, header_color, jobs, optional badge_fn)."""
    col_html = ""
    for entry in columns_data:
        label, color, s_jobs = entry[0], entry[1], entry[2]
        badge_fn = entry[3] if len(entry) > 3 else None
        tiles = "".join(_tv_tile(j, badge_fn(j) if badge_fn else None) for j in s_jobs[:cap])
        if len(s_jobs) > cap:
            tiles += f'<div style="color:#71717a;font-size:14px;">+{len(s_jobs) - cap} more</div>'
        if not s_jobs:
            tiles = '<div style="color:#52525b;font-size:14px;">—</div>'
        col_html += (f'<div style="flex:1;min-width:0;">'
                     f'<div style="background:{color};color:#fff;font-size:15px;font-weight:bold;padding:8px 10px;border-radius:8px 8px 0 0;text-align:center;letter-spacing:0.5px;">'
                     f'{label} ({len(s_jobs)})</div>'
                     f'<div style="background:#18181b;border:1px solid #27272a;border-top:none;border-radius:0 0 8px 8px;padding:10px;min-height:120px;">{tiles}</div></div>')
    return f'<div style="display:flex;gap:12px;align-items:flex-start;">{col_html}</div>'

@st.fragment(run_every="20s")
def _tv_board():
    """Auto-refreshing, rotating wall display. Each ~20s refresh re-reads the DB
    and advances to the next screen (board -> schedule -> attention)."""
    try:
        refresh_session_from_db()
    except Exception:
        pass

    # Advance rotation on a TIME basis (not per-run) so the interactive map
    # component's postbacks can't skip a screen. ~20s dwell per screen.
    if time.time() - st.session_state.get('tv_last_rotate', 0) >= 18:
        st.session_state.tv_view_idx = (st.session_state.get('tv_view_idx', -1) + 1) % len(TV_VIEWS)
        st.session_state.tv_last_rotate = time.time()
    idx = st.session_state.get('tv_view_idx', 0)
    view_key, view_label = TV_VIEWS[idx]

    jobs = list(st.session_state.jobs)
    active = [j for j in jobs if j.get('status') != 'Completed']
    today_str = now_local().strftime('%Y-%m-%d')
    completed_today = [j for j in jobs if j.get('status') == 'Completed' and any(
        r.get('timestamp', '').startswith(today_str) and 'completion_checklist' in r for r in j.get('reports', []))]
    in_progress = [j for j in active if j.get('status') == 'In Progress']
    crit = [j for j in active if j.get('priority') in ('Critical', 'High')]

    # Header: logo/title + live clock (persistent across all screens)
    logo_uri = get_logo_data_uri()
    brand = (f'<img src="{logo_uri}" style="height:54px;">' if logo_uri
             else '<span style="font-size:38px;font-weight:bold;color:#fff;letter-spacing:2px;">5G SECURITY</span>')
    clock = now_local().strftime('%A, %b %d  ·  %I:%M %p').replace(' 0', ' ')
    st.markdown(
        f'<div style="display:flex;justify-content:space-between;align-items:center;border-bottom:4px solid #b91c1c;padding-bottom:14px;margin-bottom:18px;">'
        f'<div>{brand}<div style="color:#a1a1aa;font-size:18px;margin-top:4px;">{view_label}</div></div>'
        f'<div style="text-align:right;color:#e4e4e7;font-size:26px;font-weight:bold;">{clock}</div>'
        f'</div>', unsafe_allow_html=True)

    # Big stat tiles (persistent across all screens)
    stats = [("ACTIVE JOBS", len(active), "#e4e4e7"), ("CRITICAL / HIGH", len(crit), "#ef4444"),
             ("IN PROGRESS", len(in_progress), "#3b82f6"), ("COMPLETED TODAY", len(completed_today), "#10b981")]
    cards = "".join(
        f'<div style="flex:1;background:#18181b;border:1px solid #27272a;border-radius:12px;padding:18px;text-align:center;">'
        f'<div style="font-size:52px;font-weight:bold;color:{c};line-height:1;">{v}</div>'
        f'<div style="font-size:15px;color:#a1a1aa;margin-top:8px;letter-spacing:1px;">{lbl}</div></div>'
        for lbl, v, c in stats)
    st.markdown(f'<div style="display:flex;gap:14px;margin-bottom:22px;">{cards}</div>', unsafe_allow_html=True)

    # --- Rotating content ---
    if view_key == "board":
        # Every active status needs a column — anything missing here is invisible
        # on the wall display rather than merely out of place.
        board_statuses = ["Not Started", "In Progress", "Parts not ordered",
                          "Waiting on Parts", "Parts Staged", "Customer on Hold"]
        cols = []
        for status in board_statuses:
            if status == "Not Started":
                s_jobs = [j for j in active if j.get('status') in ("Not Started", "Pending")]
            else:
                s_jobs = [j for j in active if j.get('status') == status]
            cols.append((status.upper(), get_status_color(status), s_jobs))
        st.markdown(_tv_columns(cols), unsafe_allow_html=True)

    elif view_key == "schedule":
        today_d = now_local().date()
        def _bucket(j):
            try:
                d = datetime.datetime.fromisoformat(j['date'][:19]).date()
            except (ValueError, TypeError, KeyError):
                return "later"
            if d <= today_d:
                return "today"
            if d == today_d + datetime.timedelta(days=1):
                return "tomorrow"
            if d <= today_d + datetime.timedelta(days=7):
                return "week"
            return "later"
        buckets = {"today": [], "tomorrow": [], "week": [], "later": []}
        for j in active:
            buckets[_bucket(j)].append(j)
        for k in buckets:
            buckets[k].sort(key=lambda j: str(j.get('date', '')))
        st.markdown(_tv_columns([
            ("TODAY / OVERDUE", "#b91c1c", buckets["today"]),
            ("TOMORROW", "#3b82f6", buckets["tomorrow"]),
            ("THIS WEEK", "#52525b", buckets["week"]),
            ("LATER", "#3f3f46", buckets["later"]),
        ]), unsafe_allow_html=True)

    else:  # map — job dots across the TX/NM area
        # Only plot jobs whose location is already geocoded; never geocode or write
        # to the DB from the kiosk (it bypasses auth and runs unattended).
        map_points = []
        for j in active:
            jloc = get_location(j.get('locationId'))
            if not jloc:
                continue
            lat, lon = jloc.get('lat'), jloc.get('lon')
            try:
                lat = float(lat) if lat is not None else None
                lon = float(lon) if lon is not None else None
            except (ValueError, TypeError):
                lat = lon = None
            if lat and lon:
                map_points.append((j, lat, lon))

        folium, st_folium = _load_map_libs()
        if folium and map_points:
            avg_lat = sum(p[1] for p in map_points) / len(map_points)
            avg_lon = sum(p[2] for p in map_points) / len(map_points)
            fmap = folium.Map(location=[avg_lat, avg_lon], zoom_start=6, tiles="CartoDB dark_matter")
            coord_seen = {}
            for j, lat, lon in map_points:
                ckey = (round(lat, 5), round(lon, 5))
                n = coord_seen.get(ckey, 0)
                coord_seen[ckey] = n + 1
                if n:
                    lat += 0.0005 * n
                    lon += 0.0005 * n
                folium.CircleMarker(
                    location=[lat, lon], radius=10, color="#000000", weight=1,
                    fill=True, fill_color=get_status_color(j['status']), fill_opacity=0.95,
                    tooltip=j['title'],
                ).add_to(fmap)
            st_folium(fmap, use_container_width=True, height=470, returned_objects=[], key="tv_map")
        elif not folium:
            st.markdown('<div style="text-align:center;color:#71717a;font-size:22px;margin-top:40px;">Map unavailable (folium not installed).</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="text-align:center;color:#52525b;font-size:22px;margin-top:40px;">No mapped jobs yet.</div>', unsafe_allow_html=True)

    # Footer: rotation indicator + last-updated
    dots = "".join(
        f'<span style="color:{"#b91c1c" if i == idx else "#3f3f46"};font-size:16px;margin:0 3px;">●</span>'
        for i in range(len(TV_VIEWS)))
    st.markdown(
        f'<div style="display:flex;justify-content:space-between;align-items:center;margin-top:16px;color:#52525b;font-size:13px;">'
        f'<div>{dots}</div>'
        f'<div>Rotating every 20s · Updated {now_local().strftime("%I:%M %p").lstrip("0")}</div>'
        f'</div>', unsafe_allow_html=True)


def render_tv_display(exitable=False):
    """Full-screen, read-only wall/TV board. No sensitive data (no credentials,
    no contract values) — safe for an always-on display."""
    st.markdown("""
        <style>
        [data-testid="stToolbar"], #MainMenu, header, footer,
        [data-testid="stSidebar"], [data-testid="stSidebarCollapsedControl"] { display: none !important; }
        [data-testid="stAppViewContainer"] .block-container { padding: 1.5rem 2rem !important; max-width: 100% !important; }
        .stApp { background-color: #09090b; }
        </style>
    """, unsafe_allow_html=True)
    if exitable:
        if st.button("✕ Exit Display Mode"):
            st.session_state.kiosk_mode = False
            st.rerun()
    _tv_board()
