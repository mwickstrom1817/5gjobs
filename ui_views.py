import json
import datetime

import pandas as pd
import streamlit as st

from core import (
    now_local, save_state, get_tech, get_location, get_logger, format_money,
    parts_summary, invoice_status, job_invoice, set_job_invoice,
    all_assets, asset_warranty_left,
    INVOICE_STATUSES, INVOICE_STATUS_COLORS, INVOICE_STATUS_ICONS,
    agreement_days_left, AGREEMENT_TYPES, BILLING_CYCLES,
    AGREEMENT_RENEWAL_DAYS, job_value_summary, compute_hours_rows,
    save_document_locally, resolve_image_source, esc_html, job_status_since,
)
from services_ai import get_api_key, get_available_model
from ui_dialogs import job_details_dialog
from ui_widgets import sub_nav

# --- SOPs (reference library: written procedures techs read in the field) -----
# Seed categories only — the picker also offers whatever categories already exist
# plus a free-text box, so this list never has to be edited to add one.
SOP_SEED_CATEGORIES = ["Cameras / NVR", "Access Control", "Alarms", "Cabling",
                       "Safety", "Company"]

def sop_categories():
    """Seed categories plus any in use, alphabetical."""
    used = {(s.get('category') or '').strip() for s in st.session_state.get('sops', [])}
    return sorted({c for c in (set(SOP_SEED_CATEGORIES) | used) if c})

def sop_matches(sop, q):
    """Case-insensitive match across title, category, steps, notes, attachment names."""
    if not q:
        return True
    hay = " ".join([
        sop.get('title', ''), sop.get('category', ''), sop.get('notes', ''),
        " ".join(sop.get('steps') or []),
        " ".join(a.get('name', '') for a in (sop.get('attachments') or [])),
    ]).lower()
    return q.lower() in hay

def _sop_form(sop, key_prefix, on_save, submit_label):
    """Shared add/edit form. `sop` seeds the fields; on_save(dict) persists."""
    cats = sop_categories()
    cur_cat = sop.get('category', '')
    with st.form(key=f"{key_prefix}_form"):
        title = st.text_input("Title", value=sop.get('title', ''),
                              placeholder="e.g. Commissioning a Hikvision NVR")
        c1, c2 = st.columns(2)
        cat_options = cats + ["➕ New category..."]
        cat_idx = cat_options.index(cur_cat) if cur_cat in cat_options else 0
        picked = c1.selectbox("Category", cat_options, index=cat_idx)
        new_cat = c2.text_input("New category name", value="" if picked != "➕ New category..." else cur_cat,
                                placeholder="Only if adding a new one")
        steps_txt = st.text_area(
            "Steps (one per line)", value="\n".join(sop.get('steps') or []), height=220,
            placeholder="Confirm the model matches the quote\nSet a static IP outside the DHCP pool\n...")
        notes = st.text_area("Notes / cautions (optional)", value=sop.get('notes', ''), height=90)
        files = st.file_uploader("Attachments (datasheets, diagrams)", accept_multiple_files=True,
                                 type=['pdf', 'jpg', 'jpeg', 'png'], key=f"{key_prefix}_files")
        if st.form_submit_button(submit_label):
            final_cat = (new_cat.strip() or (picked if picked != "➕ New category..." else "")).strip()
            if not title.strip():
                st.error("Title is required.")
                return
            atts = list(sop.get('attachments') or [])
            for f in (files or []):
                k = save_document_locally(f)
                if k:
                    atts.append({"name": f.name, "key": k})
            on_save({
                "title": title.strip(),
                "category": final_cat,
                "steps": [ln.strip() for ln in steps_txt.splitlines() if ln.strip()],
                "notes": notes.strip(),
                "attachments": atts,
            })

def render_sops_view(is_admin):
    st.subheader("📚 Standard Operating Procedures")
    st.caption("Reference procedures for the field. "
               + ("You can add and edit these." if is_admin
                  else "Read-only — ask an admin to add or change a procedure."))

    sops = st.session_state.get('sops', [])

    if is_admin:
        with st.expander("➕ New Procedure", expanded=not sops):
            def _create(vals):
                vals.update({
                    "id": f"sop{now_local().timestamp()}",
                    "updated_by": st.session_state.user_info.get('email', '') if "user_info" in st.session_state else '',
                    "updated_at": now_local().isoformat(),
                })
                st.session_state.sops.append(vals)
                save_state(invalidate_briefing=False)
                st.toast(f"Added '{vals['title']}'", icon="✅")
                st.rerun()
            _sop_form({}, "new_sop", _create, "Add Procedure")

    if not sops:
        st.info("No procedures yet." + (" Add the first one above." if is_admin else ""))
        return

    f1, f2 = st.columns([3, 2])
    q = f1.text_input("Search", key="sop_q", label_visibility="collapsed",
                      placeholder="🔍 Search procedures, steps, attachments...")
    cat_pick = f2.selectbox("Category", ["All categories"] + sop_categories(),
                            key="sop_cat", label_visibility="collapsed")

    shown = [s for s in sops
             if sop_matches(s, q)
             and (cat_pick == "All categories" or s.get('category') == cat_pick)]
    shown.sort(key=lambda s: ((s.get('category') or '~'), s.get('title', '')))

    st.caption(f"{len(shown)} of {len(sops)} procedure(s)"
               + (f' matching "{q}"' if q else ""))
    if not shown:
        st.info("Nothing matches those filters.")
        return

    for s in shown:
        steps = s.get('steps') or []
        atts = s.get('attachments') or []
        meta = f"{len(steps)} step(s) · {len(atts)} attachment(s)"
        if s.get('updated_at'):
            meta += f" · updated {str(s['updated_at'])[:10]}"
            if s.get('updated_by'):
                meta += f" by {s['updated_by']}"

        label = f"{s.get('title', 'Untitled')}"
        if s.get('category'):
            label += f"  ·  {s['category']}"
        with st.expander(label, expanded=bool(q) and len(shown) <= 3):
            st.caption(meta)
            if steps:
                for i, stp in enumerate(steps, 1):
                    st.markdown(f"**{i}.** {stp}")
            else:
                st.caption("No steps recorded.")

            if s.get('notes'):
                st.info(s['notes'])

            if atts:
                st.write("**Attachments**")
                for i, a in enumerate(atts):
                    url = resolve_image_source(a.get('key'))
                    ac1, ac2 = st.columns([3, 1])
                    ac1.write(f"📎 {a.get('name', 'file')}")
                    if url:
                        ac2.link_button("👁️ View", url, use_container_width=True)
                    ext = str(a.get('name', '')).lower().split('.')[-1]
                    if url and ext in ('jpg', 'jpeg', 'png'):
                        st.image(url, width=320)

            if is_admin:
                st.divider()
                ec1, ec2 = st.columns([1, 1])
                edit_key = f"sop_editing_{s['id']}"
                if ec1.button("✏️ Edit", key=f"sop_edit_btn_{s['id']}", use_container_width=True):
                    st.session_state[edit_key] = not st.session_state.get(edit_key, False)
                    st.rerun()
                if ec2.button("🗑️ Delete", key=f"sop_del_{s['id']}", use_container_width=True):
                    st.session_state.sops = [x for x in st.session_state.sops if x['id'] != s['id']]
                    save_state(invalidate_briefing=False)
                    st.toast(f"Deleted '{s.get('title', '')}'", icon="🗑️")
                    st.rerun()

                if st.session_state.get(edit_key):
                    def _update(vals, _sid=s['id'], _ek=edit_key):
                        for x in st.session_state.sops:
                            if x['id'] == _sid:
                                x.update(vals)
                                x['updated_by'] = st.session_state.user_info.get('email', '') if "user_info" in st.session_state else ''
                                x['updated_at'] = now_local().isoformat()
                                break
                        save_state(invalidate_briefing=False)
                        st.session_state[_ek] = False
                        st.toast("Procedure updated.", icon="✅")
                        st.rerun()
                    _sop_form(s, f"edit_sop_{s['id']}", _update, "Save Changes")


BROWSER_TABLES = ["Jobs", "Reports", "Parts", "Time", "Photos", "Invoices", "Assets", "Sites", "Techs", "SOPs"]

def _bnum(v):
    """Best-effort float (blank/garbage -> 0.0)."""
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0

def _bdate(s):
    """Best-effort date from an ISO-ish string, else None."""
    try:
        return datetime.datetime.fromisoformat(str(s)[:19]).date()
    except Exception:
        return None

def browser_rows(table):
    """Flatten the in-memory state into display rows for the data browser.

    Every row carries hidden _date/_tech/_site/_job_id keys used for filtering and
    row-click, which are stripped before display. Site systems/credentials are
    deliberately NOT exposed here — they hold customer passwords and IPs."""
    rows = []
    jobs = st.session_state.jobs

    if table == "Jobs":
        for j in jobs:
            loc, tech = get_location(j.get('locationId')), get_tech(j.get('techId'))
            staged, total = parts_summary(j)
            hrs = sum(_bnum(r.get('hoursWorked')) for r in (j.get('reports') or []))
            rows.append({
                "Date": str(j.get('date', ''))[:10],
                "Title": j.get('title', ''),
                "Site": loc['name'] if loc else '',
                "Tech": tech['name'] if tech else 'Unassigned',
                "Type": j.get('type', ''),
                "Priority": j.get('priority', ''),
                "Status": j.get('status', ''),
                "Reports": len(j.get('reports') or []),
                "Hours": round(hrs, 2),
                "Parts": f"{staged}/{total}" if total else "",
                "Quote": format_money(j.get('quoteValue')),
                "Invoice Status": invoice_status(j) or "",
                "_date": _bdate(j.get('date')), "_tech": tech['name'] if tech else '',
                "_site": loc['name'] if loc else '', "_job_id": j['id'],
            })

    elif table == "Reports":
        for j in jobs:
            loc = get_location(j.get('locationId'))
            for r in (j.get('reports') or []):
                rt = get_tech(r.get('techId'))
                who = r.get('techsOnSite') or (rt['name'] if rt else '')
                rows.append({
                    "Date": str(r.get('timestamp', ''))[:10],
                    "Job": j.get('title', ''),
                    "Site": loc['name'] if loc else '',
                    "Techs On Site": who,
                    "Arrived": r.get('timeArrived', '') or '',
                    "Departed": r.get('timeDeparted', '') or '',
                    "Hours": _bnum(r.get('hoursWorked')),
                    "Parts Used": r.get('partsUsed', '') or '',
                    "Billable": r.get('billableItems', '') or '',
                    "Warranty": "Yes" if r.get('isWarranty') else "",
                    "Photos": len(r.get('photos') or []),
                    "Author": r.get('authorEmail', '') or (rt['email'] if rt else ''),
                    "Notes": (r.get('content') or '').replace("\n", " "),
                    "_date": _bdate(r.get('timestamp')), "_tech": who,
                    "_site": loc['name'] if loc else '', "_job_id": j['id'],
                })

    elif table == "Parts":
        for j in jobs:
            loc = get_location(j.get('locationId'))
            for p in (j.get('parts') or []):
                rows.append({
                    "Job": j.get('title', ''), "Site": loc['name'] if loc else '',
                    "Part": p.get('name', ''), "Qty": p.get('qty', 1),
                    "Status": p.get('status', ''), "Vendor": p.get('vendor', '') or '',
                    "Cost": p.get('cost', '') or '', "Notes": p.get('notes', '') or '',
                    "Added By": p.get('added_by', '') or '',
                    "Updated": str(p.get('updated_at', ''))[:10],
                    "_date": _bdate(p.get('updated_at')), "_tech": '',
                    "_site": loc['name'] if loc else '', "_job_id": j['id'],
                })

    elif table == "Time":
        for j in jobs:
            loc = get_location(j.get('locationId'))
            for e in (j.get('time_entries') or []):
                ci, co = e.get('clock_in'), e.get('clock_out')
                dur = ''
                try:
                    if ci and co:
                        secs = (datetime.datetime.fromisoformat(co)
                                - datetime.datetime.fromisoformat(ci)).total_seconds()
                        dur = f"{round(secs / 3600, 2)} hrs"
                except Exception:
                    dur = ''
                who = e.get('tech_name') or e.get('userEmail', '') or ''
                rows.append({
                    "Tech": who, "Job": j.get('title', ''),
                    "Site": loc['name'] if loc else '',
                    "Clock In": str(ci or '')[:16].replace('T', ' '),
                    "Clock Out": str(co or '')[:16].replace('T', ' '),
                    "Duration": dur, "Running": "Yes" if (ci and not co) else "",
                    "_date": _bdate(ci), "_tech": who,
                    "_site": loc['name'] if loc else '', "_job_id": j['id'],
                })

    elif table == "Photos":
        for j in jobs:
            loc = get_location(j.get('locationId'))
            jt = get_tech(j.get('techId'))
            for k in (j.get('photos') or []):
                rows.append({
                    "Date": str(j.get('date', ''))[:10], "Job": j.get('title', ''),
                    "Site": loc['name'] if loc else '', "Tech": jt['name'] if jt else '',
                    "Source": "Job", "Key": k,
                    "_date": _bdate(j.get('date')), "_tech": jt['name'] if jt else '',
                    "_site": loc['name'] if loc else '', "_job_id": j['id'],
                })
            for r in (j.get('reports') or []):
                rt = get_tech(r.get('techId'))
                for k in (r.get('photos') or []):
                    rows.append({
                        "Date": str(r.get('timestamp', ''))[:10], "Job": j.get('title', ''),
                        "Site": loc['name'] if loc else '', "Tech": rt['name'] if rt else '',
                        "Source": "Report", "Key": k,
                        "_date": _bdate(r.get('timestamp')), "_tech": rt['name'] if rt else '',
                        "_site": loc['name'] if loc else '', "_job_id": j['id'],
                    })

    elif table == "Invoices":
        for j in jobs:
            if j.get('status') != 'Completed':
                continue
            loc = get_location(j.get('locationId'))
            inv = job_invoice(j)
            hrs = sum(_bnum(r.get('hoursWorked')) for r in (j.get('reports') or []))
            rows.append({
                "Job": j.get('title', ''), "Site": loc['name'] if loc else '',
                "Completed": str(j.get('date', ''))[:10], "Hours": round(hrs, 2),
                "Status": inv['status'], "Invoice #": inv['number'],
                "Amount": inv['amount'], "Invoice Date": inv['date'],
                "Updated By": inv['updated_by'],
                "_date": _bdate(j.get('date')), "_tech": '',
                "_site": loc['name'] if loc else '', "_job_id": j['id'],
            })

    elif table == "Sites":
        for l in st.session_state.locations:
            l_jobs = [j for j in jobs if j.get('locationId') == l['id']]
            last = max((str(j.get('date', ''))[:10] for j in l_jobs), default='')
            rows.append({
                "Name": l.get('name', ''), "Address": l.get('address', ''),
                "Contact": l.get('contact_name', '') or '',
                "Phone": l.get('contact_phone', '') or '',
                "Jobs": len(l_jobs), "Systems": len(l.get('systems') or []),
                "Documents": len(l.get('documents') or []), "Last Visit": last,
                "_date": None, "_tech": '', "_site": l.get('name', ''), "_job_id": None,
            })

    elif table == "Assets":
        for l, a in all_assets():
            months, expiry = asset_warranty_left(a)
            src = next((j for j in jobs if j['id'] == a.get('job_id')), None)
            rows.append({
                "Tag": a.get('tag', ''), "Type": a.get('type', ''),
                "Make / Model": a.get('make_model', '') or '',
                "Serial": a.get('serial', '') or '',
                "Site": l.get('name', ''), "Where": a.get('position', '') or '',
                "Installed": str(a.get('installed_date', ''))[:10],
                "Warranty Left (mo)": "" if months is None else months,
                "Expires": "" if expiry is None else str(expiry),
                "Installed On Job": src.get('title', '') if src else '',
                "Notes": (a.get('notes') or '').replace('\n', " "),
                "_date": _bdate(a.get('installed_date')), "_tech": '',
                "_site": l.get('name', ''), "_job_id": a.get('job_id'),
            })

    elif table == "SOPs":
        for s in st.session_state.get('sops', []):
            rows.append({
                "Title": s.get('title', ''), "Category": s.get('category', ''),
                "Steps": len(s.get('steps') or []),
                "Attachments": len(s.get('attachments') or []),
                "Notes": (s.get('notes') or '').replace("\n", " "),
                "Procedure": " | ".join(s.get('steps') or []),
                "Updated": str(s.get('updated_at', ''))[:10],
                "Updated By": s.get('updated_by', '') or '',
                "_date": _bdate(s.get('updated_at')), "_tech": '',
                "_site": '', "_job_id": None,
            })

    elif table == "Techs":
        for t in st.session_state.techs:
            act = [j for j in jobs
                   if j.get('techId') == t['id'] and j.get('status') != 'Completed']
            rows.append({
                "Name": t.get('name', ''), "Email": t.get('email', ''),
                "Initials": t.get('initials', ''),
                "Skills": ", ".join(t.get('skills') or []),
                "Active Jobs": len(act),
                "_date": None, "_tech": t.get('name', ''), "_site": '', "_job_id": None,
            })

    return rows

def browser_count(table):
    """Row count without building the rows — the picker reruns on every keystroke."""
    jobs = st.session_state.jobs
    if table == "Jobs":     return len(jobs)
    if table == "Reports":  return sum(len(j.get('reports') or []) for j in jobs)
    if table == "Parts":    return sum(len(j.get('parts') or []) for j in jobs)
    if table == "Time":     return sum(len(j.get('time_entries') or []) for j in jobs)
    if table == "Photos":
        return sum(len(j.get('photos') or [])
                   + sum(len(r.get('photos') or []) for r in (j.get('reports') or []))
                   for j in jobs)
    if table == "Invoices": return sum(1 for j in jobs if j.get('status') == 'Completed')
    if table == "Sites":    return len(st.session_state.locations)
    if table == "Techs":    return len(st.session_state.techs)
    if table == "SOPs":     return len(st.session_state.get('sops', []))
    if table == "Assets":   return len(all_assets())
    return 0

def render_data_browser():
    st.subheader("🗂️ Data Browser")
    st.caption("Read-only, flattened views of everything in the database. Site systems "
               "(IPs & passwords) are deliberately excluded.")

    counts = {t: browser_count(t) for t in BROWSER_TABLES}
    _fmt_tbl = lambda t: f"{t} {counts[t]:,}"
    if hasattr(st, "segmented_control"):
        table = st.segmented_control("Table", BROWSER_TABLES, format_func=_fmt_tbl,
                                     default="Jobs", key="browser_table",
                                     label_visibility="collapsed")
    else:
        table = st.radio("Table", BROWSER_TABLES, format_func=_fmt_tbl, horizontal=True,
                         key="browser_table", label_visibility="collapsed")
    if not table:
        table = "Jobs"

    f1, f2, f3, f4 = st.columns([3, 2, 2, 2])
    q = f1.text_input("Search", key="browser_q", label_visibility="collapsed",
                      placeholder="🔍 Search anything (including report notes)...")
    range_label = f2.selectbox("Range", ["All time", "Last 30 days", "Last 90 days",
                                         "Last 12 months"], key="browser_range",
                               label_visibility="collapsed")
    tech_names = ["All techs"] + sorted({t.get('name', '') for t in st.session_state.techs if t.get('name')})
    site_names = ["All sites"] + sorted({l.get('name', '') for l in st.session_state.locations if l.get('name')})
    tech_pick = f3.selectbox("Tech", tech_names, key="browser_tech", label_visibility="collapsed")
    site_pick = f4.selectbox("Site", site_names, key="browser_site", label_visibility="collapsed")

    days = {"Last 30 days": 30, "Last 90 days": 90, "Last 12 months": 365}.get(range_label)
    cutoff = (now_local().date() - datetime.timedelta(days=days)) if days else None

    rows = browser_rows(table)
    filtered = []
    for r in rows:
        if cutoff and r.get('_date') and r['_date'] < cutoff:
            continue
        if tech_pick != "All techs":
            if tech_pick.lower() not in (r.get('_tech') or '').lower():
                continue
        if site_pick != "All sites" and (r.get('_site') or '') != site_pick:
            continue
        if q:
            hay = " ".join(str(v) for k, v in r.items() if not k.startswith('_')).lower()
            if q.lower() not in hay:
                continue
        filtered.append(r)

    if not filtered:
        st.info("Nothing matches those filters.")
        return

    disp = [{k: v for k, v in r.items() if not k.startswith('_')} for r in filtered]

    # Thumbnails only for small result sets — signing thousands of URLs is wasteful
    col_cfg = None
    if table == "Photos":
        if len(disp) <= 50:
            for d in disp:
                d["Preview"] = resolve_image_source(d.get("Key"))
            col_cfg = {"Preview": st.column_config.ImageColumn("Preview", width="small")}
        else:
            st.caption(f"{len(disp):,} photos — narrow the filters below 50 to see thumbnails.")

    df = pd.DataFrame(disp)
    st.caption(f"{len(filtered):,} of {counts[table]:,} {table.lower()} row(s)"
               + (" · click a row to open the job" if filtered[0].get('_job_id') else ""))

    selected_row = None
    try:
        event = st.dataframe(df, use_container_width=True, hide_index=True,
                             column_config=col_cfg, on_select="rerun",
                             selection_mode="single-row")
        picked = list(getattr(getattr(event, "selection", None), "rows", []) or [])
        if picked:
            selected_row = filtered[picked[0]]
    except TypeError:
        # Older Streamlit without dataframe selection support
        st.dataframe(df, use_container_width=True, hide_index=True, column_config=col_cfg)

    st.download_button(
        f"⬇️ Download {table} CSV ({len(filtered):,} rows, current filters)",
        df.to_csv(index=False).encode("utf-8"),
        file_name=f"{table.lower()}_{now_local().strftime('%Y%m%d')}.csv",
        mime="text/csv", key=f"browser_csv_{table}",
    )

    if selected_row and selected_row.get('_job_id'):
        job_details_dialog(selected_row['_job_id'])


def render_invoicing_view(user_email):
    """Office-manager worklist: every Completed job grouped by invoice status, so
    'what still needs billing' is answerable at a glance. Admins only."""
    st.subheader("💵 Invoicing")
    st.caption("Every completed job and where it sits in billing. Jobs land in "
               "'Ready to Invoice' automatically when they're marked Completed "
               "(warranty work goes straight to 'No Charge').")

    completed = [j for j in st.session_state.jobs if j.get('status') == 'Completed']
    if not completed:
        st.info("No completed jobs yet.")
        return

    buckets = {s: [] for s in INVOICE_STATUSES}
    for j in completed:
        buckets.setdefault(invoice_status(j), []).append(j)

    # Status summary chips
    chips = " ".join(
        f'<span style="background:{INVOICE_STATUS_COLORS[s]};color:white;padding:3px 12px;'
        f'border-radius:12px;font-size:0.78em;margin-right:6px;">'
        f'{INVOICE_STATUS_ICONS[s]} {len(buckets.get(s, []))} {s}</span>'
        for s in INVOICE_STATUSES
    )
    st.markdown(chips, unsafe_allow_html=True)
    # Unbilled aging: completed work waiting to be invoiced should never go stale.
    _ready = buckets.get("Ready to Invoice", [])
    _ages = [(now_local().date() - d).days for j in _ready if (d := job_status_since(j))]
    if _ages:
        st.caption(f"⏰ {len(_ages)} job(s) waiting to invoice · oldest {max(_ages)} days")
    st.write("")

    view = st.radio("Show", INVOICE_STATUSES + ["All"], horizontal=True,
                    key="invoicing_filter", label_visibility="collapsed")

    rows = completed if view == "All" else buckets.get(view, [])
    # Most recently worked first
    rows = sorted(rows, key=lambda j: (j.get('date') or ''), reverse=True)

    if not rows:
        st.success(f"Nothing sitting in '{view}'. 🎉")
        return

    st.caption(f"{len(rows)} job(s)")
    for j in rows:
        loc = get_location(j['locationId'])
        tech = get_tech(j['techId'])
        cur = invoice_status(j)
        inv = job_invoice(j)

        hrs = 0.0
        for r in (j.get('reports') or []):
            try:
                hrs += float(r.get('hoursWorked') or 0)
            except (TypeError, ValueError):
                pass

        c1, c2, c3 = st.columns([5, 2, 2])
        with c1:
            # Age of the bill: how long this job has been sitting in 'Ready to Invoice'
            _age = None
            if cur == "Ready to Invoice":
                _d = job_status_since(j)
                if _d:
                    _age = (now_local().date() - _d).days
            meta = " · ".join(x for x in [
                loc['name'] if loc else "No site",
                tech['name'] if tech else "Unassigned",
                str(j.get('date', ''))[:10],
                f"{hrs:g} hrs" if hrs else "",
                f"#{inv['number']}" if inv['number'] else "",
                # Billed amount once known, otherwise what we quoted
                (f"billed {format_money(inv['amount'])}" if inv['amount']
                 else (f"quoted {format_money(j.get('quoteValue'))}" if j.get('quoteValue') else "")),
                f"⏰ unbilled {_age}d" if (_age or 0) >= 7 else "",
            ] if x)
            st.markdown(
                f"**{esc_html(j['title'])}**<br><span style='color:#71717a;font-size:0.82em;'>{esc_html(meta)}</span>",
                unsafe_allow_html=True)
        with c2:
            st.markdown(
                f'<span style="background:{INVOICE_STATUS_COLORS.get(cur, "#52525b")};color:white;'
                f'padding:2px 10px;border-radius:10px;font-size:0.75em;">'
                f'{INVOICE_STATUS_ICONS.get(cur, "")} {cur}</span>',
                unsafe_allow_html=True)
        with c3:
            # One-tap advance through the pipeline; full editing lives in the job dialog
            nxt = {"Ready to Invoice": "Invoiced", "Invoiced": "Paid"}.get(cur)
            if nxt:
                if st.button(f"Mark {nxt}", key=f"inv_adv_{j['id']}", use_container_width=True):
                    # Stamp the invoice date the first time it's marked Invoiced;
                    # leave it alone (None = unchanged) when advancing to Paid.
                    new_date = None
                    if nxt == "Invoiced" and not inv['date']:
                        new_date = now_local().strftime('%Y-%m-%d')
                    set_job_invoice(j['id'], status=nxt, date=new_date)
                    get_logger().log(f"{user_email} marked job {j['id']} '{nxt}'")
                    st.toast(f"Marked {nxt}", icon="💵")
                    st.rerun()
            if st.button("Open", key=f"inv_open_{j['id']}", use_container_width=True):
                job_details_dialog(j['id'])
        st.divider()


def render_profitability():
    st.subheader("💰 Quote vs Actual")
    st.caption("What each job was worth against the effort it took. Effort is in "
               "MAN-hours — a report's hours counted once per tech on site — so a "
               "three-man day counts as three. No pay information is used or stored "
               "anywhere in this app.")

    scope = sub_nav(["Completed only", "Include in-progress"], "profit_scope")
    include_open = scope == "Include in-progress"
    pool = [j for j in st.session_state.jobs
            if include_open or j.get('status') == 'Completed']

    rows, skipped = [], 0
    for j in pool:
        v = job_value_summary(j)
        if not v:
            skipped += 1
            continue
        loc = get_location(j.get('locationId'))
        done = j.get('status') == 'Completed'
        rows.append({
            "Done": "✓" if done else "⚠ in progress",
            "Job": j.get('title', ''),
            "Site": loc['name'] if loc else '',
            "Type": j.get('type', ''),
            "Source": "billed" if v['billed'] is not None else "quoted",
            "Quoted": v['quoted'], "Billed": v['billed'], "Revenue": v['revenue'],
            "Man-hours": round(v['man_hours'], 2),
            "Parts $": round(v['parts'], 2),
            "$ / man-hour": round(v['rev_per_hour'], 2) if v['rev_per_hour'] is not None else None,
            "Variance": v['variance'],
            "_done": done,
        })

    if not rows:
        st.info("Nothing to report yet — no job has a quote value or an invoice amount"
                + (" and is completed." if not include_open else ".")
                + " Add a Quote Value on a job to bring it into this view.")
        return

    df = pd.DataFrame(rows)
    done_df = df[df["_done"]]

    # Headline figures use COMPLETED jobs only — an unfinished job hasn't logged
    # all its hours, so its $/man-hour looks far better than it will finish.
    if done_df.empty:
        st.warning("No **completed** job has a price on it yet, so there's nothing "
                   "reliable to summarise. The rows below are still in progress — "
                   "their hours haven't all been logged.")
    else:
        rev = done_df["Revenue"].sum()
        mh = done_df["Man-hours"].sum()
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Revenue", f"${rev:,.0f}")
        m2.metric("Man-hours", f"{mh:,.1f}")
        m3.metric("$ / man-hour", f"${rev / mh:,.0f}" if mh else "—",
                  help="Revenue divided by effort. Compare jobs against each other — "
                       "a low figure means the job ate more work than it returned.")
        m4.metric("Completed jobs", len(done_df),
                  help=f"{skipped} job(s) skipped for having no price")
        st.caption("Totals cover completed jobs only.")

    if include_open and (~df["_done"]).any():
        st.warning(f"⚠️ {int((~df['_done']).sum())} row(s) below are still in progress — "
                   "their hours aren't all logged, so their $/man-hour is flattering.")

    st.write("##### Per job — lowest return per man-hour first")
    st.dataframe(df.drop(columns=["_done"]).sort_values("$ / man-hour", na_position="last"),
                 use_container_width=True, hide_index=True)

    def _per_hour(frame):
        """Revenue per man-hour, blank rather than dividing by zero."""
        return [round(r / h, 2) if h else None
                for r, h in zip(frame["Revenue"], frame["Man-hours"])]

    if not done_df.empty:
        g1, g2 = st.columns(2)
        with g1:
            st.write("##### By job type")
            by_type = done_df.groupby("Type", as_index=False).agg(
                Jobs=("Job", "count"), Revenue=("Revenue", "sum"),
                **{"Man-hours": ("Man-hours", "sum")})
            by_type["$ / man-hour"] = _per_hour(by_type)
            st.dataframe(by_type.sort_values("$ / man-hour", na_position="last"),
                         use_container_width=True, hide_index=True)
        with g2:
            st.write("##### Sites returning least per man-hour")
            by_site = done_df.groupby("Site", as_index=False).agg(
                Jobs=("Job", "count"), Revenue=("Revenue", "sum"),
                **{"Man-hours": ("Man-hours", "sum")})
            by_site["$ / man-hour"] = _per_hour(by_site)
            st.dataframe(by_site.sort_values("$ / man-hour", na_position="last").head(10),
                         use_container_width=True, hide_index=True)

        _var = done_df[done_df["Variance"].notna()]
        if not _var.empty:
            st.write("##### Quote accuracy")
            v1, v2, v3 = st.columns(3)
            v1.metric("Billed over quote", int((_var["Variance"] > 0).sum()))
            v2.metric("Billed under quote", int((_var["Variance"] < 0).sum()))
            v3.metric("Average variance", f"${_var['Variance'].mean():,.0f}",
                      help="Positive means you tend to bill more than you quote.")

    st.download_button("⬇️ Download CSV",
                       df.drop(columns=["_done"]).to_csv(index=False).encode("utf-8"),
                       file_name=f"job_value_{now_local().strftime('%Y%m%d')}.csv",
                       mime="text/csv", key="profit_csv")


def render_analytics_dashboard():
    st.subheader("📊 Operational Analytics")

    if not st.session_state.jobs:
        st.info("No job data available.")
        return

    df = pd.DataFrame(st.session_state.jobs)

    total = len(df)
    completed = len(df[df["status"] == "Completed"])
    active = total - completed
    critical = len(df[df["priority"] == "Critical"])

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Jobs", total)
    m2.metric("Active", active)
    m3.metric("Completed", completed)
    m4.metric("Critical", critical)

    st.divider()

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Jobs by Status")
        status_counts = df["status"].value_counts()
        st.bar_chart(status_counts)  # remove color param if it errors

    with c2:
        st.markdown("#### Jobs by Priority")
        prio_counts = df["priority"].value_counts()
        st.bar_chart(prio_counts)  # remove color param if it errors

    st.divider()

    c3, c4 = st.columns(2)
    with c3:
        st.markdown("#### Tech Workload (Active)")
        active_jobs = df[df["status"] != "Completed"]
        if not active_jobs.empty:
            tech_map = {t["id"]: t["name"] for t in st.session_state.techs}
            tech_map[None] = "Unassigned"
            workload = active_jobs["techId"].map(tech_map).fillna("Unassigned").value_counts()
            st.bar_chart(workload)

    with c4:
        st.markdown("#### Jobs by Type")
        type_counts = df["type"].value_counts()
        st.bar_chart(type_counts)

    st.divider()
    
    # --- AI PARTS ANALYSIS ---
    st.markdown("#### 🔩 AI Parts Usage Tracker")
    st.caption("Uses Gemini to extract and aggregate parts data from unstructured technician notes.")
    
    if st.button("🤖 Analyze Parts Usage"):
        with st.spinner("Analyzing all job reports..."):
            # 1. Gather all "Parts Used" text
            all_parts_text = []
            for j in st.session_state.jobs:
                for r in j.get('reports', []):
                    if r.get('partsUsed'):
                        all_parts_text.append(f"- {r['partsUsed']}")
            
            if not all_parts_text:
                st.warning("No parts usage recorded in reports yet.")
            else:
                # 2. Send to Gemini
                api_key = get_api_key()
                if api_key:
                    client, model_name = get_available_model(api_key)
                    prompt = f"""
                    Analyze the following list of "Parts Used" entries from technician reports.
                    Consolidate them into a single JSON object where keys are the standardized part names (e.g., "Cat6 Cable", "NVR Power Supply") and values are the total estimated quantity used (integer).
                    Ignore vague entries like "none" or "N/A".
                    
                    Input List:
                    {chr(10).join(all_parts_text)}
                    
                    Return ONLY valid JSON. Example: {{"Cat6 Cable (ft)": 500, "RJ45 Jacks": 10}}
                    """
                    try:
                        response = client.models.generate_content(model=model_name, contents=prompt)
                        # Clean response to ensure just JSON
                        json_str = response.text.strip()
                        if "```json" in json_str:
                            json_str = json_str.split("```json")[1].split("```")[0]
                        elif "```" in json_str:
                            json_str = json_str.split("```")[1].split("```")[0]
                            
                        parts_data = json.loads(json_str)
                        
                        if parts_data:
                            st.bar_chart(parts_data, horizontal=True)
                        else:
                            st.info("AI found no quantifiable parts data.")
                            
                    except Exception as e:
                        st.error(f"Analysis failed: {e}")
                else:
                    st.error("API Key missing.")

    st.divider()
    
    st.markdown("#### 🏆 Technician Leaderboard (Completed Jobs)")
    completed_jobs = df[df["status"] == "Completed"]
    if not completed_jobs.empty:
        tech_map = {t["id"]: t["name"] for t in st.session_state.techs}
        tech_map[None] = "Unassigned"
        
        # Count completed jobs per tech
        leaderboard = completed_jobs["techId"].map(tech_map).fillna("Unassigned").value_counts()
        
        # Display as horizontal bar chart
        st.bar_chart(leaderboard, horizontal=True, color="#b91c1c")
    else:
        st.info("No completed jobs yet.")



    # --- ADMIN ACCESS MANAGEMENT ---
def render_hours_report():
    st.caption("Summed from daily report 'Hours Worked'. Hours are credited to every tech listed 'On Site' for a report (or the report author if none were listed).")

    today = now_local().date()
    hc1, hc2 = st.columns(2)
    start_date = hc1.date_input("From", value=today - datetime.timedelta(days=13), key="hours_from")
    end_date = hc2.date_input("To", value=today, key="hours_to")

    rows = compute_hours_rows(st.session_state.jobs, st.session_state.techs, st.session_state.locations, start_date, end_date)

    if not rows:
        st.info("No logged hours in this date range.")
        return

    df = pd.DataFrame(rows)

    st.write("##### Total Hours by Tech")
    totals = df.groupby("Tech", as_index=False)["Hours"].sum().sort_values("Hours", ascending=False)
    st.dataframe(totals, use_container_width=True, hide_index=True)

    st.write("##### Hours by Tech & Job")
    by_job = df.groupby(["Tech", "Job", "Location"], as_index=False)["Hours"].sum().sort_values(["Tech", "Hours"], ascending=[True, False])
    st.dataframe(by_job, use_container_width=True, hide_index=True)

    with st.expander("📄 All Entries"):
        st.dataframe(df.sort_values("Date", ascending=False), use_container_width=True, hide_index=True)

    st.download_button(
        "⬇️ Download CSV (all entries)",
        df.sort_values(["Date", "Tech"]).to_csv(index=False).encode("utf-8"),
        file_name=f"hours_{start_date}_{end_date}.csv",
        mime="text/csv",
        key="hours_csv",
    )

def _agreement_monthly_value(agr):
    """Monthly-equivalent recurring value of an agreement (0 for one-time/cancelled)."""
    if not agr or agr.get('status') == 'Cancelled':
        return 0.0
    try:
        val = float(agr.get('value') or 0)
    except (ValueError, TypeError):
        return 0.0
    cycle = agr.get('billing')
    if cycle == "Monthly":
        return val
    if cycle == "Quarterly":
        return val / 3
    if cycle == "Annual":
        return val / 12
    return 0.0  # One-time

def render_service_agreements():
    st.caption("Monitoring, service, inspection, and warranty contracts by site — with renewal alerts.")

    agreements = st.session_state.agreements
    loc_by_id = {l['id']: l for l in st.session_state.locations}

    # Summary
    active = [a for a in agreements if a.get('status') != 'Cancelled']
    expiring = [a for a in active if (agreement_days_left(a) is not None and agreement_days_left(a) <= AGREEMENT_RENEWAL_DAYS)]
    monthly_total = sum(_agreement_monthly_value(a) for a in active)
    m1, m2, m3 = st.columns(3)
    m1.metric("Active Contracts", len(active))
    m2.metric(f"Renewing ≤{AGREEMENT_RENEWAL_DAYS}d", len(expiring))
    m3.metric("Recurring / mo", f"${monthly_total:,.0f}")

    # Add agreement
    with st.expander("➕ Add Service Agreement", expanded=not agreements):
        if not st.session_state.locations:
            st.warning("Add a location first.")
        else:
            with st.form("add_agreement_form", clear_on_submit=True):
                loc_names = {l['name']: l['id'] for l in st.session_state.locations}
                a_loc = st.selectbox("Site", list(loc_names.keys()))
                ac1, ac2 = st.columns(2)
                a_type = ac1.selectbox("Type", AGREEMENT_TYPES)
                a_title = ac2.text_input("Title", placeholder="e.g. 24/7 Central Station Monitoring")
                ac3, ac4 = st.columns(2)
                a_start = ac3.date_input("Start Date", value=now_local())
                a_renew = ac4.date_input("Renewal / End Date", value=now_local() + datetime.timedelta(days=365))
                ac5, ac6 = st.columns(2)
                a_value = ac5.number_input("Value ($)", min_value=0.0, step=10.0)
                a_billing = ac6.selectbox("Billing", BILLING_CYCLES)
                a_auto = st.checkbox("Auto-renews")
                a_notes = st.text_input("Notes", placeholder="Contract #, terms, contact...")

                if st.form_submit_button("Save Agreement", use_container_width=True):
                    if not a_title.strip():
                        st.warning("Please enter a title.")
                    else:
                        st.session_state.agreements.append({
                            'id': f"a{now_local().timestamp()}",
                            'locationId': loc_names[a_loc],
                            'type': a_type,
                            'title': a_title.strip(),
                            'start_date': str(a_start),
                            'renewal_date': str(a_renew),
                            'value': a_value,
                            'billing': a_billing,
                            'auto_renew': a_auto,
                            'notes': a_notes.strip(),
                            'status': 'Active',
                        })
                        save_state(invalidate_briefing=False)
                        st.toast(f"Added '{a_title.strip()}'", icon="✅")
                        st.rerun()

    if not agreements:
        st.info("No service agreements recorded yet.")
        return

    # List, soonest renewal first
    def _sort_key(a):
        d = agreement_days_left(a)
        return (d if d is not None else 999999)
    for a in sorted(agreements, key=_sort_key):
        loc = loc_by_id.get(a.get('locationId'))
        days = agreement_days_left(a)
        with st.container(border=True):
            hc1, hc2 = st.columns([3, 1])
            hc1.markdown(f"**{a.get('title', 'Agreement')}** · {a.get('type', '')}")
            hc1.caption(f"📍 {loc['name'] if loc else 'Unknown site'}")

            if a.get('status') == 'Cancelled':
                hc2.markdown(":gray-background[Cancelled]")
            elif days is None:
                hc2.caption("No renewal date")
            elif days < 0:
                hc2.markdown(f":red-background[Expired {abs(days)}d ago]")
            elif days <= AGREEMENT_RENEWAL_DAYS:
                hc2.markdown(f":orange-background[Renews in {days}d]")
            else:
                hc2.markdown(f":green-background[Renews in {days}d]")

            meta = []
            if a.get('value'):
                meta.append(f"💲{float(a['value']):,.0f} {a.get('billing', '')}")
            if a.get('renewal_date'):
                meta.append(f"📅 {a['renewal_date']}")
            if a.get('auto_renew'):
                meta.append("🔁 Auto-renews")
            if meta:
                hc1.caption(" · ".join(meta))
            if a.get('notes'):
                hc1.caption(a['notes'])

            with st.expander("✏️ Edit / Delete"):
                with st.form(f"edit_agr_{a['id']}"):
                    e_title = st.text_input("Title", value=a.get('title', ''))
                    ec1, ec2 = st.columns(2)
                    e_type = ec1.selectbox("Type", AGREEMENT_TYPES,
                                           index=AGREEMENT_TYPES.index(a['type']) if a.get('type') in AGREEMENT_TYPES else 0)
                    e_status = ec2.selectbox("Status", ["Active", "Cancelled"],
                                             index=0 if a.get('status') != 'Cancelled' else 1)
                    ec3, ec4 = st.columns(2)
                    try:
                        _rv = datetime.datetime.strptime(str(a.get('renewal_date'))[:10], "%Y-%m-%d").date()
                    except (ValueError, TypeError):
                        _rv = now_local().date()
                    e_renew = ec3.date_input("Renewal / End Date", value=_rv, key=f"agr_renew_{a['id']}")
                    e_value = ec4.number_input("Value ($)", min_value=0.0, step=10.0, value=float(a.get('value') or 0))
                    ec5, ec6 = st.columns(2)
                    e_billing = ec5.selectbox("Billing", BILLING_CYCLES,
                                              index=BILLING_CYCLES.index(a['billing']) if a.get('billing') in BILLING_CYCLES else 0)
                    e_auto = ec6.checkbox("Auto-renews", value=bool(a.get('auto_renew')))
                    e_notes = st.text_input("Notes", value=a.get('notes', ''))

                    bc1, bc2 = st.columns(2)
                    if bc1.form_submit_button("💾 Update"):
                        a.update({'title': e_title, 'type': e_type, 'status': e_status,
                                  'renewal_date': str(e_renew), 'value': e_value,
                                  'billing': e_billing, 'auto_renew': e_auto, 'notes': e_notes})
                        save_state(invalidate_briefing=False)
                        st.toast("Updated.", icon="✅")
                        st.rerun()
                    if bc2.form_submit_button("🗑️ Delete"):
                        st.session_state.agreements = [x for x in st.session_state.agreements if x['id'] != a['id']]
                        save_state(invalidate_briefing=False)
                        st.toast("Agreement deleted", icon="🗑️")
                        st.rerun()


def render_chatbot():
    st.sidebar.title("🤖 Tech Assistant")
    st.sidebar.markdown("Ask about jobs, history, or locations.")
    
    # Display History
    for msg in st.session_state.chat_history:
        with st.sidebar.chat_message(msg["role"]):
            st.write(msg["parts"][0])
    
    # Chat Input
    prompt = st.sidebar.chat_input("How can I help?")
    if prompt:
        api_key = get_api_key()
        if not api_key:
            st.sidebar.error("API Key missing.")
            return

        # Use dynamic model selector
        client, model_name = get_available_model(api_key)
        
        # Add user message
        st.session_state.chat_history.append({"role": "user", "parts": [prompt]})
        with st.sidebar.chat_message("user"):
            st.write(prompt)
        
        # Contextualize Data (remove heavy base64 strings before sending to LLM).
        simple_jobs = []
        for j in list(st.session_state.jobs):
            clean_job = {k:v for k,v in j.items() if k != 'reports'}
            
            # Include text content of reports, but strip out photos to save tokens/bandwidth
            clean_reports = []
            for r in j.get('reports', []):
                clean_reports.append({
                    'timestamp': r.get('timestamp'),
                    'techId': r.get('techId'),
                    'content': r.get('content'),
                    'photo_count': len(r.get('photos', []))
                })
            
            clean_job['reports'] = clean_reports
            simple_jobs.append(clean_job)
        
        # SECURITY: strip site credentials/systems (logins, passwords, IPs)
        # before sending location data to the external LLM API
        safe_locations = [
            {k: v for k, v in l.items() if k not in ('credentials', 'systems')}
            for l in st.session_state.locations
        ]

        system_context = f"""
       You are a 5G Security Assistant.
       Current Time: {now_local()}
       Techs: {json.dumps(st.session_state.techs)}
       Locations: {json.dumps(safe_locations)}
       Jobs: {json.dumps(simple_jobs)}
       
       Answer based strictly on this data. If searching for history, note that detailed reports are not in this context, only summaries.
       """
        
        full_prompt = f"{system_context}\n\nUser Question: {prompt}"
        
        try:
            with st.sidebar.chat_message("model"):
                with st.spinner("Thinking..."):
                    response = client.models.generate_content(model=model_name, contents=full_prompt)
                    bot_reply = response.text
                    st.write(bot_reply)
                    
            st.session_state.chat_history.append({"role": "model", "parts": [bot_reply]})
        except Exception as e:
            st.sidebar.error(f"AI Error: {str(e)}")
            try:
                # Debug: List available models to help diagnose
                all_models = list(client.models.list())
                model_names = [m.name for m in all_models]
                st.sidebar.warning(f"Available models: {model_names}")
            except Exception as debug_e:
                st.sidebar.error(f"Could not list models: {str(debug_e)}")
