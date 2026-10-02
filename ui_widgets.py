import datetime

import streamlit as st

def time_select(label, default, key, step_minutes=15):
    """Mobile-native time picker: number pad for hour, tap chips for minute/am-pm.
    No dropdowns, no popover, no keyboard-dismiss bugs inside dialogs."""
    st.caption(label)

    if isinstance(default, str):
        try:
            default = datetime.datetime.strptime(default, "%H:%M:%S").time()
        except (ValueError, TypeError):
            default = datetime.time(8, 0)
    elif not isinstance(default, datetime.time):
        default = datetime.time(8, 0)

    hour24 = default.hour
    minute = default.minute
    ampm = "AM" if hour24 < 12 else "PM"
    hour12 = hour24 % 12
    if hour12 == 0:
        hour12 = 12

    minute_opts = list(range(0, 60, step_minutes))
    rounded_min = min(minute_opts, key=lambda x: abs(x - minute))

    c1, c2, c3 = st.columns([1.2, 1.8, 1.2])
    with c1:
        h = st.number_input(
            "Hr", min_value=1, max_value=12, value=hour12,
            key=f"{key}_h", label_visibility="collapsed"
        )
    with c2:
        if hasattr(st, "segmented_control"):
            m = st.segmented_control(
                "Min", minute_opts, format_func=lambda x: f"{x:02d}",
                default=rounded_min, key=f"{key}_m", label_visibility="collapsed"
            )
        else:
            m = st.radio(
                "Min", minute_opts, format_func=lambda x: f"{x:02d}",
                index=minute_opts.index(rounded_min), horizontal=True,
                key=f"{key}_m", label_visibility="collapsed"
            )
    with c3:
        if hasattr(st, "segmented_control"):
            ap = st.segmented_control(
                "AM/PM", ["AM", "PM"], default=ampm,
                key=f"{key}_ap", label_visibility="collapsed"
            )
        else:
            ap = st.radio(
                "AM/PM", ["AM", "PM"], index=0 if ampm == "AM" else 1,
                horizontal=True, key=f"{key}_ap", label_visibility="collapsed"
            )

    h = int(h)
    m = int(m) if m is not None else rounded_min
    ap = ap or ampm

    if ap == "PM" and h != 12:
        h += 12
    elif ap == "AM" and h == 12:
        h = 0

    return datetime.time(h, m)

def sub_nav(options, key, default=None):
    """Selector for views grouped under one tab. Falls back to a radio on older
    Streamlit, and never returns None so callers can compare directly."""
    default = default or options[0]
    if hasattr(st, "segmented_control"):
        picked = st.segmented_control(key, options, default=default, key=key,
                                      label_visibility="collapsed")
    else:
        picked = st.radio(key, options, horizontal=True, key=key,
                          label_visibility="collapsed")
    return picked or default
