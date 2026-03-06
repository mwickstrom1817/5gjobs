import os
from typing import Any, Dict, Tuple

import streamlit as st
import psycopg2
from psycopg2.extras import Json

DEFAULT_DATA = {
    "jobs": [],
    "techs": [],
    "locations": [],
    "briefing": "Data required to generate briefing.",
    "adminEmails": [],
    "last_reminder_date": None,
}

def _db_url() -> str:
    if "DATABASE_URL" in st.secrets:
        return st.secrets["DATABASE_URL"]
    if "DATABASE_URL" in os.environ:
        return os.environ["DATABASE_URL"]
    raise RuntimeError("DATABASE_URL missing (Streamlit secrets or env var).")

def _state_id() -> str:
    return st.secrets.get("APP_STATE_ID", "prod")

def load_state() -> Tuple[Dict[str, Any], int]:
    with psycopg2.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            cur.execute("select state_json, version from app_state where id=%s", (_state_id(),))
            row = cur.fetchone()
            if not row:
                return dict(DEFAULT_DATA), 0

            state = row[0] or {}
            for k, v in DEFAULT_DATA.items():
                state.setdefault(k, v)
            return state, int(row[1])

def try_save_state(state: Dict[str, Any], expected_version: int) -> Tuple[bool, int]:
    with psycopg2.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update app_state
                   set state_json = %s,
                       version = version + 1,
                       updated_at = now()
                 where id = %s
                   and version = %s
                 returning version
                """,
                (Json(state), _state_id(), expected_version),
            )
            row = cur.fetchone()
            if row:
                conn.commit()
                return True, int(row[0])

            cur.execute("select version from app_state where id=%s", (_state_id(),))
            row2 = cur.fetchone()
            return False, int(row2[0]) if row2 else expected_version

def ensure_loaded_into_session() -> None:
    if "db" not in st.session_state or "_db_version" not in st.session_state:
        state, ver = load_state()
        st.session_state.db = state
        st.session_state._db_version = ver

def commit_from_session(invalidate_briefing: bool = True) -> None:
    if invalidate_briefing:
        st.session_state.db["briefing"] = "Data required to generate briefing."

    ok, new_ver = try_save_state(st.session_state.db, st.session_state._db_version)
    if ok:
        st.session_state._db_version = new_ver
        return

    st.warning(
        "Another admin saved changes while you were editing. "
        "Reloaded latest data to prevent overwriting. Please re-apply your change."
    )
    state, ver = load_state()
    st.session_state.db = state
    st.session_state._db_version = ver
    st.stop()

def force_overwrite_from_session(invalidate_briefing: bool = True) -> None:
    if invalidate_briefing:
        st.session_state.db["briefing"] = "Data required to generate briefing."

    _, ver = load_state()
    st.session_state._db_version = ver
    ok, new_ver = try_save_state(st.session_state.db, st.session_state._db_version)
    if ok:
        st.session_state._db_version = new_ver
        return

    state2, ver2 = load_state()
    st.session_state.db = state2
    st.session_state._db_version = ver2
    st.stop()
