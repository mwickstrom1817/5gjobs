import os
from typing import Any, Dict, Tuple

import streamlit as st
import psycopg2
from psycopg2.extras import Json

APP_STATE_ID = "prod"

def _db_url() -> str:
    if "DATABASE_URL" in st.secrets:
        return st.secrets["DATABASE_URL"]
    if "DATABASE_URL" in os.environ:
        return os.environ["DATABASE_URL"]
    raise RuntimeError("DATABASE_URL missing (st.secrets or env var).")

def load_state() -> Tuple[Dict[str, Any], int]:
    with psycopg2.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select state_json, version from app_state where id=%s",
                (APP_STATE_ID,),
            )
            row = cur.fetchone()
            if not row:
                return {}, 0
            return row[0] or {}, int(row[1])

def try_save_state(state: Dict[str, Any], expected_version: int) -> Tuple[bool, int]:
    """
    Returns (saved_ok, new_version).
    If saved_ok is False, new_version is the current DB version.
    """
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
                (Json(state), APP_STATE_ID, expected_version),
            )
            row = cur.fetchone()
            if row:
                conn.commit()
                return True, int(row[0])

            # version mismatch: fetch current version
            cur.execute("select version from app_state where id=%s", (APP_STATE_ID,))
            row2 = cur.fetchone()
            current_version = int(row2[0]) if row2 else expected_version
            return False, current_version

def ensure_loaded() -> None:
    if "db" not in st.session_state or "_db_version" not in st.session_state:
        state, ver = load_state()
        st.session_state.db = state
        st.session_state._db_version = ver

def commit_or_warn() -> None:
    ok, new_ver = try_save_state(st.session_state.db, st.session_state._db_version)
    if ok:
        st.session_state._db_version = new_ver
        return

    # conflict: someone else saved first
    st.warning(
        "Another admin saved changes while you were editing. "
        "Reloading the latest data to avoid overwriting."
    )
    state, ver = load_state()
    st.session_state.db = state
    st.session_state._db_version = ver
    st.stop()
