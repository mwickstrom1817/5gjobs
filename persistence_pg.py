"""
persistence_pg.py  —  Neon / PostgreSQL persistence layer
Streamlit-free core: load_state, save_state_to_db, and init_db work in any
Python process (FastAPI, scripts, tests).

The optional Streamlit session helpers (ensure_loaded_into_session,
commit_from_session, force_overwrite_from_session) are preserved at the
bottom for the Streamlit app (app.py) but are guarded so they only import
streamlit when actually called — FastAPI never touches them.
"""

import os
import json
import logging

logger = logging.getLogger(__name__)

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False
    logger.warning("psycopg2 not installed — database unavailable.")

# ── Default data structure ─────────────────────────────────────────────────────

DEFAULT_DATA = {
    "jobs":               [],
    "techs":              [],
    "locations":          [],
    "briefing":           "Data required to generate briefing.",
    "adminEmails":        [],
    "last_reminder_date": None,
}

# ── Connection ─────────────────────────────────────────────────────────────────

def get_connection():
    """
    Return a new psycopg2 connection.

    Connection string is read from the DATABASE_URL or NEON_DB_URL
    environment variable. Raises ValueError if neither is set.
    """
    if not HAS_PSYCOPG2:
        raise ImportError("psycopg2 is not installed. Add psycopg2-binary to requirements.txt.")

    db_url = os.environ.get("DATABASE_URL") or os.environ.get("NEON_DB_URL")

    if not db_url:
        raise ValueError(
            "Database URL not found. "
            "Set the DATABASE_URL or NEON_DB_URL environment variable."
        )

    return psycopg2.connect(db_url)


# ── Core DB operations (Streamlit-free) ───────────────────────────────────────

def init_db():
    """Create the app_state table and seed default data if it doesn't exist."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('app_state');")
            table_oid = cur.fetchone()[0]

            if not table_oid:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS app_state (
                        key        TEXT PRIMARY KEY,
                        value      JSONB,
                        version    SERIAL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cur.execute(
                    "INSERT INTO app_state (key, value) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    ("global_state", json.dumps(DEFAULT_DATA)),
                )
            conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error("DB init error: %s", e)
        raise
    finally:
        conn.close()


def load_state() -> tuple[dict, int]:
    """
    Load the global state from the database.

    Returns (data_dict, version_int).
    Falls back to DEFAULT_DATA if no row exists yet.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT value, version FROM app_state WHERE key = 'global_state'"
            )
            row = cur.fetchone()
            if row:
                return dict(row["value"]), row["version"]
            return DEFAULT_DATA.copy(), 0
    finally:
        conn.close()


def save_state_to_db(data: dict) -> int:
    """
    Persist data to the database, incrementing the version counter.

    Returns the new version number.
    Raises on DB errors (caller should handle).
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app_state (key, value)
                VALUES ('global_state', %s)
                ON CONFLICT (key)
                DO UPDATE SET
                    value      = EXCLUDED.value,
                    version    = app_state.version + 1,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING version;
                """,
                (json.dumps(data),),
            )
            new_version = cur.fetchone()[0]
        conn.commit()
        return new_version
    except Exception as e:
        conn.rollback()
        logger.error("save_state_to_db failed: %s", e)
        raise
    finally:
        conn.close()


# Initialize on module load (safe — logs warning but doesn't crash if DB is down)
try:
    init_db()
except Exception as _e:
    logger.warning("DB init on module load failed (will retry on first request): %s", _e)


# ── Streamlit session helpers (app.py only) ────────────────────────────────────
# These functions import streamlit lazily so that FastAPI never loads it.

def ensure_loaded_into_session():
    """Populate st.session_state.db from the DB if not already done."""
    import streamlit as st  # lazy import — not available in FastAPI
    if "db" not in st.session_state:
        data, version = load_state()
        st.session_state.db       = data
        st.session_state._db_version = version


def commit_from_session(invalidate_briefing: bool = True):
    """Flush st.session_state.db to the database."""
    import streamlit as st
    if "db" not in st.session_state:
        return

    if invalidate_briefing:
        st.session_state.db["briefing"] = "Data required to generate briefing."

    try:
        new_ver = save_state_to_db(st.session_state.db)
        st.session_state._db_version = new_ver
    except Exception as e:
        st.error(f"Failed to save to DB: {e}")


def force_overwrite_from_session(invalidate_briefing: bool = False):
    """Same as commit_from_session but named explicitly for restore operations."""
    commit_from_session(invalidate_briefing=invalidate_briefing)
