import os
import json
import streamlit as st

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

# Default data structure    
DEFAULT_DATA = {
    "jobs": [],
    "techs": [],
    "locations": [],
    "briefing": "Data required to generate briefing.",
    "adminEmails": [],
    "last_reminder_date": None
}

def get_connection():
    if not HAS_PSYCOPG2:
        raise ImportError("psycopg2 module not found. Please install it.")

    db_url = os.environ.get("DATABASE_URL") or os.environ.get("NEON_DB_URL")
    if not db_url:
        if "DATABASE_URL" in st.secrets:
            db_url = st.secrets["DATABASE_URL"]
        elif "NEON_DB_URL" in st.secrets:
            db_url = st.secrets["NEON_DB_URL"]

    if not db_url:
        raise ValueError("DATABASE_URL or NEON_DB_URL not found in environment or secrets.")

    return psycopg2.connect(db_url)

def init_db():
    """Initialize the table if it doesn't exist."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('app_state');")
            table_oid = cur.fetchone()[0]

            if not table_oid:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS app_state (
                        key TEXT PRIMARY KEY,
                        value JSONB,
                        version SERIAL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cur.execute(
                    "INSERT INTO app_state (key, value) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    ('global_state', json.dumps(DEFAULT_DATA))
                )

            conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"DB Init Error: {e}")
    finally:
        conn.close()

# Initialize on module load
try:
    init_db()
except Exception as e:
    pass

def load_state():
    """Returns (data_dict, version_int)."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT value, version FROM app_state WHERE key = 'global_state'")
            row = cur.fetchone()
            if row:
                return row['value'], row['version']
            return DEFAULT_DATA.copy(), 0
    finally:
        conn.close()

def save_state_to_db(data):
    """Saves data to DB, incrementing version."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app_state (key, value)
                VALUES ('global_state', %s)
                ON CONFLICT (key)
                DO UPDATE SET value = EXCLUDED.value, version = app_state.version + 1, updated_at = CURRENT_TIMESTAMP
                RETURNING version;
                """,
                (json.dumps(data),)
            )
            new_version = cur.fetchone()[0]
        conn.commit()
        return new_version
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def ensure_loaded_into_session():
    """Ensures st.session_state.db is populated."""
    if 'db' not in st.session_state:
        data, version = load_state()
        st.session_state.db = data
        st.session_state._db_version = version

def commit_from_session(invalidate_briefing=True):
    """Saves st.session_state.db to DB."""
    if 'db' not in st.session_state:
        return

    if invalidate_briefing:
        st.session_state.db['briefing'] = "Data required to generate briefing."

    try:
        new_ver = save_state_to_db(st.session_state.db)
        st.session_state._db_version = new_ver
    except Exception as e:
        st.error(f"Failed to save to DB: {e}")

def force_overwrite_from_session(invalidate_briefing=False):
    """Same as commit but explicitly named for restore operations."""
    commit_from_session(invalidate_briefing=invalidate_briefing)
