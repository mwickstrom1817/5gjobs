"""
persistence_pg.py — Streamlit-free version for FastAPI backend.
All st.secrets / st.session_state replaced with os.environ and in-memory dict.
"""
import os
import json

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

DEFAULT_DATA = {
    "jobs": [],
    "techs": [],
    "locations": [],
    "briefing": "Data required to generate briefing.",
    "adminEmails": [],
    "last_reminder_date": None,
}

def get_connection():
    if not HAS_PSYCOPG2:
        raise ImportError("psycopg2 not installed.")
    db_url = os.environ.get("DATABASE_URL") or os.environ.get("NEON_DB_URL")
    if not db_url:
        raise ValueError("DATABASE_URL or NEON_DB_URL not set in environment.")
    return psycopg2.connect(db_url)

def init_db():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('app_state');")
            if not cur.fetchone()[0]:
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

try:
    init_db()
except Exception:
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

def save_state_to_db(data: dict) -> int:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO app_state (key, value)
                   VALUES ('global_state', %s)
                   ON CONFLICT (key)
                   DO UPDATE SET value = EXCLUDED.value,
                                 version = app_state.version + 1,
                                 updated_at = CURRENT_TIMESTAMP
                   RETURNING version;""",
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

# The following stubs are kept for compatibility if anything imports them.
def ensure_loaded_into_session(): pass
def commit_from_session(invalidate_briefing=True): pass
def force_overwrite_from_session(invalidate_briefing=False): pass
