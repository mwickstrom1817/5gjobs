import os
import json
import time
import threading
from contextlib import contextmanager

import streamlit as st

from crypto import encrypt_state_systems, decrypt_state_systems

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import pool as _pg_pool
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
    "construction_emails": [],
    "agreements": [],
    "last_reminder_date": None
}

# --- Connection pooling -------------------------------------------------------
# Every interaction used to open a fresh TCP+TLS+auth handshake against a
# serverless Postgres (Neon), and that round trip dominated per-click latency.
# A small threaded pool reuses connections across script runs and background
# threads. If pool creation ever fails we fall back to the old
# one-connection-per-query behavior so the app keeps working either way.
_POOL = None
_POOL_FAILED = False
_POOL_LOCK = threading.Lock()
_DSN = None
_DSN_LOCK = threading.Lock()


def _resolve_dsn():
    if _DSN:
        return _DSN
    with _DSN_LOCK:
        if not _DSN:
            dsn = os.environ.get("DATABASE_URL") or os.environ.get("NEON_DB_URL")
            if not dsn:
                try:
                    if "DATABASE_URL" in st.secrets:
                        dsn = st.secrets["DATABASE_URL"]
                    elif "NEON_DB_URL" in st.secrets:
                        dsn = st.secrets["NEON_DB_URL"]
                except Exception:
                    dsn = None
            if not dsn:
                raise ValueError("DATABASE_URL or NEON_DB_URL not found in environment or secrets.")
            globals()['_DSN'] = dsn
        return globals()['_DSN']


def _get_pool():
    global _POOL, _POOL_FAILED
    if _POOL is not None or _POOL_FAILED or not HAS_PSYCOPG2:
        return _POOL
    with _POOL_LOCK:
        if _POOL is None and not _POOL_FAILED:
            try:
                _POOL = _pg_pool.ThreadedConnectionPool(minconn=1, maxconn=10, dsn=_resolve_dsn())
            except Exception:
                _POOL_FAILED = True
                _POOL = None
    return _POOL


@contextmanager
def _db_conn():
    """A database connection with commit/rollback handled, and one transparent
    retry when a pooled connection has died (serverless databases drop idle
    connections). Business errors (e.g. StaleStateError) are never retried."""
    pool = _get_pool()
    if pool is None:
        # Either psycopg2 is missing or pool creation failed: original behavior.
        if not HAS_PSYCOPG2:
            raise ImportError("psycopg2 module not found. Please install it.")
        conn = psycopg2.connect(_resolve_dsn())
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return
    for attempt in range(2):
        conn = pool.getconn()
        close_conn = False
        try:
            yield conn
            conn.commit()
            return
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            # Dead connection (serverless idle timeout): discard and retry once.
            close_conn = True
            try:
                conn.rollback()
            except Exception:
                pass
            if attempt == 1:
                raise
        except Exception:
            # Any other error (including StaleStateError) rolls back and releases.
            close_conn = True
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            # Guarantee the connection goes back to the pool so we don't leak it,
            # even if rollback() above raised an exception.
            pool.putconn(conn, close=close_conn)


def init_db():
    """Initialize the table if it doesn't exist."""
    try:
        with _db_conn() as conn:
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
    except Exception as e:
        print(f"DB Init Error: {e}")

# Initialize on module load
try:
    init_db()
except Exception as e:
    pass

def load_state():
    """Returns (data_dict, version_int)."""
    with _db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT value, version FROM app_state WHERE key = 'global_state'")
            row = cur.fetchone()
            if row:
                data = decrypt_state_systems(row['value'])
                return data, row['version']
            return DEFAULT_DATA.copy(), 0

class StaleStateError(Exception):
    """Raised when the DB row has a newer version than the one this session loaded.
    Saving anyway would silently overwrite someone else's changes."""
    pass

# --- Version cache ------------------------------------------------------------
# get_db_version() is called on every script run (the freshness check) and every
# 15s per open session (the live-update watcher). One DB round trip per
# interaction adds up, so repeat checks are answered from memory for a few
# seconds - imperceptible delay for a "someone else saved" banner. Our own
# saves go through save_state_to_db's live FOR UPDATE check, never this cache.
_VER_LOCK = threading.Lock()
_VER_CACHE = {"version": None, "at": 0.0}
VERSION_TTL_SECONDS = 5.0


def _read_db_version():
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT version FROM app_state WHERE key = 'global_state'")
            row = cur.fetchone()
            return row[0] if row else None


def get_db_version():
    """Returns the current version of the global state row (None if missing).
    Cached for VERSION_TTL_SECONDS; call _read_db_version() when the live
    value is required."""
    now = time.monotonic()
    with _VER_LOCK:
        cached, at = _VER_CACHE["version"], _VER_CACHE["at"]
        if cached is not None and (now - at) < VERSION_TTL_SECONDS:
            return cached
    version = _read_db_version()
    with _VER_LOCK:
        _VER_CACHE["version"] = version
        _VER_CACHE["at"] = now
    return version


def ping_db():
    """Cheap liveness query; calling this periodically keeps a serverless
    database from dozing off between active periods."""
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()


def save_state_to_db(data, expected_version=None):
    """Saves data to DB, incrementing version.
    If expected_version is provided and the row has moved past it (someone else
    saved first), raises StaleStateError instead of clobbering their changes."""
    # Encrypt sensitive site credentials before they hit the database. We work
    # on a copy so the in-memory state remains plaintext for the app to use.
    encrypted_data = encrypt_state_systems(data)
    with _db_conn() as conn:
        with conn.cursor() as cur:
            if expected_version is not None:
                cur.execute("SELECT version FROM app_state WHERE key = 'global_state' FOR UPDATE")
                row = cur.fetchone()
                current_version = row[0] if row else None
                if current_version is not None and current_version != expected_version:
                    raise StaleStateError(
                        f"DB is at version {current_version}, but this session loaded version {expected_version}."
                    )
            cur.execute(
                """
                INSERT INTO app_state (key, value)
                VALUES ('global_state', %s)
                ON CONFLICT (key)
                DO UPDATE SET value = EXCLUDED.value, version = app_state.version + 1, updated_at = CURRENT_TIMESTAMP
                RETURNING version;
                """,
                (json.dumps(encrypted_data),)
            )
            new_version = cur.fetchone()[0]
    return new_version


def ensure_loaded_into_session():
    """Ensures st.session_state.db is populated."""
    if 'db' not in st.session_state:
        data, version = load_state()
        st.session_state.db = data
        st.session_state._db_version = version


def commit_from_session(invalidate_briefing=True):
    """Saves st.session_state.db to DB.
    Raises StaleStateError if another session saved since this one loaded."""
    if 'db' not in st.session_state:
        return

    if invalidate_briefing:
        st.session_state.db['briefing'] = "Data required to generate briefing."

    try:
        new_ver = save_state_to_db(st.session_state.db, expected_version=st.session_state.get('_db_version'))
        st.session_state._db_version = new_ver
    except StaleStateError:
        raise
    except Exception as e:
        st.error(f"Failed to save to DB: {e}")


def force_overwrite_from_session(invalidate_briefing=False):
    """Same as commit but skips the version check - for explicit restore operations."""
    if 'db' not in st.session_state:
        return

    if invalidate_briefing:
        st.session_state.db['briefing'] = "Data required to generate briefing."

    try:
        new_ver = save_state_to_db(st.session_state.db)
        st.session_state._db_version = new_ver
    except Exception as e:
        st.error(f"Failed to save to DB: {e}")
