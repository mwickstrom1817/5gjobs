import os
import json
import time
import threading
import hashlib
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
    "sops": [],
    "settings": {},
    "smtp_settings": {},
    "last_reminder_date": None
}

# Per-entity tables. Each row is (id TEXT PK, data JSONB).
ENTITY_TABLES = ["jobs", "techs", "locations", "agreements", "sops"]

# Global scalar values stored as rows in app_settings.
SETTINGS_KEYS = [
    "briefing",
    "adminEmails",
    "construction_emails",
    "last_reminder_date",
    "settings",
    "smtp_settings",
]
VERSION_KEY = "_version"

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


def _table_ddl(table: str) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id TEXT PRIMARY KEY,
            data JSONB NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """


def _migrate_legacy_state(cur):
    """One-time copy from app_state.global_state into per-entity tables.

    Existing data is copied, not moved or deleted. The legacy row is left
    untouched so it remains a complete fallback backup.
    """
    # If the version row exists, migration already happened.
    cur.execute("SELECT 1 FROM app_settings WHERE key = %s LIMIT 1", (VERSION_KEY,))
    if cur.fetchone():
        return

    cur.execute("SELECT value, version FROM app_state WHERE key = 'global_state'")
    row = cur.fetchone()
    if not row:
        return

    data, version = row['value'], row['version']
    if not data:
        return

    for table in ENTITY_TABLES:
        for item in data.get(table, []):
            if isinstance(item, dict) and item.get("id"):
                cur.execute(
                    f"INSERT INTO {table} (id, data) VALUES (%s, %s) "
                    f"ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
                    (item["id"], json.dumps(item))
                )

    for key in SETTINGS_KEYS:
        if key in data:
            cur.execute(
                "INSERT INTO app_settings (key, value) VALUES (%s, %s) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                (key, json.dumps(data[key]))
            )

    # Seed the global version from the legacy row's version.
    cur.execute(
        "INSERT INTO app_settings (key, value, version) VALUES (%s, %s, %s) "
        "ON CONFLICT (key) DO UPDATE SET version = EXCLUDED.version",
        (VERSION_KEY, None, version or 1)
    )


def init_db():
    """Initialize the legacy table and new per-entity tables."""
    try:
        with _db_conn() as conn:
            with conn.cursor() as cur:
                # Legacy monolithic row. Kept as a fallback and dual-write backup.
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

                # New per-entity tables.
                for table in ENTITY_TABLES:
                    cur.execute(_table_ddl(table))

                # Global settings + optimistic-lock version.
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS app_settings (
                        key TEXT PRIMARY KEY,
                        value JSONB,
                        version SERIAL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)

                _migrate_legacy_state(cur)
    except Exception as e:
        print(f"DB Init Error: {e}")


# Initialize on module load
try:
    init_db()
except Exception as e:
    pass


def _load_legacy_state():
    """Fallback load from the old monolithic row."""
    with _db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT value, version FROM app_state WHERE key = 'global_state'")
            row = cur.fetchone()
            if row:
                data = decrypt_state_systems(row['value'])
                return data, row['version']
            return DEFAULT_DATA.copy(), 0


def _load_from_tables():
    """Load and assemble state from per-entity tables.

    Raises an exception if the new schema has not been initialized (no version
    row), so load_state() can fall back to the legacy row.
    """
    data = DEFAULT_DATA.copy()
    with _db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT version FROM app_settings WHERE key = %s", (VERSION_KEY,))
            row = cur.fetchone()
            if not row or row[0] is None:
                raise RuntimeError("New tables not initialized; falling back to legacy state")
            version = row[0] or 0

            for table in ENTITY_TABLES:
                cur.execute(f"SELECT data FROM {table}")
                data[table] = [r['data'] for r in cur.fetchall()]

            cur.execute("SELECT key, value FROM app_settings")
            settings = {r['key']: r['value'] for r in cur.fetchall()}

            for key in SETTINGS_KEYS:
                if key in settings:
                    data[key] = settings[key]

    data = decrypt_state_systems(data)
    return data, version


def load_state():
    """Returns (data_dict, version_int).

    Reads from the new per-entity tables. If that fails or the tables are
    empty, falls back to the legacy app_state.global_state row.
    """
    if not HAS_PSYCOPG2:
        return DEFAULT_DATA.copy(), 0

    try:
        return _load_from_tables()
    except Exception:
        return _load_legacy_state()


class StaleStateError(Exception):
    """Raised when the DB version is newer than the one this session loaded.
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
            cur.execute("SELECT version FROM app_settings WHERE key = %s", (VERSION_KEY,))
            row = cur.fetchone()
            if row and row[0] is not None:
                return row[0]
            # Fallback to legacy row.
            cur.execute("SELECT version FROM app_state WHERE key = 'global_state'")
            row = cur.fetchone()
            return row[0] if row else None


def get_db_version():
    """Returns the current global version (None if missing).
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


def save_state_to_db(data, expected_version=None, dirty_tables=None):
    """Saves data to per-entity tables, incrementing the global version.
    If expected_version is provided and the DB has moved past it, raises
    StaleStateError instead of clobbering someone else's changes.

    Only tables named in dirty_tables are rewritten. When omitted, all entity
    tables are rewritten (legacy behavior). Entity rows are sent as a single
    multi-row INSERT per table to minimize round-trips. The legacy
    app_state.global_state row is no longer dual-written on every save.
    """
    encrypted_data = encrypt_state_systems(data)
    tables_to_write = set(dirty_tables) if dirty_tables else set(ENTITY_TABLES)

    with _db_conn() as conn:
        with conn.cursor() as cur:
            if expected_version is not None:
                cur.execute("SELECT version FROM app_settings WHERE key = %s FOR UPDATE", (VERSION_KEY,))
                row = cur.fetchone()
                current_version = row[0] if row and row[0] is not None else None
                if current_version is not None and current_version != expected_version:
                    raise StaleStateError(
                        f"DB is at version {current_version}, but this session loaded version {expected_version}."
                    )

            # Replace only dirty entity tables in a single multi-row INSERT.
            for table in ENTITY_TABLES:
                if table not in tables_to_write:
                    continue
                cur.execute(f"DELETE FROM {table}")
                rows = [
                    (item["id"], json.dumps(item))
                    for item in encrypted_data.get(table, [])
                    if isinstance(item, dict) and item.get("id")
                ]
                if rows:
                    placeholders = ",".join(["(%s, %s)"] * len(rows))
                    params = [p for row in rows for p in row]
                    cur.execute(
                        f"INSERT INTO {table} (id, data) VALUES {placeholders}",
                        params
                    )

            # Upsert global settings in a single multi-row statement.
            setting_rows = [
                (key, json.dumps(encrypted_data.get(key)))
                for key in SETTINGS_KEYS
            ]
            if setting_rows:
                placeholders = ",".join(["(%s, %s)"] * len(setting_rows))
                params = [p for row in setting_rows for p in row]
                cur.execute(
                    f"INSERT INTO app_settings (key, value) VALUES {placeholders} "
                    f"ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                    params
                )

            # Increment the global version. The version column auto-increments.
            cur.execute(
                "INSERT INTO app_settings (key, value) VALUES (%s, %s) "
                "ON CONFLICT (key) DO UPDATE SET version = app_settings.version + 1 "
                "RETURNING version",
                (VERSION_KEY, None)
            )
            new_version = cur.fetchone()[0]

    return new_version


def _hash_entities(items):
    """Stable hash of an entity list for dirty-table detection."""
    return hashlib.sha256(
        json.dumps(items, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def store_db_hashes(data):
    """Remember hashes of the last-loaded/saved entity lists."""
    st.session_state._db_hashes = {
        table: _hash_entities(data.get(table, []))
        for table in ENTITY_TABLES
    }


def compute_dirty_tables():
    """Compare current session entity lists to their last-known hashes."""
    stored = st.session_state.get("_db_hashes", {})
    dirty = []
    for table in ENTITY_TABLES:
        current = st.session_state.get(table, [])
        if _hash_entities(current) != stored.get(table):
            dirty.append(table)
    return dirty


def ensure_loaded_into_session():
    """Ensures st.session_state.db is populated."""
    if 'db' not in st.session_state:
        data, version = load_state()
        st.session_state.db = data
        st.session_state._db_version = version
        store_db_hashes(data)


def commit_from_session(invalidate_briefing=True, dirty_tables=None):
    """Saves st.session_state.db to DB.
    Raises StaleStateError if another session saved since this one loaded.

    If dirty_tables is provided, only those entity tables are rewritten.
    """
    if 'db' not in st.session_state:
        return

    if invalidate_briefing:
        st.session_state.db['briefing'] = "Data required to generate briefing."

    try:
        new_ver = save_state_to_db(
            st.session_state.db,
            expected_version=st.session_state.get('_db_version'),
            dirty_tables=dirty_tables
        )
        st.session_state._db_version = new_ver
        store_db_hashes(st.session_state.db)
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
        store_db_hashes(st.session_state.db)
    except Exception as e:
        st.error(f"Failed to save to DB: {e}")
