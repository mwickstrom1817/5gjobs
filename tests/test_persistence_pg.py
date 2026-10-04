"""Tests for persistence_pg.py's new per-entity table logic.

These tests mock the DB connection so they can run without a live Postgres.
"""
import json
from unittest.mock import MagicMock, patch

import pytest

import persistence_pg as pg


@pytest.fixture
def fake_conn():
    """A mock connection/cursor that records executed SQL."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)

    # State held by the fake DB.
    state = {
        "app_state": {},
        "jobs": {},
        "techs": {},
        "locations": {},
        "agreements": {},
        "sops": {},
        "app_settings": {},
        "versions": {"app_settings": 0, "app_state": 0},
    }

    def execute(sql, params=None):
        sql = sql.strip()
        params = params or ()

        # CREATE / SELECT to_regclass: ignore in mock
        if sql.startswith(("CREATE", "SELECT to_regclass")):
            return

        # SELECT 1 FROM app_settings WHERE key = _version LIMIT 1
        if "SELECT 1 FROM app_settings" in sql and "LIMIT 1" in sql:
            cur.fetchone.return_value = (1,) if pg.VERSION_KEY in state["app_settings"] else None
            return

        # SELECT value, version FROM app_state
        if "SELECT value, version FROM app_state" in sql:
            row = state["app_state"].get("global_state")
            cur.fetchone.return_value = {"value": row["value"], "version": row["version"]} if row else None
            return

        # SELECT version FROM app_state
        if "SELECT version FROM app_state" in sql:
            row = state["app_state"].get("global_state")
            cur.fetchone.return_value = (row["version"],) if row else (None,)
            return

        # SELECT data FROM <entity>
        if "SELECT data FROM" in sql and any(t in sql for t in pg.ENTITY_TABLES):
            table = sql.split("FROM")[1].strip()
            cur.fetchall.return_value = [{"data": v} for v in state[table].values()]
            return

        # SELECT key, value FROM app_settings
        if "SELECT key, value FROM app_settings" in sql:
            cur.fetchall.return_value = [{"key": k, "value": v} for k, v in state["app_settings"].items()]
            return

        # SELECT version FROM app_settings WHERE key = _version
        if "SELECT version FROM app_settings" in sql and params == (pg.VERSION_KEY,):
            v = state["versions"]["app_settings"] if pg.VERSION_KEY in state["app_settings"] else None
            cur.fetchone.return_value = (v,) if v is not None else None
            return

        # SELECT version FROM app_settings FOR UPDATE / plain (no params)
        if "SELECT version FROM app_settings" in sql:
            v = state["versions"]["app_settings"]
            cur.fetchone.return_value = (v,)
            return

        # INSERT / UPDATE app_state
        if "INSERT INTO app_state" in sql and "ON CONFLICT" in sql:
            value = json.loads(params[0])
            state["versions"]["app_state"] += 1
            state["app_state"]["global_state"] = {
                "value": value,
                "version": state["versions"]["app_state"],
            }
            cur.fetchone.return_value = (state["versions"]["app_state"],)
            return

        # INSERT app_state default
        if "INSERT INTO app_state (key, value) VALUES" in sql and "ON CONFLICT DO NOTHING" in sql:
            if "global_state" not in state["app_state"]:
                state["app_state"]["global_state"] = {"value": json.loads(params[1]), "version": 0}
            return

        # INSERT INTO app_settings (with or without explicit version)
        if "INSERT INTO app_settings" in sql and "ON CONFLICT" in sql:
            key = params[0]
            val = params[1]
            if isinstance(val, str):
                val = json.loads(val)
            # Migration sets version explicitly as third param.
            if len(params) >= 3 and params[2] is not None:
                state["versions"]["app_settings"] = params[2]
            if "RETURNING version" in sql:
                state["versions"]["app_settings"] += 1
                state["app_settings"][key] = val
                cur.fetchone.return_value = (state["versions"]["app_settings"],)
            else:
                state["app_settings"][key] = val
            return

        # DELETE FROM <entity>
        if sql.startswith("DELETE FROM") and any(t in sql for t in pg.ENTITY_TABLES):
            table = sql.split("FROM")[1].strip()
            state[table].clear()
            return

        # INSERT INTO <entity>
        if sql.startswith("INSERT INTO") and any(t in sql.split()[2] for t in pg.ENTITY_TABLES):
            table = sql.split()[2]
            eid, data_json = params
            state[table][eid] = json.loads(data_json)
            return

        raise NotImplementedError(f"Unhandled SQL in mock: {sql} params={params}")

    cur.execute.side_effect = execute
    return conn, state


def test_migrate_legacy_state(fake_conn):
    conn, state = fake_conn
    with patch.object(pg, "_db_conn", return_value=conn):
        cur = conn.cursor.return_value.__enter__.return_value
        # Seed legacy row
        legacy = {
            "jobs": [{"id": "j1", "title": "Job 1"}],
            "techs": [{"id": "t1", "name": "Tech 1"}],
            "locations": [{"id": "l1", "name": "Loc 1"}],
            "agreements": [],
            "sops": [],
            "briefing": "hello",
            "adminEmails": ["a@x.com"],
            "last_reminder_date": None,
            "settings": {},
            "smtp_settings": {},
        }
        state["app_state"]["global_state"] = {"value": legacy, "version": 5}
        pg._migrate_legacy_state(cur)

    assert state["jobs"]["j1"]["title"] == "Job 1"
    assert state["techs"]["t1"]["name"] == "Tech 1"
    assert state["locations"]["l1"]["name"] == "Loc 1"
    assert state["app_settings"]["briefing"] == "hello"
    assert state["versions"]["app_settings"] == 5


def test_load_from_tables_assembles_state(fake_conn):
    conn, state = fake_conn
    state["versions"]["app_settings"] = 3
    state["jobs"]["j1"] = {"id": "j1", "title": "T"}
    state["techs"]["t1"] = {"id": "t1", "name": "N"}
    state["app_settings"]["briefing"] = "b"
    state["app_settings"][pg.VERSION_KEY] = None

    with patch.object(pg, "_db_conn", return_value=conn):
        data, version = pg._load_from_tables()

    assert data["jobs"] == [{"id": "j1", "title": "T"}]
    assert data["techs"] == [{"id": "t1", "name": "N"}]
    assert data["briefing"] == "b"
    assert version == 3


def test_save_state_to_db_writes_entities_and_increments_version(fake_conn):
    conn, state = fake_conn
    state["versions"]["app_settings"] = 1
    state["app_settings"][pg.VERSION_KEY] = None

    data = {
        "jobs": [{"id": "j1", "title": "T"}],
        "techs": [],
        "locations": [],
        "agreements": [],
        "sops": [],
        "briefing": "b",
        "adminEmails": [],
        "settings": {},
        "smtp_settings": {},
        "last_reminder_date": None,
    }

    with patch.object(pg, "_db_conn", return_value=conn):
        new_ver = pg.save_state_to_db(data, expected_version=1)

    assert new_ver == 2
    assert state["jobs"]["j1"]["title"] == "T"
    assert state["app_settings"]["briefing"] == "b"
    assert state["app_state"]["global_state"]["value"]["jobs"][0]["title"] == "T"


def test_save_state_to_db_raises_stale_state(fake_conn):
    conn, state = fake_conn
    state["versions"]["app_settings"] = 5
    state["app_settings"][pg.VERSION_KEY] = None

    data = pg.DEFAULT_DATA.copy()
    with patch.object(pg, "_db_conn", return_value=conn):
        with pytest.raises(pg.StaleStateError):
            pg.save_state_to_db(data, expected_version=1)


def test_load_state_falls_back_to_legacy(fake_conn):
    conn, state = fake_conn
    legacy = {
        "jobs": [{"id": "j1", "title": "Legacy"}],
        "techs": [],
        "locations": [],
        "agreements": [],
        "sops": [],
        "briefing": "legacy",
        "adminEmails": [],
        "settings": {},
        "smtp_settings": {},
        "last_reminder_date": None,
    }
    state["app_state"]["global_state"] = {"value": legacy, "version": 7}

    with patch.object(pg, "_db_conn", return_value=conn):
        data, version = pg.load_state()

    assert data["jobs"][0]["title"] == "Legacy"
    assert version == 7
