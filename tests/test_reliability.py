import datetime
import io
import json

import numpy as np
import pytest
import streamlit as st

import core
import object_store
import persistence_pg as pg
from core import _calculate_shift_hours, _has_signature_strokes
from crypto import decrypt_state_systems


@pytest.fixture(autouse=True)
def clean_session():
    st.session_state.clear()
    st.session_state.db = {}
    st.session_state.jobs = []
    st.session_state.techs = []
    st.session_state.locations = []
    st.session_state.briefing = "b"
    st.session_state.adminEmails = []
    st.session_state.agreements = []
    st.session_state.sops = []
    st.session_state.settings = {}
    st.session_state.smtp_settings = {}
    st.session_state.last_reminder_date = None
    st.session_state._db_hashes = {}
    st.session_state._db_version = 1
    yield
    st.session_state.clear()


def test_save_state_returns_false_on_commit_failure(monkeypatch):
    def fail(**kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(core, "commit_from_session", fail)
    assert core.save_state() is False


def test_save_state_returns_true_after_commit(monkeypatch):
    monkeypatch.setattr(core, "commit_from_session", lambda **kwargs: 2)
    assert core.save_state() is True


def test_force_overwrite_does_not_swallow_failure(monkeypatch):
    monkeypatch.setattr(pg, "save_state_to_db", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("down")))
    with pytest.raises(RuntimeError, match="down"):
        pg.force_overwrite_from_session()


def test_manual_backup_contains_every_canonical_field_and_no_smtp_password():
    st.session_state.smtp_settings = {
        "SMTP_SERVER": "mail.example.com",
        "SMTP_EMAIL": "jobs@example.com",
        "SMTP_PASSWORD": "must-not-export",
    }
    st.session_state.construction_emails = ["office@example.com"]
    st.session_state.last_hours_digest_date = "2026-10-24"
    st.session_state.last_warranty_report_month = "2026-10"

    data = json.loads(core.download_data_as_json())
    assert set(data) == set(pg.DEFAULT_DATA)
    assert data["construction_emails"] == ["office@example.com"]
    assert data["last_hours_digest_date"] == "2026-10-24"
    assert data["last_warranty_report_month"] == "2026-10"
    assert data["smtp_settings"]["SMTP_SERVER"] == "mail.example.com"
    assert "SMTP_PASSWORD" not in data["smtp_settings"]


def test_manual_backup_encrypts_location_systems(monkeypatch):
    monkeypatch.setenv("ENCRYPTION_KEY", "unit-test-key")
    systems = [{"name": "NVR", "username": "admin", "password": "secret"}]
    st.session_state.locations = [{"id": "l1", "name": "HQ", "systems": systems}]

    data = json.loads(core.download_data_as_json())
    assert isinstance(data["locations"][0]["systems"], str)
    assert "secret" not in data["locations"][0]["systems"]
    restored = decrypt_state_systems(data)
    assert restored["locations"][0]["systems"] == systems


def test_failed_save_reloads_or_latches_to_prevent_later_commit(monkeypatch):
    st.session_state.jobs = [{"id": "unsaved"}]
    monkeypatch.setattr(core, "commit_from_session", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("down")))

    def refresh_succeeds():
        st.session_state.jobs = [{"id": "committed"}]

    monkeypatch.setattr(core, "refresh_session_from_db", refresh_succeeds)
    assert core.save_state() is False
    assert st.session_state.jobs == [{"id": "committed"}]

    st.session_state.jobs = [{"id": "unsaved-again"}]
    monkeypatch.setattr(core, "refresh_session_from_db", lambda: (_ for _ in ()).throw(RuntimeError("offline")))
    assert core.save_state() is False
    assert st.session_state._db_load_error == "offline"
    assert core.save_state() is False


def test_add_job_dismiss_clears_open_state():
    st.session_state._open_add_job = True
    st.session_state.assign_tech_add = "Alice"
    st.session_state._add_job_suggestions = ["suggestion"]
    core.dismiss_add_job_dialog()
    assert "_open_add_job" not in st.session_state
    assert "assign_tech_add" not in st.session_state
    assert "_add_job_suggestions" not in st.session_state


def test_edit_job_dismiss_clears_open_state():
    st.session_state._open_edit_job_id = "j1"
    st.session_state.assign_tech_edit_j1 = "Alice"
    st.session_state._edit_dialog_initialized_j1 = True
    core.dismiss_edit_job_dialog()
    assert "_open_edit_job_id" not in st.session_state
    assert "assign_tech_edit_j1" not in st.session_state
    assert "_edit_dialog_initialized_j1" not in st.session_state


def test_backup_validation_rejects_duplicate_sop_ids_and_bad_items():
    state = {
        "jobs": [],
        "techs": [{"id": "t1"}],
        "locations": [{"id": "l1"}],
        "sops": [{"id": "s1"}, {"id": "s1"}],
    }
    ok, message = core.validate_state_dict(state)
    assert not ok and "Duplicate sops id" in message

    state["techs"] = ["not-a-dict"]
    ok, message = core.validate_state_dict(state)
    assert not ok and "Invalid item in techs" in message


def test_same_named_uploads_get_distinct_keys(monkeypatch):
    class FakeS3:
        def __init__(self):
            self.keys = []

        def upload_fileobj(self, fileobj, bucket, key, ExtraArgs):
            self.keys.append(key)

    class Upload(io.BytesIO):
        def __init__(self):
            super().__init__(b"same bytes")
            self.name = "photo.jpg"
            self.type = "image/jpeg"

    s3 = FakeS3()
    monkeypatch.setattr(object_store, "get_r2_client", lambda: s3)
    monkeypatch.setattr(object_store, "get_bucket_name", lambda: "bucket")
    monkeypatch.setattr(object_store, "_boto", lambda: (None, Exception, None))

    first = object_store.upload_streamlit_file(Upload(), folder="photos")
    second = object_store.upload_streamlit_file(Upload(), folder="photos")
    assert first != second
    assert s3.keys == [first, second]


def test_overnight_shift_calculation():
    assert _calculate_shift_hours(datetime.time(22, 0), datetime.time(6, 0)) == 8
    assert _calculate_shift_hours(datetime.time(9, 0), datetime.time(17, 30)) == 8.5
    assert _calculate_shift_hours(datetime.time(9, 0), datetime.time(9, 0)) is None


def test_blank_signature_is_rejected_but_strokes_are_accepted():
    blank = np.full((40, 80, 4), 255, dtype=np.uint8)
    assert not _has_signature_strokes(blank)
    signed = blank.copy()
    signed[10:12, 10:30, :3] = 0
    assert _has_signature_strokes(signed)
