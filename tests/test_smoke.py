"""Boot smoke test: the app must import and render the login screen cleanly."""
from pathlib import Path

from streamlit.testing.v1 import AppTest

APP = Path(__file__).resolve().parent.parent / "app.py"


def test_app_boots_to_login_screen():
    at = AppTest.from_file(str(APP), default_timeout=120)
    at.run()
    assert not at.exception, f"App raised: {[str(e.value) for e in at.exception]}"
    # With no secrets configured the app must degrade to the friendly
    # "OAuth not configured" screen, not a traceback.
    assert any("Google OAuth is not configured" in e.value for e in at.error)


def test_admin_data_backup_page_renders():
    """The Data & Backup admin page (incl. cloud-backup controls) must render."""
    at = AppTest.from_file(str(APP), default_timeout=120)
    at.session_state["user_info"] = {"email": "boss@x.com", "name": "Boss", "picture": ""}
    at.session_state["adminEmails"] = ["boss@x.com"]
    at.session_state["techs"] = []
    at.session_state["jobs"] = []
    at.session_state["locations"] = []
    at.session_state["agreements"] = []
    at.session_state["sops"] = []
    at.session_state["settings"] = {}
    at.session_state["smtp_settings"] = {}
    at.session_state["briefing"] = "b"
    at.session_state["last_reminder_date"] = None
    at.session_state["chat_history"] = []
    at.session_state["_db_version"] = 1
    at.run()
    assert not at.exception, f"App raised: {[str(e.value) for e in at.exception]}"
    at.session_state["admin_view"] = "data"
    at.run()
    assert not at.exception, f"Admin page raised: {[str(e.value) for e in at.exception]}"
    assert any("Back Up Now" in (b.label or "") or "backup_now" in (b.key or "") for b in at.button)


def test_daily_report_has_single_submit_path():
    """One submit button; the old second 'Email Report to Admins' button is gone."""
    at = AppTest.from_file(str(APP), default_timeout=120)
    at.session_state["user_info"] = {"email": "boss@x.com", "name": "Boss", "picture": ""}
    at.session_state["adminEmails"] = ["boss@x.com"]
    at.session_state["jobs"] = [{
        "id": "j100_1", "title": "Camera install", "description": "d",
        "type": "Service", "priority": "High", "status": "In Progress",
        "locationId": "l1", "techId": "t1", "date": "2026-10-01T09:00:00",
        "reports": [], "contacts": [], "documents": [],
    }]
    at.session_state["techs"] = [{"id": "t1", "name": "Boss", "email": "boss@x.com",
                                  "initials": "BO", "color": "#b91c1c", "skills": []}]
    at.session_state["locations"] = [{"id": "l1", "name": "HQ", "address": "a"}]
    at.session_state["agreements"] = []
    at.session_state["sops"] = []
    at.session_state["settings"] = {}
    at.session_state["smtp_settings"] = {}
    at.session_state["briefing"] = "b"
    at.session_state["last_reminder_date"] = None
    at.session_state["chat_history"] = []
    at.session_state["_db_version"] = 1
    at.run()
    assert not at.exception
    # Use the app's own deep-link mechanism (same one Site History uses) to
    # open the dialog straight onto the daily section.
    at.session_state["_open_job_after_rerun"] = "j100_1"
    at.session_state["jobsection_j100_1"] = "daily"
    at.run()
    assert not at.exception, f"Daily section raised: {[str(e.value) for e in at.exception]}"
    labels = [(b.label or "") for b in at.button]
    assert "Submit Daily Report" in labels
    assert not any("Email Report to Admins" in l for l in labels)
