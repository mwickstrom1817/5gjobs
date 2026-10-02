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
