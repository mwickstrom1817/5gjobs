from pathlib import Path

from streamlit.testing.v1 import AppTest

APP = Path(__file__).resolve().parent.parent / "app.py"


def make_app():
    at = AppTest.from_file(str(APP), default_timeout=120)
    at.session_state["user_info"] = {"email": "boss@x.com", "name": "Boss", "picture": ""}
    at.session_state["adminEmails"] = ["boss@x.com"]
    at.session_state["techs"] = [
        {"id": "t1", "name": "Alice", "email": "alice@x.com", "initials": "AL", "color": "#b91c1c", "skills": ["Access Control"]},
        {"id": "t2", "name": "Bob", "email": "bob@x.com", "initials": "BO", "color": "#52525b", "skills": []},
    ]
    at.session_state["jobs"] = []
    at.session_state["locations"] = [{"id": "l1", "name": "HQ", "address": "123 Main St"}]
    at.session_state["agreements"] = []
    at.session_state["sops"] = []
    at.session_state["settings"] = {}
    at.session_state["smtp_settings"] = {}
    at.session_state["briefing"] = "b"
    at.session_state["last_reminder_date"] = None
    at.session_state["chat_history"] = []
    at.session_state["db"] = {}
    at.session_state["_db_hashes"] = {}
    at.session_state["_db_version"] = 1
    return at


def test_new_job_defaults_to_unassigned():
    at = make_app()
    at.session_state["_open_add_job"] = True
    at.run()
    assert not at.exception, f"New Job dialog raised: {[str(e.value) for e in at.exception]}"
    assign = next(s for s in at.selectbox if s.label == "Assign Tech")
    assert assign.value == "Unassigned"


def test_sop_with_markup_in_title_renders_without_pdf_crash():
    at = make_app()
    at.session_state["sops"] = [{
        "id": "s1", "title": "A <b> B", "category": "R&D <root>",
        "content": "# Procedure\nUse **bold** safely.", "notes": "",
        "attachments": [], "updated_by": "boss@x.com",
    }]
    at.run()
    assert not at.exception, f"SOP view raised: {[str(e.value) for e in at.exception]}"
    assert any("A <b> B" in str(e.label) for e in at.expander)


def test_sop_export_failure_is_graceful(monkeypatch):
    monkeypatch.setattr("ui_views.generate_sop_pdf", lambda sop: None)
    at = make_app()
    at.session_state["sops"] = [{
        "id": "s1", "title": "Broken PDF", "category": "Access Control",
        "content": "Procedure", "notes": "", "attachments": [],
    }]
    at.run()
    next(b for b in at.button if b.key == "sop_pdf_prepare_s1").click()
    at.run()
    assert not at.exception
    assert any("PDF export failed" in e.value for e in at.error)


def test_dispatch_radio_preselects_edit_job_tech():
    at = make_app()
    at.session_state["jobs"] = [{
        "id": "j1", "title": "Access control repair", "description": "HID controller",
        "type": "Service", "priority": "Medium", "status": "Not Started",
        "locationId": "l1", "techId": "t2", "date": "2026-10-01T09:00:00",
        "reports": [], "contacts": [], "documents": [],
    }]
    at.session_state["_open_edit_job_id"] = "j1"
    at.run()
    assert not at.exception, f"Edit Job dialog raised: {[str(e.value) for e in at.exception]}"

    at.session_state["toggle_suggest_tech_j1"] = True
    at.run()
    assert not at.exception
    radio = next(r for r in at.radio if r.key == "radio_suggest_tech_j1")
    choice = next(v for v in radio.options if v.startswith("1. Alice"))
    radio.set_value(choice)
    at.run()
    assert not at.exception, f"Suggestion selection raised: {[str(e.value) for e in at.exception]}"
    assign = next(s for s in at.selectbox if s.label == "Assign Tech")
    assert assign.value.startswith("Alice")

    next(b for b in at.button if b.key == "cancel_edit_job_j1").click()
    at.run()
    at.session_state["_open_edit_job_id"] = "j1"
    at.run()
    assert not at.exception
    assign = next(s for s in at.selectbox if s.label == "Assign Tech")
    assert assign.value.startswith("Bob")


def test_final_admin_delete_is_disabled():
    at = make_app()
    at.session_state["admin_view"] = "access"
    at.run()
    assert not at.exception, f"Admin access page raised: {[str(e.value) for e in at.exception]}"
    delete = next(b for b in at.button if b.key == "del_admin_boss@x.com")
    assert delete.disabled


def test_job_brief_action_generates_download():
    at = make_app()
    at.session_state["jobs"] = [{
        "id": "j1", "title": "Access control repair", "description": "HID controller",
        "type": "Service", "priority": "Medium", "status": "Not Started",
        "locationId": "l1", "techId": "t1", "date": "2026-10-01T09:00:00",
        "reports": [], "contacts": [], "documents": [],
    }]
    at.session_state["_open_job_after_rerun"] = "j1"
    at.run()
    assert not at.exception, f"Job Details raised: {[str(e.value) for e in at.exception]}"
    brief = next(b for b in at.button if b.label == "📋 Job Brief")
    brief.click()
    at.run()
    assert not at.exception, f"Job Brief generation raised: {[str(e.value) for e in at.exception]}"
    assert any(b.key == "download_job_brief_j1" for b in at.download_button)
