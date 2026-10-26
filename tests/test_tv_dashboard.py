import datetime
from pathlib import Path

from streamlit.testing.v1 import AppTest

from ui_tv import _tv_dashboard_html, _tv_date

APP = Path(__file__).resolve().parent.parent / "app.py"
NOW = datetime.datetime(2026, 10, 25, 9, 30)


def _job(job_id, title, day, tech="t1", priority="Medium", status="In Progress"):
    return {
        "id": job_id,
        "title": title,
        "date": day.isoformat() + "T09:00:00" if day else "",
        "techId": tech,
        "locationId": "l1",
        "priority": priority,
        "status": status,
        "reports": [],
    }


def test_dashboard_has_today_week_future_attention_and_no_credentials():
    today = NOW.date()
    jobs = [
        _job("j1", "Camera install", today, priority="High"),
        _job("j2", "Overdue repair", today - datetime.timedelta(days=2), tech="t2", priority="Critical"),
        _job("j3", "Tomorrow upgrade", today + datetime.timedelta(days=1)),
        _job("j4", "Future project", today + datetime.timedelta(days=10), tech="t2"),
        _job("j5", "Unscheduled survey", None, tech=None, priority="Low"),
    ]
    completed = _job("j6", "Completed job", today, status="Completed")
    completed["reports"] = [{"timestamp": today.isoformat() + "T12:00:00", "completion_checklist": ["Done"]}]
    jobs.append(completed)
    techs = [{"id": "t1", "name": "Alice"}, {"id": "t2", "name": "Bob"}]
    locations = [{"id": "l1", "name": "Warehouse", "systems": [{"password": "secret"}]}]

    html = _tv_dashboard_html(jobs, techs, locations, generated=NOW)

    assert "OPERATIONS COMMAND CENTER" in html
    assert "TODAY" in html
    assert "THIS WEEK" in html
    assert "LOOKING AHEAD" in html
    assert "NEEDS ATTENTION" in html
    assert "TEAM LOAD" in html
    assert "Camera install" in html
    assert "Overdue repair" in html
    assert "Tomorrow upgrade" in html
    assert "Future project" in html
    assert "Unscheduled" in html
    assert "DONE TODAY" in html
    assert "secret" not in html
    assert "Rotating" not in html


def test_dashboard_caps_long_today_list():
    jobs = [_job(f"j{i}", f"Job {i}", NOW.date()) for i in range(20)]
    html = _tv_dashboard_html(jobs, [{"id": "t1", "name": "Alice"}], [], generated=NOW)
    assert "+14 more today" in html


def test_invalid_job_date_is_safe():
    assert _tv_date({"date": "not-a-date"}) is None
    assert _tv_date({}) is None


def test_display_mode_renders_single_dashboard():
    at = AppTest.from_file(str(APP), default_timeout=120)
    at.session_state["user_info"] = {"email": "boss@x.com", "name": "Boss", "picture": ""}
    at.session_state["adminEmails"] = ["boss@x.com"]
    at.session_state["techs"] = [{"id": "t1", "name": "Alice", "email": "alice@x.com", "initials": "AL", "skills": []}]
    at.session_state["jobs"] = [_job("j1", "Camera install", NOW.date())]
    at.session_state["locations"] = [{"id": "l1", "name": "Warehouse", "address": "123 Main St"}]
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
    at.session_state["kiosk_mode"] = True
    at.run()

    assert not at.exception, f"Display Mode raised: {[str(e.value) for e in at.exception]}"
    rendered = "\n".join(str(m.value) for m in at.markdown)
    assert "OPERATIONS COMMAND CENTER" in rendered
    assert "Camera install" in rendered
    assert "LOOKING AHEAD" in rendered
