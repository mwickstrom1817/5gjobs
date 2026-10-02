"""Unit tests for core.py's pure (session-independent) functions."""
import datetime

from core import (
    esc_html, money_to_float, format_money, _fmt_duration, parts_summary,
    job_is_warranty, invoice_status, job_man_hours, job_parts_cost,
    job_value_summary, asset_warranty_left, _parse_report_time,
    get_job_stale_days, agreement_days_left, apply_job_status, days_in_status,
    job_followup, followup_jobs, clocked_hours, compute_hours_rows, now_local,
)
from services_email import daily_summary_recipients


# --- money ---

def test_money_to_float_parses_currency():
    assert money_to_float("$1,450.00") == 1450.0
    assert money_to_float("1450") == 1450.0
    assert money_to_float(0) is None       # falsy -> None
    assert money_to_float("TBD") is None   # free text -> None, not 0
    assert money_to_float("") is None


def test_format_money_round_trips():
    assert format_money("1450") == "$1,450"
    assert format_money("1450.50") == "$1,450.50"
    assert format_money("TBD") == "TBD"    # free text passes through
    assert format_money(None) == ""


def test_money_round_trip():
    assert money_to_float(format_money("$2,000")) == 2000.0


# --- misc formatting ---

def test_esc_html_escapes_markup_and_quotes():
    assert esc_html('<b>&"\'') == "&lt;b&gt;&amp;&quot;&#x27;"
    assert esc_html(None) == ""


def test_fmt_duration():
    assert _fmt_duration(2.5) == "2h 30m"
    assert _fmt_duration(0) == "0h 0m"
    assert _fmt_duration(1 / 60) == "0h 1m"


def test_parse_report_time():
    assert _parse_report_time("08:30", None) == datetime.time(8, 30)
    assert _parse_report_time("08:30:00", None) == datetime.time(8, 30)
    assert _parse_report_time("", datetime.time(7, 0)) == datetime.time(7, 0)
    assert _parse_report_time("garbage", datetime.time(7, 0)) == datetime.time(7, 0)


# --- parts ---

def test_parts_summary_counts_staged():
    job = {"parts": [
        {"status": "Staged"}, {"status": "Staged"}, {"status": "Ordered"},
    ]}
    assert parts_summary(job) == (2, 3)
    assert parts_summary({}) == (0, 0)


def test_job_parts_cost_multiplies_qty_and_skips_garbage():
    job = {"parts": [
        {"cost": "$10.00", "qty": 3},
        {"cost": "TBD", "qty": 5},          # free text ignored
        {"cost": None},
    ]}
    assert job_parts_cost(job) == 30.0


# --- warranty / invoicing ---

def test_job_is_warranty_reads_reports():
    assert not job_is_warranty({"isWarranty": False, "reports": []})
    assert job_is_warranty({"isWarranty": True})
    assert job_is_warranty({"reports": [{"isWarranty": True}]})


def test_invoice_status_pipeline():
    assert invoice_status({"status": "In Progress"}) is None
    # Completed with no record -> billable, so it self-populates the worklist
    assert invoice_status({"status": "Completed"}) == "Ready to Invoice"
    # ... but warranty work defaults to No Charge instead
    assert invoice_status({"status": "Completed", "isWarranty": True}) == "No Charge"
    assert invoice_status({"status": "Completed", "invoice": {"status": "Paid"}}) == "Paid"


# --- hours ---

def test_job_man_hours_counts_crew_members():
    job = {"reports": [
        {"hoursWorked": "8", "techsOnSite": "A, B, C"},   # 8h x 3 people = 24
        {"hoursWorked": "2"},                             # no crew listed = 2
        {"hoursWorked": "bad"},                           # garbage skipped
    ]}
    assert job_man_hours(job) == 26.0


def test_clocked_hours_closed_entries():
    entries = [{
        "clock_in": "2026-09-30T08:00:00",
        "clock_out": "2026-09-30T12:00:00",
    }]
    assert clocked_hours(entries) == 4.0


def test_clocked_hours_open_entry_capped_at_12h():
    long_ago = (now_local() - datetime.timedelta(hours=20)).isoformat()
    entries = [{"clock_in": long_ago, "clock_out": None}]
    assert clocked_hours(entries) == 12.0  # forgotten timer must not run forever


def test_compute_hours_rows_credits_everyone_on_site():
    jobs = [{
        "id": "j1", "title": "Install", "locationId": "l1", "techId": "t1",
        "reports": [
            {"timestamp": "2026-09-28T17:00:00", "hoursWorked": "8",
             "techsOnSite": "Alice, Bob", "isWarranty": True},
            {"timestamp": "2026-09-29T17:00:00", "hoursWorked": "0"},  # no hours, skipped
        ],
    }]
    techs = [{"id": "t1", "name": "Carol"}]
    locations = [{"id": "l1", "name": "HQ"}]
    rows = compute_hours_rows(jobs, techs, locations,
                              datetime.date(2026, 9, 28), datetime.date(2026, 9, 28))
    assert len(rows) == 2                      # 8h credited to EACH person on site
    assert {r["Tech"] for r in rows} == {"Alice", "Bob"}
    assert all(r["Hours"] == 8 for r in rows)
    assert all(r["Warranty"] == "Yes" for r in rows)
    assert all(r["Location"] == "HQ" for r in rows)


def test_compute_hours_rows_falls_back_to_report_author():
    jobs = [{
        "id": "j1", "title": "Solo", "locationId": None, "techId": "t9",
        "reports": [{"timestamp": "2026-09-28T17:00:00", "hoursWorked": "3", "techId": "t9"}],
    }]
    techs = [{"id": "t9", "name": "Dave"}]
    rows = compute_hours_rows(jobs, techs, [], datetime.date(2026, 1, 1), datetime.date(2026, 12, 31))
    assert len(rows) == 1 and rows[0]["Tech"] == "Dave"
    assert rows[0]["Location"] == "Unknown"


# --- stale jobs / follow-ups ---

def _active_job(status="In Progress", **kw):
    job = {"id": "j1", "title": "T", "status": status,
           "date": "2026-09-20T08:00:00", "reports": []}
    job.update(kw)
    return job


def test_get_job_stale_days():
    assert get_job_stale_days(_active_job(status="Completed")) is None
    future = (now_local() + datetime.timedelta(days=5)).isoformat()
    assert get_job_stale_days(_active_job(date=future)) is None
    old = (now_local() - datetime.timedelta(days=10)).isoformat()
    assert get_job_stale_days(_active_job(date=old)) == 10


def test_apply_job_status_stamps_and_days_in_status():
    job = _active_job()
    assert apply_job_status(job, "In Progress") is False   # no change, no stamp
    assert apply_job_status(job, "Customer on Hold", actor="boss@x.com") is True
    assert job["status"] == "Customer on Hold"
    assert job["status_changed_by"] == "boss@x.com"
    assert days_in_status(job) == 0


def test_followup_rules_and_ordering():
    held = _active_job(status="Customer on Hold")
    ten_ago = (now_local() - datetime.timedelta(days=10)).isoformat()
    held["status_changed_at"] = ten_ago
    result = job_followup(held)
    assert result == (10, 7, "Chase the customer")

    parts = _active_job(status="Parts not ordered")
    four_ago = (now_local() - datetime.timedelta(days=4)).isoformat()
    parts["status_changed_at"] = four_ago
    assert job_followup(parts)[2] == "Order the parts"

    fresh = _active_job(status="Customer on Hold")  # changed today: below threshold
    apply_job_status(fresh, "Customer on Hold")
    fresh["status_changed_at"] = now_local().isoformat()
    assert job_followup(fresh) is None

    out = followup_jobs([parts, held])
    assert [j["id"] for j, _d, _t, _a in out] == ["j1", "j1"]  # both match
    assert out[0][1] == 10 and out[1][1] == 4                  # most overdue first


# --- agreements ---

def test_agreement_days_left():
    soon = (now_local() + datetime.timedelta(days=20)).strftime("%Y-%m-%d")
    assert agreement_days_left({"renewal_date": soon}) == 20
    past = (now_local() - datetime.timedelta(days=5)).strftime("%Y-%m-%d")
    assert agreement_days_left({"renewal_date": past}) == -5
    assert agreement_days_left({"status": "Cancelled", "renewal_date": soon}) is None
    assert agreement_days_left({}) is None


# --- assets ---

def test_asset_warranty_left():
    months, expiry = asset_warranty_left({"installed_date": "2024-01-15", "warranty_months": 36})
    assert months is not None and months > 0
    assert expiry.year == 2027 and expiry.month == 1
    # unparseable / missing data -> (None, None), never an exception
    assert asset_warranty_left({}) == (None, None)
    assert asset_warranty_left({"installed_date": "bad", "warranty_months": 12}) == (None, None)


# --- job value ---

def test_job_value_summary():
    job = {
        "quoteValue": "1000", "status": "Completed",
        "reports": [{"hoursWorked": "4", "techsOnSite": "A, B"}],
        "parts": [{"cost": "50", "qty": 2}],
    }
    v = job_value_summary(job)
    assert v["revenue"] == 1000.0
    assert v["man_hours"] == 8.0
    assert v["parts"] == 100.0
    assert v["rev_per_hour"] == 125.0
    billed = dict(job, invoice={"amount": "1200"})
    v2 = job_value_summary(billed)
    assert v2["revenue"] == 1200.0 and v2["variance"] == 200.0
    assert job_value_summary({"status": "Completed"}) is None  # no price at all


# --- email recipients ---

def test_daily_summary_recipients_dedups_case_insensitive():
    techs = [{"email": "A@x.com"}, {"email": "b@x.com"}, {"email": "a@x.com"}]
    out = daily_summary_recipients(techs, ["B@X.com", "", None, "c@x.com"])
    assert out == ["A@x.com", "b@x.com", "c@x.com"]
