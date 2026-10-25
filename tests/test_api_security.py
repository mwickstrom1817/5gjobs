import asyncio
import csv
import io
import json

import pytest
from fastapi import HTTPException

import api
import services_push


@pytest.fixture
def state_with_files():
    return {
        "adminEmails": ["admin@example.com"],
        "jobs": [{
            "id": "j1",
            "documents": [{"name": "plans", "key": "jobs/j1/docs/plans.pdf"}],
            "photos": ["photos/job-cover.jpg"],
            "reports": [{
                "photos": ["photos/report.jpg"],
                "signature_key": "signatures/report.png",
                "attachments": [{"key": "docs/report-notes.pdf"}],
            }],
        }],
        "locations": [{"documents": [{"key": "locations/l1/docs/site.pdf"}]}],
        "sops": [{"attachments": [{"key": "docs/sop.pdf"}]}],
    }


@pytest.mark.parametrize("key", [
    "jobs/j1/docs/plans.pdf",
    "photos/job-cover.jpg",
    "photos/report.jpg",
    "signatures/report.png",
    "docs/report-notes.pdf",
    "locations/l1/docs/site.pdf",
    "docs/sop.pdf",
])
def test_non_admin_can_access_referenced_files(state_with_files, key):
    assert api._file_key_allowed(state_with_files, key)


@pytest.mark.parametrize("key", [
    "backups/latest.json",
    "photos/unreferenced.jpg",
    "docs/unreferenced.pdf",
])
def test_non_admin_cannot_access_backup_or_unreferenced_files(state_with_files, key):
    assert not api._file_key_allowed(state_with_files, key)


def test_admin_can_access_backups_but_not_other_unreferenced_keys(state_with_files):
    assert api._file_key_allowed(state_with_files, "backups/latest.json", is_admin=True)
    assert not api._file_key_allowed(state_with_files, "photos/unreferenced.jpg", is_admin=True)


def test_referenced_backup_is_still_denied_to_non_admin(state_with_files):
    state_with_files["jobs"][0]["documents"].append({"key": "backups/latest.json"})
    assert not api._file_key_allowed(state_with_files, "backups/latest.json")


@pytest.mark.parametrize("key", [
    "", "/photos/report.jpg", "photos/../backups/latest.json",
    "photos\\report.jpg", "photos//report.jpg", "photos/./report.jpg",
])
def test_file_key_validation_rejects_noncanonical_paths(state_with_files, key):
    assert not api._file_key_allowed(state_with_files, key, is_admin=True)


def test_safe_tech_serializer_uses_explicit_field_allowlist():
    tech = {
        "id": "t1", "name": "Tech", "email": "tech@example.com",
        "initials": "TE", "color": "#123456", "skills": ["CCTV"],
        "notify_topic": "secret-topic", "private_notes": "not public",
    }
    safe = api._safe_tech(tech)
    assert safe == {
        "id": "t1", "name": "Tech", "email": "tech@example.com",
        "initials": "TE", "color": "#123456", "skills": ["CCTV"],
    }
    assert "notify_topic" not in safe
    assert tech["notify_topic"] == "secret-topic"


def test_list_techs_strips_notify_topic(monkeypatch):
    monkeypatch.setattr(api, "get_state", lambda: {"techs": [{
        "id": "t1", "name": "Tech", "notify_topic": "secret-topic",
    }]})
    result = api.list_techs(user={})
    assert "notify_topic" not in result["techs"][0]


def test_gemini_context_does_not_contain_notify_topic(monkeypatch):
    captured = {}

    class Models:
        def generate_content(self, model, contents):
            captured["contents"] = contents
            return type("Response", (), {"text": "ok"})()

    client = type("Client", (), {"models": Models()})()
    monkeypatch.setattr(api, "HAS_GENAI", True)
    monkeypatch.setattr(api, "get_api_key", lambda: "test-key")
    monkeypatch.setattr(api, "get_model", lambda key: (client, "test-model"))
    monkeypatch.setattr(api, "get_state", lambda: {
        "jobs": [], "locations": [],
        "techs": [{"id": "t1", "name": "Tech", "notify_topic": "secret-topic"}],
    })

    assert api.chat(api.ChatIn(message="hello"), user={}) == {"reply": "ok"}
    context = json.dumps(captured["contents"])
    assert "secret-topic" not in context
    assert "notify_topic" not in context


def test_final_admin_guard_is_case_insensitive_and_counts_unique_admins():
    with pytest.raises(HTTPException) as exc_info:
        api._ensure_admin_removal_allowed(["Admin@Example.com"], "admin@example.com")
    assert exc_info.value.status_code == 400

    with pytest.raises(HTTPException):
        api._ensure_admin_removal_allowed(
            ["Admin@Example.com", "admin@example.com"], "ADMIN@example.com"
        )

    api._ensure_admin_removal_allowed(
        ["admin@example.com", "other@example.com"], "admin@example.com"
    )


def test_empty_admin_list_does_not_bootstrap_caller(monkeypatch):
    monkeypatch.setattr(api, "get_state", lambda: {"adminEmails": [], "techs": []})
    monkeypatch.setattr(api, "save_state", lambda **kwargs: pytest.fail("must not save"))
    result = api.get_me(user={"email": "next-user@example.com"})
    assert result["is_admin"] is False


def test_job_patch_requires_admin_and_stamps_status(monkeypatch):
    patch_route = next(
        route for route in api.app.routes
        if route.path == "/jobs/{job_id}" and "PATCH" in route.methods
    )
    assert any(
        dependency.call is api.require_admin
        for dependency in patch_route.dependant.dependencies
    )

    job = {"id": "j1", "status": "Not Started"}
    monkeypatch.setattr(api, "get_state", lambda: {"jobs": [job]})
    monkeypatch.setattr(api, "save_state", lambda: None)
    result = api.update_job(
        "j1", api.JobUpdate(status="In Progress"), user={"email": "admin@example.com"}
    )
    assert result["status"] == "In Progress"
    assert result["status_changed_at"]
    assert result["status_changed_by"] == "admin@example.com"


def test_csv_export_uses_ordered_union_for_heterogeneous_jobs(monkeypatch):
    monkeypatch.setattr(api, "get_state", lambda: {"jobs": [
        {"id": "j1", "title": "First"},
        {"id": "j2", "priority": "High", "title": "Second"},
        {"status": "Completed", "id": "j3"},
    ]})
    response = api.export_csv(user={})

    async def read_body():
        return b"".join([chunk async for chunk in response.body_iterator])

    rows = list(csv.DictReader(io.StringIO(asyncio.run(read_body()).decode())))
    assert list(rows[0]) == ["id", "title", "priority", "status"]
    assert rows[0] == {"id": "j1", "title": "First", "priority": "", "status": ""}
    assert rows[1]["priority"] == "High"
    assert rows[2]["status"] == "Completed"


def test_new_notify_topic_has_128_bits_and_existing_topic_is_unchanged(monkeypatch):
    saves = []
    monkeypatch.setattr(services_push, "save_state", lambda **kwargs: saves.append(kwargs))

    def random_bytes(size):
        assert size == 16
        return b"\x01" * size

    monkeypatch.setattr(services_push.os, "urandom", random_bytes)
    tech = {"name": "Field Tech"}
    topic = services_push.get_or_create_notify_topic(tech)
    assert topic == "5gsec-fieldtech-" + "01" * 16
    assert len(saves) == 1

    existing = {"name": "Tech", "notify_topic": "legacy-topic"}
    assert services_push.get_or_create_notify_topic(existing) == "legacy-topic"
    assert len(saves) == 1


class FakeUpload:
    def __init__(self, data, filename="photo.jpg", content_type="image/jpeg"):
        self.data = data
        self.filename = filename
        self.content_type = content_type
        self.read_sizes = []

    async def read(self, size=-1):
        self.read_sizes.append(size)
        return self.data if size < 0 else self.data[:size]


def test_uploads_use_uuid_keys_and_bounded_reads(monkeypatch):
    uploaded = []
    monkeypatch.setattr(
        api, "upload_bytes", lambda data, key, content_type: uploaded.append((data, key)) or key
    )
    photo = FakeUpload(b"photo", filename="../../camera.jpg")
    result = asyncio.run(api.upload_photo(photo, user={}))
    assert result["key"].startswith("photos/")
    assert result["key"].endswith(".jpg")
    assert "camera" not in result["key"]
    assert photo.read_sizes == [api.MAX_UPLOAD_BYTES + 1]

    signature = FakeUpload(b"signature")
    result = asyncio.run(api.upload_signature(signature, user={}))
    assert result["key"].startswith("signatures/")
    assert result["key"].endswith(".png")
    assert len(uploaded) == 2


def test_upload_rejects_invalid_folder_and_oversized_body(monkeypatch):
    monkeypatch.setattr(api, "upload_bytes", lambda *args: pytest.fail("must not upload"))
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(api.upload_photo(FakeUpload(b"x"), folder="backups", user={}))
    assert exc_info.value.status_code == 400

    oversized = FakeUpload(b"x" * (api.MAX_UPLOAD_BYTES + 1))
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(api.upload_signature(oversized, user={}))
    assert exc_info.value.status_code == 413
    assert oversized.read_sizes == [api.MAX_UPLOAD_BYTES + 1]
