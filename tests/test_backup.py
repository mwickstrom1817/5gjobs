import io
import json
from datetime import date, datetime
from unittest.mock import Mock

import restore_from_r2 as restore
import services_backup as backup


class PagedS3:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def list_objects_v2(self, **kwargs):
        self.calls.append(kwargs)
        return self.pages[kwargs.get("ContinuationToken")]


def test_manual_keys_are_timestamped_and_retention_spans_key_formats():
    nightly = backup.backup_key(date(2026, 10, 24))
    manual = backup.backup_key(datetime(2026, 10, 24, 13, 14, 15, 123456), manual=True)

    assert nightly == "backups/app_state_2026-10-24.json"
    assert manual == "backups/app_state_2026-10-24T13-14-15-123456.json"
    keys = [
        manual,
        "unrelated/key.json",
        "backups/app_state_2026-10-23.json",
        nightly,
        "backups/app_state_2026-10-24T09-00-00-000000.json",
    ]
    assert backup.expired_keys(keys, keep=2) == [
        "backups/app_state_2026-10-23.json",
        "backups/app_state_2026-10-24.json",
    ]


def test_backup_listing_follows_continuation_tokens():
    s3 = PagedS3({
        None: {
            "Contents": [{"Key": "backups/app_state_2026-10-23.json"}],
            "IsTruncated": True,
            "NextContinuationToken": "next",
        },
        "next": {
            "Contents": [{"Key": "backups/app_state_2026-10-24.json"}],
            "IsTruncated": False,
        },
    })

    assert backup.list_backup_keys(s3, "bucket") == [
        "backups/app_state_2026-10-23.json",
        "backups/app_state_2026-10-24.json",
    ]
    assert s3.calls[1]["ContinuationToken"] == "next"


def test_restore_listing_paginates_and_sorts_keys():
    s3 = PagedS3({
        None: {
            "Contents": [{"Key": "backups/app_state_2026-10-24.json"}],
            "IsTruncated": True,
            "NextContinuationToken": "next",
        },
        "next": {
            "Contents": [{"Key": "backups/app_state_2026-10-23.json"}],
        },
    })

    assert restore.list_backups(s3, "bucket") == [
        "backups/app_state_2026-10-23.json",
        "backups/app_state_2026-10-24.json",
    ]


def test_manual_backup_uploads_encrypted_state(monkeypatch):
    plaintext = {
        "jobs": [],
        "techs": [],
        "locations": [{"id": "l1", "systems": [{"password": "plaintext"}]}],
    }
    encrypted = {
        "jobs": [],
        "techs": [],
        "locations": [{"id": "l1", "systems": "salt:ciphertext"}],
    }
    s3 = Mock()
    s3.list_objects_v2.return_value = {"Contents": []}
    encrypt = Mock(return_value=encrypted)
    logger = Mock()
    monkeypatch.setattr(backup, "get_r2_client", lambda: s3)
    monkeypatch.setattr(backup, "get_bucket_name", lambda: "bucket")
    monkeypatch.setattr(backup, "load_state", lambda: (plaintext, 7))
    monkeypatch.setattr(backup, "encrypt_state_systems", encrypt)
    monkeypatch.setattr(backup, "now_local", lambda: datetime(2026, 10, 24, 13, 14, 15, 123456))
    monkeypatch.setattr(backup, "get_logger", lambda: logger)

    ok, message = backup.run_daily_backup(manual=True)

    assert ok is True
    assert "2026-10-24T13-14-15-123456" in message
    encrypt.assert_called_once_with(plaintext)
    put = s3.put_object.call_args.kwargs
    assert put["Key"] == "backups/app_state_2026-10-24T13-14-15-123456.json"
    assert json.loads(put["Body"].decode("utf-8")) == encrypted
    assert b"plaintext" not in put["Body"]


def test_cli_restore_uses_active_schema_writer(monkeypatch, capsys):
    encrypted = {
        "jobs": [],
        "techs": [],
        "locations": [{"id": "l1", "systems": "salt:ciphertext"}],
        "settings": {"theme": "dark"},
    }
    decrypted = {
        "jobs": [],
        "techs": [],
        "locations": [{"id": "l1", "systems": [{"username": "tech"}]}],
        "settings": {"theme": "dark"},
    }
    s3 = Mock()
    s3.list_objects_v2.return_value = {
        "Contents": [{"Key": "backups/app_state_2026-10-24.json"}],
    }
    s3.get_object.return_value = {
        "Body": io.BytesIO(json.dumps(encrypted).encode("utf-8")),
    }
    validate = Mock(return_value=(True, "State dict is valid"))
    decrypt = Mock(return_value=decrypted)
    init_db = Mock()
    save = Mock(return_value=42)
    monkeypatch.setattr(restore, "get_r2_client", lambda: s3)
    monkeypatch.setattr(restore, "get_bucket", lambda: "bucket")
    monkeypatch.setattr(restore, "validate_state_dict", validate)
    monkeypatch.setattr(restore, "decrypt_state_systems", decrypt)
    monkeypatch.setattr(restore, "init_db", init_db)
    monkeypatch.setattr(restore, "save_state_to_db", save)
    monkeypatch.setattr("builtins.input", lambda _prompt: "yes")
    monkeypatch.setenv("DATABASE_URL", "mocked")
    monkeypatch.setattr("sys.argv", ["restore_from_r2.py"])

    restore.main()

    validate.assert_called_once_with(encrypted)
    decrypt.assert_called_once_with(encrypted)
    init_db.assert_called_once_with()
    save.assert_called_once_with(decrypted)
    assert "Restored. New DB version: 42" in capsys.readouterr().out
