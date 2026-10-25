import services_pdf
from services_pdf import _md_to_rl_html, generate_sop_pdf


def assert_valid_pdf(pdf_bytes):
    assert isinstance(pdf_bytes, bytes)
    assert pdf_bytes.startswith(b"%PDF-")
    assert pdf_bytes.rstrip().endswith(b"%%EOF")
    assert len(pdf_bytes) > 1000


def test_new_format_sop_escapes_metacharacters_in_every_field():
    sop = {
        "id": "sop-<1>&",
        "title": "A <b> B & C > D",
        "category": "Safety <critical> & wiring",
        "updated_at": "<2026&10>24T12:00:00",
        "updated_by": "tech <admin>&@example.com",
        "content": (
            "# Inspect <hardware> & connections\n"
            "Use **bold** and *italic* text with <script> & symbols.\n"
            "- Check <input> & output"
        ),
        "notes": "## Notes <b>&\n__Be careful__ & _verify_ <voltage>.",
        "attachments": [{"name": "diagram <final>&.dwg", "key": "local-key"}],
    }

    assert_valid_pdf(generate_sop_pdf.__wrapped__(sop))


def test_legacy_sop_steps_escape_metacharacters():
    sop = {
        "id": "legacy-1",
        "title": "Legacy <procedure> & checklist",
        "steps": ["A <b> B & C", "2 > 1 & 1 < 2"],
        "attachments": [{"name": "old <steps>&.txt"}],
    }

    assert_valid_pdf(generate_sop_pdf.__wrapped__(sop))


def test_markdown_conversion_escapes_html_but_preserves_lightweight_formatting():
    converted = _md_to_rl_html("**bold** *italic* <b>literal</b> & text")

    assert converted == "<b>bold</b> <i>italic</i> &lt;b&gt;literal&lt;/b&gt; &amp; text"


def test_sop_generation_failure_returns_none(monkeypatch):
    def fail(*args):
        raise ValueError("invalid flowable")

    monkeypatch.setattr(services_pdf, "_sop_content_flowables", fail)

    assert generate_sop_pdf.__wrapped__({"title": "SOP", "content": "Procedure"}) is None
