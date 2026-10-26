from services_pdf import generate_job_brief_pdf


def test_job_brief_contains_job_site_credentials_and_history(monkeypatch):
    texts = []
    import reportlab.platypus

    original_paragraph = reportlab.platypus.Paragraph

    def recording_paragraph(text, style, *args, **kwargs):
        texts.append(str(text))
        return original_paragraph(text, style, *args, **kwargs)

    monkeypatch.setattr(reportlab.platypus, "Paragraph", recording_paragraph)

    job = {
        "id": "j1",
        "title": "Camera <b> upgrade",
        "description": "Install & configure the new system",
        "type": "Project",
        "priority": "High",
        "status": "In Progress",
        "date": "2026-10-25T09:00:00",
        "quoteValue": "$4,500",
        "contacts": [{"label": "Site Contact", "name": "Customer", "phone": "555-0100", "email": "customer@example.com"}],
        "parts": [{"name": "Cat6 cable", "qty": 2, "status": "Staged", "vendor": "ADI", "cost": "$100", "notes": "Plenum"}],
        "documents": [{"name": "Current Scope.pdf", "key": "docs/current"}],
        "reports": [{"timestamp": "2026-10-25T12:00:00", "authorEmail": "tech@example.com", "content": "Current work <tag>"}],
    }
    tech = {"id": "t1", "name": "Alice"}
    location = {
        "id": "l1",
        "name": "Warehouse",
        "address": "123 Main St",
        "systems": [{"name": "NVR", "username": "router-admin", "password": "s3cr3t", "ip": "192.168.1.10", "notes": "VLAN 20"}],
        "documents": [{"name": "Floorplan.pdf", "key": "docs/floor"}],
        "assets": [{"tag": "5GS-1", "type": "NVR", "make_model": "Test NVR", "position": "Office", "serial": "SN123"}],
    }
    previous = {
        "id": "j-old",
        "title": "Previous service",
        "date": "2026-09-01T09:00:00",
        "status": "Completed",
        "techId": "t1",
        "reports": [{"timestamp": "2026-09-01T15:00:00", "techId": "t1", "content": "Previous site note"}],
    }

    pdf = generate_job_brief_pdf.__wrapped__(job, tech, location, [previous])

    assert pdf.startswith(b"%PDF")
    assert len(pdf) > 5000
    joined = "\n".join(texts)
    assert "Camera &lt;b&gt; upgrade" in joined
    assert "Install &amp; configure" in joined
    assert "Cat6 cable" in joined
    assert "Current Scope.pdf" in joined
    assert "Floorplan.pdf" in joined
    assert "router-admin" in joined
    assert "s3cr3t" in joined
    assert "192.168.1.10" in joined
    assert "Current work &lt;tag&gt;" in joined
    assert "Previous site note" in joined
    assert "5GS-1" in joined


def test_job_brief_handles_missing_optional_data():
    job = {"id": "j2", "title": "Simple service", "status": "Not Started", "reports": []}
    pdf = generate_job_brief_pdf.__wrapped__(job, None, None, [])
    assert pdf.startswith(b"%PDF")
    assert len(pdf) > 1000
