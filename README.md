# 5G Security Job Board

Operational dashboard for 5G Security — a physical-security company (cameras &
NVRs, access control, alarm systems, infrastructure cabling; **not** 5G towers).

**Live app:** https://5gjobs.streamlit.app

## What it does

- **Job dispatch** — create/assign jobs, kanban board by status, calendar and map views
- **Field reporting** — daily reports with photos, voice-note transcription, time clock, customer digital signature
- **Parts pipeline** — Needed → Ordered → Received → Staged, with vendor/cost tracking
- **Equipment registry** — QR-tagged assets per site with warranty tracking and printable Avery labels
- **Site knowledge** — per-location credentials/systems (IPs & passwords), shared documents, site history
- **Contracts & invoicing** — service agreements with renewal alerts; completed jobs flow into an invoicing worklist
- **AI assistant** — Gemini-powered morning briefing, report summaries, and a data-aware chatbot
- **Notifications** — assignment/completion emails, daily ops summary (weekday 7 AM), weekly hours digest (Friday), ntfy push to techs' phones
- **Wall display** — auto-rotating read-only TV/kiosk board (`?kiosk=<KIOSK_TOKEN>`)
- **Admin panel** — technicians, locations, hours report, data browser, analytics, backup/restore, diagnostics

## Architecture

Originally a single 7,500-line `app.py`; now split into focused modules with
`app.py` as the thin Streamlit entry point:

| Module | Responsibility |
|--------|----------------|
| `app.py` | Entry point: page setup, Google OAuth, session init, tab routing |
| `core.py` | Constants, state save/load, domain rules, formatting, helpers |
| `services_ai.py` | Gemini client, model selection, summaries, briefing |
| `services_geo.py` | Open-Meteo geocoding + weather |
| `services_push.py` | ntfy push notifications |
| `services_pdf.py` | ReportLab PDF report generation |
| `services_email.py` | SMTP email builders/senders |
| `services_scheduler.py` | Keep-awake pinger + background reminder scheduler |
| `ui_widgets.py` | Reusable input widgets (mobile time picker, sub-nav) |
| `ui_dialogs.py` | `@st.dialog` modals (job details, completion, assets) |
| `ui_cards.py` | Job cards/grids + interactive map view |
| `ui_tv.py` | Kiosk / TV wall display |
| `ui_views.py` | SOPs, data browser, invoicing, analytics, hours, chatbot |
| `ui_admin.py` | Admin panel tiles |
| `api.py` | FastAPI backend (`uvicorn api:app`) exposing the same data to the iOS client |
| `persistence_pg.py` | Postgres persistence (single versioned JSONB state row, optimistic locking) |
| `object_store.py` | Cloudflare R2 / S3 file storage (photos, signatures, docs) |

## Tech stack

Streamlit · Neon Postgres · Cloudflare R2 · Google Gemini · Google OAuth 2.0 ·
SMTP email · ntfy push · Folium maps · ReportLab PDFs · FastAPI (iOS backend)

## Running locally

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements.txt   # Windows
# source .venv/bin/activate && pip install -r requirements.txt   # macOS/Linux
streamlit run app.py
```

Opens at http://localhost:8501. Without any secrets configured you should see
the "Google OAuth is not configured" screen — that confirms the app is healthy.

For a fully working local instance, create `.streamlit/secrets.toml`
(gitignored) with the values below.

## Configuration

Set via Streamlit Cloud secrets (production) or `.streamlit/secrets.toml` /
environment variables (local). All are read with a secrets → env fallback.

| Variable | Purpose |
|----------|---------|
| `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` / `GOOGLE_REDIRECT_URI` | Google OAuth login (required) |
| `DATABASE_URL` (or `NEON_DB_URL`) | Postgres connection string (required) |
| `GEMINI_API_KEY` | Gemini AI features (briefing, chat, summaries) |
| `R2_ENDPOINT_URL` / `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` / `R2_BUCKET_NAME` | Cloudflare R2 photo/doc storage (AWS_* / S3_* names also accepted) |
| `SMTP_SERVER` / `SMTP_PORT` / `SMTP_EMAIL` / `SMTP_PASSWORD` | Email notifications (can also be set in Admin → Email & SMTP, stored in DB) |
| `APP_URL` | Public app URL (email links, QR codes, keep-alive ping) |
| `COOKIE_SECRET` | Signs persistent login cookies (falls back to `GOOGLE_CLIENT_SECRET`) |
| `ALLOWED_EMAIL_DOMAIN` | Anyone at this domain may log in (e.g. `fivegsecurity.net`) |
| `KIOSK_TOKEN` | Enables the read-only TV display at `?kiosk=<token>` |
| `APP_TIMEZONE` | IANA timezone for timestamps (default `America/Chicago`) |
| `NTFY_SERVER` | Push server (default `https://ntfy.sh`) |
| `LOGO_URL` | Public logo URL for email headers (optional) |

## Deploying

The live app redeploys automatically on every push to `main` (~1–2 min).

```bash
git add -A
git commit -m "describe the change"
git push origin main
```

Safety nets from the August refactor: branch `backup/pre-refactor` and tag
`pre-refactor` point at the pre-modularization code. Rollback:

```bash
git revert <commit> && git push origin main        # revert a change cleanly
# or: git reset --hard pre-refactor && git push --force origin main
```

## Notes

- All app state lives in one Postgres `app_state` JSONB row with a version
  column — saves are optimistic-locking, so two users can't silently
  overwrite each other (the app shows a refresh-and-retry warning instead).
- `debug_models.py` / `debug_weather.py` are standalone scripts for checking
  Gemini model availability and the geocoding/weather APIs.
- `.github/workflows/keepalive.yml` pings the app every 2 hours so Streamlit
  Community Cloud doesn't sleep it.
