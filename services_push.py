import os
import re

import requests
import streamlit as st

from core import save_state

def get_ntfy_server():
    """ntfy server to publish to. Defaults to the public ntfy.sh; override with
    NTFY_SERVER (secret/env) if we ever self-host or move to ntfy Pro."""
    server = os.getenv("NTFY_SERVER")
    if not server:
        try:
            server = st.secrets.get("NTFY_SERVER")
        except Exception:
            server = None
    return (server or "https://ntfy.sh").rstrip("/")

def send_push(topic, title, message, tags=None, priority=3):
    """Sends a push notification via ntfy. Pure/thread-safe (safe in the scheduler).
    Keep payloads generic — job titles only, never addresses or credentials.
    Returns True on success."""
    if not topic:
        return False
    try:
        r = requests.post(
            get_ntfy_server() + "/",
            json={
                "topic": topic,
                "title": title,
                "message": message,
                "tags": tags or ["hammer_and_wrench"],
                "priority": priority,
            },
            timeout=8,
        )
        return r.status_code == 200
    except Exception:
        return False

def get_or_create_notify_topic(tech):
    """Returns the tech's personal push topic, generating and persisting an
    unguessable one on first use (random suffix = the 'password')."""
    if not tech:
        return None
    if not tech.get('notify_topic'):
        slug = re.sub(r'[^a-z0-9]', '', (tech.get('name') or 'tech').lower())[:10] or 'tech'
        tech['notify_topic'] = f"5gsec-{slug}-{os.urandom(4).hex()}"
        save_state(invalidate_briefing=False)
    return tech['notify_topic']

def push_assignment(job, tech):
    """New-assignment push to the tech's phone. Generic payload by design.
    Generates the tech's topic on first use so pushes work out of the box
    (they just won't be received until the tech subscribes in the ntfy app)."""
    if not tech:
        return False
    topic = get_or_create_notify_topic(tech)
    return send_push(
        topic,
        "New Job Assignment",
        f"{job.get('title', 'A job')} ({job.get('priority', 'N/A')}) — open the job board for details.",
        tags=["clipboard"],
    )
