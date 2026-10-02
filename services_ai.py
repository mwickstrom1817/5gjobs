import os
import datetime

import streamlit as st

from core import get_logger, now_local, STALE_JOB_DAYS, get_job_stale_days

_GENAI = None
_GENAI_TYPES = None


def _genai():
    """Lazy import: google-genai and its dependencies cost real startup time,
    and most runs never call the AI."""
    global _GENAI, _GENAI_TYPES
    if _GENAI is None:
        from google import genai as _g
        from google.genai import types as _t
        _GENAI, _GENAI_TYPES = _g, _t
    return _GENAI, _GENAI_TYPES

def get_api_key():
    # Try getting from Streamlit secrets, then Env, then return None
    if "GEMINI_API_KEY" in st.secrets:
        return st.secrets["GEMINI_API_KEY"]
    return os.getenv("GEMINI_API_KEY") or os.getenv("API_KEY")

@st.cache_resource
def get_available_model(api_key):
    """
    Dynamically lists models available to the API key and returns the client and best model name.
    Prefers current stable Flash models. (Google retired the Gemini 1.x family -
    the old hardcoded 1.5 names now 404.)
    """
    logger = get_logger()
    genai, _ = _genai()
    try:
        client = genai.Client(api_key=api_key)
    except Exception as e:
        # A bad key or an SDK hiccup must not propagate — callers treat a None
        # client as 'AI unavailable' and carry on without it.
        logger.log(f"Gemini client init failed: {e}")
        return None, 'gemini-flash-latest'

    def _gen_actions(m):
        # google-genai SDK exposes 'supported_actions'; the legacy SDK used
        # 'supported_generation_methods'. Check both so the filter actually works.
        return getattr(m, 'supported_actions', None) or getattr(m, 'supported_generation_methods', None)

    try:
        all_models = list(client.models.list())
        logger.log(f"Discovery: Found {len(all_models)} available models.")

        # Text-capable Gemini models only - specialty variants (TTS, image,
        # live audio, embeddings) reject plain generate_content calls.
        EXCLUDE = ('tts', 'image', 'audio', 'live', 'embed', 'veo', 'imagen', 'aqa')
        candidates = []
        for m in all_models:
            lname = m.name.lower()
            if 'gemini' not in lname:
                continue
            if any(x in lname for x in EXCLUDE):
                continue
            actions = _gen_actions(m)
            if actions and not ('generateContent' in actions or 'generate_content' in actions):
                continue
            candidates.append(m)

        # Preference order: newest stable Flash -> rolling alias -> 2.0 Flash -> Pro -> any Flash
        preferences = ['gemini-2.5-flash', 'gemini-flash-latest', 'gemini-2.0-flash', 'gemini-2.5-pro', 'flash']

        # Pass 1: exact model names
        for pref in preferences:
            best = next((m for m in candidates if m.name.lower().split('/')[-1] == pref), None)
            if best:
                logger.log(f"Using Gemini model: {best.name}")
                return client, best.name

        # Pass 2: substring match, preferring stable over preview/experimental builds
        for pref in preferences:
            best = next((m for m in candidates if pref in m.name.lower() and 'preview' not in m.name.lower() and 'exp' not in m.name.lower()), None)
            if not best:
                best = next((m for m in candidates if pref in m.name.lower()), None)
            if best:
                logger.log(f"Using Gemini model: {best.name}")
                return client, best.name

        if candidates:
            logger.log(f"Using first available Gemini model: {candidates[0].name}")
            return client, candidates[0].name

        logger.log("No usable Gemini models found via listing. Defaulting to gemini-flash-latest.")
        return client, 'gemini-flash-latest'

    except Exception as e:
        logger.log(f"Error listing models: {e}. Defaulting to gemini-flash-latest.")
        return client, 'gemini-flash-latest'

def generate_technician_summary(notes, job_title):
    """Uses Gemini to summarize the daily work for the PDF Report.

    Runs while a tech is closing a job, so EVERY failure path returns None rather
    than raising — the summary is a nice-to-have and must never be the reason a
    completion fails. Model selection is inside the try for that reason.
    """
    try:
        api_key = get_api_key()
        if not api_key:
            return None
        client, model_name = get_available_model(api_key)
        if client is None:
            return None
        prompt = f"Summarize the following technician notes for job '{job_title}' into a concise, professional paragraph (approx 50 words) suitable for a client report:\n\n{notes}"
        response = client.models.generate_content(model=model_name, contents=prompt)
        return response.text
    except Exception as e:
        try:
            get_logger().log(f"AI summary skipped for '{job_title}': {e}")
        except Exception:
            pass
        return None

def transcribe_audio(audio_file):
    """Transcribes audio using Gemini 1.5 Flash."""
    api_key = get_api_key()
    if not api_key: return None
    
    client, model_name = get_available_model(api_key)

    _, types = _genai()
    try:
        audio_bytes = audio_file.read()
        response = client.models.generate_content(
            model=model_name,
            contents=[
                types.Content(
                    parts=[
                        types.Part.from_text(text="Transcribe this audio note exactly as spoken. Do not add any commentary."),
                        types.Part.from_bytes(data=audio_bytes, mime_type="audio/wav")
                    ]
                )
            ]
        )
        return response.text
    except Exception as e:
        return None


def suggest_address_with_gemini(partial_address):
    """Uses Gemini to autocomplete/validate an address."""
    api_key = get_api_key()
    if not api_key: return partial_address
    client, model_name = get_available_model(api_key)
    prompt = f"You are an address autocomplete tool. The user typed: '{partial_address}'. Return the most likely full address. If ambiguous, return the best guess. Return ONLY the address text, no other words."
    try:
        response = client.models.generate_content(model=model_name, contents=prompt)
        return response.text.strip()
    except:
        return partial_address


def generate_morning_briefing():
    """Generates the morning briefing using Gemini."""
    api_key = get_api_key()
    if not api_key:
        return "⚠️ API Key missing. Please set GEMINI_API_KEY in secrets.toml or environment."
    
    if not st.session_state.jobs:
        return "No active jobs to analyze. Please add jobs via the 'New Job' button."

    # Use dynamic model selector
    client, model_name = get_available_model(api_key)

    sec_jobs = list(st.session_state.jobs)
    active_jobs = [j for j in sec_jobs if j['status'] != 'Completed']
    critical_jobs = [j for j in active_jobs if j['priority'] in ['Critical', 'High']]

    stale_lines = []
    for j in active_jobs:
        d = get_job_stale_days(j)
        if d is not None and d >= STALE_JOB_DAYS:
            stale_lines.append(f"- {j['title']} ({d} days without an update)")

    current_date = now_local().strftime("%B %d, %Y")

    prompt = f"""
      You are the Operations Manager for 5G Security. Generate a concise "Morning Briefing" for the dashboard.
      5G Security is a company that specializes in cameras and NVR systems, access control, alarm systems, and infrastructure cabling. We dont do work on 5G Towers.

     Today's Date: {current_date}

     Data:
     - Active Jobs: {len(active_jobs)}
     - Critical: {len(critical_jobs)}
     - Techs: {', '.join([t['name'] for t in st.session_state.techs])}

     Active Job List:
     {chr(10).join([f"- {j['title']} ({j['priority']})" for j in active_jobs])}

     Stale Jobs (no updates in {STALE_JOB_DAYS}+ days):
     {chr(10).join(stale_lines) if stale_lines else "None"}

     Format:
     Start with the header: **Morning Briefing: 5G Security - {current_date}**

     Then:
     1. Security Focus (Motivation)
     2. Critical Focus (Briefly summarize the active jobs list, highlighting critical ones if any. If there are stale jobs, call them out and ask for a status update on them.)
     3. Safety Tip.

     Max 150 words. No markdown headers (#), use Bold instead.
   """
    
    try:
        response = client.models.generate_content(model=model_name, contents=prompt)
        return response.text
    except Exception as e:
        err_msg = str(e)
        if "429" in err_msg or "RESOURCE_EXHAUSTED" in err_msg:
            return "⏳ **System is currently busy (Rate Limit or Quota Reached).** \n\nPlease wait a minute and click 'Refresh Briefing' below to try again. If you just upgraded to 'Paid 1', it may take a few minutes to fully activate across all regions."
        
        # Help text for Paid 1 users or other errors
        help_tip = ""
        if "API_KEY_INVALID" in err_msg:
            help_tip = "\n\n💡 **Tip:** Your API Key appears to be invalid. Check AI Studio settings."
        elif "PERMISSION_DENIED" in err_msg:
            help_tip = "\n\n💡 **Tip:** Permission denied. If you just upgraded to 'Paid 1', it may take a few minutes to activate."
            
        return f"Error generating briefing: {err_msg}{help_tip}"
