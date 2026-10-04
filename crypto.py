"""At-rest encryption for sensitive site credentials.

Location system records (usernames, passwords, IPs) are encrypted before the
state is persisted to Postgres and decrypted when loaded back. The encryption
key is read from the ENCRYPTION_KEY env var / Streamlit secret and is never
stored in the database.

Existing plaintext records are detected on load and left as-is; they will be
encrypted automatically on the next save.
"""
import base64
import copy
import json
import os
from typing import Optional

import streamlit as st

try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False


def get_encryption_key() -> Optional[str]:
    """Return the configured encryption key, or None if not set."""
    key = os.environ.get("ENCRYPTION_KEY")
    if not key:
        try:
            key = st.secrets.get("ENCRYPTION_KEY")
        except Exception:
            key = None
    return key


def _derive_key(password: str, salt: bytes) -> bytes:
    """Derive a Fernet-compatible key from the password and salt."""
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=480000,
    )
    return base64.urlsafe_b64encode(kdf.derive(password.encode("utf-8")))


def _encrypt(password: str, plaintext: str) -> str:
    salt = os.urandom(16)
    key = _derive_key(password, salt)
    token = Fernet(key).encrypt(plaintext.encode("utf-8"))
    return f"{base64.urlsafe_b64encode(salt).decode()}:{base64.urlsafe_b64encode(token).decode()}"


def _decrypt(password: str, token: str) -> str:
    salt_b64, ct_b64 = token.split(":", 1)
    salt = base64.urlsafe_b64decode(salt_b64)
    key = _derive_key(password, salt)
    return Fernet(key).decrypt(base64.urlsafe_b64decode(ct_b64)).decode("utf-8")


def encrypt_state_systems(state: dict) -> dict:
    """Return a deep copy of state with location['systems'] encrypted.

    Plaintext list values are serialized and encrypted; already-encrypted string
    values are left untouched. If no encryption key is configured, the state is
    returned unchanged.
    """
    password = get_encryption_key()
    if not password or not HAS_CRYPTO:
        return state

    state = copy.deepcopy(state)
    for loc in state.get("locations", []):
        systems = loc.get("systems")
        if isinstance(systems, list):
            try:
                loc["systems"] = _encrypt(password, json.dumps(systems))
            except Exception:
                # Encryption failure must not block a save; leave plaintext so
                # the app keeps working. A warning is logged by the caller.
                pass
    return state


def decrypt_state_systems(state: dict) -> dict:
    """Decrypt location['systems'] values in place.

    Encrypted string values are decrypted back to lists. Plaintext lists are
    left as-is (legacy migration path). If no encryption key is configured, the
    state is returned unchanged.
    """
    password = get_encryption_key()
    if not password or not HAS_CRYPTO:
        return state

    for loc in state.get("locations", []):
        systems = loc.get("systems")
        if isinstance(systems, str) and ":" in systems:
            try:
                loc["systems"] = json.loads(_decrypt(password, systems))
            except Exception:
                # Wrong key or corrupt data: leave the raw value so the app can
                # still render the location; systems will be unusable until fixed.
                pass
    return state
