"""Authentication module – standalone bcrypt + session cookies."""

import hashlib
import hmac
import json
import os
import time
from functools import wraps

import bcrypt
from fastapi import Request, Response
from fastapi.responses import RedirectResponse

# Session secret – generated once on first import, or read from env
SESSION_SECRET = os.environ.get("SESSION_SECRET", os.urandom(32).hex())

# Session duration: 7 days
SESSION_MAX_AGE = 7 * 24 * 3600


def hash_password(password: str) -> str:
    """Hash a password with bcrypt."""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def check_password(password: str, password_hash: str) -> bool:
    """Verify a password against a bcrypt hash."""
    try:
        return bcrypt.checkpw(password.encode(), password_hash.encode())
    except Exception:
        return False


def _sign(data: str) -> str:
    """Create HMAC signature for session data."""
    return hmac.new(SESSION_SECRET.encode(), data.encode(), hashlib.sha256).hexdigest()


def create_session_cookie(user_id: int, username: str, role: str) -> str:
    """Create a signed session cookie value."""
    payload = json.dumps({
        "user_id": user_id,
        "username": username,
        "role": role,
        "ts": int(time.time()),
    }, separators=(",", ":"))
    sig = _sign(payload)
    return f"{payload}.{sig}"


def parse_session_cookie(cookie_value: str) -> dict | None:
    """Parse and verify a session cookie. Returns user dict or None."""
    if not cookie_value:
        return None
    try:
        last_dot = cookie_value.rfind(".")
        if last_dot < 0:
            return None
        payload = cookie_value[:last_dot]
        sig = cookie_value[last_dot + 1:]
        if not hmac.compare_digest(sig, _sign(payload)):
            return None
        data = json.loads(payload)
        # Check expiration
        if time.time() - data.get("ts", 0) > SESSION_MAX_AGE:
            return None
        return data
    except Exception:
        return None


def get_current_user(request: Request) -> dict | None:
    """Extract current user from request cookies."""
    cookie = request.cookies.get("session")
    return parse_session_cookie(cookie)


def set_session_cookie(response: Response, user_id: int, username: str, role: str) -> None:
    """Set session cookie on response."""
    value = create_session_cookie(user_id, username, role)
    response.set_cookie(
        key="session",
        value=value,
        max_age=SESSION_MAX_AGE,
        httponly=True,
        samesite="lax",
        path="/",
    )


def clear_session_cookie(response: Response) -> None:
    """Remove session cookie."""
    response.delete_cookie(key="session", path="/")


def require_auth(request: Request) -> dict | None:
    """Check if user is authenticated. Returns user or None."""
    return get_current_user(request)


def require_role(user: dict, *roles: str) -> bool:
    """Check if user has one of the specified roles."""
    if not user:
        return False
    return user.get("role") in roles
