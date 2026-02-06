from fastapi import Request, HTTPException, status
from typing import Optional
import base64

from app.auth_service import AuthService

def _decode_basic_token(token: str) -> Optional[tuple]:
    try:
        decoded = base64.b64decode(token).decode("utf-8")
        username, password = decoded.split(":", 1)
        return username, password
    except Exception:
        return None

def get_current_user(request: Request):
    """
    Supports:
    - Authorization: Basic <base64(user:pass)>
    - ?auth=<base64(user:pass)> query param (useful for EventSource from browser)
    """
    # 1) Try Authorization header
    auth_header = request.headers.get("authorization") or request.headers.get("Authorization")
    creds = None

    if auth_header:
        parts = auth_header.split(" ", 1)
        if len(parts) == 2 and parts[0].lower() == "basic":
            creds = _decode_basic_token(parts[1])

    # 2) Fallback to query param ?auth=
    if creds is None:
        q = request.query_params.get("auth")
        if q:
            creds = _decode_basic_token(q)

    if not creds:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication credentials",
            headers={"WWW-Authenticate": "Basic"},
        )

    username, password = creds
    user = AuthService.authenticate_user(username, password)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )

    return user