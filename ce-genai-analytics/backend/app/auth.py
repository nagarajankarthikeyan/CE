from fastapi import Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from app.auth_service import AuthService
import base64

security = HTTPBasic()

def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):

    user = AuthService.authenticate_user(
        credentials.username,
        credentials.password
    )

    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    return user
