from typing import Optional
from sqlalchemy import text
from passlib.context import CryptContext
from app.db import engine
from passlib import exc as passlib_exc
import hashlib
import logging

log = logging.getLogger(__name__)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class AuthService:

    @staticmethod
    def hash_password(password: str) -> str:
        return pwd_context.hash(password)

    @staticmethod
    def verify_password(plain: str, hashed: str) -> bool:
        """
        Verify password using passlib; if Passlib can't identify the stored hash,
        fall back to common legacy hex digests (MD5/SHA1/SHA256) and plaintext.
        """
        if not hashed:
            return False

        # Try passlib first
        try:
            return pwd_context.verify(plain, hashed)
        except passlib_exc.UnknownHashError:
            # fallback for legacy hex digests or plaintext
            h = hashed.strip().lower()

            # plaintext match (legacy)
            if plain == hashed:
                log.warning("Legacy plaintext password detected")
                return True

            # hex digest detection -> md5(32), sha1(40), sha256(64)
            try:
                int(h, 16)
                is_hex = True
            except Exception:
                is_hex = False

            if is_hex:
                if len(h) == 32:   # md5
                    return hashlib.md5(plain.encode("utf-8")).hexdigest() == h
                if len(h) == 40:   # sha1
                    return hashlib.sha1(plain.encode("utf-8")).hexdigest() == h
                if len(h) == 64:   # sha256
                    return hashlib.sha256(plain.encode("utf-8")).hexdigest() == h

            return False

    @staticmethod
    def authenticate_user(email: str, password: str) -> Optional[dict]:
        sql = text("""
            SELECT UserID, Email, Username, PasswordHash
            FROM Users
            WHERE Email = :email AND IsActive = 1
        """)

        with engine.connect() as conn:
            row = conn.execute(sql, {"email": email}).fetchone()

            if not row:
                return None

            user = dict(row._mapping)

            if not AuthService.verify_password(password, user["PasswordHash"]):
                return None

            return {
                "user_id": user["UserID"],
                "email": user["Email"],
                "username": user["Username"]
            }
