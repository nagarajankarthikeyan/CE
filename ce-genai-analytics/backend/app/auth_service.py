from typing import Optional
from google.cloud import bigquery
from passlib.context import CryptContext
from passlib import exc as passlib_exc
from app.config import (
    BIGQUERY_PROJECT,
    BIGQUERY_DATASET,
    BIGQUERY_LOCATION,
    GOOGLE_APPLICATION_CREDENTIALS
)
import hashlib

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

client = bigquery.Client.from_service_account_json(
    GOOGLE_APPLICATION_CREDENTIALS,
    location=BIGQUERY_LOCATION
)

USERS_TABLE = "bounteous-bi.constellation_media_AI_ANALYST.Users"


class AuthService:

    @staticmethod
    def hash_password(password: str) -> str:
        return pwd_context.hash(password)

    @staticmethod
    def verify_password(plain: str, hashed: str) -> bool:
        if not hashed:
            return False

        try:
            return pwd_context.verify(plain, hashed)
        except passlib_exc.UnknownHashError:
            if plain == hashed:
                return True
            h = hashed.strip().lower()
            try:
                int(h, 16)
                is_hex = True
            except Exception:
                is_hex = False

            if is_hex:
                if len(h) == 32:
                    return hashlib.md5(plain.encode()).hexdigest() == h
                if len(h) == 40:
                    return hashlib.sha1(plain.encode()).hexdigest() == h
                if len(h) == 64:
                    return hashlib.sha256(plain.encode()).hexdigest() == h

            return False

    @staticmethod
    def authenticate_user(email: str, password: str) -> Optional[dict]:

        query = f"""
        SELECT userid, email, username, passwordhash
        FROM `{USERS_TABLE}`
        WHERE email = @email AND isactive = TRUE
        LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", email)
            ]
        )

        result = client.query(query, job_config=job_config).result()
        rows = [dict(row) for row in result]

        if not rows:
            return None

        user = rows[0]

        if not AuthService.verify_password(password, user["passwordhash"]):
            return None

        return {
            "user_id": user["userid"],
            "email": user["email"],
            "username": user["username"]
        }
