from fastapi import APIRouter, Depends, HTTPException, status
from google.cloud import bigquery
from app.admin_guard import require_admin
from app.auth_service import AuthService
from app.config import GOOGLE_APPLICATION_CREDENTIALS, BIGQUERY_LOCATION
from datetime import datetime, timezone
from functools import lru_cache

router = APIRouter(prefix="/admin", tags=["Admin"])

client = bigquery.Client.from_service_account_json(
    GOOGLE_APPLICATION_CREDENTIALS,
    location=BIGQUERY_LOCATION
)

USERS_TABLE = "bounteous-bi.constellation_media_AI_ANALYST.Users"


def _normalize_col(name: str) -> str:
    return "".join(ch.lower() for ch in name if ch.isalnum())


@lru_cache(maxsize=1)
def _user_field_map() -> dict:
    table = client.get_table(USERS_TABLE)
    return {field.name: field.field_type for field in table.schema}


def _find_timestamp_column(target: str) -> tuple[str, str] | tuple[None, None]:
    target_normalized = _normalize_col(target)
    for name, field_type in _user_field_map().items():
        if _normalize_col(name) == target_normalized:
            return name, field_type
    return None, None


def _current_time_for_bq_type(field_type: str):
    now_utc = datetime.now(timezone.utc)
    if field_type == "TIMESTAMP":
        return now_utc.isoformat()
    if field_type == "DATETIME":
        return now_utc.strftime("%Y-%m-%d %H:%M:%S")
    if field_type == "DATE":
        return now_utc.date().isoformat()
    return now_utc.isoformat()


@router.get("/users")
def list_users(user=Depends(require_admin)):
    query = f"""
    SELECT userid, email, username, role, isactive
    FROM `{USERS_TABLE}`
    ORDER BY userid ASC
    """
    rows = client.query(query).result()

    result = []
    for r in rows:
        result.append({
            "UserID": r["userid"],
            "Email": r["email"],
            "Username": r["username"],
            "Role": r.get("role", "user"),
            "IsActive": r["isactive"]
        })

    return result


@router.post("/users")
def create_user(data: dict, user=Depends(require_admin)):
    email = data["email"].strip()
    username = data["username"].strip()
    password = data["password"].strip()
    print("CREATE USER password:", password)
    print("CREATE USER password length:", len(password.encode("utf-8")))
    role = data.get("role", "user")

    hashed = AuthService.hash_password(password)

    # get next UserID
    q = f"SELECT IFNULL(MAX(UserID), 0) + 1 AS next_id FROM `{USERS_TABLE}`"
    next_id = list(client.query(q).result())[0]["next_id"]

    row = {
        "UserID": next_id,
        "email": email,
        "username": username,
        "passwordhash": hashed,
        "role": role,
        "isactive": True
    }

    created_col, created_type = _find_timestamp_column("createdat")
    modified_col, modified_type = _find_timestamp_column("modifiedat")

    if created_col:
        row[created_col] = _current_time_for_bq_type(created_type)
    if modified_col:
        row[modified_col] = _current_time_for_bq_type(modified_type)

    job = client.load_table_from_json([row], USERS_TABLE)
    job.result()
    
    if job.errors:
        raise HTTPException(400, str(job.errors))

    return {"status": "created"}


@router.put("/users/{user_id}")
def update_user(user_id: int, data: dict, user=Depends(require_admin)):
    sets = []
    params = {"id": user_id}

    for k in ("email", "username", "role"):
        if k in data:
            sets.append(f"{k}=@{k}")
            params[k] = data[k]

    if "is_active" in data:
        sets.append("isactive=@isactive")
        params["isactive"] = bool(data["is_active"])

    if "password" in data:
        sets.append("passwordhash=@passwordhash")
        params["passwordhash"] = AuthService.hash_password(data["password"])

    modified_col, modified_type = _find_timestamp_column("modifiedat")
    if modified_col:
        sets.append(f"`{modified_col}`=@_modifiedat")
        params["_modifiedat"] = _current_time_for_bq_type(modified_type)

    if not sets:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields provided to update"
        )

    query = f"""
    UPDATE `{USERS_TABLE}`
    SET {", ".join(sets)}
    WHERE userid=@id
    """

    bq_params = []
    for k, v in params.items():
        if k == "id":
            bq_type = "INT64"
        elif isinstance(v, bool):
            bq_type = "BOOL"
        elif k == "_modifiedat":
            bq_type = modified_type
        else:
            bq_type = "STRING"

        bq_params.append(bigquery.ScalarQueryParameter(k, bq_type, v))

    job = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=bq_params
    ))
    job.result()
    return {"status": "updated"}


@router.delete("/users/{user_id}")
def delete_user(user_id: int, user=Depends(require_admin)):
    query = f"DELETE FROM `{USERS_TABLE}` WHERE userid=@id"
    job = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("id", "INT64", user_id)]
    ))
    job.result()
    return {"status": "deleted"}
