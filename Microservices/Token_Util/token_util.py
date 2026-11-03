import os
from datetime import datetime, timedelta, timezone

from jose import JWTError, jwt

SECRET_KEY = os.getenv("SECRET_KEY", "your_super_secret_access_key")
REFRESH_SECRET_KEY = os.getenv("REFRESH_SECRET_KEY", "your_super_secret_refresh_key")
ALGORITHM = "HS256"

ACCESS_TOKEN_EXPIRE_MINUTES = 15
REFRESH_TOKEN_EXPIRE_DAYS = 7


class TokenValidationError(Exception):
    pass


def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire, "type": "access"})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def create_refresh_token(data: dict):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire, "type": "refresh"})
    return jwt.encode(to_encode, REFRESH_SECRET_KEY, algorithm=ALGORITHM)


def _decode_token(token: str, secret_key: str, expected_type: str):
    try:
        payload = jwt.decode(token, secret_key, algorithms=[ALGORITHM])
    except JWTError as error:
        raise TokenValidationError("Invalid or expired token") from error

    if payload.get("user_id") is None or payload.get("type") != expected_type:
        raise TokenValidationError(f"Invalid {expected_type} token")

    return payload


def decode_access_token(token: str):
    return _decode_token(token, SECRET_KEY, "access")


def decode_refresh_token(token: str):
    return _decode_token(token, REFRESH_SECRET_KEY, "refresh")
