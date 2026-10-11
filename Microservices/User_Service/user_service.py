import os
import secrets
import smtplib
import string
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path as LocalPath

import bcrypt
import redis
from bson.objectid import ObjectId
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Path, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel, EmailStr
from pymongo import MongoClient

from Message_Queue_Util.consumer import start_consumer
from Message_Queue_Util.producer import send_message
import uvicorn

# Connect to MongoDB container
client = MongoClient("mongodb://localhost:27017/")
# Select Database and Collection (automatically created if they don't exist)
db = client["maindb"]
users_collection = db["Users"]

app = FastAPI()
security = HTTPBearer()

# Connect to Redis container on localhost:6379
# decode_responses=True ensures Redis returns string instead of bytes
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

OTP_EXPIRATION_SECONDS = 300
LOCAL_SMTP_HOST = "localhost"
LOCAL_SMTP_PORT = 1025

SECRET_KEY = os.getenv("SECRET_KEY", "your_super_secret_access_key")
REFRESH_SECRET_KEY = os.getenv("REFRESH_SECRET_KEY", "your_super_secret_refresh_key")
ALGORITHM = "HS256"

ACCESS_TOKEN_EXPIRE_MINUTES = 15
REFRESH_TOKEN_EXPIRE_DAYS = 7


class RefreshRequest(BaseModel):
    refresh_token: str


class UserCreate(BaseModel):
    mail_id: EmailStr
    password: str
    user_name: str
    otp: str


class SendOTPRequest(BaseModel):
    email: EmailStr


class VerifyOTPRequest(BaseModel):
    email: EmailStr
    otp: str


class UserLogin(BaseModel):
    mail_id: EmailStr
    password: str


class UserPasswordReset(BaseModel):
    mail_id: EmailStr
    new_password: str
    otp: str


def generate_otp(length: int = 6) -> str:
    return "".join(secrets.choice(string.digits) for _ in range(length))


def send_local_email(email_to: str, name: str, otp: str):
    """Submit an OTP email to the local debugging SMTP server on localhost:1025."""
    message = MIMEMultipart()
    message["From"] = "noreply@yourapp.local"
    message["To"] = email_to
    message["Subject"] = "Your Verification OTP Code"

    body = f"Hello {name},\n\nYour OTP is: {otp}\nIt expires in {OTP_EXPIRATION_SECONDS // 60} minutes."
    message.attach(MIMEText(body, "plain"))

    with smtplib.SMTP(LOCAL_SMTP_HOST, LOCAL_SMTP_PORT) as server:
        server.send_message(message)


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


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials or token expired",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: str = payload.get("sub")
        token_type: str = payload.get("type")

        if user_id is None or token_type != "access":
            raise credentials_exception

        return {"user_id": user_id, "role": payload.get("role")}
    except JWTError:
        raise credentials_exception


@app.get("/")
def read_root():
    return {"message": "Hello World"}


@app.get("/login", include_in_schema=False)
def login_page():
    page_path = LocalPath(__file__).with_name("login_page.html")
    return FileResponse(page_path, media_type="text/html")


@app.get("/register", include_in_schema=False)
def registration_page():
    page_path = LocalPath(__file__).with_name("registration_page.html")
    return FileResponse(page_path, media_type="text/html")


@app.get("/forgot_password", include_in_schema=False)
def forgot_password_page():
    page_path = LocalPath(__file__).with_name("forgot_password_page.html")
    return FileResponse(page_path, media_type="text/html")


@app.post("/user/session/", status_code=status.HTTP_200_OK)
def login_user(user: UserLogin):
    user_data = user.model_dump(mode="json")
    password_entered = user_data["password"]
    login_bytes = password_entered.encode("utf-8")
    stored_user = users_collection.find_one({"mail_id": str(user.mail_id)})

    if stored_user is None or not bcrypt.checkpw(login_bytes, stored_user.get("password")):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid user ID or password")

    user_payload = {"sub": user.mail_id, "role": stored_user.get("role")}
    access_token = create_access_token(data=user_payload)
    refresh_token = create_refresh_token(data=user_payload)

    return {
        "message": "You are logged in successfully",
        "mail_id": str(user.mail_id),
        "user_name": stored_user.get("user_name"),
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "role": stored_user.get("role"),
    }


@app.get("/user/registration/")
def register_user_otp_request(mail_id: EmailStr, user_name: str, background_tasks: BackgroundTasks):
    user = users_collection.find_one({"mail_id": mail_id})
    if user is not None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="User already exists.")

    otp = generate_otp()
    redis_key = f"otp:{mail_id}"

    # Set key with TTL (auto-deletes after 300 seconds)
    redis_client.setex(redis_key, OTP_EXPIRATION_SECONDS, otp)

    try:
        send_local_email(mail_id, user_name, otp)
    except (OSError, smtplib.SMTPException) as error:
        redis_client.delete(redis_key)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="OTP could not be sent. Ensure the local SMTP debug server is running on localhost:1025.",
        ) from error

    return {
        "message": "OTP submitted to the local SMTP debug server. Check its console output; this server does not deliver email to inboxes."
    }


@app.post("/user/registration/", status_code=status.HTTP_201_CREATED)
def register_user(user: UserCreate):
    redis_key = f"otp:{user.mail_id}"
    stored_otp = redis_client.get(redis_key)

    # 1. Key expired or non-existent
    if not stored_otp:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="OTP has expired or was never requested.",
        )

    # 2. Check value match
    if stored_otp != user.otp:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid OTP.",
        )

    # 3. Immediately delete upon verification to prevent replay
    redis_client.delete(redis_key)

    password_bytes = user.password.encode("utf-8")
    hashed_password = bcrypt.hashpw(password_bytes, bcrypt.gensalt())
    user_obj = {
        "mail_id": user.mail_id,
        "password": hashed_password,
        "user_name": user.user_name,
        "role": "user",
    }

    users_collection.insert_one(user_obj)
    return {
        "message": "You are registered successfully",
        "mail_id": str(user.mail_id),
        "user_name": user.user_name,
    }


@app.post("/refresh")
def refresh_token_endpoint(body: RefreshRequest):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired refresh token",
    )

    try:
        payload = jwt.decode(body.refresh_token, REFRESH_SECRET_KEY, algorithms=[ALGORITHM])
        user_id: str = payload.get("sub")
        token_type: str = payload.get("type")
        if user_id is None or token_type != "refresh":
            raise credentials_exception

        new_access_token = create_access_token(data={"sub": user_id, "role": payload.get("role")})
        return {"access_token": new_access_token, "token_type": "bearer"}
    except JWTError:
        raise credentials_exception


@app.get("/protected")
def protected_route(current_user: dict = Depends(get_current_user)):
    return {
        "message": "Access granted",
        "user": current_user,
    }


@app.get("/user/{mail_id}/")
def get_user(mail_id: str = Path(...)):
    user = users_collection.find_one({"mail_id": mail_id})
    return jsonable_encoder(user, custom_encoder={ObjectId: str})


@app.get("/user/reset_password")
def reset_password_otp_request(mail_id: str):
    user = users_collection.find_one({"mail_id": mail_id})
    if user is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="User does not exists.")

    otp = generate_otp()
    redis_key = f"otp:{mail_id}"

    # Set key with TTL (auto-deletes after 300 seconds)
    redis_client.setex(redis_key, OTP_EXPIRATION_SECONDS, otp)

    # local email dispatch
    send_local_email(mail_id, user.get("user_name"), otp)

    return {"message": f"OTP successfully sent to {mail_id}"}


@app.patch("/user/reset_password/", status_code=status.HTTP_200_OK)
def reset_password_user(user: UserPasswordReset):
    redis_key = f"otp:{user.mail_id}"
    stored_otp = redis_client.get(redis_key)

    # 1. Key expired or non-existent
    if not stored_otp:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="OTP has expired or was never requested.",
        )

    # 2. Check value match
    if stored_otp != user.otp:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid OTP.",
        )

    # 3. Immediately delete upon verification to prevent replay
    redis_client.delete(redis_key)

    user_data = user.model_dump(mode="json")
    password_entered = user_data["new_password"]
    password_bytes = password_entered.encode("utf-8")
    hashed_password = bcrypt.hashpw(password_bytes, bcrypt.gensalt())
    result = users_collection.update_one({"mail_id": str(user.mail_id)}, {"$set": {"password": hashed_password}})

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="user not found.")

    return {
        "message": "password reset successfully",
        "mail_id": str(user.mail_id),
    }


@app.delete("/user/session/current/")
def logout_user(mail_id: str):
    return {"message": "logout successfully"}
