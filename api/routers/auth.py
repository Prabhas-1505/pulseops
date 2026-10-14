from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from api import schemas
from api.auth import create_access_token, hash_password, verify_password
from api.deps import get_db
from api.models import User
from api.rate_limit import RateLimiter

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=schemas.UserResponse, status_code=status.HTTP_201_CREATED)
def register(req: schemas.RegisterRequest, db: Session = Depends(get_db)) -> User:
    existing = db.query(User).filter(User.username == req.username).one_or_none()
    if existing is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="username already taken")
    user = User(username=req.username, hashed_password=hash_password(req.password), role="viewer")
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


@router.post(
    "/login",
    response_model=schemas.TokenResponse,
    dependencies=[Depends(RateLimiter(times=5, seconds=60))],
)
def login(req: schemas.LoginRequest, db: Session = Depends(get_db)) -> schemas.TokenResponse:
    user = db.query(User).filter(User.username == req.username).one_or_none()
    if user is None or not verify_password(req.password, user.hashed_password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid username or password")
    return schemas.TokenResponse(access_token=create_access_token(user.username))
