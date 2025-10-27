from fastapi import HTTPException, Depends, Header
from sqlalchemy.orm import Session
from database import get_db, User
from typing import Optional

def get_current_user(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)) -> Optional[User]:
    """Get the current user by authorization token"""
    if not authorization:
        return None
    
    # Format: "TOKEN api_key"
    try:
        token_type, api_key = authorization.split(" ", 1)
        if token_type != "TOKEN":
            raise HTTPException(status_code=401, detail="Неверный формат токена")
    except ValueError:
        raise HTTPException(status_code=401, detail="Неверный формат токена")
    
    user = db.query(User).filter(User.api_key == api_key).first()
    if not user:
        raise HTTPException(status_code=401, detail="Неверный токен")
    
    return user

def require_auth(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)) -> User:
    """Require user authorization"""
    user = get_current_user(authorization, db)
    if not user:
        raise HTTPException(status_code=401, detail="Требуется авторизация")
    return user

def require_admin(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)) -> User:
    """Require administrator authorization"""
    user = require_auth(authorization, db)
    if user.role != "ADMIN":
        raise HTTPException(status_code=403, detail="Требуются права администратора")
    return user
