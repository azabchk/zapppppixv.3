from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Union
from datetime import datetime, timezone
from enum import Enum

# Enums
class Direction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

class OrderStatus(str, Enum):
    NEW = "NEW"
    EXECUTED = "EXECUTED"
    PARTIALLY_EXECUTED = "PARTIALLY_EXECUTED"
    CANCELLED = "CANCELLED"

class UserRole(str, Enum):
    USER = "USER"
    ADMIN = "ADMIN"

# Base models
class NewUser(BaseModel):
    name: str = Field(..., min_length=3, description="Имя пользователя")

class User(BaseModel):
    id: str
    name: str
    role: UserRole
    api_key: str

class Instrument(BaseModel):
    name: str
    ticker: str = Field(..., pattern=r"^[A-Z]{2,10}$")

class Level(BaseModel):
    price: int
    qty: int

class L2OrderBook(BaseModel):
    bid_levels: List[Level]
    ask_levels: List[Level]

class Transaction(BaseModel):
    ticker: str
    amount: int
    price: int
    timestamp: datetime = Field(description="Transaction timestamp")

# Order models
class LimitOrderBody(BaseModel):
    direction: Direction
    ticker: str
    qty: int = Field(..., ge=1)
    price: int = Field(..., gt=0)

class MarketOrderBody(BaseModel):
    direction: Direction
    ticker: str
    qty: int = Field(..., ge=1)

class LimitOrder(BaseModel):
    model_config = ConfigDict(
        # Disable strict datetime timezone validation
        validate_assignment=True,
        arbitrary_types_allowed=True,
    )
    
    id: str
    status: OrderStatus
    user_id: str
    timestamp: datetime = Field(description="Order timestamp")
    body: LimitOrderBody
    filled: int = 0

class MarketOrder(BaseModel):
    model_config = ConfigDict(
        # Disable strict datetime timezone validation
        validate_assignment=True,
        arbitrary_types_allowed=True,
    )
    
    id: str
    status: OrderStatus
    user_id: str
    timestamp: datetime = Field(description="Order timestamp")
    body: MarketOrderBody

class CreateOrderResponse(BaseModel):
    success: bool = True
    order_id: str

# Administrative models
class DepositWithdrawBody(BaseModel):
    user_id: str = Field(..., description="ID пользователя")
    ticker: str = Field(..., description="Тикер инструмента")
    amount: int = Field(..., gt=0, description="Сумма")

class Ok(BaseModel):
    success: bool = True

# Error responses
class ValidationError(BaseModel):
    loc: List[Union[str, int]]
    msg: str
    type: str

class HTTPValidationError(BaseModel):
    detail: List[ValidationError]
