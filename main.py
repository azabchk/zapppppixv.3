from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from typing import List, Optional, Union, Dict
from datetime import datetime, timezone
import uuid
import asyncio

from database import get_db, User as UserDB, Instrument as InstrumentDB, Balance as BalanceDB, Order as OrderDB, Transaction as TransactionDB, create_tables
from schemas import *
from auth import get_current_user, require_auth, require_admin
from trading_engine import TradingEngine, balance_update_lock

def make_timezone_aware(dt: datetime) -> datetime:
    """Convert naive datetime to timezone-aware datetime (UTC)"""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt

def validate_uuid(uuid_string: str, field_name: str = "UUID") -> uuid.UUID:
    """Validate and parse UUID string, raise HTTPException if invalid"""
    try:
        return uuid.UUID(uuid_string)
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail=f"Invalid {field_name} format")

# Create a FastAPI application with OpenAPI settings
app = FastAPI(
    title="Good Exchange API",
    version="0.1.0",
    description="Биржевая торговая платформа"
)

# Create tables on startup
try:
    create_tables()
except Exception as e:
    print(f"Предупреждение: Не удалось создать таблицы: {e}")
    print("Приложение будет запущено, но могут возникнуть проблемы с базой данных")

# Create a default administrator
ADMIN_TOKEN = "0710931528bc1680f5ab26a40f2a2569d4453871c3bd70c5e3bb632ab2b197c8"

def init_default_instruments():
    """Initialize basic instruments (currencies)"""
    db = next(get_db())
    try:
        # Check if RUB already exists
        rub_exists = db.query(InstrumentDB).filter(InstrumentDB.ticker == "RUB").first()
        if not rub_exists:
            rub = InstrumentDB(
                ticker="RUB",
                name="Российский рубль",
                type="CURRENCY"
            )
            db.add(rub)
            print("Создан базовый инструмент: RUB")
        
        # Also add USD for currency pair trading
        usd_exists = db.query(InstrumentDB).filter(InstrumentDB.ticker == "USD").first()
        if not usd_exists:
            usd = InstrumentDB(
                ticker="USD",
                name="Доллар США",
                type="CURRENCY"
            )
            db.add(usd)
            print("Создан базовый инструмент: USD")
        
        db.commit()
        print("Инициализация базовых инструментов завершена")
    except Exception as e:
        print(f"Ошибка при инициализации базовых инструментов: {e}")
        db.rollback()
    finally:
        db.close()

@app.on_event("startup")
async def startup_event():
    """Create administrator and base instruments on application startup"""
    db = next(get_db())
    admin_user = db.query(UserDB).filter(UserDB.api_key == ADMIN_TOKEN).first()
    if not admin_user:
        admin = UserDB(
            id=uuid.uuid4(),
            name="Admin",
            role="ADMIN",
            api_key=ADMIN_TOKEN
        )
        db.add(admin)
        db.commit()
    
    init_default_instruments()

# ============= HEALTH CHECK =============

@app.get("/health", tags=["health"])
def health_check():
    """Health check endpoint for monitoring"""
    return {"status": "healthy"}

# ============= PUBLIC API =============

@app.post("/api/v1/public/register", 
          response_model=User, 
          tags=["public"],
          summary="Register",
          description="Регистрация пользователя в платформе. Обязательна для совершения сделок\napi_key полученный из этого метода следует передавать в другие через заголовок Authorization\n\nНапример для api_key='key-bee6de4d-7a23-4bb1-a048-523c2ef0ea0c` знаначение будет таким:\n\nAuthorization: TOKEN key-bee6de4d-7a23-4bb1-a048-523c2ef0ea0c")
def register_user(new_user: NewUser, db: Session = Depends(get_db)):
    """Register a new user"""
    user_id = uuid.uuid4()
    api_key = f"key-{uuid.uuid4()}"
    
    user = UserDB(
        id=user_id,
        name=new_user.name,
        role="USER",
        api_key=api_key
    )
    
    db.add(user)
    db.commit()
    db.refresh(user)
    
    return User(
        id=str(user.id),
        name=user.name,
        role=user.role,
        api_key=user.api_key
    )

@app.get("/api/v1/public/instrument", 
         response_model=List[Instrument], 
         tags=["public"],
         summary="List Instruments",
         description="Список доступных инструментов")
def list_instruments(db: Session = Depends(get_db)):
    """Get the list of available instruments"""
    instruments = db.query(InstrumentDB).all()
    return [Instrument(name=i.name, ticker=i.ticker) for i in instruments]

@app.get("/api/v1/public/orderbook/{ticker}", 
         response_model=L2OrderBook, 
         tags=["public"],
         summary="Get Orderbook",
         description="Текущие заявки")
def get_orderbook(ticker: str, limit: int = 10, db: Session = Depends(get_db)):
    """Retrieve the order book for a given instrument"""
    if limit > 25:
        limit = 25
    
    engine = TradingEngine(db)
    return engine.get_orderbook(ticker, limit)

@app.get("/api/v1/public/transactions/{ticker}", 
         response_model=List[Transaction], 
         tags=["public"],
         summary="Get Transaction History",
         description="История сделок")
def get_transaction_history(ticker: str, limit: int = 10, db: Session = Depends(get_db)):
    """Get the transaction history for an instrument"""
    if limit > 100:
        limit = 100
    
    transactions = db.query(TransactionDB).filter(
        TransactionDB.ticker == ticker
    ).order_by(TransactionDB.timestamp.desc()).limit(limit).all()
    return [Transaction(
        ticker=t.ticker,
        amount=t.amount,
        price=t.price,
        timestamp=make_timezone_aware(t.timestamp)
    ) for t in transactions]

# ============= BALANCES API =============

@app.get("/api/v1/balance", 
         response_model=Dict[str, int], 
         tags=["balance"],
         summary="Get Balances")
def get_balances(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    """Get balances for the current user"""
    user = require_auth(authorization, db)
    
    balances = db.query(BalanceDB).filter(BalanceDB.user_id == user.id).all()
    return {balance.ticker: balance.amount for balance in balances}

# ============= ORDERS API =============

@app.post("/api/v1/order", 
          response_model=CreateOrderResponse, 
          tags=["order"],
          summary="Create Order")
async def create_order(
    body: Union[LimitOrderBody, MarketOrderBody],
    authorization: Optional[str] = Header(None), 
    db: Session = Depends(get_db)
):
    """Create a new order"""
    user = require_auth(authorization, db)
    engine = TradingEngine(db)
    try:
        order_id = await engine.create_order(user, body)
        return CreateOrderResponse(success=True, order_id=order_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/v1/order", 
         response_model=List[Union[LimitOrder, MarketOrder]], 
         tags=["order"],
         summary="List Orders")
def list_orders(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    """Get the list of orders for the current user"""
    user = require_auth(authorization, db)
    
    orders = db.query(OrderDB).filter(OrderDB.user_id == user.id).all()
    result = []
    for order in orders:
        if order.order_type == "LIMIT":
            body = LimitOrderBody(
                direction=order.direction,
                ticker=order.ticker,
                qty=order.qty,
                price=order.price
            )
            result.append(LimitOrder(
                id=str(order.id),
                status=order.status,
                user_id=str(order.user_id),
                timestamp=make_timezone_aware(order.timestamp),
                body=body,
                filled=order.filled
            ))
        else:
            body = MarketOrderBody(
                direction=order.direction,
                ticker=order.ticker,
                qty=order.qty
            )
            result.append(MarketOrder(
                id=str(order.id),
                status=order.status,
                user_id=str(order.user_id),
                timestamp=make_timezone_aware(order.timestamp),
                body=body
            ))
    
    return result

@app.get("/api/v1/order/{order_id}", 
         response_model=Union[LimitOrder, MarketOrder], 
         tags=["order"],
         summary="Get Order")
def get_order(order_id: str, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    """Get information about a specific order"""
    user = require_auth(authorization, db)
    
    # Validate UUID format
    validate_uuid(order_id, "order_id")
    
    order = db.query(OrderDB).filter(
        OrderDB.id == order_id,
        OrderDB.user_id == user.id
    ).first()
    
    if not order:
        raise HTTPException(status_code=404, detail="Заявка не найдена")
    
    if order.order_type == "LIMIT":
        body = LimitOrderBody(
            direction=order.direction,
            ticker=order.ticker,
            qty=order.qty,
            price=order.price
        )           
        return LimitOrder(
            id=str(order.id),
            status=order.status,
            user_id=str(order.user_id),
            timestamp=make_timezone_aware(order.timestamp),
            body=body,
            filled=order.filled
        )
    else:
        body = MarketOrderBody(
            direction=order.direction,
            ticker=order.ticker,
            qty=order.qty
        )        
        return MarketOrder(
            id=str(order.id),
            status=order.status,
            user_id=str(order.user_id),
            timestamp=make_timezone_aware(order.timestamp),
            body=body
        )

@app.delete("/api/v1/order/{order_id}", 
            response_model=Ok, 
            tags=["order"],
            summary="Cancel Order")
def cancel_order(order_id: str, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    """Cancel an order"""
    user = require_auth(authorization, db)
    
    # Validate UUID format
    validate_uuid(order_id, "order_id")
    
    engine = TradingEngine(db)
    
    if engine.cancel_order(order_id, user):
        return Ok(success=True)
    else:
        raise HTTPException(status_code=404, detail="Заявка не найдена или не может быть отменена")

# ============= ADMINISTRATIVE API =============

@app.delete("/api/v1/admin/user/{user_id}", 
            response_model=User, 
            tags=["admin", "user"],
            summary="Delete User")
async def delete_user(user_id: str, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    """Delete a user"""
    admin = require_admin(authorization, db)
    validate_uuid(user_id, "user_id")
    user = db.query(UserDB).filter(UserDB.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    user_data = User(
        id=str(user.id),
        name=user.name,
        role=user.role,
        api_key=user.api_key
    )
    async with balance_update_lock:
        db.query(TransactionDB).filter((TransactionDB.buyer_id == user.id) | (TransactionDB.seller_id == user.id)).delete(synchronize_session=False)
        db.query(BalanceDB).filter(BalanceDB.user_id == user.id).delete()
        db.query(OrderDB).filter(OrderDB.user_id == user.id).delete()
        db.delete(user)
        db.commit()
    return user_data

@app.post("/api/v1/admin/instrument", 
          response_model=Ok, 
          tags=["admin"],
          summary="Add Instrument")
def add_instrument(instrument: Instrument, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    """Add a new trading instrument"""
    admin = require_admin(authorization, db)
    
    # Check that the instrument does not already exist
    existing = db.query(InstrumentDB).filter(InstrumentDB.ticker == instrument.ticker).first()
    if existing:
        raise HTTPException(status_code=400, detail="Инструмент уже существует")
    
    new_instrument = InstrumentDB(
        ticker=instrument.ticker,
        name=instrument.name
    )
    
    db.add(new_instrument)
    db.commit()
    
    return Ok(success=True)

@app.delete("/api/v1/admin/instrument/{ticker}", 
            response_model=Ok, 
            tags=["admin"],
            summary="Delete Instrument",
            description="Удаление инструмента")
async def delete_instrument(ticker: str, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    """Delete a trading instrument"""
    admin = require_admin(authorization, db)
    instrument = db.query(InstrumentDB).filter(InstrumentDB.ticker == ticker).first()
    if not instrument:
        raise HTTPException(status_code=404, detail="Инструмент не найден")
    async with balance_update_lock:
        db.query(BalanceDB).filter(BalanceDB.ticker == ticker).delete()
        db.query(OrderDB).filter(OrderDB.ticker == ticker).delete()
        db.query(TransactionDB).filter(TransactionDB.ticker == ticker).delete()
        db.delete(instrument)
        db.commit()
    return Ok(success=True)

@app.post("/api/v1/admin/balance/deposit", 
          response_model=Ok, 
          tags=["admin", "balance"],
          summary="Deposit",
          description="Пополнение баланса")
async def deposit_balance(operation: DepositWithdrawBody, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    """Deposit funds into a user's balance"""
    admin = require_admin(authorization, db)
    validate_uuid(operation.user_id, "user_id")
    user = db.query(UserDB).filter(UserDB.id == operation.user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    instrument = db.query(InstrumentDB).filter(InstrumentDB.ticker == operation.ticker).first()
    if not instrument:
        raise HTTPException(status_code=404, detail="Инструмент не найден")
    async with balance_update_lock:
        balance = db.query(BalanceDB).filter(
            BalanceDB.user_id == operation.user_id,
            BalanceDB.ticker == operation.ticker
        ).first()
        if not balance:
            balance = BalanceDB(
                user_id=operation.user_id,
                ticker=operation.ticker,
                amount=operation.amount
            )
            db.add(balance)
        else:
            balance.amount += operation.amount
        db.commit()
    return Ok(success=True)

@app.post("/api/v1/admin/balance/withdraw", 
          response_model=Ok, 
          tags=["admin", "balance"],
          summary="Withdraw",
          description="Вывод доступных средств с баланса")
async def withdraw_balance(operation: DepositWithdrawBody, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    """Withdraw funds from a user's balance"""
    admin = require_admin(authorization, db)
    validate_uuid(operation.user_id, "user_id")
    user = db.query(UserDB).filter(UserDB.id == operation.user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    async with balance_update_lock:
        balance = db.query(BalanceDB).filter(
            BalanceDB.user_id == operation.user_id,
            BalanceDB.ticker == operation.ticker
        ).first()
        if not balance or balance.amount < operation.amount:
            raise HTTPException(status_code=400, detail="Недостаточно средств")
        balance.amount -= operation.amount
        db.commit()
    return Ok(success=True)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
