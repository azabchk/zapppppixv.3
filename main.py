from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from typing import List, Optional, Union, Dict
from datetime import datetime
import uuid

from database import get_db, User as UserDB, Instrument as InstrumentDB, Balance as BalanceDB, Order as OrderDB, Transaction as TransactionDB, create_tables
from schemas import *
from auth import get_current_user, require_auth, require_admin
from trading_engine import TradingEngine

# Создаем приложение FastAPI с настройками из OpenAPI
app = FastAPI(
    title="Toy exchange",
    version="0.1.0",
    description="Биржевая торговая платформа"
)

# Создаем таблицы при запуске
try:
    create_tables()
except Exception as e:
    print(f"Предупреждение: Не удалось создать таблицы: {e}")
    print("Приложение будет запущено, но могут возникнуть проблемы с базой данных")

# Создаем администратора по умолчанию
ADMIN_TOKEN = "admin-token-12345"

def init_default_instruments():
    """Инициализация базовых инструментов (валют)"""
    db = next(get_db())
    try:
        # Проверяем, есть ли уже RUB
        rub_exists = db.query(InstrumentDB).filter(InstrumentDB.ticker == "RUB").first()
        if not rub_exists:
            rub = InstrumentDB(
                ticker="RUB",
                name="Российский рубль",
                type="CURRENCY"
            )
            db.add(rub)
            print("Создан базовый инструмент: RUB")
        
        # Добавляем также USD для торговли валютными парами
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
    """Создать администратора и базовые инструменты при запуске приложения"""
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
    """Health check endpoint для мониторинга"""
    return {"status": "healthy"}

# ============= ПУБЛИЧНОЕ API =============

@app.post("/api/v1/public/register", 
          response_model=User, 
          tags=["public"],
          summary="Register",
          description="Регистрация пользователя в платформе. Обязательна для совершения сделок\napi_key полученный из этого метода следует передавать в другие через заголовок Authorization\n\nНапример для api_key='key-bee6de4d-7a23-4bb1-a048-523c2ef0ea0c` знаначение будет таким:\n\nAuthorization: TOKEN key-bee6de4d-7a23-4bb1-a048-523c2ef0ea0c")
def register_user(new_user: NewUser, db: Session = Depends(get_db)):
    """Регистрация нового пользователя"""
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
    """Получить список доступных инструментов"""
    instruments = db.query(InstrumentDB).all()
    return [Instrument(name=i.name, ticker=i.ticker) for i in instruments]

@app.get("/api/v1/public/orderbook/{ticker}", 
         response_model=L2OrderBook, 
         tags=["public"],
         summary="Get Orderbook",
         description="Текущие заявки")
def get_orderbook(ticker: str, limit: int = 10, db: Session = Depends(get_db)):
    """Получить стакан заявок для инструмента"""
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
    """Получить историю сделок по инструменту"""
    if limit > 100:
        limit = 100
    
    transactions = db.query(TransactionDB).filter(
        TransactionDB.ticker == ticker
    ).order_by(TransactionDB.timestamp.desc()).limit(limit).all()
    
    return [Transaction(
        ticker=t.ticker,
        amount=t.amount,
        price=t.price,
        timestamp=t.timestamp
    ) for t in transactions]

# ============= API БАЛАНСОВ =============

@app.get("/api/v1/balance", 
         response_model=Dict[str, int], 
         tags=["balance"],
         summary="Get Balances")
def get_balances(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    """Получить балансы пользователя"""
    user = require_auth(authorization, db)
    
    balances = db.query(BalanceDB).filter(BalanceDB.user_id == user.id).all()
    return {balance.ticker: balance.amount for balance in balances}

# ============= API ЗАЯВОК =============

@app.post("/api/v1/order", 
          response_model=CreateOrderResponse, 
          tags=["order"],
          summary="Create Order")
def create_order(
    body: Union[LimitOrderBody, MarketOrderBody],
    authorization: Optional[str] = Header(None), 
    db: Session = Depends(get_db)
):
    """Создать заявку"""
    user = require_auth(authorization, db)
    engine = TradingEngine(db)
    
    try:
        order_id = engine.create_order(user, body)
        return CreateOrderResponse(success=True, order_id=order_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/v1/order", 
         response_model=List[Union[LimitOrder, MarketOrder]], 
         tags=["order"],
         summary="List Orders")
def list_orders(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    """Получить список заявок пользователя"""
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
                timestamp=order.timestamp,
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
                timestamp=order.timestamp,
                body=body
            ))
    
    return result

@app.get("/api/v1/order/{order_id}", 
         response_model=Union[LimitOrder, MarketOrder], 
         tags=["order"],
         summary="Get Order")
def get_order(order_id: str, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    """Получить информацию о заявке"""
    user = require_auth(authorization, db)
    
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
            timestamp=order.timestamp,
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
            timestamp=order.timestamp,
            body=body
        )

@app.delete("/api/v1/order/{order_id}", 
            response_model=Ok, 
            tags=["order"],
            summary="Cancel Order")
def cancel_order(order_id: str, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    """Отменить заявку"""
    user = require_auth(authorization, db)
    engine = TradingEngine(db)
    
    if engine.cancel_order(order_id, user):
        return Ok(success=True)
    else:
        raise HTTPException(status_code=404, detail="Заявка не найдена или не может быть отменена")

# ============= АДМИНИСТРАТИВНОЕ API =============

@app.delete("/api/v1/admin/user/{user_id}", 
            response_model=User, 
            tags=["admin", "user"],
            summary="Delete User")
def delete_user(user_id: str, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    """Удалить пользователя"""
    admin = require_admin(authorization, db)
    
    user = db.query(UserDB).filter(UserDB.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    # Сохраняем данные для ответа
    user_data = User(
        id=str(user.id),
        name=user.name,
        role=user.role,
        api_key=user.api_key
    )
    
    # Удаляем пользователя и связанные данные
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
    """Добавить новый торговый инструмент"""
    admin = require_admin(authorization, db)
    
    # Проверяем, что инструмент не существует
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
def delete_instrument(ticker: str, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    """Удалить торговый инструмент"""
    admin = require_admin(authorization, db)
    
    instrument = db.query(InstrumentDB).filter(InstrumentDB.ticker == ticker).first()
    if not instrument:
        raise HTTPException(status_code=404, detail="Инструмент не найден")
    
    # Удаляем связанные данные
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
def deposit_balance(operation: DepositWithdrawBody, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    """Пополнить баланс пользователя"""
    admin = require_admin(authorization, db)
    
    # Проверяем существование пользователя
    user = db.query(UserDB).filter(UserDB.id == operation.user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    # Проверяем существование инструмента
    instrument = db.query(InstrumentDB).filter(InstrumentDB.ticker == operation.ticker).first()
    if not instrument:
        raise HTTPException(status_code=404, detail="Инструмент не найден")
    
    # Пополняем баланс
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
def withdraw_balance(operation: DepositWithdrawBody, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    """Списать средства с баланса пользователя"""
    admin = require_admin(authorization, db)
    
    # Проверяем существование пользователя
    user = db.query(UserDB).filter(UserDB.id == operation.user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    
    # Проверяем баланс
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
