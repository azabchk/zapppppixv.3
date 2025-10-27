from sqlalchemy import create_engine, Column, String, Integer, DateTime, Float, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid
import os
from datetime import datetime

# Get the database URL from an environment variable or use the default PostgreSQL for Docker
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@db:5432/zappppppix_exchange")

engine = create_engine(DATABASE_URL, pool_pre_ping=True, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Database models
class User(Base):
    __tablename__ = "users"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    role = Column(String, default="USER")  # USER or ADMIN
    api_key = Column(String, unique=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    balances = relationship("Balance", back_populates="user")
    orders = relationship("Order", back_populates="user")

class Instrument(Base):
    __tablename__ = "instruments"
    
    ticker = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    type = Column(String, default="STOCK")  # STOCK, CURRENCY, BOND, etc.
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    orders = relationship("Order", back_populates="instrument")
    transactions = relationship("Transaction", back_populates="instrument")

class Balance(Base):
    __tablename__ = "balances"
    
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), primary_key=True)
    ticker = Column(String, ForeignKey("instruments.ticker"), primary_key=True)
    amount = Column(Integer, default=0)
    updated_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="balances")
    instrument = relationship("Instrument")

class Order(Base):
    __tablename__ = "orders"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    ticker = Column(String, ForeignKey("instruments.ticker"), nullable=False)
    direction = Column(String, nullable=False)  # BUY or SELL
    qty = Column(Integer, nullable=False)
    price = Column(Integer, nullable=True)  # Price for limit orders
    status = Column(String, default="NEW")  # NEW, EXECUTED, PARTIALLY_EXECUTED, CANCELLED
    filled = Column(Integer, default=0)  # Executed quantity
    order_type = Column(String, nullable=False)  # LIMIT or MARKET
    timestamp = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="orders")
    instrument = relationship("Instrument", back_populates="orders")

class Transaction(Base):
    __tablename__ = "transactions"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String, ForeignKey("instruments.ticker"), nullable=False)
    amount = Column(Integer, nullable=False)
    price = Column(Integer, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)
    buyer_id = Column(UUID(as_uuid=True), ForeignKey("users.id"))
    seller_id = Column(UUID(as_uuid=True), ForeignKey("users.id"))
    
    # Relationships
    instrument = relationship("Instrument", back_populates="transactions")
    buyer = relationship("User", foreign_keys=[buyer_id])
    seller = relationship("User", foreign_keys=[seller_id])

# Dependency for obtaining a DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Create database tables
def create_tables():
    """Create tables in the database"""
    try:
        Base.metadata.create_all(bind=engine)
        print("Таблицы успешно созданы")
    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")
        raise
