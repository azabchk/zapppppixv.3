from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from database import User as UserDB, Instrument as InstrumentDB, Balance as BalanceDB, Order as OrderDB, Transaction as TransactionDB
from schemas import *
import uuid
import time
import random
from datetime import datetime
from typing import List, Dict, Optional, Union
import threading
import asyncio

balance_update_lock = asyncio.Lock()

class TradingEngine:
    """zapppppix v.3 azab-adam"""
    
    def __init__(self, db: Session):
        self.db = db
    
    async def create_order(self, user: UserDB, order_data: Union[LimitOrderBody, MarketOrderBody]) -> str:
        """Create an order"""
        # Check if the instrument exists
        instrument = self.db.query(InstrumentDB).filter(InstrumentDB.ticker == order_data.ticker).first()
        if not instrument:
            raise ValueError("Инструмент не найден")
        # Check the balance before creating an order
        if order_data.direction == "BUY":
            # For a purchase, RUB balance is required
            total_cost = order_data.qty * (getattr(order_data, 'price', 0) or 1)
            rub_balance = self.db.query(BalanceDB).filter(BalanceDB.user_id == user.id, BalanceDB.ticker == "RUB").first()
            if not rub_balance or rub_balance.amount < total_cost:
                raise ValueError("Недостаточно средств для покупки")
        elif order_data.direction == "SELL":
            # For a sale, the asset must be available
            asset_balance = self.db.query(BalanceDB).filter(BalanceDB.user_id == user.id, BalanceDB.ticker == order_data.ticker).first()
            if not asset_balance or asset_balance.amount < order_data.qty:
                raise ValueError("Недостаточно актива для продажи")
        order_id = str(uuid.uuid4())
        order_type = "LIMIT" if isinstance(order_data, LimitOrderBody) else "MARKET"
        
        # Create the order
        order = OrderDB(
            id=order_id,
            user_id=user.id,
            ticker=order_data.ticker,
            direction=order_data.direction,
            qty=order_data.qty,
            price=getattr(order_data, 'price', None),
            order_type=order_type,
            status="NEW",
            filled=0
        )
        
        self.db.add(order)
        
        # For market orders, attempt to execute immediately
        if order_type == "MARKET":
            await self._execute_market_order(order)
        else:
            await self._try_execute_limit_order(order)
        
        self.db.commit()
        return order_id
    
    async def _execute_market_order(self, order: OrderDB):
        """Execute a market order"""
        # Find the best opposite orders
        opposite_direction = "SELL" if order.direction == "BUY" else "BUY"
        opposite_orders = self.db.query(OrderDB).filter(
            OrderDB.ticker == order.ticker,
            OrderDB.direction == opposite_direction,
            OrderDB.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
            OrderDB.order_type == "LIMIT"
        ).order_by(
            OrderDB.price.asc() if order.direction == "BUY" else OrderDB.price.desc()
        ).all()
        
        remaining_qty = order.qty
        
        for opposite_order in opposite_orders:
            if remaining_qty <= 0:
                break
            
            # Determine the quantity to execute
            available_qty = opposite_order.qty - opposite_order.filled
            execute_qty = min(remaining_qty, available_qty)
            
            if execute_qty > 0:
                # Create a transaction
                transaction = TransactionDB(
                    ticker=order.ticker,
                    amount=execute_qty,
                    price=opposite_order.price,
                    buyer_id=order.user_id if order.direction == "BUY" else opposite_order.user_id,
                    seller_id=order.user_id if order.direction == "SELL" else opposite_order.user_id
                )
                self.db.add(transaction)
                
                # Update the orders
                order.filled += execute_qty
                opposite_order.filled += execute_qty
                remaining_qty -= execute_qty
                
                # Update the statuses
                if opposite_order.filled >= opposite_order.qty:
                    opposite_order.status = "EXECUTED"
                else:
                    opposite_order.status = "PARTIALLY_EXECUTED"
                
                # Update the balances
                await self._update_balances_after_trade(order, opposite_order, execute_qty, opposite_order.price)
        
        # Update the status of the market order
        if order.filled >= order.qty:
            order.status = "EXECUTED"
        elif order.filled > 0:
            order.status = "PARTIALLY_EXECUTED"
        else:
            # If the market order is not filled, cancel it
            order.status = "CANCELLED"
    
    async def _try_execute_limit_order(self, order: OrderDB):
        """Attempt to execute a limit order"""
        # Similar to a market order, but with a price check
        opposite_direction = "SELL" if order.direction == "BUY" else "BUY"
        if order.direction == "BUY":
            # For a buy: find sell orders with price <= our price
            opposite_orders = self.db.query(OrderDB).filter(
                OrderDB.ticker == order.ticker,
                OrderDB.direction == opposite_direction,
                OrderDB.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
                OrderDB.price <= order.price
            ).order_by(OrderDB.price.asc()).all()        
        else:
            # For a sell: find buy orders with price >= our price
            opposite_orders = self.db.query(OrderDB).filter(
                OrderDB.ticker == order.ticker,
                OrderDB.direction == opposite_direction,
                OrderDB.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
                OrderDB.price >= order.price
            ).order_by(OrderDB.price.desc()).all()
        
        remaining_qty = order.qty
        
        for opposite_order in opposite_orders:
            if remaining_qty <= 0:
                break
            
            available_qty = opposite_order.qty - opposite_order.filled
            execute_qty = min(remaining_qty, available_qty)
            
            if execute_qty > 0:
                # Execution price is the price of the order that was first in the book
                execution_price = opposite_order.price
                
                transaction = TransactionDB(
                    ticker=order.ticker,
                    amount=execute_qty,
                    price=execution_price,
                    buyer_id=order.user_id if order.direction == "BUY" else opposite_order.user_id,
                    seller_id=order.user_id if order.direction == "SELL" else opposite_order.user_id
                )
                self.db.add(transaction)
                order.filled += execute_qty
                opposite_order.filled += execute_qty
                remaining_qty -= execute_qty
                
                if opposite_order.filled >= opposite_order.qty:
                    opposite_order.status = "EXECUTED"
                else:
                    opposite_order.status = "PARTIALLY_EXECUTED"
                
                await self._update_balances_after_trade(order, opposite_order, execute_qty, execution_price)
        
        # Update the status of our order
        if order.filled >= order.qty:
            order.status = "EXECUTED"
        elif order.filled > 0:
            order.status = "PARTIALLY_EXECUTED"
        # Otherwise it remains NEW
    
    async def _update_balances_after_trade(self, order1: OrderDB, order2: OrderDB, qty: int, price: int):
        """Update balances after a trade"""
        buyer_id = order1.user_id if order1.direction == "BUY" else order2.user_id
        seller_id = order1.user_id if order1.direction == "SELL" else order2.user_id
        ticker = order1.ticker
        total_cost = qty * price
        
        # Create a dictionary to track balance changes
        balance_changes = {}
        
        # Function for safely modifying a balance
        def update_balance(user_id: str, ticker: str, amount_change: int):
            key = (user_id, ticker)
            if key not in balance_changes:
                balance_changes[key] = 0
            balance_changes[key] += amount_change
        
        # Record all changes
        update_balance(buyer_id, ticker, qty)       # Buyer receives the asset
        update_balance(buyer_id, "RUB", -total_cost) # Buyer loses RUB
        update_balance(seller_id, ticker, -qty)     # Seller loses the asset
        update_balance(seller_id, "RUB", total_cost) # Seller gains RUB
        
        # Sort the changes by keys to avoid deadlocks
        sorted_changes = sorted(balance_changes.items(), key=lambda x: (str(x[0][0]), x[0][1]))
        
        # Critical section: updating balances
        async with balance_update_lock:
            # Apply the changes to the database with retry logic
            for (user_id, ticker), amount_change in sorted_changes:
                if amount_change == 0:
                    continue
                
                await self._upsert_balance_with_retry(user_id, ticker, amount_change)
    
    async def _upsert_balance_with_retry(self, user_id: str, ticker: str, amount_change: int, max_retries: int = 3):
        """Safely update a balance with upsert and retry support for deadlocks"""
        for attempt in range(max_retries):
            try:
                # Use PostgreSQL ON CONFLICT for an atomic upsert
                upsert_sql = text("""
                    INSERT INTO balances (user_id, ticker, amount, updated_at)
                    VALUES (:user_id, :ticker, :amount, :updated_at)
                    ON CONFLICT (user_id, ticker)
                    DO UPDATE SET 
                        amount = balances.amount + :amount,
                        updated_at = :updated_at
                """)
                
                self.db.execute(upsert_sql, {
                    'user_id': user_id,
                    'ticker': ticker,
                    'amount': amount_change,
                    'updated_at': datetime.utcnow()
                })
                break  # Successfully executed, exit the loop
                
            except OperationalError as e:
                if "deadlock detected" in str(e).lower() and attempt < max_retries - 1:
                    # Deadlock detected, wait a random time and retry
                    wait_time = random.uniform(0.01, 0.1) * (2 ** attempt)  # Exponential backoff
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    # Last attempt or a different error - re-raise the exception
                    raise
    
    def cancel_order(self, order_id: str, user: UserDB) -> bool:
        """Cancel an order"""
        order = self.db.query(OrderDB).filter(
            OrderDB.id == order_id,
            OrderDB.user_id == user.id,
            OrderDB.status.in_(["NEW", "PARTIALLY_EXECUTED"])
        ).first()
        
        if not order:
            return False
        
        order.status = "CANCELLED"
        self.db.commit()
        return True
    
    def get_orderbook(self, ticker: str, limit: int = 10) -> L2OrderBook:
        """Get the order book"""
        # Buy orders (bids) - sort by descending price
        bids = self.db.query(OrderDB).filter(
            OrderDB.ticker == ticker,
            OrderDB.direction == "BUY",
            OrderDB.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
            OrderDB.order_type == "LIMIT"
        ).order_by(OrderDB.price.desc()).all()
        # Sell orders (asks) - sort by ascending price
        asks = self.db.query(OrderDB).filter(
            OrderDB.ticker == ticker,
            OrderDB.direction == "SELL",
            OrderDB.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
            OrderDB.order_type == "LIMIT"
        ).order_by(OrderDB.price.asc()).all()
        # Group by price only the unfilled quantities
        bid_levels = {}
        for bid in bids:
            qty = bid.qty - bid.filled
            if qty <= 0:
                continue
            price = bid.price
            if price in bid_levels:
                bid_levels[price] += qty
            else:
                bid_levels[price] = qty
        ask_levels = {}
        for ask in asks:
            qty = ask.qty - ask.filled
            if qty <= 0:
                continue
            price = ask.price
            if price in ask_levels:
                ask_levels[price] += qty
            else:
                ask_levels[price] = qty
        # Sort the levels of the order book
        bid_levels_sorted = sorted(bid_levels.items(), key=lambda x: -x[0])
        ask_levels_sorted = sorted(ask_levels.items(), key=lambda x: x[0])
        return L2OrderBook(
            bid_levels=[Level(price=price, qty=qty) for price, qty in bid_levels_sorted[:limit]],
            ask_levels=[Level(price=price, qty=qty) for price, qty in ask_levels_sorted[:limit]]
        )
