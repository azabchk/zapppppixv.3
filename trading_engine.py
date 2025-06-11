from sqlalchemy.orm import Session
from database import User as UserDB, Instrument as InstrumentDB, Balance as BalanceDB, Order as OrderDB, Transaction as TransactionDB
from schemas import *
import uuid
from datetime import datetime
from typing import List, Dict, Optional, Union

class TradingEngine:
    """Движок биржевой торговли"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def create_order(self, user: UserDB, order_data: Union[LimitOrderBody, MarketOrderBody]) -> str:
        """Создать заявку"""
        # Проверяем существование инструмента
        instrument = self.db.query(InstrumentDB).filter(InstrumentDB.ticker == order_data.ticker).first()
        if not instrument:
            raise ValueError("Инструмент не найден")
        order_id = str(uuid.uuid4())
        order_type = "LIMIT" if isinstance(order_data, LimitOrderBody) else "MARKET"
        
        # Создаем заявку
        order = OrderDB(
            id=order_id,
            user_id=user.id,
            ticker=order_data.ticker,
            direction=order_data.direction,
            qty=order_data.qty,
            price=getattr(order_data, 'price', None),
            order_type=order_type,
            status="NEW",
            filled=0  # Инициализируем поле filled
        )
        
        self.db.add(order)
        
        # Для рыночных заявок пытаемся исполнить немедленно
        if order_type == "MARKET":
            self._execute_market_order(order)
        else:
            self._try_execute_limit_order(order)
        
        self.db.commit()
        return order_id
    
    def _execute_market_order(self, order: OrderDB):
        """Исполнить рыночную заявку"""
        # Находим лучшие противоположные заявки
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
            
            # Определяем количество для исполнения
            available_qty = opposite_order.qty - opposite_order.filled
            execute_qty = min(remaining_qty, available_qty)
            
            if execute_qty > 0:
                # Создаем транзакцию
                transaction = TransactionDB(
                    ticker=order.ticker,
                    amount=execute_qty,
                    price=opposite_order.price,
                    buyer_id=order.user_id if order.direction == "BUY" else opposite_order.user_id,
                    seller_id=order.user_id if order.direction == "SELL" else opposite_order.user_id
                )
                self.db.add(transaction)
                
                # Обновляем заявки
                order.filled += execute_qty
                opposite_order.filled += execute_qty
                remaining_qty -= execute_qty
                
                # Обновляем статусы
                if opposite_order.filled >= opposite_order.qty:
                    opposite_order.status = "EXECUTED"
                else:
                    opposite_order.status = "PARTIALLY_EXECUTED"
                
                # Обновляем балансы
                self._update_balances_after_trade(order, opposite_order, execute_qty, opposite_order.price)
        
        # Обновляем статус рыночной заявки
        if order.filled >= order.qty:
            order.status = "EXECUTED"
        elif order.filled > 0:
            order.status = "PARTIALLY_EXECUTED"
        else:
            # Рыночная заявка не исполнена - отменяем
            order.status = "CANCELLED"
    
    def _try_execute_limit_order(self, order: OrderDB):
        """Попытаться исполнить лимитную заявку"""
        # Аналогично рыночной заявке, но с проверкой цены
        opposite_direction = "SELL" if order.direction == "BUY" else "BUY"
        if order.direction == "BUY":
            # Покупка: ищем заявки на продажу с ценой <= нашей цены
            opposite_orders = self.db.query(OrderDB).filter(
                OrderDB.ticker == order.ticker,
                OrderDB.direction == opposite_direction,
                OrderDB.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
                OrderDB.price <= order.price
            ).order_by(OrderDB.price.asc()).all()
        else:
            # Продажа: ищем заявки на покупку с ценой >= нашей цены            opposite_orders = self.db.query(OrderDB).filter(
                OrderDB.ticker == order.ticker,
                OrderDB.direction == opposite_direction,
                OrderDB.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
                OrderDB.price >= order.price
                ().order_by(OrderDB.price.desc()).all()
        
        remaining_qty = order.qty
        
        for opposite_order in opposite_orders:
            if remaining_qty <= 0:
                break
            
            available_qty = opposite_order.qty - opposite_order.filled
            execute_qty = min(remaining_qty, available_qty)
            
            if execute_qty > 0:
                # Цена исполнения - цена заявки, которая была в стакане первой
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
                
                self._update_balances_after_trade(order, opposite_order, execute_qty, execution_price)
        
        # Обновляем статус нашей заявки
        if order.filled >= order.qty:
            order.status = "EXECUTED"
        elif order.filled > 0:
            order.status = "PARTIALLY_EXECUTED"
        # Иначе остается NEW
    
    def _update_balances_after_trade(self, order1: OrderDB, order2: OrderDB, qty: int, price: int):
        """Обновить балансы после сделки"""
        buyer_id = order1.user_id if order1.direction == "BUY" else order2.user_id
        seller_id = order1.user_id if order1.direction == "SELL" else order2.user_id
        ticker = order1.ticker
        total_cost = qty * price
        
        # Обновляем баланс покупателя (получает актив, теряет RUB)
        buyer_asset_balance = self.db.query(BalanceDB).filter(
            BalanceDB.user_id == buyer_id,
            BalanceDB.ticker == ticker
        ).first()
        
        if not buyer_asset_balance:
            buyer_asset_balance = BalanceDB(user_id=buyer_id, ticker=ticker, amount=0)
            self.db.add(buyer_asset_balance)
        
        buyer_asset_balance.amount += qty
        
        # Покупатель теряет RUB
        buyer_rub_balance = self.db.query(BalanceDB).filter(
            BalanceDB.user_id == buyer_id,
            BalanceDB.ticker == "RUB"
        ).first()
        
        if buyer_rub_balance:
            buyer_rub_balance.amount -= total_cost
        
        # Обновляем баланс продавца (теряет актив, получает RUB)
        seller_asset_balance = self.db.query(BalanceDB).filter(
            BalanceDB.user_id == seller_id,
            BalanceDB.ticker == ticker
        ).first()
        
        if seller_asset_balance:
            seller_asset_balance.amount -= qty
        
        # Продавец получает RUB
        seller_rub_balance = self.db.query(BalanceDB).filter(
            BalanceDB.user_id == seller_id,
            BalanceDB.ticker == "RUB"
        ).first()
        
        if not seller_rub_balance:
            seller_rub_balance = BalanceDB(user_id=seller_id, ticker="RUB", amount=0)
            self.db.add(seller_rub_balance)
        
        seller_rub_balance.amount += total_cost
    
    def cancel_order(self, order_id: str, user: UserDB) -> bool:
        """Отменить заявку"""
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
        """Получить стакан заявок"""        # Заявки на покупку (bids) - сортируем по убыванию цены
        bids = self.db.query(OrderDB).filter(
            OrderDB.ticker == ticker,
            OrderDB.direction == "BUY",
            OrderDB.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
            OrderDB.order_type == "LIMIT"
        ).order_by(OrderDB.price.desc()).limit(limit).all()
        
        # Заявки на продажу (asks) - сортируем по возрастанию цены
        asks = self.db.query(OrderDB).filter(
            OrderDB.ticker == ticker,
            OrderDB.direction == "SELL",
            OrderDB.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
            OrderDB.order_type == "LIMIT"
        ).order_by(OrderDB.price.asc()).limit(limit).all()
        
        # Группируем по ценам
        bid_levels = {}
        for bid in bids:
            price = bid.price
            qty = bid.qty - bid.filled
            if price in bid_levels:
                bid_levels[price] += qty
            else:
                bid_levels[price] = qty
        
        ask_levels = {}
        for ask in asks:
            price = ask.price
            qty = ask.qty - ask.filled
            if price in ask_levels:
                ask_levels[price] += qty
            else:
                ask_levels[price] = qty
        
        return L2OrderBook(
            bid_levels=[Level(price=price, qty=qty) for price, qty in bid_levels.items()],
            ask_levels=[Level(price=price, qty=qty) for price, qty in ask_levels.items()]
        )