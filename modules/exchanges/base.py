import abc
import inspect
from decimal import Decimal
from typing import Dict, Optional, List, Callable, Set

from modules.models import ContractModel, AccountModel, FundingRateModel, CandleModel, OrderModel, DepthModel, \
    BookUpdateModel
from modules.models.types import (
    Symbol, Timeframe,
    OrderType, OrderSide, OrderId,
    MarginType, TimeInForce, StreamEntity,
)


class BaseExchangeClient(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def get_contracts(self) -> Dict[Symbol, ContractModel]:
        """
        Current exchange trading rules and symbol information
        """
        ...

    @abc.abstractmethod
    async def get_funding_rate(self, contract: ContractModel) -> Dict[Symbol, FundingRateModel]:
        """
        Mark Price and Funding Rate
        """
        ...

    @abc.abstractmethod
    async def get_historical_candles(
            self,
            symbol: Symbol,
            timeframe: Timeframe,
            limit: int = 1000,
            start_time: Optional[str] = None
    ) -> List[CandleModel]:
        """
        Candlestick bars snapshot for a symbol.
        """
        ...

    @abc.abstractmethod
    async def get_book(self) -> Dict[Symbol, BookUpdateModel]:
        """
        Best price/qty on the order book for a symbol or symbols.
        """
        ...

    @abc.abstractmethod
    async def get_depth(self, symbol: Symbol, limit: int = 1000) -> DepthModel:
        """
        Order book snapshot for a symbol.
        """
        ...


class BaseExchangeUserClient(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def get_account_info(self) -> AccountModel:
        ...

    @abc.abstractmethod
    async def change_leverage(self, symbol: Symbol, leverage: int):
        """
        Change user's initial leverage of specific symbol market.
        """
        ...

    @abc.abstractmethod
    async def is_hedge_mode(self) -> bool:
        """
        Check current position mode.
        """
        ...

    @abc.abstractmethod
    async def change_position_mode(self, hedge_mode: bool):
        """
        Change position mode.
        """
        ...

    @abc.abstractmethod
    async def change_margin_type(self, symbol: Symbol, margin_type: MarginType):
        ...

    @abc.abstractmethod
    async def place_order(
            self,
            client_order_id: str,
            contract: ContractModel,
            order_type: OrderType,
            quantity: Decimal,
            order_side: OrderSide,
            price: Decimal = None,
            tif: Optional[TimeInForce] = None,
    ) -> OrderModel:
        """
        Send in a new order.
        """
        ...

    @abc.abstractmethod
    async def cancel_order(self, contract: ContractModel, order_id: OrderId):
        """
        Cancel an active order.
        """
        ...

    @abc.abstractmethod
    async def get_order(self, contract: ContractModel, order_id: OrderId) -> OrderModel:
        """
        Check an order's status.
        """
        ...


class BaseExchangeStreamClient(metaclass=abc.ABCMeta):
    def __init__(self):
        self._callbacks: Dict[str, Set[Callable]] = {}

    @abc.abstractmethod
    async def connect(self):
        ...

    def add_connect_callback(self, cb: Callable):
        assert callable(cb)
        self._callbacks.setdefault('connect', set()).add(cb)

    def add_update_callback(self, entity: StreamEntity, cb: Callable):
        assert StreamEntity.has_value(entity)
        assert callable(cb)
        self._callbacks.setdefault(entity, set()).add(cb)

    def del_update_callback(self, entity: StreamEntity, cb: Callable):
        assert StreamEntity.has_value(entity)
        assert callable(cb)
        self._callbacks.get(entity, set()).discard(cb)

    async def _trigger_callbacks(self, action, *args, **kwargs):
        callbacks = self._callbacks.get(action, set())

        for callback in callbacks:
            result = callback(*args, **kwargs)

            if inspect.isawaitable(result):
                await result
