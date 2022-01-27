import abc
import asyncio
import logging
from decimal import Decimal
from itertools import chain
from time import time
from typing import Optional, Set, Callable, List, Dict, Any

from helpers import remove_exponent
from modules.models.commands import TrailingStop, PlaceOrder

from modules.mongo import MongoClient
from modules.models import ContractModel, OrderModel, PositionModel, AccountModel
from modules.models.line import TradeUpdateModel, BookUpdateModel, DepthUpdateModel
from modules.models.types import (
    PositionStatus, OrderSide, StreamEntity, PositionSide, CommandAction, TickType, Timestamp
)
from modules.models.strategy import StopLossConfig, TakeProfitConfig
from modules.models.indexes import INDEXES
from modules.exchanges.base import BaseExchangeClient
from modules.exchanges.fake import FakeExchangeClient
from modules.exchanges import BinanceClient
from modules.line_client import LineClient, ReplayClient

from services.bot.strategies.base.command_handler import CommandHandler
from services.bot.strategies.base.storage import LocalStorage
from services.bot.settings import Settings
from services.bot.candles import Candles
from services.bot.depth import Depth


class BaseStrategy(metaclass=abc.ABCMeta):
    name: str
    exchange_class: BaseExchangeClient
    stop_loss: Optional[StopLossConfig] = None
    take_profit: Optional[TakeProfitConfig] = None

    def __init__(self, settings: Settings):
        self.settings = settings

        self.db = MongoClient(
            mongo_uri=settings.mongo_uri,
            indexes=INDEXES,
        )
        self.candles = Candles(
            timeframe=settings.timeframe,
            candles_limit=settings.candles_limit,
        )

        if settings.depth_limit:
            self.depth = Depth(
                limit=settings.depth_limit,
            )
            self.depth.add_gap_callback(self._set_gap_snapshot)

        if settings.replay:
            logging.warning('*** The strategy is processing historical data! ***')
            self.line = ReplayClient(
                db=self.db,
                symbol=settings.symbol,
                replay_speed=settings.replay_speed,
                replay_from=settings.replay_from,
                replay_to=settings.replay_to,
            )
            self.exchange = FakeExchangeClient(self.line)
            self.line.add_done_callback(self.stop)

        else:
            self.line = LineClient(
                symbol=settings.symbol,
                uri=settings.broker_amqp_uri,
                entities=settings.entities,
            )
            self.exchange = BinanceClient(
                public_key=settings.binance_public_key,
                private_key=settings.binance_private_key,
                testnet=settings.binance_testnet,
            )

        self._ready = asyncio.Event()
        self._last_signal_check: int = 0
        self._callbacks: Dict[str, Set] = {}
        self._loop = asyncio.get_event_loop()

        self.storage = LocalStorage()
        self.command_handler = CommandHandler(
            db=self.db,
            exchange=self.exchange,
            storage=self.storage,
            strategy=self.name,
        )
        self.contract: Optional[ContractModel] = None
        self.price: Optional[BookUpdateModel] = None
        self.account: Optional[AccountModel] = None

        self.line.add_update_callback(StreamEntity.BOOK, self._on_book_update)
        self.line.add_update_callback(StreamEntity.TRADE, self._on_trade_update)
        self.line.add_update_callback(StreamEntity.DEPTH, self._on_depth_update)

        self.command_handler.add_callback(CommandAction.ORDER_FILLED, self._on_order_filled)

    def add_start_callback(self, cb: Callable):
        assert asyncio.iscoroutinefunction(cb)
        self._callbacks.setdefault('start', set()).add(cb)

    def add_stop_callback(self, cb: Callable):
        assert asyncio.iscoroutinefunction(cb)
        self._callbacks.setdefault('stop', set()).add(cb)

    async def start(self):
        await self._prepare_resources()
        await self._preload_data()
        await self._configure_mode()
        await self._configure_leverage()
        await self._trigger_callbacks('start')
        self._ready.set()

    async def stop(self):
        await self.line.stop()
        await self._trigger_callbacks('stop')
        self._loop.stop()

    @abc.abstractmethod
    def check_signal(self, tick_type: TickType):
        ...

    def close_position(self, position: PositionModel, context: Optional[Dict] = None, trailing: bool = False):
        kwargs = dict(
            quantity=position.quantity,
            context=context,
            trailing=trailing,
        )

        if position.side is PositionSide.LONG:
            self.close_long(**kwargs)
        else:
            self.close_short(**kwargs)

    def open_long(self, quantity: Decimal, context: Optional[Dict] = None, trailing: bool = False):
        self.place_order(
            quantity=quantity,
            position_side=PositionSide.LONG,
            order_side=OrderSide.BUY,
            trailing=trailing,
            context=context
        )

    def open_short(self, quantity: Decimal, context: Optional[Dict] = None, trailing: bool = False):
        self.place_order(
            quantity=quantity,
            position_side=PositionSide.SHORT,
            order_side=OrderSide.SELL,
            trailing=trailing,
            context=context
        )

    def close_long(self, quantity: Decimal, trailing: bool = False, context: Optional[Dict] = None):
        self.place_order(
            position_side=PositionSide.LONG,
            order_side=OrderSide.SELL,
            quantity=quantity,
            trailing=trailing,
            context=context
        )

    def close_short(self, quantity: Decimal, trailing: bool = False, context: Optional[Dict] = None):
        self.place_order(
            position_side=PositionSide.SHORT,
            order_side=OrderSide.BUY,
            quantity=quantity,
            trailing=trailing,
            context=context
        )

    def place_order(
            self,
            position_side: PositionSide,
            order_side: OrderSide,
            quantity: Decimal,
            trailing: bool = False,
            context: Optional[Dict] = None
    ):
        if not self.contract or not self.price:
            return

        if self.command_handler.is_pending:
            return

        command = PlaceOrder(
            contract=self.contract,
            position_side=position_side,
            order_side=order_side,
            quantity=quantity,
            context=context
        )

        if trailing and self.settings.trailing_callback_rate:
            command = TrailingStop(
                contract=self.contract,
                price=self.price,
                order_side=order_side,
                callback_rate=self.settings.trailing_callback_rate,
                next_command=command,
            )

        self.command_handler.append(command)

    def calc_trade_quantity(self, balance_stake: Decimal, order_side: OrderSide) -> Optional[Decimal]:
        if not (self.price and self.contract and self.account):
            return

        price = self.price.bid if order_side is OrderSide.BUY else self.price.ask
        balance = self.account.assets[self.contract.quote_asset].wallet_balance
        quantity = balance * balance_stake * self.settings.leverage / price
        quantity = remove_exponent(round(quantity / self.contract.lot_size) * self.contract.lot_size)

        if quantity * price < self.contract.min_notional:
            logging.error(f'"calc_trade_quantity": The quantity is too small! '
                          f'quote_asset={self.contract.quote_asset}; '
                          f'base_asset={self.contract.base_asset}'
                          f'balance={balance}; '
                          f'balance_stake={balance_stake}; '
                          f'quantity={quantity}; '
                          f'price={price}; '
                          f'lot_size={self.contract.lot_size}; '
                          f'min_notional={self.contract.min_notional};')
            return

        return quantity

    async def _set_gap_snapshot(self):
        depth = await self.exchange.get_depth(
            symbol=self.settings.symbol,
            limit=self.settings.depth_limit,
        )
        self.depth.set_snapshot(depth)

    def _on_book_update(self, model: BookUpdateModel):
        self.price = model

        if not self._ready.is_set():
            return

        self.command_handler.set_price(model)

        if not self.command_handler.is_pending and self.command_handler.has_outgoing_commands:
            self.command_handler.execute()

        else:
            for position_side in (PositionSide.LONG, PositionSide.SHORT):
                position = self.storage.get_position(position_side)

                if position:
                    pnl = position.calc_pnl(self.price)
                    print(position.side, pnl)

                    self.check_stop_loss(position)
                    self.check_take_profit(position)

    def _on_trade_update(self, model: TradeUpdateModel):
        if not self._ready.is_set():
            return

        self._check_delay(model.timestamp)
        tick_type = self.candles.update(model)

        if not self.price:
            return

        if self.command_handler.is_pending:
            return

        interval = self.settings.signal_check_interval
        diff = model.timestamp - self._last_signal_check

        # Limit signal check to interval
        if (diff >= interval * 1000) or tick_type is TickType.NEW_CANDLE:
            self.check_signal(tick_type)
            self._last_signal_check = model.timestamp

    async def _on_depth_update(self, model: DepthUpdateModel):
        if not self._ready.is_set():
            return

        self._check_delay(model.timestamp)
        self.depth.update(model)
        await asyncio.sleep(0)  # to prevent loop locks

    async def _on_order_filled(self):
        self.account = await self.exchange.get_account_info()
        repr_balances = {i.asset: i.wallet_balance for i in self.account.assets.values()}
        logging.info(f'Current balances: {repr_balances}')

    def check_stop_loss(self, position: PositionModel):
        if not self.stop_loss:
            return

        if position.is_closed or not position.quantity:
            return

        if position.side is PositionSide.LONG:
            price = self.price.bid
            trigger = position.entry_price * (1 - self.stop_loss.rate)
            triggered = price <= trigger

        else:
            price = self.price.ask
            trigger = position.entry_price * (1 + self.stop_loss.rate)
            triggered = price >= trigger

        if triggered:
            logging.warning(f'Stop loss triggered! '
                            f'position_id={position.id}; '
                            f'trigger={trigger}; '
                            f'price={price};')

            self.close_position(
                position=position,
                context={'reason': 'Stop loss triggered'},
                trailing=True,
            )

    def check_take_profit(self, position: PositionModel):
        if not self.take_profit:
            return

        if position.is_closed or not position.quantity:
            return

        triggered = False
        order_side = None
        steps_count = self.take_profit.steps_count
        exit_side = position.get_exit_order_side()
        exit_orders = self.storage.get_orders(position.id, exit_side)
        next_step = len(exit_orders) + 1

        if steps_count < next_step:
            return

        step = self.take_profit.steps[next_step - 1]

        if position.side is PositionSide.LONG:
            order_side = OrderSide.SELL
            triggered = self.price.bid >= position.entry_price * (1 + step.level)

        elif position.side is PositionSide.SHORT:
            order_side = OrderSide.BUY
            triggered = self.price.ask <= position.entry_price * (1 - step.level)

        if triggered and order_side:
            logging.info(f'Take profit level {step.level} reached! '
                         f'position_id={position.id}')
            quantity = position.total_quantity * step.stake
            price = self.price.bid if position.side is PositionSide.LONG else self.price.ask
            diff_quantity = 0

            if quantity * price < self.contract.min_notional:
                # Increase to the min_notional
                prev_quantity = quantity
                quantity = self.contract.min_notional / price
                diff_quantity = quantity - prev_quantity

            # If stake of the remaining steps less than min_notional - use the entire quantity
            rest_stake = sum([self.take_profit.steps[i - 1].stake for i in range(next_step, steps_count)])
            rest_quantity = position.total_quantity * rest_stake - diff_quantity

            if rest_quantity * price < self.contract.min_notional:
                quantity = position.quantity

            if quantity >= position.quantity:
                self.close_position(
                    position=position,
                    context={'reason': 'Last take profit level reached'},
                    trailing=True,
                )

            else:
                quantity = remove_exponent(round(quantity / self.contract.lot_size) * self.contract.lot_size)
                self.place_order(
                    position_side=position.side,
                    order_side=order_side,
                    quantity=quantity,
                    trailing=True,
                    context={'reason': f'Take profit level {step.level} reached'},
                )

    async def _prepare_resources(self):
        await self.db.connect()
        await self.line.start()

    async def _preload_data(self):
        contracts = await self.exchange.get_contracts()
        self.contract = contracts[self.settings.symbol]
        self.account = await self.exchange.get_account_info()

        if self.settings.candles_limit:
            candles = await self.exchange.get_historical_candles(
                symbol=self.settings.symbol,
                timeframe=self.settings.timeframe,
                limit=self.settings.candles_limit,
            )
            self.candles.set_snapshot(candles)

        if self.settings.depth_limit:
            await self._set_gap_snapshot()

        await self._set_positions()

    async def _set_positions(self):
        positions: List[PositionModel] = await self.db.find(
            model=PositionModel,
            query={
                'symbol': self.settings.symbol,
                'strategy': self.name,
                'status': PositionStatus.OPEN,
            }
        )
        account_positions = {p.side: p for p in self.account.positions if p.symbol == self.settings.symbol}
        db_positions = {p.side: p for p in positions}
        actual_positions = []

        for side in set(account_positions) & set(db_positions):
            db_position = db_positions[side]
            position = account_positions[side]

            if (position.quantity and
                    db_position.entry_price == position.entry_price and
                    db_position.quantity == position.quantity):
                actual_positions.append(db_position)

        orders: List[OrderModel] = await self.db.find(
            model=OrderModel,
            query={
                'order_id': {
                    '$in': list(chain.from_iterable([position.orders for position in actual_positions]))
                }
            }
        )
        self.storage.set_snapshot(actual_positions, orders)

    async def _configure_mode(self):
        hedge_mode = await self.exchange.is_hedge_mode()

        if not hedge_mode:
            await self.exchange.change_position_mode(hedge_mode=True)

    async def _configure_leverage(self):
        position = next(filter(lambda x: x.symbol == self.settings.symbol, self.account.positions), None)

        if position and self.settings.leverage != position.leverage:
            await self.exchange.change_leverage(self.contract, self.settings.leverage)

    def _check_delay(self, timestamp: Timestamp):
        if not self.settings.replay:
            diff = time() * 1000 - timestamp
            exchange_name = self.exchange.__class__.__name__

            if diff >= 2000:
                logging.warning(f'{exchange_name}: Messages processing delay of {diff / 1000}s!')

            if diff >= 10000:
                logging.error('Stopping the program due to critical delay!')
                self._loop.create_task(self.stop())

    async def _trigger_callbacks(self, action: Any, *args, **kwargs):
        callbacks = self._callbacks.get(action, set())

        for cb in callbacks:
            self._loop.create_task(cb(*args, **kwargs))
