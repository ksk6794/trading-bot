import abc
import asyncio
import inspect
import logging
from decimal import Decimal
from itertools import chain
from typing import Optional, Set, Callable, List, Dict, Any, Tuple

from helpers import remove_exponent, to_decimal_places

from modules.mongo import MongoClient
from modules.models import OrderModel, PositionModel, AccountModel, AccountConfigModel
from modules.models.commands import TrailingStop, PlaceOrder
from modules.models.exchange import AccountPositionModel, AccountBalanceModel
from modules.models.types import (
    PositionStatus, OrderSide, UserStreamEntity,
    PositionSide, Asset, Timeframe, Symbol, Indicator
)
from modules.exchanges import BinanceUserClient, BinanceUserStreamClient
from modules.exchanges.base import BaseExchangeClient
from modules.exchanges.fake.client import FakeExchangeUserClient

from services.bot.strategies.command_handler import CommandHandler
from services.bot.strategies.storage import LocalStorage
from services.bot.state import ExchangeState
from services.bot.settings import StrategyRules


class Strategy(metaclass=abc.ABCMeta):
    name: str
    exchange_class: BaseExchangeClient

    def __init__(
            self,
            rules: StrategyRules,
            db: MongoClient,
            state: ExchangeState,
            replay: bool = False
    ):
        self.rules = rules
        self.db = db
        self.state = state

        if replay:
            self.exchange = FakeExchangeUserClient(self.state)
            self.user_stream = self.exchange.user_stream

        else:
            self.exchange = BinanceUserClient(
                public_key=rules.binance_public_key,
                private_key=rules.binance_private_key,
                testnet=rules.binance_testnet,
            )
            self.user_stream = BinanceUserStreamClient(
                exchange=self.exchange,
                testnet=rules.binance_testnet,
            )

        self.assets: Dict[Asset, AccountBalanceModel] = {}

        self._ready: bool = False
        self._callbacks: Dict[str, Set] = {}
        self._loop = asyncio.get_event_loop()

        self.storage = LocalStorage()
        self.command_handler = CommandHandler(
            db=self.db,
            exchange=self.exchange,
            user_stream=self.user_stream,
            storage=self.storage,
            strategy_id=self.rules.id,
            symbols=self.rules.symbols,
            state=self.state
        )

        self.user_stream.add_update_callback(UserStreamEntity.ACCOUNT_UPDATE, self._on_account_update)
        self.user_stream.add_update_callback(UserStreamEntity.ACCOUNT_CONFIG_UPDATE, self._on_account_config_update)

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
        await self._trigger_callbacks('start')
        self._ready = True

    async def stop(self):
        await self._trigger_callbacks('stop')
        self._loop.stop()

    def check_signal(self, symbol: Symbol):
        output: Dict[Tuple[PositionSide, OrderSide], Dict[Tuple[Indicator, Timeframe], bool]] = {}
        quantity = self.calc_trade_quantity(symbol, self.rules.balance_stake, OrderSide.BUY)

        for condition in self.rules.conditions:
            candles = self.state.get_candles(symbol, condition.timeframe)
            result = False

            for index in list(map(lambda i: i * -1, range(1, condition.save_signal_candles + 1))):
                parameters = {**{i.field: i.value for i in condition.parameters}, 'index': index}
                values = candles.get(condition.indicator, parameters)

                for cond in condition.conditions:
                    result = self._compare(cond.condition, values[cond.field], cond.value)

                    if result:
                        break

                else:
                    continue

                break

            output.setdefault((condition.position_side, condition.order_side), {})[(condition.indicator, condition.timeframe)] = result

        for (position_side, order_side), results in output.items():
            triggered_cnt = len(list(filter(lambda i: i is True, results.values())))

            if triggered_cnt >= self.rules.conditions_trigger_count:
                position = self.storage.get_position(symbol, position_side)

                if position:
                    orders = self.storage.get_orders(symbol, position.id, order_side)

                    if orders:
                        continue

                self.place_order(
                    symbol=symbol,
                    position_side=position_side,
                    order_side=order_side,
                    quantity=quantity,
                    trailing=self.rules.trailing,
                    context={},
                )

    @staticmethod
    def _compare(condition, a1, a2):
        if a1 is None or a1 is None:
            return False

        if condition == 'eq':
            return a1 == a2
        elif condition == 'lt':
            return a1 < a2
        elif condition == 'lte':
            return a1 <= a2
        elif condition == 'gt':
            return a1 > a2
        elif condition == 'gte':
            return a1 >= a2

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

    def open_long(self, symbol: Symbol, quantity: Decimal, context: Optional[Dict] = None, trailing: bool = False):
        self.place_order(
            symbol=symbol,
            quantity=quantity,
            position_side=PositionSide.LONG,
            order_side=OrderSide.BUY,
            trailing=trailing,
            context=context
        )

    def open_short(self, symbol: Symbol, quantity: Decimal, context: Optional[Dict] = None, trailing: bool = False):
        self.place_order(
            symbol=symbol,
            quantity=quantity,
            position_side=PositionSide.SHORT,
            order_side=OrderSide.SELL,
            trailing=trailing,
            context=context
        )

    def close_long(self, symbol: Symbol, quantity: Decimal, trailing: bool = False, context: Optional[Dict] = None):
        self.place_order(
            symbol=symbol,
            position_side=PositionSide.LONG,
            order_side=OrderSide.SELL,
            quantity=quantity,
            trailing=trailing,
            context=context
        )

    def close_short(self, symbol: Symbol, quantity: Decimal, trailing: bool = False, context: Optional[Dict] = None):
        self.place_order(
            symbol=symbol,
            position_side=PositionSide.SHORT,
            order_side=OrderSide.BUY,
            quantity=quantity,
            trailing=trailing,
            context=context
        )

    def place_order(
            self,
            symbol: Symbol,
            position_side: PositionSide,
            order_side: OrderSide,
            quantity: Decimal,
            trailing: bool = False,
            context: Optional[Dict] = None
    ):
        book = self.state.get_book(symbol)
        contract = self.state.get_contract(symbol)

        command = PlaceOrder(
            contract=contract,
            position_side=position_side,
            order_side=order_side,
            quantity=quantity,
            context=context
        )

        if trailing and self.rules.trailing_callback_rate:
            command = TrailingStop(
                contract=contract,
                price=book,
                order_side=order_side,
                callback_rate=self.rules.trailing_callback_rate,
                next_command=command,
            )

        self.command_handler.append(symbol, command)

    def calc_trade_quantity(self, symbol: Symbol, balance_stake: Decimal, order_side: OrderSide) -> Optional[Decimal]:
        contract = self.state.get_contract(symbol)
        book = self.state.get_book(symbol)

        if not self.assets:
            return

        price = book.bid if order_side is OrderSide.BUY else book.ask

        if price <= 0:
            logging.warning('Abnormal price during calc quantity!')
            return

        balance = self.assets[contract.quote_asset].wallet_balance
        quantity = balance * balance_stake * self.rules.leverage / price
        quantity = remove_exponent(round(quantity / contract.lot_size) * contract.lot_size)

        if quantity * price < contract.min_notional:
            logging.error(f'"calc_trade_quantity": The quantity is too small! '
                          f'quote_asset={contract.quote_asset}; '
                          f'base_asset={contract.base_asset}; '
                          f'balance={balance}; '
                          f'balance_stake={balance_stake}; '
                          f'quantity={quantity}; '
                          f'price={price}; '
                          f'lot_size={contract.lot_size}; '
                          f'min_notional={contract.min_notional};')
            return

        return quantity

    async def on_book_update(self, symbol: Symbol):
        if symbol not in self.rules.symbols:
            return

        if not self._ready:
            return

        # Execute commands (trailing)
        if self.command_handler.has_outgoing_commands(symbol):
            await self.command_handler.execute(symbol)
            return

        for position_side in PositionSide.values():
            position = self.storage.get_position(symbol, position_side)

            if position:
                self.check_stop_loss(symbol, position)
                self.check_take_profit(symbol, position)

                # Execute commands
                if self.command_handler.has_outgoing_commands(symbol):
                    await self.command_handler.execute(symbol)

    async def on_candles_update(self, symbol: Symbol):
        if not self._ready:
            return

        if symbol not in self.rules.symbols:
            return

        self.check_signal(symbol)

        # Execute commands
        if self.command_handler.has_outgoing_commands(symbol):
            await self.command_handler.execute(symbol)

    def _on_account_update(self, model: AccountModel):
        for asset, balance in model.assets.items():
            self.assets[asset] = balance

        str_assets = '; '.join([f'{a.asset}={a.wallet_balance}' for a in self.assets.values()])
        logging.info('Account updated! %s', str_assets)

    @staticmethod
    def _on_account_config_update(model: AccountConfigModel):
        logging.info('Account config updated! symbol=%s; leverage=%d', model.symbol, model.leverage)

    def check_stop_loss(self, symbol: Symbol, position: PositionModel):
        if not self.rules.stop_loss:
            return

        if position.is_closed or not position.quantity:
            return

        book = self.state.get_book(symbol)

        triggered = False
        trigger = 0
        price = 0

        if position.side is PositionSide.LONG:
            price = book.bid
            trigger = position.entry_price * (1 - self.rules.stop_loss.rate)
            triggered = price <= trigger

        elif position.side is PositionSide.SHORT:
            price = book.ask
            trigger = position.entry_price * (1 + self.rules.stop_loss.rate)
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

    def check_take_profit(self, symbol: Symbol, position: PositionModel):
        if not self.rules.take_profit:
            return

        if position.is_closed or not position.quantity:
            return

        book = self.state.get_book(symbol)
        contract = self.state.get_contract(symbol)

        triggered = False
        order_side = None
        steps_count = self.rules.take_profit.steps_count
        exit_side = position.get_exit_order_side()
        exit_orders = self.storage.get_orders(position.symbol, position.id, exit_side)
        next_step = len(exit_orders) + 1

        if steps_count < next_step:
            return

        step = self.rules.take_profit.steps[next_step - 1]

        if position.side is PositionSide.LONG:
            order_side = OrderSide.SELL
            triggered = book.bid >= position.entry_price * (1 + step.level)

        elif position.side is PositionSide.SHORT:
            order_side = OrderSide.BUY
            triggered = book.ask <= position.entry_price * (1 - step.level)

        if triggered and order_side:
            logging.info(f'Take profit level {step.level} reached! '
                         f'position_id={position.id}')
            quantity = position.total_quantity * step.stake
            price = book.bid if position.side is PositionSide.LONG else book.ask
            diff_quantity = 0

            if quantity * price < contract.min_notional:
                # Increase to the min_notional
                prev_quantity = quantity
                quantity = contract.min_notional / price
                diff_quantity = quantity - prev_quantity

            # If stake of the remaining steps less than min_notional - use the entire quantity
            rest_stake = sum([self.rules.take_profit.steps[i - 1].stake for i in range(next_step, steps_count)])
            rest_quantity = position.total_quantity * rest_stake - diff_quantity

            if rest_quantity * price < contract.min_notional:
                quantity = position.quantity

            if quantity >= position.quantity:
                self.close_position(
                    position=position,
                    context={'reason': 'Last take profit level reached'},
                    trailing=True,
                )

            else:
                quantity = remove_exponent(round(quantity / contract.lot_size) * contract.lot_size)
                self.place_order(
                    symbol=symbol,
                    position_side=position.side,
                    order_side=order_side,
                    quantity=quantity,
                    trailing=True,
                    context={'reason': f'Take profit level {step.level} reached'},
                )

    async def _prepare_resources(self):
        await self.db.connect()
        await self.user_stream.connect()

    async def _preload_data(self):
        account = await self.exchange.get_account_info()
        self.assets = account.assets
        await self._set_positions(account.positions)

    async def _set_positions(self, positions: List[AccountPositionModel]):
        db_positions_list: List[PositionModel] = await self.db.find(
            model=PositionModel,
            query={
                'symbol': {'$in': self.rules.symbols},
                'strategy_id': self.rules.id,
                'status': PositionStatus.OPEN,
            }
        )

        db_positions: Dict[Symbol, Dict[PositionSide, PositionModel]] = {}
        for position in db_positions_list:
            db_positions.setdefault(position.symbol, {})[position.side] = position

        acc_positions: Dict[Symbol, Dict[PositionSide, AccountPositionModel]] = {}
        for position in positions:
            acc_positions.setdefault(position.symbol, {})[position.side] = position

        for symbol in self.rules.symbols:
            actual_positions = []
            cur_positions = [position for position in positions if position.symbol == symbol and position.quantity > 0]
            contract = self.state.get_contract(symbol)

            # Match positions
            for side in set(db_positions.get(symbol, {})) & set(acc_positions.get(symbol, {})):
                db_position = db_positions[symbol][side]
                acc_position = acc_positions[symbol][side]
                db_position_price = to_decimal_places(db_position.entry_price, contract.lot_size)
                acc_position_price = to_decimal_places(acc_position.entry_price, contract.lot_size)

                if db_position_price == acc_position_price and db_position.quantity == acc_position.quantity:
                    actual_positions.append(db_position)

            # Check for unknown positions
            if len(actual_positions) != len(cur_positions):
                raise RuntimeError(f'Unknown position found! symbol={symbol}')

            await self._configure_leverage(symbol)

            orders: List[OrderModel] = await self.db.find(
                model=OrderModel,
                query={
                    'id': {
                        '$in': list(chain.from_iterable([
                            position.orders
                            for position in actual_positions
                        ]))
                    }
                }
            )
            self.storage.set_snapshot(symbol, actual_positions, orders)

    async def _configure_mode(self):
        hedge_mode = await self.exchange.is_hedge_mode()

        if not hedge_mode:
            await self.exchange.change_position_mode(hedge_mode=True)

    async def _configure_leverage(self, symbol: Symbol):
        await self.exchange.change_leverage(symbol, self.rules.leverage)

    @staticmethod
    def _pct_change(prev_price: Decimal, cur_price: Decimal):
        return (prev_price - cur_price) / cur_price * 100

    async def _trigger_callbacks(self, action: Any, *args, **kwargs):
        callbacks = self._callbacks.get(action, set())

        for cb in callbacks:
            result = cb(*args, **kwargs)

            if inspect.isawaitable(result):
                await result
