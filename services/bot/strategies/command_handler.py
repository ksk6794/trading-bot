import asyncio
import logging
from uuid import uuid4
from decimal import Decimal
from time import time
from typing import Optional, Callable, Dict, Set, List

from orderedset import OrderedSet

from modules.mongo import MongoClient
from modules.models import PositionModel, OrderModel
from modules.exchanges.base import BaseExchangeUserClient
from modules.models.commands import Command, TrailingStop, PlaceOrder
from modules.models.types import (
    PositionId, PositionStatus, PositionSide, ClientOrderId, OrderType, Symbol, StrategyId,
)

from services.bot.state import ExchangeState
from services.bot.strategies.decorators import method_dispatch
from services.bot.strategies.storage import LocalStorage


class CommandHandler:
    def __init__(
            self,
            db: MongoClient,
            exchange: BaseExchangeUserClient,
            storage: LocalStorage,
            strategy_id: StrategyId,
            symbols: List[Symbol],
            state: ExchangeState
    ):
        self.db = db
        self.exchange = exchange
        self.storage = storage
        self.strategy_id = strategy_id
        self.symbols = symbols
        self.state = state

        self._commands: Dict[Symbol, OrderedSet[Command]] = {}
        self._waiting: Dict[ClientOrderId, PlaceOrder] = {}
        self._callbacks: Dict[str, Set[Callable]] = {}
        self._loop = asyncio.get_event_loop()

    def has_outgoing_commands(self, symbol: Symbol) -> bool:
        return bool(self._commands.get(symbol, set()))

    def append(self, symbol: Symbol, command: Command):
        if command not in self._commands:
            self._commands.setdefault(symbol, OrderedSet()).add(command)
        else:
            logging.warning('Duplicate command ignored!')

    async def execute(self, symbol: Symbol):
        commands = self._commands.get(symbol, set())

        if not commands:
            return

        next_commands = OrderedSet()

        for command in commands:
            while command:
                command = await self.handle(command)

                if command and command.next_time:
                    next_commands.add(command)
                    break

        self._commands[symbol] = next_commands

    @method_dispatch  # pragma: no cover
    async def handle(self, command: Command, **kwargs) -> Optional[Command]:
        raise RuntimeError('Inconsistent command!')

    @handle.register(TrailingStop)
    async def handle_trailing_stop(self, command: TrailingStop) -> Command:
        book = self.state.get_book(command.symbol)
        triggered = book and command.update(book)

        if triggered:
            next_command = command.next_command
        else:
            next_command = command
            next_command.next_time = True

        return next_command

    @handle.register(PlaceOrder)
    async def handle_place_order(self, command: PlaceOrder):
        client_order_id = ClientOrderId(uuid4().hex)
        self._waiting[client_order_id] = command

        logging.info(f'Placing order: symbol={command.contract.symbol}')

        order = await self.exchange.place_order(
            client_order_id=client_order_id,
            contract=command.contract,
            order_type=OrderType.MARKET,
            quantity=command.quantity,
            position_side=command.position_side,
            order_side=command.order_side
        )

        if order:
            if not order.is_processed:
                order = await self._wait_for_processed(order)
            await self.update_order(order)

        else:
            del self._waiting[client_order_id]

    async def update_order(self, order: OrderModel):
        if order.symbol not in self.symbols:
            return

        # Skip unknown order updates
        if order.client_order_id not in self._waiting:
            return

        count = await self.db.count(OrderModel, query={'id': order.id})

        if not count:
            command = self._waiting[order.client_order_id]
            order.context = command.context if command and command.context else None
            await self.db.create(order)

        else:
            order = await self.db.partial_update(
                model=OrderModel,
                update_fields=order.dict(exclude_none=True),
                query={'id': order.id}
            )

        if order.is_filled:
            position = self.storage.get_position(order.symbol, order.position_side)

            if not position:
                position = await self._create_position(order.symbol, order.position_side)

            order = await self.db.partial_update(
                model=OrderModel,
                update_fields={'position_id': position.id},
                query={'id': order.id}
            )
            self.storage.add_order(order)

            logging.info(f'Order filled! '
                         f'position_id={position.id}; '
                         f'side={order.side}; '
                         f'quantity={order.quantity}; '
                         f'price={order.entry_price};')

            await self._update_position(position, order)

        if order.is_processed:
            del self._waiting[order.client_order_id]

    async def _create_position(
            self,
            symbol: Symbol,
            position_side: PositionSide,
    ) -> PositionModel:
        position = PositionModel(
            id=PositionId(uuid4().hex),
            symbol=symbol,
            side=position_side,
            strategy_id=self.strategy_id,
            status=PositionStatus.OPEN,
            quantity=Decimal('0'),
            total_quantity=Decimal('0'),
            margin=Decimal('0'),
            entry_price=Decimal('0'),
            exit_price=Decimal('0'),
            orders=[],
            create_timestamp=int(time() * 1000),
        )
        await self.db.create(position)
        self.storage.add_position(position)

        logging.info(f'Position created! '
                     f'position_id={position.id};')

        return position

    async def _update_position(self, position: PositionModel, order: OrderModel):
        entry_side = position.get_entry_order_side()
        exit_side = position.get_exit_order_side()

        # If the order is a position entry
        if order.side is entry_side:
            # Calculate avg position entry price
            entry_orders = self.storage.get_orders(position.symbol, position.id, entry_side)
            total_price = sum([order.quantity * order.entry_price for order in entry_orders])
            total_quantity = sum([order.quantity for order in entry_orders])
            position.entry_price = total_price / total_quantity
            position.quantity += order.quantity
            position.total_quantity += order.quantity

        else:
            # Calculate avg position exit price
            exit_orders = self.storage.get_orders(position.symbol, position.id, exit_side)
            total_price = sum([order.quantity * order.entry_price for order in exit_orders])
            total_quantity = sum([order.quantity for order in exit_orders])
            position.exit_price = total_price / total_quantity
            position.quantity -= order.quantity

            # Close position
            if position.quantity == 0:
                position.status = PositionStatus.CLOSED

        position.orders.append(order.id)
        position.update_timestamp = int(time() * 1000)

        # Update position
        await self.db.update(position, query={'id': position.id})

        if position.is_closed:
            # Clean up local state
            self.storage.drop_position(position.symbol, position.side)
            self.storage.drop_orders(position.symbol, position.id)

            logging.info(f'Position closed! '
                         f'position_id={position.id}; '
                         f'total_quantity={position.total_quantity}; '
                         f'entry_price={position.entry_price}; '
                         f'exit_price={position.exit_price};')

    async def _wait_for_processed(self, order: OrderModel) -> OrderModel:
        while True:
            await asyncio.sleep(1)
            order = await self.exchange.get_order(order.symbol, order.id)

            if order.is_processed:
                return order
