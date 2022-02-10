import asyncio
import logging
from uuid import uuid4
from decimal import Decimal
from time import time
from typing import Optional, Callable, Dict, Set

from orderedset import OrderedSet
from expiringdict import ExpiringDict

from modules.mongo import MongoClient
from modules.models import PositionModel, OrderModel, BookUpdateModel
from modules.exchanges import BinanceClient, BinanceUserStreamClient
from modules.models.commands import Command, TrailingStop, PlaceOrder
from modules.models.types import (
    PositionId, PositionStatus, PositionSide, ClientOrderId, OrderType,
    UserStreamEntity, Symbol,
)

from services.bot.strategies.base.utils import method_dispatch
from services.bot.strategies.base.storage import LocalStorage


class CommandHandler:
    def __init__(
            self,
            db: MongoClient,
            exchange: BinanceClient,
            user_stream: BinanceUserStreamClient,
            storage: LocalStorage,
            strategy: str
    ):
        self.db = db
        self.exchange = exchange
        self.user_stream = user_stream
        self.storage = storage
        self.strategy = strategy
        self.price: Optional[BookUpdateModel] = None

        self._commands: OrderedSet[Command] = OrderedSet()
        self._waiting: ExpiringDict[ClientOrderId, PlaceOrder] = ExpiringDict(
            max_len=2,
            max_age_seconds=30
        )
        self._callbacks: Dict[str, Set[Callable]] = {}
        self._loop = asyncio.get_event_loop()

        self.user_stream.add_update_callback(UserStreamEntity.ORDER_TRADE_UPDATE, self._update_order)

    def __len__(self):
        return len(self._commands)

    @property
    def has_outgoing_commands(self) -> bool:
        return bool(self._commands)

    def append(self, command: Command):
        if command not in self._commands:
            self._commands.add(command)
        else:
            logging.warning('Duplicate command ignored!')

    def set_price(self, price: BookUpdateModel):
        self.price = price

    async def execute(self):
        if not self._commands:
            return

        next_commands = OrderedSet()

        for command in self._commands:
            while command:
                command = await self.handle(command)

                if command and command.next_time:
                    next_commands.add(command)
                    break

        self._commands = next_commands

    @method_dispatch  # pragma: no cover
    async def handle(self, command: Command, **kwargs) -> Optional[Command]:
        raise RuntimeError('Inconsistent command!')

    @handle.register(TrailingStop)
    async def handle_trailing_stop(self, command: TrailingStop) -> Command:
        triggered = command.update(self.price)

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
            await self._update_order(order)

    async def _update_order(self, order: OrderModel):
        if not order.is_processed:
            return

        exists = await self.db.count(OrderModel, query={'id': order.id})

        if exists:
            return

        position = self.storage.get_position(order.position_side)
        command = self._waiting.pop(order.client_order_id, None)

        if not position:
            position = await self._create_position(order.symbol, order.position_side)

        if command and command.context:
            order.context = command.context

        order.position_id = position.id
        await self.db.create(order)

        if order.is_filled:
            logging.info(f'Order filled! '
                         f'position_id={position.id}; '
                         f'side={order.side}; '
                         f'quantity={order.quantity}; '
                         f'price={order.entry_price};')
            self.storage.add_order(order)
            await self._update_position(position, order)

    async def _create_position(
            self,
            symbol: Symbol,
            position_side: PositionSide,
    ) -> PositionModel:
        # Create position
        position = PositionModel(
            id=PositionId(uuid4().hex),
            symbol=symbol,
            side=position_side,
            strategy=self.strategy,
            status=PositionStatus.OPEN,
            quantity=Decimal('0'),
            total_quantity=Decimal('0'),
            entry_price=Decimal('0'),
            exit_price=Decimal('0'),
            orders=[],
            create_timestamp=int(time() * 1000),
        )
        await self.db.create(position)
        self.storage.set_position(position)
        logging.info(f'Position created! '
                     f'position_id={position.id};')

        return position

    async def _update_position(self, position: PositionModel, order: OrderModel):
        entry_side = position.get_entry_order_side()
        exit_side = position.get_exit_order_side()

        # If the order is a position entry
        if order.side is entry_side:
            # Calculate avg position entry price
            entry_orders = self.storage.get_orders(position.id, entry_side)
            total_price = sum([order.quantity * order.entry_price for order in entry_orders])
            total_quantity = sum([order.quantity for order in entry_orders])
            position.entry_price = total_price / total_quantity
            position.quantity += order.quantity
            position.total_quantity += order.quantity

        else:
            # Calculate avg position exit price
            exit_orders = self.storage.get_orders(position.id, exit_side)
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
            self.storage.drop_position(position.side)
            self.storage.drop_orders(position.id)

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
