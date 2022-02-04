import asyncio
import logging
from uuid import uuid4
from decimal import Decimal
from time import time
from typing import Optional, Callable, Dict, Set

from orderedset import OrderedSet

from modules.mongo import MongoClient
from modules.exchanges import BinanceClient, BinanceUserStreamClient
from modules.models import PositionModel, OrderModel, BookUpdateModel, ContractModel
from modules.models.types import (
    PositionId, PositionStatus, PositionSide, OrderId, ClientOrderId, OrderType,
    UserStreamEntity, Symbol,
)
from modules.models.commands import Command, TrailingStop, PlaceOrder

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
        self._pending = False
        self._waiting: Dict[ClientOrderId, PlaceOrder] = {}
        self._callbacks: Dict[str, Set[Callable]] = {}
        self._loop = asyncio.get_event_loop()

        self.user_stream.add_update_callback(UserStreamEntity.ORDER_TRADE_UPDATE, self._on_order_trade_update)

    def __len__(self):
        return len(self._commands)

    @property
    def is_pending(self) -> bool:
        return bool(self._pending or self._waiting)

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

    def execute(self):
        if not self._pending and self._commands:
            self._pending = True
            self._loop.call_soon(lambda: self._loop.create_task(self._execute_commands()))

    async def _execute_commands(self):
        next_commands = OrderedSet()

        for command in self._commands:
            while command:
                command = await self.handle(command)

                if command and command.next_time:
                    next_commands.add(command)
                    break

        self._commands = next_commands
        self._pending = False

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

        await self.user_stream.wait_connected()
        await self.exchange.place_order(
            client_order_id=client_order_id,
            contract=command.contract,
            order_type=OrderType.MARKET,
            quantity=command.quantity,
            position_side=command.position_side,
            order_side=command.order_side
        )

    async def _on_order_trade_update(self, order: OrderModel):
        position = self.storage.get_position(order.position_side)
        command = self._waiting.get(order.client_order_id, None)

        if not position:
            position = await self._open_position(order.symbol, order.position_side)

        if command and command.context and not order.context:
            order.context = command.context

        order.position_id = position.id

        await self.db.upsert(order, query={'id': order.id})
        self.storage.add_order(order)

        if order.is_filled:
            logging.info(f'Order placed! '
                         f'position_id={position.id}; '
                         f'side={order.side}; '
                         f'quantity={order.quantity}; '
                         f'price={order.entry_price};')
            await self._update_position(position, order)
            self._waiting.pop(order.client_order_id, None)

    async def _open_position(
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
        logging.info(f'Position opened! '
                     f'position_id={position.id};')

        return position

    async def _update_position(self, position: PositionModel, order: OrderModel):
        entry_side = position.get_entry_order_side()
        exit_side = position.get_exit_order_side()

        # If the order is a position entry
        if order.side is entry_side:
            # Calculate avg position entry price
            entry_orders = self.storage.get_orders(position.id, entry_side)
            entry_price = sum([order.entry_price for order in entry_orders]) / len(entry_orders)
            position.entry_price = entry_price
            position.quantity += order.quantity
            position.total_quantity += order.quantity

        else:
            # Calculate avg position exit price
            exit_orders = self.storage.get_orders(position.id, exit_side)
            exit_price = sum([order.entry_price for order in exit_orders]) / len(exit_orders)
            position.exit_price = exit_price
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

    async def _wait_for_filled(self, contract: ContractModel, order_id: OrderId) -> OrderModel:
        while True:
            await asyncio.sleep(1)
            order = await self.exchange.get_order(contract, order_id)

            if order and order.is_filled:
                return order
