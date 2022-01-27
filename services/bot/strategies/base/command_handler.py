import asyncio
import logging
from uuid import uuid4
from decimal import Decimal
from time import time
from typing import Any, Optional, List, Callable, Dict, Set

from modules.mongo import MongoClient
from modules.exchanges import BinanceClient
from modules.models import PositionModel, OrderModel, BookUpdateModel, ContractModel
from modules.models.types import PositionId, OrderSide, OrderType, PositionStatus, PositionSide, OrderId, CommandAction
from modules.models.commands import Command, TrailingStop, PlaceOrder

from services.bot.strategies.base.utils import method_dispatch
from services.bot.strategies.base.storage import LocalStorage


class CommandHandler:
    def __init__(self, db: MongoClient, exchange: BinanceClient, storage: LocalStorage, strategy: str):
        self.db = db
        self.exchange = exchange
        self.storage = storage
        self.strategy = strategy
        self.price: Optional[BookUpdateModel] = None

        self._commands: List[Command] = []
        self._pending = False
        self._callbacks: Dict[str, Set[Callable]] = {}
        self._loop = asyncio.get_event_loop()

    def __len__(self):
        return len(self._commands)

    @property
    def is_pending(self) -> bool:
        return self._pending

    @property
    def has_outgoing_commands(self) -> bool:
        return bool(self._commands)

    def add_callback(self, action: CommandAction, cb: Callable):
        assert callable(cb)
        self._callbacks.setdefault(action, set()).add(cb)

    def append(self, command: Command):
        self._commands.append(command)

    def set_price(self, price: BookUpdateModel):
        self.price = price

    def execute(self):
        if not self._pending and self._commands:
            self._pending = True
            self._loop.call_soon(lambda: self._loop.create_task(self._execute_commands()))

    async def _execute_commands(self):
        next_commands = []

        for command in self._commands:
            while command:
                command = await self.handle(command)

                if command and command.next_time:
                    next_commands.append(command)
                    break

        self._commands = next_commands
        self._pending = False

    @method_dispatch  # pragma: no cover
    async def handle(self, command: Command, **kwargs):
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
    async def handle_place_order(self, command: PlaceOrder) -> Optional[Command]:
        position = self.storage.get_position(command.position_side)

        if position:
            exit_side = position.get_exit_order_side()

            if command.order_side is exit_side and command.quantity > position.quantity:
                logging.error(f'The quantity exceeds the allowable! '
                              f'position_id={position.id};')
                return

        else:
            # Open new position
            position = await self._open_position(command.contract, command.position_side)

        price = self.price.bid if command.order_side is OrderSide.BUY else self.price.ask

        if command.quantity * price < command.contract.min_notional:
            logging.error(f'The quantity is too small! '
                          f'position_id={position.id}; '
                          f'quantity={command.quantity}; '
                          f'price={price}; '
                          f'min_notional={command.contract.min_notional};')
            return

        order = await self.exchange.place_order(
            contract=command.contract,
            order_type=OrderType.MARKET,
            quantity=command.quantity,
            position_side=command.position_side,
            order_side=command.order_side
        )

        if not order:
            logging.error('Could not place order! '
                          f'position_id={position.id};')
            position.status = PositionStatus.CLOSED
            await self.db.update(position, query={'id': position.id})
            self.storage.drop_position(position.side)
            return

        if not order.is_filled:
            order = await self._wait_for_filled(command.contract, order.id)

        # Create order
        order.position_id = position.id
        order.context = command.context

        await self.db.create(order)
        self.storage.add_order(order)
        await self._trigger_callbacks(CommandAction.ORDER_FILLED)
        logging.info(f'Order placed! '
                     f'position_id={position.id}; '
                     f'side={order.side}; '
                     f'quantity={order.quantity}; '
                     f'price={order.entry_price};')

        await self._update_position(position, order)

    async def _open_position(
            self,
            contract: ContractModel,
            position_side: PositionSide,
    ) -> PositionModel:
        # Create position
        position = PositionModel(
            id=PositionId(uuid4().hex),
            symbol=contract.symbol,
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
                         f'exit_price={position.exit_price}; ')

    async def _wait_for_filled(self, contract: ContractModel, order_id: OrderId) -> OrderModel:
        while True:
            await asyncio.sleep(1)
            order = await self.exchange.get_order(contract, order_id)

            if order and order.is_filled:
                return order

    async def _trigger_callbacks(self, action: Any, *args, **kwargs):
        callbacks = self._callbacks.get(action, set())

        for cb in callbacks:
            self._loop.create_task(cb(*args, **kwargs))
