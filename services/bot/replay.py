import asyncio
from decimal import Decimal
from typing import Dict, List

from logger import setup_logging
from modules.exchanges.fake import FakeExchangeClient
from modules.mongo import MongoClient
from modules.line_client import ReplayClient

from modules.models import TradeUpdateModel, BookUpdateModel
from modules.models.types import StreamEntity, Symbol, TickType, PositionSide, OrderSide
from modules.models.indexes import INDEXES

from services.bot.state import ExchangeState
from services.bot.settings import Settings, StrategyRules
from services.bot.strategies import Strategy


class StrategiesOrchestrator:
    def __init__(self, settings: Settings):
        self.settings = settings

        self.db = MongoClient(
            mongo_uri=settings.mongo_uri,
            indexes=INDEXES,
        )
        self.line = ReplayClient(
            db=self.db,
            symbols=settings.symbols,
            replay_speed=settings.replay_speed,
            replay_from=settings.replay_from,
            replay_to=settings.replay_to,
        )
        self.line.add_done_callback(self._replay_summary)
        self.exchange = FakeExchangeClient()

        self.state = ExchangeState(
            exchange=self.exchange,
            symbols=self.settings.symbols,
            candles_limit=self.settings.candles_limit
        )

        self._strategies: Dict[Symbol: List[Strategy]] = []
        self._loop = asyncio.get_event_loop()
        self._event = asyncio.Event()

        self.line.add_update_callback(StreamEntity.BOOK, self._on_book_update)
        self.line.add_update_callback(StreamEntity.TRADE, self._on_trade_update)

    async def start(self):
        await self.db.connect()
        await self.state.preload()
        await self.line.connect()
        self._event.set()

    async def stop(self):
        await self.line.disconnect()

    async def run_strategy(self, rules: StrategyRules):
        strategy = Strategy(rules, self.db, self.state, replay=True)
        self._strategies.append(strategy)

        if not self._event.is_set():
            await self._event.wait()

        await strategy.start()

    async def _on_book_update(self, symbol: Symbol, model: BookUpdateModel):
        self.state.update_book(symbol, model)

        for strategy in self._strategies:
            await strategy.on_book_update(symbol)

    async def _on_trade_update(self, symbol: Symbol, model: TradeUpdateModel):
        tick_types = self.state.update_candles(symbol, model)

        if any([tick_type is TickType.NEW_CANDLE for tick_type in tick_types.values()]):
            for strategy in self._strategies:
                await strategy.on_candles_update(symbol)

    async def _replay_summary(self):
        # TODO: Implement summary!
        await self.stop()


async def main(orchestrator: StrategiesOrchestrator):
    await orchestrator.start()

    data = {
        'id': '7179559cb2724ff9b86f9cada8387748',
        'name': 'scalp',

        'binance_testnet': True,
        'binance_public_key': '082ee3aa4fce336c05145402b36ca2c6f7c3c442d75432b851673076299772e3',
        'binance_private_key': 'df3c5b194cd3d8f88e8b7fa88ecb2190075286c46e583b556789901b9e964f3f',

        'trailing': True,
        'balance_stake': Decimal('0.1'),
        'symbols': ['BTCUSDT', 'ETHUSDT'],
        'conditions': [
            {
                'position_side': PositionSide.LONG,
                'order_side': OrderSide.BUY,
                'timeframe': '5m',
                'indicator': 'rsi',
                'parameters': [
                    {'field': 'period', 'value': 14},
                ],
                'conditions': [
                    {'field': 'rsi', 'condition': 'lte', 'value': 35}
                ],
                'save_signal_candles': 2
            },
            {
                'position_side': PositionSide.LONG,
                'order_side': OrderSide.BUY,
                'timeframe': '5m',
                'indicator': 'stochastic',
                'parameters': [
                    {'field': 'k_period', 'value': 14},
                    {'field': 'd_period', 'value': 3},
                ],
                'conditions': [
                    {'field': '%K', 'condition': 'lte', 'value': 40},
                    {'field': '%D', 'condition': 'lte', 'value': 40},
                ],
                'save_signal_candles': 2
            },
        ],
        'conditions_trigger_count': 2,
        'stop_loss': {
            'rate': Decimal('0.025')
        },
        'take_profit': {
            'steps': [
                {'level': Decimal('0.005'), 'stake': Decimal('0.2')},
                {'level': Decimal('0.010'), 'stake': Decimal('0.4')},
                {'level': Decimal('0.015'), 'stake': Decimal('0.4')},
            ]
        }
    }

    strategy = StrategyRules.parse_obj(data)
    await orchestrator.run_strategy(strategy)


if __name__ == '__main__':
    setup_logging()
    loop = asyncio.get_event_loop()
    settings = Settings()
    orchestrator = StrategiesOrchestrator(settings)
    loop.create_task(main(orchestrator))
    loop.run_forever()
