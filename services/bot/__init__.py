import asyncio
from decimal import Decimal
from signal import SIGINT, SIGTERM

import uvloop

from logger import setup_logging

from modules.models.strategy import StrategyRules
from modules.models.types import PositionSide, OrderSide

from services.bot.candles import Candles
from services.bot.orchestrator import StrategiesOrchestrator
from services.bot.settings import Settings


async def start(orchestrator: StrategiesOrchestrator):
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


def main():
    uvloop.install()
    setup_logging()
    loop = asyncio.get_event_loop()
    settings = Settings()
    orchestrator = StrategiesOrchestrator(settings)

    for signal in (SIGINT, SIGTERM):
        loop.add_signal_handler(signal, lambda: loop.create_task(orchestrator.stop()))

    loop.run_until_complete(start(orchestrator))
    loop.run_forever()
