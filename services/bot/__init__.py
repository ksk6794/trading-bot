import asyncio
from signal import SIGINT, SIGTERM

import uvloop

from logger import setup_logging

from services.bot.candles import Candles
from services.bot.strategies import get_strategy
from services.bot.settings import Settings


def main():
    uvloop.install()
    setup_logging()
    loop = asyncio.get_event_loop()
    settings = Settings()
    strategy_class = get_strategy(settings.strategy)
    strategy = strategy_class(settings)

    for signal in (SIGINT, SIGTERM):
        loop.add_signal_handler(signal, lambda: loop.create_task(strategy.stop()))

    loop.run_until_complete(strategy.start())
    loop.run_forever()
