import asyncio
from signal import SIGINT, SIGTERM

import uvloop

from logger import setup_logging

from .service import LineLogger
from .settings import Settings


def main():
    uvloop.install()
    setup_logging()
    loop = asyncio.get_event_loop()
    settings = Settings()
    line_logger = LineLogger(settings)

    for signal in (SIGINT, SIGTERM):
        loop.add_signal_handler(signal, lambda: loop.create_task(line_logger.stop()))

    loop.run_until_complete(line_logger.start())
    loop.run_forever()
