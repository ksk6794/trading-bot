import asyncio
from signal import SIGINT, SIGTERM

import uvloop

from logger import setup_logging

from .publisher import LinePublisher
from .server import LineServer
from .settings import Settings


def main():
    uvloop.install()
    setup_logging()
    loop = asyncio.get_event_loop()
    settings = Settings()
    server = LineServer(settings)

    for signal in (SIGINT, SIGTERM):
        loop.add_signal_handler(signal, lambda: loop.create_task(server.stop()))

    loop.run_until_complete(server.start())
    loop.run_forever()
