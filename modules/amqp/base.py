import logging

from aio_pika.exceptions import DuplicateConsumerTag
from aio_pika.robust_connection import RobustConnection as BaseRobustConnection
from aio_pika.robust_channel import RobustChannel as BaseRobustChannel


class RobustChannel(BaseRobustChannel):
    async def reopen(self) -> None:
        try:
            await super().reopen()

        except DuplicateConsumerTag as err:
            logging.warning(repr(err))


class RobustConnection(BaseRobustConnection):
    CHANNEL_CLASS = RobustChannel
