from typing import List

from pydantic import BaseSettings

from modules.models.types import Symbol, StreamEntity


class Settings(BaseSettings):
    broker_amqp_uri: str
    mongo_uri: str

    symbols: List[Symbol]
    bulk_interval: int = 10
    entities: List[StreamEntity]
