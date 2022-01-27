from typing import List

from pydantic import BaseSettings

from modules.models.types import Symbol


class Settings(BaseSettings):
    broker_amqp_uri: str
    mongo_uri: str

    symbols: List[Symbol]
    binance_testnet: bool = False
