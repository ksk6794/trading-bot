from typing import Optional, List

from pydantic import BaseSettings, conint

from modules.models.types import Symbol, Timestamp


class Settings(BaseSettings):
    broker_amqp_uri: str
    mongo_uri: str

    candles_limit: int = 100
    binance_testnet: bool = False
    symbols: List[Symbol]

    replay: bool = False
    replay_speed: conint(ge=0, le=100) = 0
    replay_from: Optional[Timestamp]
    replay_to: Optional[Timestamp]
