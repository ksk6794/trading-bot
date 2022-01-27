from decimal import Decimal
from typing import Optional, List

from pydantic import BaseSettings, conint, validator

from modules.models.types import Symbol, Timeframe, Timestamp, StreamEntity


class Settings(BaseSettings):
    broker_amqp_uri: str
    mongo_uri: str

    binance_testnet: bool = False
    binance_public_key: Optional[str]
    binance_private_key: Optional[str]

    symbol: Symbol
    strategy: str
    timeframe: Timeframe = '1h'
    entities: List[StreamEntity] = ['trade', 'book', 'depth']
    candles_limit: int = 100
    depth_limit: int = 100
    leverage: conint(ge=1, le=25) = 1

    signal_check_interval: Optional[int] = 10  # in seconds, 0 - no limits
    trailing_callback_rate: Optional[Decimal]

    replay: bool = False
    replay_speed: conint(ge=0, le=100) = 0
    replay_from: Optional[Timestamp]
    replay_to: Optional[Timestamp]

    @validator('depth_limit', always=True)
    def validate_depth_limit(cls, value, values) -> int:
        if StreamEntity.DEPTH not in values['entities']:
            value = 0
        return value
