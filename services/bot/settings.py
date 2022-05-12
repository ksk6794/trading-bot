from decimal import Decimal
from typing import Optional, List, Any

from pydantic import BaseSettings, conint
from pydantic.main import BaseModel
from pydantic.types import condecimal

from modules.models import StopLossConfig, TakeProfitConfig
from modules.models.types import Symbol, PositionSide, OrderSide, Timeframe, Indicator, Condition, StrategyId, Timestamp


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


class StrategyRules(BaseModel):
    class StrategyCondition(BaseModel):
        class IndicatorParameter(BaseModel):
            field: str
            value: Any

        class IndicatorCondition(BaseModel):
            field: str
            condition: Condition
            value: Decimal

        position_side: PositionSide
        order_side: OrderSide
        timeframe: Timeframe
        indicator: Indicator
        parameters: List[IndicatorParameter]
        conditions: List[IndicatorCondition]
        save_signal_candles: conint(ge=1, le=10) = 1

    id: StrategyId
    name: str

    binance_testnet: bool = False
    binance_public_key: Optional[str]
    binance_private_key: Optional[str]

    trailing: bool = False
    balance_stake: condecimal(gt=Decimal('0'), le=Decimal('1'))
    leverage: conint(ge=1, le=25) = 1
    trailing_callback_rate: Optional[Decimal]

    symbols: List[Symbol]
    conditions: List[StrategyCondition]
    conditions_trigger_count: int

    stop_loss: Optional[StopLossConfig] = None
    take_profit: Optional[TakeProfitConfig] = None
