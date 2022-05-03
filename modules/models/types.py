from typing import NewType, Literal
from enum import unique, Enum, auto


class BaseEnum(Enum):
    @classmethod
    def keys(cls):
        return cls.__members__.keys()

    @classmethod
    def values(cls):
        return cls.__members__.values()


class AutoName(str, BaseEnum):
    """https://docs.python.org/3.8/library/enum.html#using-automatic-values"""

    # noinspection PyMethodParameters
    def _generate_next_value_(name, start, count, last_values):
        return name.lower()

    @classmethod
    def has_value(cls, value: str):
        return value in cls._value2member_map_


class AutoNameUp(str, BaseEnum):
    """https://docs.python.org/3.8/library/enum.html#using-automatic-values"""

    # noinspection PyMethodParameters
    def _generate_next_value_(name, start, count, last_values):
        return name.upper()

    @classmethod
    def has_value(cls, value: str):
        return value in cls._value2member_map_


PositionId = NewType('PositionId', str)
OrderId = NewType('OrderId', int)
ClientOrderId = NewType('ClientOrderId', str)
Timestamp = NewType('Timestamp', int)
Symbol = NewType('Symbol', str)
Asset = NewType('Asset', str)
Timeframe = Literal['1m', '5m', '15m', '30m', '1h', '4h', '6h', '1d']


@unique
class StreamEntity(AutoName):
    BOOK = auto()
    TRADE = auto()
    DEPTH = auto()


@unique
class UserStreamEntity(AutoNameUp):
    ACCOUNT_UPDATE = auto()
    ACCOUNT_CONFIG_UPDATE = auto()
    ORDER_TRADE_UPDATE = auto()


@unique
class TickType(BaseEnum):
    NEW_CANDLE = 1
    SAME_CANDLE = 2
    MISSING_CANDLE = 3


@unique
class MarginType(BaseEnum):
    ISOLATED = auto()
    CROSSED = auto()


@unique
class OrderStatus(AutoNameUp):
    NEW = auto()
    PARTIALLY_FILLED = auto()
    FILLED = auto()
    CANCELED = auto()
    REJECTED = auto()
    EXPIRED = auto()


@unique
class OrderType(AutoNameUp):
    LIMIT = auto()
    MARKET = auto()
    STOP = auto()
    STOP_MARKET = auto()
    TAKE_PROFIT = auto()
    TAKE_PROFIT_MARKET = auto()
    TRAILING_STOP_MARKET = auto()
    LIQUIDATION = auto()


@unique
class OrderSide(AutoNameUp):
    BUY = auto()
    SELL = auto()


class PositionSide(AutoNameUp):
    BOTH = auto()
    LONG = auto()
    SHORT = auto()


@unique
class PositionStatus(AutoNameUp):
    OPEN = auto()
    CLOSED = auto()


@unique
class TimeInForce(AutoNameUp):
    GTC = auto()  # Good Till Cancel
    OC = auto()   # Immediate or Cancel
    FOK = auto()  # Fill or Kill
    GTX = auto()  # Good Till Crossing (Post Only)
