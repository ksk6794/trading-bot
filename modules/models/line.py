from decimal import Decimal
from typing import Dict, Union, List

from pydantic import BaseModel, Field

from modules.models.types import Symbol, Timestamp, StreamEntity, OrderSide
from helpers import remove_exponent


class TradeUpdateModel(BaseModel):
    class Config:
        allow_population_by_field_name = True

    price: Decimal = Field(..., alias='p')
    quantity: Decimal = Field(..., alias='q')
    timestamp: Timestamp = Field(..., alias='t')
    is_buyer_maker: bool = Field(..., alias='m')

    def encode(self):
        return {
            'p': str(remove_exponent(self.price)),
            'q': str(remove_exponent(self.quantity)),
            't': self.timestamp,
            'm': self.is_buyer_maker
        }

    @classmethod
    def decode(cls, data: Dict):
        return cls(**data)


class BookUpdateModel(BaseModel):
    class Config:
        allow_population_by_field_name = True

    bid: Decimal = Field(..., alias='b')
    ask: Decimal = Field(..., alias='a')

    def get_price(self, order_side: OrderSide) -> Decimal:
        book = {
            OrderSide.BUY: self.bid,
            OrderSide.SELL: self.ask,
        }
        return book[order_side]

    def encode(self):
        return {
            'b': str(remove_exponent(self.bid)),
            'a': str(remove_exponent(self.ask)),
        }

    @classmethod
    def decode(cls, data: Dict):
        return cls(**data)


class DepthUpdateModel(BaseModel):
    class Config:
        allow_population_by_field_name = True

    symbol: Symbol = Field(..., alias='s')
    first_update_id: int = Field(..., alias='U')
    last_update_id: int = Field(..., alias='u')
    bids: List[List[Decimal]] = Field(..., alias='b')
    asks: List[List[Decimal]] = Field(..., alias='a')
    timestamp: Timestamp = Field(..., alias='t')

    def encode(self):
        return {
            's': self.symbol,
            'U': self.first_update_id,
            'u': self.last_update_id,
            'b': [[str(remove_exponent(i)) for i in b] for b in self.bids],
            'a': [[str(remove_exponent(i)) for i in a] for a in self.asks],
            't': self.timestamp,
        }

    @classmethod
    def decode(cls, data: Dict):
        return cls(**data)


class UpdateLogModel(BaseModel):
    class Config:
        allow_population_by_field_name = True

    symbol: Symbol = Field(..., alias='s')
    entity: StreamEntity = Field(..., alias='e')
    timestamp: Timestamp = Field(..., alias='t')
    data: Dict = Field(..., alias='d')

    def get_entity_model(self) -> Union[TradeUpdateModel, BookUpdateModel, DepthUpdateModel]:
        model_class = None

        if self.entity is StreamEntity.TRADE:
            model_class = TradeUpdateModel

        elif self.entity is StreamEntity.BOOK:
            model_class = BookUpdateModel

        elif self.entity is StreamEntity.DEPTH:
            model_class = DepthUpdateModel

        if model_class:
            return model_class(**self.data)
