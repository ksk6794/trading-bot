from typing import Union, Protocol

from modules.models import TradeUpdateModel, BookUpdateModel, DepthUpdateModel
from modules.models.types import Symbol

EntityModel = Union[TradeUpdateModel, BookUpdateModel, DepthUpdateModel]


class LineCallback(Protocol):
    def __call__(self, model: EntityModel):
        ...


class BulkLineCallback(Protocol):
    def __call__(self, symbol: Symbol, model: EntityModel):
        ...
