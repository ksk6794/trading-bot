import time
from decimal import Decimal
from typing import List, Dict, Optional

from pydantic import BaseModel, validator

from modules.models.types import (
    PositionId, OrderId, Timestamp, OrderStatus, Symbol, Asset, OrderType, OrderSide, PositionSide
)
from helpers import remove_exponent


class AccountBalanceModel(BaseModel):
    asset: Asset
    wallet_balance: Decimal

    @validator(
        'initial_margin',
        'maintenance_margin',
        'margin_balance',
        'wallet_balance',
        'unrealized_pnl',
        always=True,
        check_fields=False
    )
    def validate_decimals(cls, value):
        return remove_exponent(value)

    @classmethod
    def from_binance(cls, data: Dict):
        return cls(
            asset=data['asset'],
            wallet_balance=data['walletBalance'],
        )


class AccountPositionModel(BaseModel):
    symbol: Symbol
    side: PositionSide
    quantity: Decimal
    entry_price: Decimal
    leverage: int
    isolated: bool

    @validator('entry_price', always=True)
    def validate_entry_price(cls, value):
        return remove_exponent(value)

    @validator('quantity', always=True)
    def validate_quantity(cls, value):
        return remove_exponent(abs(value))

    @classmethod
    def from_binance(cls, data: Dict):
        return cls(
            symbol=data['symbol'],
            side=data['positionSide'],
            quantity=data['positionAmt'],
            entry_price=data['entryPrice'],
            leverage=data['leverage'],
            isolated=data['isolated'],
        )


class AccountModel(BaseModel):
    assets: Dict[Asset, AccountBalanceModel]
    positions: List[AccountPositionModel]

    @classmethod
    def from_binance(cls, data: Dict):
        return cls(
            assets={i['asset']: AccountBalanceModel.from_binance(i) for i in data['assets']},
            positions=[AccountPositionModel.from_binance(i) for i in data['positions']]
        )


class CandleModel(BaseModel):
    open: Optional[Decimal]
    high: Optional[Decimal]
    low: Optional[Decimal]
    close: Optional[Decimal]
    volume: Optional[Decimal]
    timestamp: Timestamp

    @classmethod
    def from_binance(cls, data: List):
        return cls(
            timestamp=data[0],
            open=data[1],
            high=data[2],
            low=data[3],
            close=data[4],
            volume=data[5],
        )


class DepthModel(BaseModel):
    last_update_id: int
    bids: List[List[Decimal]] = []
    asks: List[List[Decimal]] = []

    @classmethod
    def from_binance(cls, data: Dict):
        return cls(
            last_update_id=data['lastUpdateId'],
            bids=[
                [Decimal(price), Decimal(quantity)]
                for price, quantity in data['bids']
            ],
            asks=[
                [Decimal(price), Decimal(quantity)]
                for price, quantity in data['asks']
            ]
        )


class ContractModel(BaseModel):
    symbol: Symbol
    base_asset: Asset
    quote_asset: Asset
    price_decimals: int
    quantity_decimals: int
    tick_size: Decimal
    lot_size: Decimal
    min_notional: Decimal  # min available notional (price * quantity) to place order

    @validator(
        'tick_size',
        'lot_size',
        'min_notional',
        always=True
    )
    def validate_decimals(cls, value):
        return remove_exponent(value)

    @classmethod
    def from_binance(cls, data: Dict):
        n_filter = next(filter(lambda x: x['filterType'] == 'MIN_NOTIONAL', data['filters']), None)

        return cls(
            symbol=data['symbol'],
            base_asset=data['baseAsset'],
            quote_asset=data['quoteAsset'],
            price_decimals=data['pricePrecision'],
            quantity_decimals=data['quantityPrecision'],
            tick_size=1 / pow(10, data['pricePrecision']),
            lot_size=1 / pow(10, data['quantityPrecision']),
            min_notional=n_filter['notional'],
        )


class FundingRateModel(BaseModel):
    symbol: Symbol
    mark_price: Decimal
    index_price: Decimal
    last_funding_rate_pct: Decimal
    next_funding_timestamp: Timestamp
    interest_rate: Decimal
    timestamp: Timestamp

    @validator(
        'mark_price',
        'index_price',
        'last_funding_rate_pct',
        'interest_rate',
        always=True
    )
    def validate_decimals(cls, value):
        return remove_exponent(value)

    @classmethod
    def from_binance(cls, data: Dict):
        return cls(
            symbol=data['symbol'],
            mark_price=data['markPrice'],
            index_price=data['indexPrice'],
            last_funding_rate_pct=Decimal(data['lastFundingRate']) * 100,
            next_funding_timestamp=data['nextFundingTime'],
            interest_rate=data['interestRate'],
            timestamp=data['time']
        )


class OrderModel(BaseModel):
    id: OrderId
    position_id: Optional[PositionId]
    symbol: Symbol
    status: OrderStatus
    type: OrderType
    side: OrderSide
    quantity: Decimal
    entry_price: Decimal
    context: Optional[Dict]
    timestamp: Timestamp

    @property
    def is_filled(self):
        return self.status == OrderStatus.FILLED

    @classmethod
    def from_binance(cls, data):
        return cls(
            id=data['orderId'],
            symbol=data['symbol'],
            status=data['status'],
            type=data['type'],
            side=data['side'],
            quantity=data['origQty'],
            entry_price=data['avgPrice'],
            timestamp=int(time.time() * 1000)
        )
