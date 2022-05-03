import time
from decimal import Decimal
from typing import List, Dict, Optional

from pydantic import BaseModel, validator

from helpers import remove_exponent
from modules.models.types import (
    PositionId, OrderId, ClientOrderId, Timestamp, Symbol, Asset,
    OrderStatus, OrderType, OrderSide, PositionSide
)


class AccountBalanceModel(BaseModel):
    asset: Asset
    wallet_balance: Decimal

    @validator('wallet_balance', always=True, check_fields=False)
    def validate_decimals(cls, value):
        return remove_exponent(value)

    @classmethod
    def from_binance(cls, data: Dict):
        return cls(
            asset=data['asset'],
            wallet_balance=data['crossWalletBalance'],
        )

    @classmethod
    def from_user_stream(cls, data: Dict):
        return cls(
            asset=data['a'],
            wallet_balance=data['cw']
        )


class AccountPositionModel(BaseModel):
    symbol: Symbol
    side: PositionSide
    quantity: Decimal
    entry_price: Decimal
    isolated: bool
    margin: Decimal

    @validator('entry_price', always=True)
    def validate_entry_price(cls, value):
        return remove_exponent(value)

    @validator('quantity', always=True)
    def validate_quantity(cls, value):
        return remove_exponent(abs(value))

    @validator('margin', always=True)
    def validate_margin(cls, value):
        return remove_exponent(abs(value))

    @classmethod
    def from_binance(cls, data: Dict):
        return cls(
            symbol=data['symbol'],
            side=data['positionSide'],
            quantity=data['positionAmt'],
            entry_price=data['entryPrice'],
            isolated=data['isolated'],
            margin=data['isolatedWallet'],
        )

    @classmethod
    def from_user_stream(cls, data: Dict):
        return cls(
            symbol=data['s'],
            side=data['ps'],
            quantity=data['pa'],
            entry_price=data['ep'],
            isolated=data['mt'] == 'isolated',
            margin=data['iw'],
        )

    def calc_pnl(self, price, quantity: Optional[Decimal] = None) -> Decimal:
        pnl = Decimal('0')

        if quantity > self.quantity:
            raise RuntimeError('Invalid quantity!')

        quantity = quantity or self.quantity

        if self.side is PositionSide.LONG:
            pnl = (price.bid - self.entry_price) * quantity

        elif self.side == PositionSide.SHORT:
            pnl = (self.entry_price - price.ask) * quantity

        return pnl


class AccountModel(BaseModel):
    assets: Dict[Asset, AccountBalanceModel]
    positions: List[AccountPositionModel]

    @classmethod
    def from_binance(cls, data: Dict):
        return cls(
            assets={i['asset']: AccountBalanceModel.from_binance(i) for i in data['assets']},
            positions=[AccountPositionModel.from_binance(i) for i in data['positions']]
        )

    @classmethod
    def from_user_stream(cls, data: Dict):
        return cls(
            assets={i['a']: AccountBalanceModel.from_user_stream(i) for i in data['a']['B']},
            positions=[AccountPositionModel.from_user_stream(i) for i in data['a']['P']]
        )


class AccountConfigModel(BaseModel):
    symbol: Symbol
    leverage: int

    @classmethod
    def from_user_stream(cls, data: Dict):
        return cls(
            symbol=data['ac']['s'],
            leverage=data['ac']['l'],
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
    client_order_id: ClientOrderId
    position_id: Optional[PositionId]
    symbol: Symbol
    status: OrderStatus
    type: OrderType
    side: OrderSide
    position_side: PositionSide
    quantity: Decimal
    entry_price: Decimal
    context: Optional[Dict]
    timestamp: Timestamp

    @property
    def is_filled(self):
        return self.status == OrderStatus.FILLED

    @property
    def is_processed(self):
        return self.status in (
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
            OrderStatus.CANCELED
        )

    @classmethod
    def from_binance(cls, data):
        return cls(
            id=data['orderId'],
            client_order_id=data['clientOrderId'],
            symbol=data['symbol'],
            status=data['status'],
            type=data['type'],
            side=data['side'],
            position_side=data['positionSide'],
            quantity=data['origQty'],
            entry_price=data['avgPrice'],
            timestamp=int(time.time() * 1000)
        )

    @classmethod
    def from_user_stream(cls, data):
        order_data = data['o']
        return cls(
            id=order_data['i'],
            client_order_id=order_data['c'],
            symbol=order_data['s'],
            status=order_data['X'],
            type=order_data['ot'],
            side=order_data['S'],
            position_side=order_data['ps'],
            quantity=order_data['q'],
            entry_price=order_data['ap'],
            timestamp=data['T']
        )
