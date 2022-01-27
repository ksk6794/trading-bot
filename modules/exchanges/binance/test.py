import asyncio
from decimal import Decimal

from modules.exchanges.binance.client import BinanceClient
from modules.models import BookUpdateModel
from modules.models.types import Symbol, OrderType, OrderSide


async def main(client: BinanceClient):
    contracts = await client.get_contracts()
    contract = contracts[Symbol('BTCUSDT')]

    # await client.change_leverage(contract, 5)
    acc = await client.get_account_info()

    await client.change_position_mode(hedge_mode=True)
    print(acc)

    # res = await client.place_order(
    #     contract=contract,
    #     order_type=OrderType.MARKET,
    #     quantity=Decimal('0.001'),
    #     order_side=OrderSide.BUY,
    # )
    # print(res)

    # res = await client.get_order(contract, 2859466804)
    # print(res)


if __name__ == '__main__':
    client = BinanceClient(
        public_key='082ee3aa4fce336c05145402b36ca2c6f7c3c442d75432b851673076299772e3',
        private_key='df3c5b194cd3d8f88e8b7fa88ecb2190075286c46e583b556789901b9e964f3f',
        testnet=True,
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(client))
