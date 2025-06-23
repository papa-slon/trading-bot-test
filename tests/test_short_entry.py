import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from core.trading_logic import TradingBot
from market_api.core import MarketClient
from sqlalchemy_enums import ReasonEnum


class DummyClient(MarketClient):
    def __init__(self):
        super().__init__("k", "s")
        self.orders = []
        self.batch_orders = []
        self.leverage = None

    async def get_balance(self):
        return 1000.0

    async def get_positions(self, symbol: str | None = None):
        return []

    async def get_open_orders(self, symbol: str):
        return []

    async def get_ticker_price(self, symbol: str) -> float:
        return 10.0

    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        *,
        pos_side: str = "LONG",
        quantity: float = 0.0,
        price: float = 0.0,
        time_in_force: str = "GTC",
        extra: dict | None = None,
    ) -> int:
        self.orders.append({
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "pos_side": pos_side,
            "quantity": quantity,
            "price": price,
            "extra": extra,
        })
        return len(self.orders)

    async def cancel_order(self, symbol: str, order_id: int) -> bool:
        return True

    async def cancel_all_orders(self, symbol: str) -> bool:
        return True

    async def close_position(self, symbol: str, pos_side: str = "LONG") -> bool:
        return True

    async def close_all_positions(self) -> bool:
        return True

    async def get_klines(self, symbol: str, interval: str = "1m", start: int | None = None, limit: int = 14):
        return []

    async def place_batch_orders(self, orders: list[dict]):
        self.batch_orders.extend(orders)
        return [i for i, _ in enumerate(orders, 100)]

    async def set_leverage(self, symbol, leverage):
        self.leverage = leverage

    async def normalize_qty(self, symbol, qty, price):
        return qty

    async def set_credentials(self, api_key, secret_key):
        pass

    async def get_all_prices(self, symbol_list):
        return {s: 10.0 for s in symbol_list}


class ShortEntryTest(unittest.IsolatedAsyncioTestCase):
    async def test_open_short_position_places_trailing_stop(self):
        client = DummyClient()
        dummy_feed = SimpleNamespace(subscribe=lambda s, cb: None)

        with patch("services.price_feed.PriceFeed.instance", return_value=dummy_feed), \
             patch("core.trading_logic.PriceCache.get", AsyncMock(return_value=10.0)), \
             patch("core.trading_logic.PriceCache.update", AsyncMock()), \
             patch("core.trading_logic.get_active_slot", AsyncMock(return_value=None)), \
             patch("core.trading_logic.can_open_extra", AsyncMock(return_value=(True, 1))), \
             patch("core.trading_logic.add_slot", AsyncMock()), \
             patch("core.trading_logic.add_order", AsyncMock()), \
             patch("core.trading_logic.close_slot", AsyncMock()), \
             patch.object(TradingBot, "_report_status", AsyncMock()):

            bot = TradingBot(
                client,
                ["AVAX-USDT"],
                1,
                100,
                1,
                [6],
                [20],
                use_balance_percent=20,
                leverage=5,
                trailing_stop_percent=0.6,
                activation=0.6,
                base_stop=0.6,
            )
            bot.api_instance = SimpleNamespace(pub_id="api123")

            await bot.open_position("AVAX-USDT", ReasonEnum.YELLOW)

            self.assertEqual(len(client.orders), 1)
            order = client.orders[0]
            self.assertEqual(order["side"], "SELL")
            self.assertEqual(order["pos_side"], "SHORT")
            self.assertEqual(order["order_type"], "TRAILING_TP_SL")
            self.assertIsInstance(order["extra"].get("stopLoss"), dict)
            self.assertEqual(order["extra"]["stopLoss"]["type"], "STOP_MARKET")
            self.assertGreater(order["extra"]["stopLoss"]["stopPrice"], 10.0)
            self.assertTrue(all(o["side"] == "SELL" for o in client.batch_orders))
            self.assertTrue(all(o["positionSide"] == "SHORT" for o in client.batch_orders))


if __name__ == "__main__":
    unittest.main()
