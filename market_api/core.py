import os
from abc import ABCMeta, abstractmethod
from asyncio import run
from typing import List, Union, Any, Dict

from pydantic import BaseModel, Field, AliasChoices
#from gui.app_gui import cls_dict
#from models import get_apis
from sqlalchemy_enums import MarketEnum


class PositionModel(BaseModel):
    symbol: str = Field()
    amount: float = Field(validation_alias= AliasChoices("availableAmt"))
    avg_price: float = Field(validation_alias= AliasChoices("avgPrice"))

class OrderModel(BaseModel):
    order_id: str | int = Field(validation_alias= AliasChoices("orderId"))
    symbol: str = Field()


class TickerKline(BaseModel):
    open_price: float = Field(validation_alias= AliasChoices("open"))
    close_price: float = Field(validation_alias= AliasChoices("close"))
    high_price: float = Field(validation_alias= AliasChoices("high"))
    low_price: float = Field(validation_alias= AliasChoices("low"))
    volume: float = Field(validation_alias= AliasChoices("volume"))
    time: int = Field(validation_alias= AliasChoices("time"))


class MarketClient(metaclass=ABCMeta):

    def __init__(self, api_key, secret_key, market_type: MarketEnum = MarketEnum.BINGX):
        self.api_key = api_key
        self.secret_key = secret_key
        self.market_type = market_type

    @abstractmethod
    async def get_balance(self) -> float:
        """Get user balance(and margin)"""

    @abstractmethod
    async def get_positions(self, symbol: str = None) -> PositionModel:
        """Get user positions"""

    @abstractmethod
    async def get_open_orders(self, symbol: str) -> List[OrderModel]:
        """Get user orders"""

    @abstractmethod
    async def get_ticker_price(self, symbol: str) -> float:
        """Get user ticker price"""

    @abstractmethod
    async def place_order(
            self,
            symbol: str,
            side: str,  # "BUY" / "SELL"
            order_type: str,  # "MARKET" / "LIMIT"
            pos_side: str = "LONG",
            quantity: float = 0,
            price: float = 0,
            time_in_force: str = "GTC",
            extra: Dict[str, Any] = None
    ) -> int:
        """Place order"""

    @abstractmethod
    async def cancel_order(self, symbol: str, order_id: Union[str, int]) -> str:
        """Cancel order"""

    @abstractmethod
    async def cancel_all_orders(self, symbol: str) -> bool:
        """Cancel all orders"""

    @abstractmethod
    async def close_position(self, symbol: str, pos_side: str = "LONG") -> bool:
        """Close position"""

    @abstractmethod
    async def close_all_positions(self) -> bool:
        """Close all positions"""

    @abstractmethod
    async def get_klines(self, symbol: str, interval: str = "1m", start: int = None, limit: int = 14) -> List[TickerKline]:
        """Get klines"""

    async def set_leverage(self, symbol, leverage):
        pass

    async def normalize_qty(self, symbol, raw_qty_entry, cur_price):
        pass

    async def place_batch_orders(self, batch_orders):
        pass

    async def get_all_prices(self, symbol_list):
        pass

    async def set_credentials(self, api_key, secret_key):
        pass

