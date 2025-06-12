from pydantic import Field, PositiveInt, PositiveFloat, NonNegativeInt
from typing import List

from pydantic import BaseModel

import sqlalchemy_enums



class SettingsModel(BaseModel):
    symbol: str = "ADA-USDT"

    take_profit_percent: PositiveFloat = 1.0
    initial_entry_percent: PositiveFloat = 100.0
    averaging_levels: PositiveInt = 3
    averaging_percents: str = "6,12,35"
    averaging_volume_percents: str = "20,30,40"
    timeframe: str = "15m"
    use_delay_filter: bool = False
    use_delta_filter: bool = False
    use_rsi: bool = True
    use_cci: bool = False
    rsi_length: PositiveInt = 14
    cci_length: PositiveInt = 20

    rsi_threshold: float = 30.0
    cci_threshold: float = -100.0


    use_short: bool = False
    activation_price: PositiveFloat = 0.6
    trailing_stop_percentage: PositiveFloat = 0.6
    base_stop: PositiveFloat = 0.6

    sma_fast: PositiveInt = 30
    sma_slow: PositiveInt = 200
    leverage: PositiveInt = 5

    delay_bars: PositiveInt = 2  # сколько свечей надо ждать
    delta_filter: float = 0.0
    use_balance_percent: float = 20.0
    #accept_signals: List[sqlalchemy_enums.ReasonEnum] = Field(default_factory= lambda: list)

    active: bool = True

class StageConfigModel(BaseModel):
    stage: PositiveInt = 1
    rsi_slots: NonNegativeInt = 0
    cci_slots: NonNegativeInt = 0