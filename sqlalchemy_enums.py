import enum

class OrderTypeEnum(str, enum.Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    TRAILING_STOP_MARKET = "TRAILING_STOP_MARKET"
    TRAILING_TP_SL = "TRAILING_TP_SL"

class PositionSideEnum(str, enum.Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class SideEnum(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"

class MarketEnum(str, enum.Enum):
    BINANCE = "BINANCE"      # ← новый источник котировок
    BINGX   = "BINGX"        # ордер-менеджмент остаётся здесь

class NoteEnum(str, enum.Enum):
    TAKE_PROFIT = "TAKE_PROFIT"
    AVERAGING = "AVERAGING"
    BUY = "BUY"

class ReasonEnum(str, enum.Enum):
    RED = "RED"
    YELLOW = "YELLOW"
