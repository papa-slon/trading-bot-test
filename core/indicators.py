# core/indicators.py

import pandas as pd
import ta

def calculate_rsi(dataframe: pd.DataFrame, period: int = 14) -> float:
    """
    Считаем RSI на базе close-цен. Возвращаем последнее значение (float) или None при недостатке строк.
    """
    if len(dataframe) < period:
        return None
    rsi_series = ta.momentum.RSIIndicator(dataframe['close'], window=period).rsi()
    return rsi_series.iloc[-1]

def calculate_cci(dataframe: pd.DataFrame, period: int = 20) -> float:
    """
    Считаем CCI на базе high, low, close. Возвращаем последнее значение или None при недостатке строк.
    """
    if len(dataframe) < period:
        return None
    cci_series = ta.trend.CCIIndicator(dataframe['high'], dataframe['low'], dataframe['close'], window=period).cci()
    return cci_series.iloc[-1]

def convert_klines_to_df(kline_data):
    """
    Ожидаем kline_data: [
      [timestamp, open, high, low, close, volume],
      [timestamp, open, high, low, close, volume], ...
    ]
    Если структура не соответствует, возвращаем пустой DataFrame.
    """
    # Если нет данных или не список -> пустой DF
    if not kline_data or not isinstance(kline_data, list):
        return pd.DataFrame()

    # Если это одна "свеча" вида [ts, open, high, low, close, vol] => обернём
    if len(kline_data) == 6 and all(isinstance(x, (int,float,str)) for x in kline_data):
        kline_data = [kline_data]

    # Проверим первый элемент
    first_elem = kline_data[0]
    if not isinstance(first_elem, list) or len(first_elem) < 6:
        # Значит структура не та
        return pd.DataFrame()

    # Создаем DF
    df = pd.DataFrame(kline_data, columns=['timestamp','open','high','low','close','volume'])

    # Приводим нужные столбцы к float
    df[['open','high','low','close','volume']] = df[['open','high','low','close','volume']].astype(float)

    return df
