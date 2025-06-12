"""
utils/symbols.py
Хелперы для преобразования имён символов разных бирж.
"""

def to_binance(sym: str) -> str:
    """
    'BNB-USDT' → 'BNBUSDT'
    """
    return sym.replace("-", "").upper()
