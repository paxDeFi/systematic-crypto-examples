import pandas as pd
import requests

BASE_URL = "https://api.bybit.com/v5/market/kline"


def load_ohlcv(symbol: str, timeframe: str, limit: int = 5000) -> pd.DataFrame:
    """
    Load OHLCV from Bybit public API.

    Parameters
    ----------
    symbol : str
        e.g. 'BTCUSDT', 'ETHUSDT'
    timeframe : str
        Bybit interval, e.g. '5', '60', '240'
    limit : int
        Number of candles to fetch (max 200 for some intervals)

    Returns
    -------
    pd.DataFrame
        Columns: timestamp, open, high, low, close, volume
    """
    params = {
        "symbol": symbol,
        "interval": timeframe,
        "limit": limit,
    }

    r = requests.get(BASE_URL, params=params, timeout=10)
    r.raise_for_status()

    payload = r.json()
    data = payload["result"]["list"]

    df = pd.DataFrame(
        data,
        columns=[
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "_",
        ],
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"].astype("int64"), unit="ms")
    df = df.drop(columns=["_"])

    float_cols = ["open", "high", "low", "close", "volume"]
    df[float_cols] = df[float_cols].astype(float)

    df = df.sort_values("timestamp").reset_index(drop=True)
    return df
