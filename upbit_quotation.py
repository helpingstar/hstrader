import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

def kst_to_utc(kst_time):
    utc = datetime.strptime(kst_time, "%Y-%m-%dT%H:%M:%S") - timedelta(hours=9)
    return utc.strftime("%Y-%m-%dT%H:%M:%SZ")

def get_minute_candle(unit, market, count, end=None, tz='KST'):
    """get minute candle

    Args:
        unit (int): 분 단위
        market (string): ex.KRW-BTC
        count (int): The number of candles
        end (string, optional): Last Candle Time. Defaults to The latest Time.
        tz (str, optional): TimeZone. Defaults to 'KST'.
    Returns:
        DataFrame: Return Candle Data
    """
    if end:
        if tz == 'KST':
            end = kst_to_utc(end)
        url = f"https://api.upbit.com/v1/candles/minutes/{unit}?market={market}&count={count}&to={end}"
    else:      
        url = f"https://api.upbit.com/v1/candles/minutes/{unit}?market={market}&count={count}"
    headers = {"Accept": "application/json"}
    response = requests.request("GET", url, headers=headers)
    return pd.read_json(response.text)

def get_minute_candle_from_to(unit, market, start, end=None, tz='KST'):
    """get minute candle from [start] to [end], Since the sleep function is used, errors may occur depending on the implementation environment.

    Args:
        unit (int): minute unit
        market (string): ex. 'KRW-BTC'
        start (string): Start Candle Time. format : yyyy-MM-dd'T'HH:mm:ss'Z'
        end (string, optional): Last Candle Time. format: yyyy-MM-dd'T'HH:mm:ss'Z', Defaults to None.
        tz (str, optional): TimeZone. Defaults to 'KST'.
    Returns:
        DataFrame: Return Candle Data from [start] to [end]
    """
    
    MAX_COUNT = 200
    result = pd.DataFrame([])
    while 1:
        temp = get_minute_candle(unit, market, MAX_COUNT, end, tz)
        start_in_temp = temp['candle_date_time_kst'].iloc[-1]
        if start_in_temp < start:
            temp = temp[temp['candle_date_time_kst'] >= start]
            result = pd.concat([result, temp], ignore_index=True)
            break
        result = pd.concat([result, temp], ignore_index=True)
        end = start_in_temp
        time.sleep(0.1)
    result.reset_index()
    return result