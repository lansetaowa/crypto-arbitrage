"""
This module monitors depth data and captures good moments and prices to close positions.
"""
import time
import logging
import pandas as pd
from data import BinanceDataHandler, GateDataHandler

def get_orderbook_price_binance(bdata_handler, symbol):
    try:
        ob = bdata_handler.client.futures_order_book(symbol=symbol, limit=5)
        bid = float(ob['bids'][0][0])
        ask = float(ob['asks'][0][0])
        return bid, ask
    except Exception as e:
        logging.error(f"[ERROR] Failed to fetch Binance orderbook for {symbol}: {e}")
        return None, None

def get_orderbook_price_gate(gdata_handler, symbol):
    try:
        ob = gdata_handler.futures_api.list_futures_order_book(settle='usdt', contract=symbol, interval='0')
        bid = float(ob.bids[0].p)
        ask = float(ob.asks[0].p)
        return bid, ask
    except Exception as e:
        logging.error(f"[ERROR] Failed to fetch Gate orderbook for {symbol}: {e}")
        return None, None

def calculate_pnl(entry_price, exit_price, direction):
    if direction == 'long':
        return exit_price - entry_price
    else:
        return entry_price - exit_price



if __name__ == '__main__':
    bdata_handler = BinanceDataHandler()
    gdata_handler = GateDataHandler()
    print(get_orderbook_price_binance(bdata_handler, 'BTCUSDT'))
    print(get_orderbook_price_gate(gdata_handler, 'BTC_USDT'))