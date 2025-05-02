import pandas as pd
import time
from binance.client import Client

from config import *

pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.max_rows', None)     # 显示所有行
pd.set_option('display.width', 1000)        # 设置显示宽度
pd.set_option('display.max_colwidth', None) # 设置列内容的最大宽度

class BinanceTradeFetcher:

    def __init__(self, api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET):

        self.client = Client(api_key=api_key, api_secret=api_secret,
                             requests_params={
                'proxies': {
                    'http': BINANCE_PROXY,
                    'https': BINANCE_PROXY,
                    }
                })

    def fetch_trades(self, symbol='BTCUSDT', start_time=None, end_time=None, limit=1000):
        """
                拉取指定symbol的成交记录
                """
        all_trades = []
        from_id = None

        while True:
            params = {
                'limit': limit,
            }
            if symbol:
                params['symbol'] = symbol
            if start_time:
                params['startTime'] = start_time
            if end_time:
                params['endTime'] = end_time
            if from_id:
                params['fromId'] = from_id

            trades = self.client.futures_account_trades(**params)
            if not trades:
                break

            all_trades.extend(trades)
            if len(trades) < limit:
                break
            else:
                from_id = trades[-1]['id'] + 1
                time.sleep(0.2)  # 避免过快触发IP限速

        return self._to_dataframe(all_trades)

    @staticmethod
    def _to_dataframe(trades):
        """
        将trades列表转为DataFrame
        """
        if not trades:
            return pd.DataFrame()

        df = pd.DataFrame(trades)
        # 转换时间戳为可读时间
        df['time'] = pd.to_datetime(df['time'], unit='ms')
        # 规范字段排序
        cols = ['time', 'symbol', 'side', 'positionSide', 'price', 'qty', 'realizedPnl', 'commission',
                'commissionAsset', 'maker', 'buyer', 'orderId']
        df = df[cols]
        return df

if __name__ == "__main__":

    bfetcher = BinanceTradeFetcher()

    # 示例：拉过去7天 ALPACAUSDT 的成交
    now = int(time.time() * 1000)
    seven_days_ago = now - 7 * 24 * 60 * 60 * 1000

    df = bfetcher.fetch_trades(symbol="ALPACAUSDT", start_time=seven_days_ago, end_time=now)
    print(df)
    df.to_csv("binance_trades.csv", index=False)