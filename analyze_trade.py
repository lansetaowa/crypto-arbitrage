# trade_analyzer.py

import pandas as pd
from datetime import datetime, timedelta
from future_trade import BFutureTrader, GateFuturesTrader
from config import BINANCE_API_KEY, BINANCE_API_SECRET, GATEIO_API_KEY, GATEIO_API_SECRET

pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.max_rows', None)     # 显示所有行
pd.set_option('display.width', 1000)        # 设置显示宽度
pd.set_option('display.max_colwidth', None) # 设置列内容的最大宽度

class TradeAnalyzer(BFutureTrader, GateFuturesTrader):
    def __init__(self):
        BFutureTrader.__init__(self, api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET)
        GateFuturesTrader.__init__(self, gate_key=GATEIO_API_KEY, gate_secret=GATEIO_API_SECRET)

    def fetch_binance_trades(self, symbol, start_str=None, end_str=None):
        """
        获取某合约在指定时间段内的成交记录。
        - 若未提供时间段，则默认返回最近7天内的记录。
        - 提供的时间段不能超过7天
        - 时间格式为字符串，例如 '2025-04-01 00:00:00'

        :param symbol: 例如 'BTCUSDT'
        :param start_str: 起始时间字符串（可选）
        :param end_str: 截止时间字符串（可选）
        :return: DataFrame
        """
        try:
            params = {'symbol': symbol}

            # 转换时间为毫秒时间戳
            if start_str:
                start_ts = int(pd.to_datetime(start_str).timestamp() * 1000)
                params['startTime'] = start_ts
            if end_str:
                end_ts = int(pd.to_datetime(end_str).timestamp() * 1000)
                params['endTime'] = end_ts

            # 请求数据
            trades = self.client.futures_account_trades(**params)
            print(trades[0])

            df = pd.DataFrame(trades)
            df['qty'] = df['qty'].astype(float)
            df['price'] = df['price'].astype(float)
            df['time'] = pd.to_datetime(df['time'], unit='ms')
            df['commission'] = df['commission'].astype(float)

            df = df[['symbol', 'orderId', 'side', 'positionSide', 'qty', 'price', 'time', 'commission']]

            grouped = df.groupby(['symbol', 'orderId', 'side', 'positionSide'], as_index=False).agg({
                'qty': 'sum',
                'price': 'mean',
                'time': 'min',  # 平均时间
                'commission': 'sum'
            })

            return grouped

        except Exception as e:
            print(f"❌ Error fetching Binance trades for {symbol}: {e}")
            return pd.DataFrame()

    def fetch_gate_trades(self, symbol=None, limit=100):
        """
        获取Gate.io上的合约成交历史
        """
        try:
            trades = self.futures_api.get_my_trades(
                settle='usdt',
                contract=symbol,  # 例如 'BTC_USDT'
                limit=limit
            )

            df = pd.DataFrame([{
                'symbol': t.contract,
                'order_id': t.order_id,
                'price': float(t.price),
                'size': float(t.size),
                'time': pd.to_datetime(t.create_time, unit='s'),
                'fee':float(t.fee),
                'role': t.text
            } for t in trades])

            grouped = df.groupby(['symbol', 'order_id', 'role'], as_index=False).agg({
                'price': 'mean',
                'size': 'sum',
                'time': 'mean',
                'fee': 'sum'
            })
            return grouped

        except Exception as e:
            print(f"❌ Error fetching Gate.io trades: {e}")
            return pd.DataFrame()

    @staticmethod
    def match_trades(bi_df, gate_df, time_diff_seconds=60):
        symbol = bi_df['symbol'].values[0]

        matches = []
        for _, b_row in bi_df.iterrows():
            # Find gate trades within time threshold
            close_matches = gate_df[
                (gate_df['symbol'] == b_row['symbol'].replace('USDT', '_USDT')) &
                (abs((gate_df['time'] - b_row['time']).dt.total_seconds()) <= time_diff_seconds)
            ]
            for _, g_row in close_matches.iterrows():
                matches.append({
                    'symbol': symbol,
                    'binance_time': b_row['time'],
                    'gate_time': g_row['time'],
                    'binance_price': float(b_row['price']),
                    'gate_price': float(g_row['price']),
                    'binance_qty': float(b_row['qty']),
                    'gate_size': float(g_row['size']),
                    'binance_side': b_row['side'],
                    'binance_positionside': b_row['positionSide'],
                    'gate_role': g_row['role'],
                    'binance_fee': b_row['commission'],
                    'gate_fee': g_row['fee']
                })

        return pd.DataFrame(matches)

    def structure_arbitrage_trades(self, df):
        # 定义开仓逻辑（Binance Buy Long 或 Sell Short；Gate t-api_market）
        is_open = (
                      ((df['binance_side'] == 'BUY') & (df['binance_positionside'] == 'LONG')) |
                      ((df['binance_side'] == 'SELL') & (df['binance_positionside'] == 'SHORT'))
                  ) & (df['gate_role'] == 't-api_market')

        # 定义平仓逻辑（Binance Sell Long 或 Buy Short；Gate t-api_market_close）
        is_close = (
                       ((df['binance_side'] == 'SELL') & (df['binance_positionside'] == 'LONG')) |
                       ((df['binance_side'] == 'BUY') & (df['binance_positionside'] == 'SHORT'))
                   ) & (df['gate_role'] == 't-api_market_close')

        # 提取并排序开/平仓记录
        df_open = df[is_open].sort_values(by='binance_time').reset_index(drop=True)
        df_close = df[is_close].sort_values(by='binance_time').reset_index(drop=True)

        # 仅保留所需列，并加前缀
        df_open = df_open.add_prefix('open_')
        df_close = df_close.add_prefix('close_')

        # 合并开仓和平仓记录
        structured_df = pd.concat([df_open, df_close], axis=1)

        structured_df['quanto_multiplier'] = structured_df['open_symbol'].apply(
            lambda sym: float(self.futures_api.get_futures_contract(settle='usdt', contract=sym.replace("USDT", "_USDT")).quanto_multiplier)
        )

        return structured_df

    @staticmethod
    def calculate_pnl(row):
        # gate_size 是张数，每张的币数量 = quanto_multiplier
        gate_qty = abs(row['open_gate_size']) * row['quanto_multiplier']

        # Binance PnL
        if row['open_binance_positionside'] == 'LONG':
            binance_pnl = (row['close_binance_price'] - row['open_binance_price']) * row['open_binance_qty'] # 币安做多
            gate_pnl = (row['open_gate_price'] - row['close_gate_price']) * gate_qty # gate做空
        elif row['open_binance_positionside'] == 'SHORT':
            binance_pnl = (row['open_binance_price'] - row['close_binance_price']) * row['open_binance_qty'] # 币安做空
            gate_pnl = (row['close_gate_price'] - row['open_gate_price']) * gate_qty # gate做多
        else:
            binance_pnl = 0
            gate_pnl = 0

        fee_total = row['open_binance_fee'] + row['open_gate_fee'] + row['close_binance_fee'] + row['close_gate_fee']

        return binance_pnl + gate_pnl - fee_total

    def add_pnl(self, df):

        df['pnl'] = df.apply(self.calculate_pnl, axis=1)
        return df

if __name__ == '__main__':
    trade_analyzer = TradeAnalyzer()
    bi_trades = trade_analyzer.fetch_binance_trades(symbol='FUNUSDT', start_str='2025-04-01 00:00:00', end_str='2025-04-07 00:00:00')
    print(bi_trades)
    # print(bi_trades.info())
    g_trades = trade_analyzer.fetch_gate_trades(symbol='FUN_USDT')
    print(g_trades)
    # print(g_trades.info())
    matched = trade_analyzer.match_trades(bi_trades, g_trades)
    print(matched)
    structured_df = trade_analyzer.structure_arbitrage_trades(matched)
    print(structured_df)
    # print(structured_df.info())
    pnl_df = trade_analyzer.add_pnl(structured_df)
    print(pnl_df)
