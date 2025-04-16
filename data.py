"""
获取相关数据的模块：
- gate/binance 实时资金费率，以及下次的资金费率
- 合并g/b的实时资金费率，并计算差额
- gate/binance 各symbol的细节，例如min_qty, quanto_multiplier
- 根据amount，计算g/b上各自下单的设置，
    - b包括：symbol, qty
    - g包括：symbol, size
"""

import pandas as pd
import numpy as np
from binance.client import Client
from gate_api import FuturesApi, Configuration, ApiClient
from config import *

pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.max_rows', None)     # 显示所有行
pd.set_option('display.width', 1000)        # 设置显示宽度
pd.set_option('display.max_colwidth', None) # 设置列内容的最大宽度

class BinanceDataHandler:

    def __init__(self, api_key=None, api_secret=None):

        self.client = Client(api_key, api_secret,
                             requests_params={
                'proxies': {
                    'http': BINANCE_PROXY,
                    'https': BINANCE_PROXY,
                    }
                })

    # Binance所有合约的实时funding rate，以及下次funding生效时间
    def bi_get_funding_rates(self):

        # Convert to DataFrame
        df = pd.DataFrame(self.client.futures_mark_price())
        df = df[['symbol','markPrice','lastFundingRate','nextFundingTime','time']]

        df['lastFundingRate'] = df['lastFundingRate'].astype(float)
        df['markPrice'] = df['markPrice'].astype(float)
        df['nextFundingTime'] = pd.to_datetime(df['nextFundingTime'], unit='ms')
        df['time'] = pd.to_datetime(df['time'], unit='ms')

        # Sort by funding rate (descending)
        df.sort_values(by="lastFundingRate", ascending=False, inplace=True)

        return df

    # Binance上获取某个合约的实时价格
    def bi_get_price(self, symbol='BTCUSDT'):
        try:
            ticker = self.client.futures_symbol_ticker(symbol=symbol)
            return float(ticker['price'])
        except Exception as e:
            print(f"Error getting price for {symbol}: {e}")
            return None

    # Binance上获取某个合约的细节
    def bi_get_contract_info(self, symbol='BTCUSDT'):

        data = self.client.futures_exchange_info()
        for symbol_info in data['symbols']:
            if symbol_info['symbol'] == symbol:
                try:
                    min_qty = float(symbol_info['filters'][1]['minQty'])
                    max_qty = float(symbol_info['filters'][1]['maxQty'])
                    step_size = float(symbol_info['filters'][1]['stepSize'])
                    min_notional = float(symbol_info['filters'][5]['notional'])
                    tick_size = float(symbol_info['filters'][0]['tickSize'])
                    price = self.bi_get_price(symbol)
                    status = symbol_info['status']

                    return {"symbol": symbol,
                            "min_qty": min_qty,
                            "max_qty": max_qty,
                            "step_size": step_size,
                            "min_notional": min_notional,
                            "tick_size": tick_size,
                            "price": price,
                            "status": status}

                except Exception as e:
                    print(e)

    # Binance上所有合约symbol的status
    def bi_get_all_contract_status(self):
        data = self.client.futures_exchange_info()
        symbols = data.get('symbols', [])
        rows = []
        for symbol_info in symbols:
            rows.append({
                'symbol': symbol_info['symbol'],
                'status': symbol_info['status']
            })
        df = pd.DataFrame(rows)

        return df

    # Binance上所有合约的funding rate间隔
    def bi_get_funding_interval_df(self):
        result = []
        symbols = [s['symbol'] for s in self.client.futures_exchange_info()['symbols']]
        for symbol in symbols:
            try:
                fr_history = self.client.futures_funding_rate(symbol=symbol, limit=2)
                if len(fr_history) >= 2:
                    t1 = int(fr_history[0]['fundingTime'])
                    t2 = int(fr_history[1]['fundingTime'])
                    interval_sec = abs(t1 - t2) // 1000
                    result.append({'symbol': symbol, 'interval_sec': interval_sec})

                    df = pd.DataFrame(result)
                    df['interval_hour'] = df['interval_sec'] / 3600

                    return df
            except Exception as e:
                print(f"error getting funding rate for {symbol}: {e}")
                continue

    # Binance上某合约的实时深度
    def get_binance_orderbook(self, symbol, limit=5):
        try:
            orderbook = self.client.futures_order_book(symbol=symbol, limit=limit)
            return {
                'bids': [(float(price), float(qty)) for price, qty in orderbook['bids']],
                'asks': [(float(price), float(qty)) for price, qty in orderbook['asks']]
            }
        except Exception as e:
            print(f"❌ Error fetching Binance orderbook: {e}")
            return None

class GateDataHandler:

    def __init__(self, gate_key=None, gate_secret=None):

        self.config = Configuration(key=gate_key, secret=gate_secret)
        self.config.proxy = GATE_PROXY

        self.api_client = ApiClient(self.config)
        self.futures_api = FuturesApi(self.api_client)

    # Gateio所有合约实时资金费率
    def gate_get_funding_rates(self, symbol_filter="usdt"):

        contracts = self.futures_api.list_futures_contracts(settle=symbol_filter)
        df = pd.DataFrame([{
            'symbol': c.name,
            'mark_price': c.mark_price,
            'gate_funding_rate': c.funding_rate,
            'next_funding_time': c.funding_next_apply,
        } for c in contracts])

        df['gate_funding_rate'] = df['gate_funding_rate'].astype(float)
        df['mark_price'] = df['mark_price'].astype(float)
        df['symbol_renamed'] = df['symbol'].apply(lambda x: x.replace("_", ""))
        df['next_funding_time'] = pd.to_datetime(df['next_funding_time'], unit='s')

        df.sort_values(by="gate_funding_rate", ascending=False, inplace=True)

        return df

    # Gateio获取单个合约规格
    def gate_get_contract_info(self, contract_name='BTC_USDT'):
        try:
            info = self.futures_api.get_futures_contract(settle='usdt', contract=contract_name)

            return {
                "symbol": info.name,
                "current_price": info.last_price,
                "quanto_multiplier": info.quanto_multiplier,  # 每一张合约面值
                "min_qty": info.order_size_min,  # 最小下单量
                "max_qty": info.order_size_max,  # 最大下单量
                "order_price_round": info.order_price_round,  # 数量步长
            }

        except Exception as e:
            print(f"获取合约规格时出错: {e}")
            return pd.DataFrame()

    # Gateio所有合约funding rate间隔
    def gate_get_funding_interval_df(self):
        contracts = self.futures_api.list_futures_contracts(settle='usdt')
        df = pd.DataFrame([{
            'symbol': c.name,
            'interval_sec': int(c.funding_interval) if hasattr(c, 'funding_interval') else None
        } for c in contracts])
        df['interval_hour'] = df['interval_sec']/3600
        return df

    # Gateio上某合约的实时深度
    def get_gate_orderbook(self, symbol, limit=5):
        try:
            gate_symbol = symbol.replace("USDT", "_USDT")
            book = self.futures_api.list_futures_order_book(settle="usdt", contract=gate_symbol, limit=limit)

            return {
                'bids': [(float(entry.p), float(entry.s)) for entry in book.bids],
                'asks': [(float(entry.p), float(entry.s)) for entry in book.asks]
            }
        except Exception as e:
            print(f"❌ Error fetching Gate.io orderbook: {e}")
            return None

class ArbitrageUtils:

    # 合并两个funding rate dataframe
    @staticmethod
    def merge_funding_rates(binance_df, gate_df):

        merged_df = pd.merge(
            left = gate_df[['symbol_renamed', 'mark_price', 'gate_funding_rate', 'next_funding_time']],
            right = binance_df[['symbol', 'markPrice','lastFundingRate', 'nextFundingTime']],
            left_on='symbol_renamed', right_on='symbol'
        )
        merged_df['fr_diff'] = merged_df['gate_funding_rate'] - merged_df['lastFundingRate']
        # merged_df['abs_price_diff'] = np.abs((merged_df['markPrice'] - merged_df['mark_price']) / merged_df['mark_price'])
        merged_df.sort_values(by='fr_diff', ascending=False, inplace=True)

        return merged_df

    # 合并两个funding rate interval dataframe
    @staticmethod
    def merge_funding_intervals(binance_df, gate_df):

        gate_df['symbol_renamed'] = gate_df['symbol'].apply(lambda x: x.replace("_", ""))
        merged_df = pd.merge(
            left=gate_df[['symbol_renamed', 'interval_hour']].rename(columns={'interval_hour': 'gate_interval'}),
            right=binance_df[['symbol', 'interval_hour']].rename(columns={'interval_hour': 'binance_interval'}),
            left_on='symbol_renamed',
            right_on='symbol',
            how='inner'
        )

        return merged_df

    # 获取funding rate interval不一致的symbol列表
    @staticmethod
    def update_interval_mismatch_list():
        bdata_handler = BinanceDataHandler()
        gdata_handler = GateDataHandler()

        bi_df = bdata_handler.bi_get_funding_interval_df()
        gate_df = gdata_handler.gate_get_funding_interval_df()

        merged = ArbitrageUtils.merge_funding_intervals(bi_df, gate_df)
        mismatch = list(merged[merged['gate_interval'] != merged['binance_interval']]['symbol'])

        with open("output/mismatch_symbols.txt", "w") as f:
            for sym in mismatch:
                f.write(sym + "\n")

        print(f"[UPDATE] Mismatch symbols updated: {len(mismatch)} symbols")

    # 获取最近的下一次funding生效时间
    @staticmethod
    def get_next_funding_time(merged_df):
        return min(merged_df['nextFundingTime'].min(), merged_df['next_funding_time'].min())

    # 最近的下一次funding生效时间，获取两边都有的币，并由此更新合并后的表
    @staticmethod
    def filter_next_funding_symbols(merged_df, next_funding_time):

        cond1 = merged_df['nextFundingTime'] == next_funding_time
        cond2 = merged_df['next_funding_time'] == next_funding_time

        return  merged_df[cond1&cond2]

    # 计算两边的下单量
    @staticmethod
    def calculate_trade_quantity(amount, symbol, binance_handler, gate_handler):

        # 获取合约信息
        gate_info = gate_handler.gate_get_contract_info(symbol.replace("USDT", "_USDT"))
        binance_info = binance_handler.bi_get_contract_info(symbol)

        if not gate_info or not binance_info:
            print(f"❌ 无法获取合约规格信息: {symbol}")
            return None, None

        try:
            # Gate合约信息
            gate_price = float(gate_info['current_price'])
            gate_quanto_multiplier = float(gate_info['quanto_multiplier'])
            gate_min_qty = float(gate_info['min_qty'])
            # print(gate_price, gate_quanto_multiplier, gate_min_qty)

            # Binance合约信息
            binance_price = float(binance_info['price'])
            binance_min_qty = float(binance_info['min_qty'])
            binance_step_size = float(binance_info['step_size'])
            # print(binance_price, binance_min_qty, binance_step_size)

            # 根据总合约数量和最小数量调整
            gate_size = np.floor(amount / (gate_price * gate_quanto_multiplier))  # 合约张数
            binance_step_qty = np.floor((amount / binance_price - binance_min_qty) / binance_step_size)  # 在min_qtr上，增加的step_size的数量
            # print(gate_size, binance_step_qty)

            # 计算实际下单数量
            gate_order_qty = gate_size * gate_quanto_multiplier
            binance_order_qty = binance_step_qty*binance_step_size + binance_min_qty
            # print(gate_order_qty, binance_order_qty)
            # print(gate_order_qty*gate_price, binance_order_qty*binance_price)

            # 调整两边数量，使得gate和binance的数量尽量一致
            if gate_order_qty > binance_order_qty:
                gate_size = np.round(binance_order_qty / gate_quanto_multiplier,0)
            elif binance_order_qty > gate_order_qty:
                binance_order_qty = np.floor(gate_order_qty / binance_step_size) * binance_step_size

            # 检查是否满足最小下单量要求
            if gate_size < gate_min_qty:
                print("❌ Gate合约下单量不足")
                return None, None
            if binance_order_qty < binance_min_qty:
                print("❌ Binance合约下单量不足")
                return None, None

            print(f"✅ 交易量计算成功 - Gate Size: {gate_size}, Binance Quantity: {binance_order_qty}")
            return gate_size, binance_order_qty

        except Exception as e:
            print(f"❌ 计算下单量时出错: {e}")
            return None, None

    # 计算两个平台的实时价差
    @staticmethod
    def calculate_price_diff(symbol, binance_handler, gate_handler):
        try:
            gate_info = gate_handler.gate_get_contract_info(symbol.replace("USDT", "_USDT"))
            binance_info = binance_handler.bi_get_contract_info(symbol)
            gate_price = float(gate_info['current_price'])
            binance_price = float(binance_info['price'])

            # 计算价格差异率
            price_diff = (gate_price - binance_price) / binance_price

            return np.abs(price_diff)

        except Exception as e:
            print(f"❌ 计算价格差异率时出错: {e}")
            return None

    # 基于两个平台的实时深度和开仓价格，计算最差pnl的情况
    @staticmethod
    def calculate_worst_case_pnl(entry_price_gate, entry_price_binance, trade_type,
                                  orderbook_gate, orderbook_binance):
        """
        计算当前 orderbook 情况下的最差盈利情况（是否能不亏）。

        :param entry_price_gate: Gate 开仓价格
        :param entry_price_binance: Binance 开仓价格
        :param trade_type: 'type1' or 'type2'
        :param orderbook_gate: Gate 实时 orderbook dict，包含 'bids' 和 'asks'
        :param orderbook_binance: Binance 实时 orderbook dict，包含 'bids' 和 'asks'
        :return: normalized_pnl 盈亏比，例如 0.001 表示 0.1% 收益
        """

        if trade_type == 'type1':
            # Gate 做空 → ask 平仓；Binance 做多 → bid 平仓
            gate_exit_price = orderbook_gate['asks'][-1][0]
            binance_exit_price = orderbook_binance['bids'][-1][0]

            gate_pnl = entry_price_gate - gate_exit_price
            binance_pnl = binance_exit_price - entry_price_binance
            print(f"gate pnl is {gate_pnl}, binance pnl is {binance_pnl}")

        elif trade_type == 'type2':
            # Gate 做多 → bid 平仓；Binance 做空 → ask 平仓
            gate_exit_price = orderbook_gate['bids'][-1][0]
            binance_exit_price = orderbook_binance['asks'][-1][0]

            gate_pnl = gate_exit_price - entry_price_gate
            binance_pnl = entry_price_binance - binance_exit_price
            print(f"gate pnl is {gate_pnl}, binance pnl is {binance_pnl}")

        else:
            raise ValueError("trade_type must be 'type1' or 'type2'")

        # 返回合并盈亏占平均开仓价格的比例（近似为总收益率）
        avg_entry = (entry_price_gate + entry_price_binance) / 2
        total_pnl = gate_pnl + binance_pnl
        normalized_pnl = total_pnl / avg_entry

        return normalized_pnl


if __name__ == '__main__':

    bdata_handler = BinanceDataHandler()
    gdata_handler = GateDataHandler()

    # bi_depth = bdata_handler.get_binance_orderbook(symbol='EDUUSDT')
    # print(bi_depth)
    #
    # g_depth = gdata_handler.get_gate_orderbook(symbol='EDUUSDT')
    # print(g_depth)
    #
    # pnl = ArbitrageUtils.calculate_worst_case_pnl(entry_price_gate=0.14109,
    #                                               entry_price_binance=0.1423,
    #                                               orderbook_gate=g_depth,
    #                                               orderbook_binance=bi_depth,
    #                                               trade_type='type1')
    # print(pnl)

    # ArbitrageUtils.update_interval_mismatch_list()

    # df = bdata_handler.bi_get_all_contract_status()
    # print(df['status'].value_counts())

    # s = 'BTCUSDT'
    # bi_status = bdata_handler.bi_get_contract_info(s)['status']
    # print(bi_status == 'TRADING')

    #
    # amount = 1000
    # symbol = 'BTCUSDT'
    #
    gate_size, bi_quantity = ArbitrageUtils.calculate_trade_quantity(amount=20,
                                                                     symbol='ADAUSDT',
                                                                     binance_handler=bdata_handler,
                                                                     gate_handler=gdata_handler)
    print(f"Gate Size: {gate_size}, Binance Quantity: {bi_quantity}")
    #
    # bi_df = bdata_handler.bi_get_funding_rates()
    # bi_df.to_csv('binance_fr.csv')
    # gate_df = gdata_handler.gate_get_funding_rates()
    # merged_df = ArbitrageUtils.merge_funding_rates(bi_df, gate_df)
    # # print(merged_df.info())
    # print(merged_df.tail())
    # print(merged_df.head())
    #
    # bi_interval = bdata_handler.bi_get_funding_interval_df()
    # gate_interval = gdata_handler.gate_get_funding_interval_df()
    #
    # interval_merged = ArbitrageUtils.merge_funding_intervals(bi_interval,gate_interval)
    # INTERVAL_MISMATCH_SYMBOLS = list(interval_merged[interval_merged['gate_interval'] != interval_merged['binance_interval']]['symbol'])
    # print(INTERVAL_MISMATCH_SYMBOLS)

    # interval_merged.to_csv('merged_funding_rate_interval.csv')
    #
    # fr_history = pd.DataFrame(bdata_handler.client.futures_funding_rate(symbol='FUNUSDT', limit=40))
    # fr_history['fundingTime'] = pd.to_datetime(fr_history['fundingTime'], unit='ms')
    # print(fr_history)

    #
    # nexttime = ArbitrageUtils.get_next_funding_time(merged_df)
    # print(nexttime)
    # #
    # filtered_df = ArbitrageUtils.filter_next_funding_symbols(merged_df, next_funding_time=nexttime)
    # print(filtered_df.info())
    # print(filtered_df.shape[0], filtered_df.shape[1])

    # print(gdata_handler.futures_api.get_futures_contract(settle='usdt', contract='BTC_USDT').funding_interval/3600)

    # print(ArbitrageUtils.calculate_price_diff(symbol='AERGOUSDT',
    #                                           binance_handler=bdata_handler,
    #                                           gate_handler=gdata_handler))

    # print(gdata_handler.gate_get_contract_info(contract_name='BTC_USDT'))
    #
    # print(gdata_handler.futures_api.list_futures_contracts(settle='usdt')[0])
