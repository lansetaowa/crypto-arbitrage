"""
合约下单的模块
- gate/binance设置合约杠杆
- gate/binance合约下单，市价单
"""
import pandas as pd
from binance.client import Client

from gate_api import FuturesApi, Configuration, ApiClient
from gate_api.exceptions import ApiException

class BFutureTrader:

    def __init__(self, api_key=None, api_secret=None):

        self.client = Client(api_key, api_secret,
                             requests_params={
                'proxies': {
                    'http': 'socks5://127.0.0.1:10808',
                    'https': 'socks5://127.0.0.1:10808',
                    }
                })

    # 调整合约杠杆
    def set_leverage(self, symbol, leverage):
        try:
            response = self.client.futures_change_leverage(
                symbol=symbol,
                leverage=leverage
            )
            return response
        except Exception as e:
            print(f"❌ 设置杠杆时出错: {e}")

    # 查询合约usdt余额
    def get_available_balance(self):
        try:
            account_info = self.client.futures_account_balance()
            for asset in account_info:
                if asset['asset'] == 'USDT':
                    return float(asset['availableBalance'])
        except Exception as e:
            print(f"❌ 获取 Binance 合约账户余额出错: {e}")
            return 0.0

    # 限价单，开多
    def place_limit_long_order(self, symbol, quantity, order_price):

        future_order = self.client.futures_create_order(
            symbol=symbol,
            side='BUY',  # buy long
            positionSide='LONG',  # long position
            type='LIMIT',
            timeInForce='GTC',  # good till cancel
            quantity=quantity,
            price=order_price
        )
        print("Long order is placed: ", future_order)

        return future_order

    # 限价单，开空
    def place_limit_short_order(self, symbol, quantity, order_price):

        future_order = self.client.futures_create_order(
            symbol=symbol,
            side='SELL',  # sell short
            positionSide='SHORT',  # short position
            type='LIMIT',
            timeInForce='GTC',  # good till cancel
            quantity=quantity,
            price=order_price
        )
        print("Short order is placed: ", future_order)
        return future_order

    # 市价单，开多
    def place_market_long_order(self, symbol, quantity):

        try:
            future_order = self.client.futures_create_order(
                symbol=symbol,
                side='BUY',  # buy long
                positionSide='LONG',  # long position
                type='MARKET',  # market order
                quantity=quantity
            )
            return future_order
        except Exception as e:
            print(f"❌ Error placing market long order: {e}")
            return None

    # 市价单，开空
    def place_market_short_order(self, symbol, quantity):

        try:
            future_order = self.client.futures_create_order(
                symbol=symbol,
                side='SELL',  # sell short
                positionSide='SHORT',  # short position
                type='MARKET',  # market order
                quantity=quantity
            )
            return future_order
        except Exception as e:
            print(f"❌ Error placing market short order: {e}")
            return None

    # 市价单，平多
    def close_market_long_order(self, symbol, quantity):

        try:
            future_order = self.client.futures_create_order(
                symbol=symbol,
                side='SELL',  # sell to close long
                positionSide='LONG',  # closing long position
                type='MARKET',  # market order
                quantity=quantity
            )
            return future_order
        except Exception as e:
            print(f"❌ Error closing market long order: {e}")
            return None

    # 市价单，平空
    def close_market_short_order(self, symbol, quantity):

        try:
            future_order = self.client.futures_create_order(
                symbol=symbol,
                side='BUY',  # buy to close short
                positionSide='SHORT',  # closing short position
                type='MARKET',  # market order
                quantity=quantity
            )
            return future_order
        except Exception as e:
            print(f"❌ Error closing market short order: {e}")
            return None


class GateFuturesTrader:

    def __init__(self, gate_key, gate_secret):

        self.config = Configuration(key=gate_key, secret=gate_secret)
        self.config.proxy = "http://127.0.0.1:10808"

        self.api_client = ApiClient(self.config)
        self.futures_api = FuturesApi(self.api_client)

    """
    持仓方向: 
    - 开多:  size为正
    - 开空:  size为负
    - 平多:  reduce_only = True & size = 0 & auto_size='close_long'
    - 平空:  reduce_only = True & size = 0 & auto_size='close_short'

    重要参数: 

    - contract:  交易对, 如 "BTC_USDT"
    - size:  下单时指定的是合约张数 size , 而非币的数量, 每一张合约对应的币的数量是合约详情接口里返回的 quanto_multiplier
    - price:  期望成交价格
    - close: 设置为 true 的时候执行平仓操作, 并且size应设置为0
    - 设置 reduce_only 为 true 可以防止在减仓的时候穿仓
    - 单仓模式下, 如果需要平仓, 需要设置 size 为 0 , close 为 true
    - 双仓模式下, 平仓需要使用 auto_size 来设置平仓方向, 并同时设置 reduce_only 为 true, size 为 0
    - time_in_force:  有效时间策略
        - gtc: Good Till Canceled(挂单直到成交或手动取消)
        - ioc: Immediate Or Cancel(立即成交, 否则取消)
        - poc: Post Only Cancel(只挂单, 不主动成交)
    """

    # 设置合约杠杆
    def set_leverage(self, symbol, leverage):
        try:
            response = self.futures_api.update_position_leverage(
                settle="usdt",
                contract=symbol,
                leverage=str(leverage)  # 杠杆倍数为字符串类型
            )
            return response
        except ApiException as e:
            print(f"❌ 设置杠杆时出错: {e}")

    # 查询合约usdt余额
    def get_available_balance(self):
        try:
            balance_info = self.futures_api.list_futures_accounts(settle='usdt')
            return float(balance_info.available)
        except ApiException as e:
            print(f"❌ 获取 Gate 合约账户余额出错: {e}")
            return 0.0


    ##------------------------------------LIMIT ORDERS----------------------------------------
    # PLACE a limit long/short order
    # 开多:  size为正
    # 开空:  size为负
    def place_future_limit_order(self, symbol, size, price):
        try:
            order = self.futures_api.create_futures_order(
                settle="usdt",
                futures_order={
                    "contract": symbol,  # 交易对
                    "size": size,  # 合约数量
                    "price": price,  # 限价
                    "tif": "gtc",  # Good Till Canceled
                    "text": "t-api_limit_order",  # 自定义标签
                    "reduce_only": False,  # 是否只减仓
                    "close": False  # 是否平仓
                }
            )
            return order
        except ApiException as e:
            print(f"❌ 下单时出错: {e}")

    ##------------------------------------MARKET ORDERS----------------------------------------
    # PLACE a market long/short order
    # 开多:  size为正
    # 开空:  size为负
    def place_future_market_order(self, symbol, size):
        try:
            order = self.futures_api.create_futures_order(
                settle="usdt",
                futures_order={
                    "contract": symbol,  # 交易对
                    "size": size,  # 合约数量 (正数为开多)
                    "price": "0",  # 市价单, 价格设置为0
                    "tif": "ioc",  # 立即成交或取消
                    "text": "t-api_market",  # 自定义标签
                    "reduce_only": False,  # 不减仓
                    "close": False  # 开仓
                }
            )
            return order
        except ApiException as e:
            print(f"❌ 开多市价单时出错: {e}")
            return None

    # place a market order to close position
    def close_future_market_order(self, symbol, auto_size=None):
        """
        auto_size: "close_long" or "close_short"
        """
        try:
            order = self.futures_api.create_futures_order(
                settle="usdt",
                futures_order={
                    "contract": symbol,  # 交易对
                    "size": 0,  # 平仓, 合约数量为0
                    "price": "0",  # 市价单, 价格设置为0
                    "tif": "ioc",  # 立即成交或取消
                    "text": "t-api_market_close",
                    "reduce_only": True,  # 只减仓
                    "close": False,  # 平仓
                    "auto_size": auto_size  # "close_long" or "close_short"
                }
            )
            return order
        except ApiException as e:
            print(f"❌ 平多市价单时出错: {e}")
            return None

    # cancel an unfilled limit order
    def cancel_futures_order(self, order_id):
        try:
            self.futures_api.cancel_futures_order("usdt", order_id)
        except ApiException as e:
            print(f"❌ 取消订单时出错: {e}")

if __name__ == '__main__':
    from dotenv import load_dotenv
    import os

    # load Gateio api
    load_dotenv("gate_api.env")
    GATEIO_API_KEY = os.getenv('G_KEY')
    GATEIO_API_SECRET = os.getenv('G_SECRET')

    # load Binance api
    load_dotenv("binance_api.env")
    BINANCE_API_KEY = os.getenv('B_KEY')
    BINANCE_API_SECRET = os.getenv('B_SECRET')
    #
    bfuture_trader = BFutureTrader(api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET)
    # print(bfuture_trader.set_leverage('BTCUSDT',1))
    # print(bfuture_trader.get_available_balance())

    gfuture_trader = GateFuturesTrader(gate_key=GATEIO_API_KEY, gate_secret=GATEIO_API_SECRET)
    # print(gfuture_trader.get_available_balance())
    # gfuture_trader.futures_api.update_position_leverage(
    #             settle="usdt",
    #             contract='ETH_USDT',
    #             leverage='2'  # 杠杆倍数为字符串类型
    #         )
    #
    # gfuture_trader.place_future_market_order('ETH_USDT', size=-1)

    gate_positions = gfuture_trader.futures_api.list_positions(settle='usdt')
    # print(gate_positions)
    pos = [p for p in gate_positions if float(p.size) != 0]
    print(pos)

    print(gfuture_trader.futures_api.list_futures_orders(settle='usdt', status='finished'))

    print(gfuture_trader.futures_api.list_futures_funding_rate_history(settle='usdt',contract='FUN_USDT'))

    positions = bfuture_trader.client.futures_account()['positions']
    active_positions = [p for p in positions if float(p.get("positionAmt", 0)) != 0]
    print(active_positions)

    print(bfuture_trader.client.futures_get_all_orders(symbol='FUNUSDT'))




