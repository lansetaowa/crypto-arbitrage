"""
合约下单的模块
- gate/binance设置合约杠杆
- gate/binance合约下单，市价单
"""
import pandas as pd
from binance.client import Client

from gate_api import FuturesApi, Configuration, ApiClient
from gate_api.exceptions import ApiException
from config import *

class BFutureTrader:

    def __init__(self, api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET):

        self.client = Client(api_key=api_key, api_secret=api_secret,
                             requests_params={
                'proxies': {
                    'http': BINANCE_PROXY,
                    'https': BINANCE_PROXY,
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

    # 限价单，平多
    def close_limit_long_order(self, symbol, quantity, price):
        try:
            future_order = self.client.futures_create_order(
                symbol=symbol,
                side='SELL',
                positionSide='LONG',
                type='LIMIT',
                timeInForce='GTC',
                quantity=quantity,
                price=str(price)
            )
            return future_order
        except Exception as e:
            print(f"❌ Error placing limit close long order on Binance: {e}")
            return None

    # 限价单，平多，带止损价
    # 先用close_limit_long_order设置平仓价，再用以下方法设置止损
    def setup_long_order_stop_loss(self, symbol, quantity, stop_price):
        try:
            order = self.client.futures_create_order(
                symbol=symbol,
                side='SELL',
                positionSide='LONG',
                type='STOP_MARKET',
                stopPrice=str(stop_price),
                closePosition=False,
                timeInForce='GTC',
                quantity=quantity,
                workingType='MARK_PRICE'  # 或 'CONTRACT_PRICE'
            )
            return order
        except Exception as e:
            print(f"❌ Error placing stop-limit close long order on Binance: {e}")
            return None

    # 限价单，平空
    def close_limit_short_order(self, symbol, quantity, price):
        try:
            future_order = self.client.futures_create_order(
                symbol=symbol,
                side='BUY',
                positionSide='SHORT',
                type='LIMIT',
                timeInForce='GTC',
                quantity=quantity,
                price=str(price)
            )
            return future_order
        except Exception as e:
            print(f"❌ Error placing limit close short order on Binance: {e}")
            return None

    # 取消挂单
    def cancel_futures_limit_order(self, symbol, order_id):
        try:
            self.client.futures_cancel_order(symbol=symbol, orderId=order_id)
        except Exception as e:
            print(f"❌ Binance取消订单时出错: {e}")

    # 查询订单状态是否被fill
    def check_order_filled(self, symbol, order_id):
        try:
            order = self.client.futures_get_order(symbol=symbol, orderId=order_id)
            # print(order)
            return order.get("status") == "FILLED"
        except Exception as e:
            print(f"❌ Error checking order status on Binance: {e}")
            return False

    # 查询订单fill的价格
    def check_fill_price(self, symbol, order_id):
        try:
            order = self.client.futures_get_order(symbol=symbol, orderId=order_id)
            return order['avgPrice']
        except Exception as e:
            print(f"❌ Error checking order status on Binance: {e}")
            return 0

class GateFuturesTrader:

    def __init__(self, gate_key=GATEIO_API_KEY, gate_secret=GATEIO_API_SECRET):

        self.config = Configuration(key=gate_key, secret=gate_secret)
        self.config.proxy = GATE_PROXY

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

    # PLACE a limit order to close position
    def close_future_limit_order(self, symbol, price, direction):
        try:
            return self.futures_api.create_futures_order(
                settle="usdt",
                futures_order={
                    "contract": symbol,
                    "size": 0,
                    "price": str(price),
                    "tif": "gtc",
                    "text": "t-api_limit_close",
                    "reduce_only": True,
                    "close": False,
                    "auto_size": "close_long" if direction == 'long' else "close_short"
                }
            )
        except ApiException as e:
            print(f"❌ Gate 限价平仓下单失败: {e}")
            return None

    # 查询订单状态是否被fill
    def check_order_filled(self, order_id):
        try:
            order = self.futures_api.get_futures_order("usdt", order_id=str(order_id))
            # print(order)
            return order.status == 'finished'
        except ApiException as e:
            print(f"❌ 查询Gate订单状态失败: {e}")
            return False

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

    # PLACE a market order to close position
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

    # 设置止损
    # 先用close_future_limit_order来设置限价平仓的单，再用以下方法设置止损
    def close_future_stop_loss_order(self, symbol, trigger_price, order_price, direction):
        """
        提交 Gate 止损平仓触发订单（价格触发），支持市价或限价
        :param symbol: 例如 'BTC_USDT'
        :param trigger_price: 触发价（例如 entry_price * 0.7）
        :param order_price: 成交价；设为 '0' 代表市价
        :param direction: 'long' 或 'short'，用以确定 order_type 与 auto_size
        """
        try:
            rule = 2 if direction == 'long' else 1  # long: 小于等于触发；short: 大于等于触发

            order = self.futures_api.create_price_triggered_order(
                settle="usdt",
                futures_price_triggered_order={
                    "initial": {
                        "contract": symbol,
                        "size": 0,  # 全部平仓
                        "price": str(order_price),  # 设为 "0" 表示市价
                        "tif": "ioc",  # 市价必须为 ioc
                        "reduce_only": True,
                        "auto_size": "close_long" if direction == 'long' else "close_short",
                        "text": "t-api_stoploss"
                    },
                    "trigger": {
                        "strategy_type": 0,  # 价格触发
                        "price_type": 1,  # 标记价格
                        "price": str(trigger_price),
                        "rule": rule,
                    }
                }
            )
            return order
        except ApiException as e:
            print(f"❌ 创建 Gate 止损订单失败: {e}")
            return None

    # cancel an unfilled limit order
    def cancel_futures_order(self, order_id):
        try:
            self.futures_api.cancel_futures_order("usdt", order_id)
        except ApiException as e:
            print(f"❌ Gate取消订单时出错: {e}")


if __name__ == '__main__':

    bfuture_trader = BFutureTrader()

    # long_order = bfuture_trader.place_market_long_order(symbol='ADAUSDT', quantity=20)
    # close_long_limit_sl = bfuture_trader.setup_long_order_stop_loss(symbol='ADAUSDT',
    #                                                                       quantity=20,
    #                                                                       stop_price=0.56)

    # trades = bfuture_trader.client.futures_account_trades(symbol='ALPACAUSDT', limit=10)
    # print(trades)

    # order = bfuture_trader.place_limit_long_order(symbol='BTCUSDT', quantity=0.004, order_price=30000)
    # print(order)

    # cancel = bfuture_trader.cancel_futures_limit_order(symbol='BTCUSDT', order_id=670217956833)
    # print(cancel)

    # close_long = bfuture_trader.close_limit_long_order(symbol='NKNUSDT', quantity=490, price = 0.04423)
    # close_short = bfuture_trader.close_limit_short_order(symbol='ADAUSDT', quantity=30, price = 0.5)
    #
    # print('close long---------------')
    # print(close_long)
    # print('close short---------------')
    # print(close_short)
    # print(bfuture_trader.check_order_filled(symbol='ADAUSDT', order_id=53130156249))

    # print(bfuture_trader.set_leverage('ADAUSDT',1))
    # print(bfuture_trader.get_available_balance())
    # long_order = bfuture_trader.place_market_long_order(symbol='ADAUSDT', quantity=30)
    # short_order = bfuture_trader.place_market_short_order(symbol='ADAUSDT', quantity=30)
    # print("long order --------------")
    # print(long_order)
    # print("short order --------------")
    # print(short_order)

    # print(bfuture_trader.check_filled_price(symbol='ADAUSDT', order_id=53130156110))

    gfuture_trader = GateFuturesTrader()

    # future_stop_loss = gfuture_trader.close_future_stop_loss_order(symbol='ADA_USDT',
    #                                                                trigger_price=0.56,
    #                                                                order_price=0,
    #                                                                direction='long')

    # gfuture_trader.close_future_limit_order(symbol='ADA_USDT', price=0.8, direction='long')
    # gfuture_trader.close_future_limit_order(symbol='NKN_USDT', price=0.04374, direction='short')

    # print(gfuture_trader.get_available_balance())
    # gfuture_trader.futures_api.update_position_leverage(
    #             settle="usdt",
    #             contract='ADA_USDT',
    #             leverage=1  # 杠杆倍数为字符串类型
    #         )
    # long_order = gfuture_trader.place_future_market_order('ADA_USDT', size=3)
    # short_order = gfuture_trader.place_future_market_order('ADA_USDT', size=-3)
    #
    # print('long order -----------')
    # print(long_order)
    # print('short order ----------')
    # print(short_order)

    # print(gfuture_trader.check_order_filled(order_id='5066550085564938'))

    # #
    # gfuture_trader.place_future_market_order('ADA_USDT', size=2)
    #
    # gate_positions = gfuture_trader.futures_api.list_positions(settle='usdt')
    # # print(gate_positions)
    # pos = [p for p in gate_positions if float(p.size) != 0]
    # # print(pos)
    #
    # print(gfuture_trader.futures_api.list_futures_orders(settle='usdt', status='finished'))
    #
    # print(gfuture_trader.futures_api.list_futures_funding_rate_history(settle='usdt',contract='FUN_USDT'))
    #
    # positions = bfuture_trader.client.futures_account()['positions']
    # active_positions = [p for p in positions if float(p.get("positionAmt", 0)) != 0]
    # print(active_positions)

    # print(bfuture_trader.client.futures_get_all_orders(symbol='FUNUSDT'))




