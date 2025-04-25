"""
This module monitors depth data and captures good moments and prices to close positions.
"""
import time
from data import BinanceDataHandler, GateDataHandler, ArbitrageUtils
from config import *

from arbitrage import current_position

# 判断资金费率差是否反转
# def should_trigger_exit(symbol, trade_type, bdata_handler, gdata_handler, threshold=MONITOR_PROFIT_THRESHOLD):
#     try:
#         bin_fr = bdata_handler.get_funding_rate(symbol=symbol)
#         gate_symbol = symbol.replace("USDT",'_USDT')
#         gate_fr = gdata_handler.get_funding_rate(symbol=gate_symbol)
#         fr_diff = float(gate_fr) - float(bin_fr)  # gate - binance
#         print(f"[CHECK] {symbol} funding rate diff: {fr_diff} (gate: {gate_fr}, binance: {bin_fr})")
#
#         if trade_type == "type1":
#             return fr_diff < threshold
#         elif trade_type == "type2":
#             return fr_diff > -threshold
#         else:
#             print("trade_type must be 'type1' or 'type2'")
#             return False
#
#     except Exception as e:
#         print(f"[ERROR] 获取资金费率失败 {symbol}: {e}")
#         return False

# 通过 gate 订单簿计算 binance 限价价并挂单
def place_break_even_exit_orders(bdata_handler,gdata_handler, bf_trader, gf_trader):
    global current_position

    symbol = current_position['symbol']
    gate_symbol = symbol.replace("USDT", "_USDT")
    gate_orderbook = gdata_handler.get_gate_orderbook(symbol)

    if not gate_orderbook:
        print(f"[EXIT] 无法获取 Gate orderbook: {symbol}")
        return False

    bi_entry = float(current_position['bi_entry_price'])
    gate_entry = float(current_position['gate_entry_price'])
    direction = current_position['trade_type']
    bi_qty = current_position['bi_qty']
    bi_tick_size = bdata_handler.bi_get_contract_info(symbol)['tick_size']

    if direction == 'type1': # type1: gate 平空、binance 平多，gate用ask1
        gate_exit_price = float(gate_orderbook['asks'][0][0])
        break_even_price = bi_entry - gate_entry + gate_exit_price
        round_price = round(break_even_price/bi_tick_size) * bi_tick_size
        gate_order = gf_trader.close_future_limit_order(gate_symbol, price=gate_exit_price, direction='short')
        bi_order = bf_trader.close_limit_long_order(symbol, quantity=bi_qty, price=round_price)
        print(f"[DEBUG] Gate order: {gate_order}")
        print(f"[DEBUG] Binance order: {bi_order}")
        print(f"[EXIT] {symbol} 限价平仓单已下: Gate={gate_exit_price}, Binance={round_price}")
        if gate_order and bi_order:
            print(f"[EXIT] 平仓限价单已下达 {symbol}")
            current_position['exit_time'] = time.time()
            current_position['gate_order_id'] = gate_order.id
            current_position['bin_order_id'] = bi_order['orderId']
            return True

    elif direction == 'type2': # type2: gate 平多、binance 平空，gate用bid1
        gate_exit_price = float(gate_orderbook['bids'][0][0])
        break_even_price = bi_entry + gate_exit_price - gate_entry
        round_price = round(break_even_price / bi_tick_size) * bi_tick_size
        gate_order = gf_trader.close_future_limit_order(gate_symbol, price=gate_exit_price, direction='long')
        bi_order = bf_trader.close_limit_short_order(symbol, quantity=bi_qty, price=round_price)
        print(f"[DEBUG] Gate order: {gate_order}")
        print(f"[DEBUG] Binance order: {bi_order}")
        print(f"[EXIT] {symbol} 限价平仓单已下: Gate={gate_exit_price}, Binance={round_price}")
        if gate_order and bi_order:
            print(f"[EXIT] 平仓限价单已下达 {symbol}")
            current_position['exit_time'] = time.time()
            current_position['gate_order_id'] = gate_order.id
            current_position['bin_order_id'] = bi_order['orderId']
            return True

    return False

# 超时未成交强制市价平
def force_market_exit(bf_trader, gf_trader):
    global current_position

    symbol = current_position['symbol']
    gate_symbol = symbol.replace("USDT", "_USDT")
    trade_type = current_position['trade_type']
    bi_qty = current_position['bi_qtr']

    gate_exit_order_id = current_position['gate_order_id']
    bi_exit_order_id = current_position['bin_order_id']

    try:
        gate_filled = gf_trader.check_order_filled(gate_exit_order_id)
        bin_filled = bf_trader.check_order_filled(symbol, bi_exit_order_id)

        if gate_filled and bin_filled:
            print(f"[FORCE] {symbol} 限价单已全部成交，无需强平")
            return

        print(f"[FORCE] {symbol} 超时未完成，执行强制市价平仓")

        if not gate_filled:
            print(f"[FORCE] {symbol} Gate 限价未成交，取消并市价平仓")
            gf_trader.cancel_futures_order(gate_exit_order_id)
            if trade_type == 'type1':
                gf_trader.close_future_market_order(gate_symbol, auto_size='close_short')
            elif trade_type == 'type2':
                gf_trader.close_future_market_order(gate_symbol, auto_size='close_long')

        if not bin_filled:
            print(f"[FORCE] {symbol} Binance 限价未成交，取消并市价平仓")
            bf_trader.cancel_binance_order(symbol, bi_exit_order_id)
            if trade_type == 'type1':
                bf_trader.close_market_long_order(symbol, bi_qty)
            elif trade_type == 'type2':
                bf_trader.close_market_short_order(symbol, bi_qty)

    except Exception as e:
        print(f"[FORCE] 强制市价平仓失败 {symbol}: {e}")

# exit主流程
def manage_exit(bdata_handler, gdata_handler, bf_trader, gf_trader):
    global current_position

    symbol = current_position['symbol']
    funding_time = current_position['funding_time']
    now = time.time()
    if now < funding_time.timestamp():
        print(f"[WAIT] 资金费率未发放，暂不平仓 {symbol}")
        return False

    print(f"[MANAGE] 尝试限价平仓 {symbol}")
    success = place_break_even_exit_orders(bdata_handler, gdata_handler, bf_trader, gf_trader)
    if not success:
        print(f"[MANAGE] {symbol} 限价下单失败")
        return False

    timeout_start = time.time()
    while time.time() - timeout_start <= MONITOR_EXIT_TIMEOUT:
        try:
            gate_filled = gf_trader.check_order_filled(current_position['gate_order_id'])
            bin_filled = bf_trader.check_order_filled(symbol, current_position['bin_order_id'])
            if gate_filled and bin_filled:
                print(f"[MANAGE] {symbol} 平仓完成")
                return True
            else:
                time.sleep(60)
        except Exception as e:
            print(f"[MANAGE] 查询成交状态失败 {symbol}: {e}")
            break

    print(f"[MANAGE] {symbol} 超时未成交，执行强制平仓")
    force_market_exit(bf_trader, gf_trader)
    return True


if __name__ == '__main__':

    from active_positions import reinitialize_active_positions
    from future_trade import BFutureTrader, GateFuturesTrader
    bf_trader = BFutureTrader()
    gf_trader = GateFuturesTrader()
    bdata_handler = BinanceDataHandler()
    gdata_handler = GateDataHandler()

    from arbitrage import current_position

    active_type1, active_type2 = reinitialize_active_positions(bf_trader, gf_trader)
    print(active_type1, active_type2)

    # current_position.update({
    #     'symbol': 'LPTUSDT',
    #     **active_type2['LPTUSDT']
    # })
    #
    # manage_exit(bdata_handler, gdata_handler, bf_trader, gf_trader)





    #
    # print(should_trigger_exit(symbol='ETHUSDT', trade_type='type1',bdata_handler=bdata_handler, gdata_handler=gdata_handler))

    # active_type1 = {'ADAUSDT': {'bi_qty': 30.0, 'gate_size': -3.0, 'funding_time': '2025-04-17 00:00:00',
    #              'bi_entry_price': 0.5959, 'gate_entry_price': 0.6087, 'trade_type': 'type1'}}
    #
    # pnl = evaluate_exit_profit(symbol='ADAUSDT', active_record=active_type1['ADAUSDT'], bdata_handler=bdata_handler, gdata_handler=gdata_handler)
    # print(pnl)

    # monitor_exit_loop(bdata_handler, gdata_handler, bf_trader, gf_trader, active_type1)

    # pass



