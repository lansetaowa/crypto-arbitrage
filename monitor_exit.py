"""
This module monitors depth data and captures good moments and prices to close positions.
"""
import time
from data import BinanceDataHandler, GateDataHandler, ArbitrageUtils
from config import *

# 判断资金费率差是否反转
def should_trigger_exit(symbol, trade_type, bdata_handler, gdata_handler, threshold=MONITOR_PROFIT_THRESHOLD):
    try:
        bin_fr = bdata_handler.get_funding_rate(symbol=symbol)
        gate_symbol = symbol.replace("USDT",'_USDT')
        gate_fr = gdata_handler.get_funding_rate(symbol=gate_symbol)
        fr_diff = float(gate_fr) - float(bin_fr)  # gate - binance
        print(f"[CHECK] {symbol} funding rate diff: {fr_diff} (gate: {gate_fr}, binance: {bin_fr})")

        if trade_type == "type1":
            return fr_diff < threshold
        elif trade_type == "type2":
            return fr_diff > -threshold
        else:
            print("trade_type must be 'type1' or 'type2'")
            return False

    except Exception as e:
        print(f"[ERROR] 获取资金费率失败 {symbol}: {e}")
        return False

# 通过 gate 订单簿计算 binance 限价价并挂单
def place_break_even_exit_orders(symbol, record, gdata_handler, bf_trader, gf_trader):
    gate_symbol = symbol.replace("USDT", "_USDT")
    gate_orderbook = gdata_handler.get_gate_orderbook(symbol)
    if not gate_orderbook:
        print(f"[EXIT] 无法获取 Gate orderbook: {symbol}")
        return None, None

    bi_entry = float(record['bi_entry_price'])
    gate_entry = float(record['gate_entry_price'])
    direction = record['trade_type']
    bi_qty = record['bi_qty']

    if direction == 'type1': # type1: gate 平空、binance 平多，gate用ask1
        gate_exit_price = float(gate_orderbook['asks'][0][0])
        break_even_price = bi_entry + (gate_entry - gate_exit_price)
        gate_order = gf_trader.close_future_limit_order(gate_symbol, price=gate_exit_price, direction='short')
        bin_order = bf_trader.close_limit_long_order(symbol, quantity=bi_qty, price=break_even_price)
        print(f"[EXIT] {symbol} 限价平仓单已下: Gate={gate_exit_price}, Binance={break_even_price}")
        return gate_order, bin_order
    elif direction == 'type2': # type2: gate 平多、binance 平空，gate用bid1
        gate_exit_price = float(gate_orderbook['bids'][0][0])
        break_even_price = bi_entry - (gate_exit_price - gate_entry)
        gate_order = gf_trader.close_future_limit_order(gate_symbol, price=gate_exit_price, direction='long')
        bin_order = bf_trader.close_limit_short_order(symbol, quantity=bi_qty, price=break_even_price)
        print(f"[EXIT] {symbol} 限价平仓单已下: Gate={gate_exit_price}, Binance={break_even_price}")
        return gate_order, bin_order

# 超时未成交强制市价平
def enforce_market_exit_if_timeout(active_type_dict, bf_trader, gf_trader, active_type_lock):
    now = time.time()
    with active_type_lock:
        # 仅读取持仓快照，避免长时间占用锁
        symbols_snapshot = active_type_dict

    for symbol, record in list(symbols_snapshot.items()):
        if 'exit_time' not in record:
            continue

        gate_symbol = symbol.replace("USDT", "_USDT")
        try:
            gate_filled = gf_trader.check_order_filled(record['gate_order_id'])
            bin_filled = bf_trader.check_order_filled(symbol, record['bin_order_id'])

            if gate_filled and bin_filled:
                print(f"[FORCE] {symbol} 限价单已全部成交，移除持仓")
                with active_type_lock:
                    del active_type_dict[symbol]
                continue

            if now - record['exit_time'] <= MONITOR_EXIT_TIMEOUT:
                continue  # 未超时，继续等待

            print(f"[FORCE] {symbol} 超时未完成，执行强制市价平仓")

            if not gate_filled:
                print(f"[FORCE] {symbol} Gate 限价未成交，取消并市价平仓")
                gf_trader.cancel_futures_order(record['gate_order_id'])
                if record['trade_type'] == 'type1':
                    gf_trader.close_future_market_order(gate_symbol, auto_size='close_short')
                elif record['trade_type'] == 'type2':
                    gf_trader.close_future_market_order(gate_symbol, auto_size='close_long')
                with active_type_lock:
                    del active_type_dict[symbol]

            if not bin_filled:
                print(f"[FORCE] {symbol} Binance 限价未成交，取消并市价平仓")
                bf_trader.cancel_binance_order(symbol, record['bin_order_id'])
                if record['trade_type'] == 'type1':
                    bf_trader.close_market_long_order(symbol, record['bi_qty'])
                elif record['trade_type'] == 'type2':
                    bf_trader.close_market_short_order(symbol, record['bi_qty'])
                with active_type_lock:
                    del active_type_dict[symbol]

        except Exception as e:
            print(f"[FORCE] 强制市价平仓失败 {symbol}: {e}")

#
# # 根据当前订单簿和持仓记录判断最差盈亏情况
# def evaluate_exit_profit(symbol, active_record, bdata_handler, gdata_handler):
#     # 获取实时订单簿深度
#     orderbook_binance = bdata_handler.get_binance_orderbook(symbol, limit=5)
#     orderbook_gate = gdata_handler.get_gate_orderbook(symbol, limit=5)
#
#     if not orderbook_binance or not orderbook_gate:
#         return None  # 无法获取深度
#
#     trade_type = active_record.get('trade_type')
#     bi_entry_price = active_record.get('bi_entry_price')
#     gate_entry_price = active_record.get('gate_entry_price')
#
#     pnl = ArbitrageUtils.calculate_worst_case_pnl(
#         entry_price_gate=gate_entry_price,
#         entry_price_binance=bi_entry_price,
#         trade_type=trade_type,
#         orderbook_gate=orderbook_gate,
#         orderbook_binance=orderbook_binance
#     )
#     return pnl
#
# # 限价单监控函数
# def monitor_limit_order(bf_trader, gf_trader, symbol, binance_order, gate_order, timeout=120):
#     start = time.time()
#     bin_filled, gate_filled = False, False
#
#     while time.time() - start < timeout:
#         if not bin_filled:
#             bin_filled = bf_trader.check_order_filled(symbol, binance_order.get('orderId'))
#         if not gate_filled:
#             gate_filled = gf_trader.check_order_filled(gate_order.id)
#
#         if bin_filled and gate_filled:
#             # print('both binance and gate orders are filled')
#             return True, bin_filled, gate_filled  # 都已fill
#         time.sleep(5)
#
#     print(f"binance order status is {bin_filled}, gate order status is {gate_filled}")
#     return False, bin_filled, gate_filled  # 超时返回各平台的fill情况

# 主循环监控函数
def monitor_exit_loop(bdata_handler, gdata_handler, bf_trader, gf_trader, active_type_dict, active_type_lock):
    print("[MONITOR] 平仓监控线程已启动")
    while True:
        with active_type_lock:
            # 仅读取持仓快照，避免长时间占用锁
            symbols_snapshot = active_type_dict

        # 检查是否需要平仓
        for symbol, record in list(symbols_snapshot.items()):
            if 'exit_time' in record: # 已触发限价平仓，是否成交由 enforce_market_exit_if_timeout() 处理
                # 此处无需重复处理，直接跳过即可
                continue

            # 尚未发起平仓的
            elif 'exit_time' not in record:
                # 检查是否触发退出条件
                if should_trigger_exit(symbol, record['trade_type'], bdata_handler, gdata_handler):
                    print(f"[MONITOR] {symbol} 触发平仓条件，开始平仓逻辑")
                    gate_order, bin_order = place_break_even_exit_orders(symbol, record, gdata_handler,
                                                                         bf_trader, gf_trader)
                    if gate_order and bin_order:
                        print(f'[MONITOR] {symbol} 平仓限价单已下')
                        record['exit_time'] = time.time()
                        record['gate_order_id'] = gate_order.id
                        record['bin_order_id'] = bin_order['orderId']

        enforce_market_exit_if_timeout(active_type_dict, bf_trader, gf_trader, active_type_lock)

        time.sleep(MONITOR_POLL_INTERVAL)


if __name__ == '__main__':

    # from active_positions import reinitialize_active_positions
    # from future_trade import BFutureTrader, GateFuturesTrader
    # bf_trader = BFutureTrader()
    # gf_trader = GateFuturesTrader()
    #
    # active_type1, active_type2 = reinitialize_active_positions(bf_trader, gf_trader)
    # print(active_type1, active_type2)
    #
    # bdata_handler = BinanceDataHandler()
    # gdata_handler = GateDataHandler()
    #
    # print(should_trigger_exit(symbol='ETHUSDT', trade_type='type1',bdata_handler=bdata_handler, gdata_handler=gdata_handler))

    # active_type1 = {'ADAUSDT': {'bi_qty': 30.0, 'gate_size': -3.0, 'funding_time': '2025-04-17 00:00:00',
    #              'bi_entry_price': 0.5959, 'gate_entry_price': 0.6087, 'trade_type': 'type1'}}
    #
    # pnl = evaluate_exit_profit(symbol='ADAUSDT', active_record=active_type1['ADAUSDT'], bdata_handler=bdata_handler, gdata_handler=gdata_handler)
    # print(pnl)

    # monitor_exit_loop(bdata_handler, gdata_handler, bf_trader, gf_trader, active_type1)

    pass



