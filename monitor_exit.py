"""
This module monitors depth data and captures good moments and prices to close positions.
"""
import time
import logging
from data import BinanceDataHandler, GateDataHandler, ArbitrageUtils

# 根据当前订单簿和持仓记录判断最差盈亏情况
def evaluate_exit_profit(symbol, active_record, bdata_handler, gdata_handler):
    # 获取实时订单簿深度
    orderbook_binance = bdata_handler.get_binance_orderbook(symbol, limit=5)
    orderbook_gate = gdata_handler.get_gate_orderbook(symbol, limit=5)

    if not orderbook_binance or not orderbook_gate:
        return None  # 无法获取深度

    trade_type = active_record.get('trade_type')
    bi_entry_price = active_record.get('bi_entry_price')
    gate_entry_price = active_record.get('gate_entry_price')

    pnl = ArbitrageUtils.calculate_worst_case_pnl(
        entry_price_gate=gate_entry_price,
        entry_price_binance=bi_entry_price,
        trade_type=trade_type,
        orderbook_gate=orderbook_gate,
        orderbook_binance=orderbook_binance
    )
    return pnl

# 限价单监控函数
def monitor_limit_order(bf_trader, gf_trader, symbol, binance_order, gate_order, timeout=120):
    start = time.time()
    bin_filled, gate_filled = False, False

    while time.time() - start < timeout:
        if not bin_filled:
            bin_filled = bf_trader.check_order_filled(symbol, binance_order.get('orderId'))
        if not gate_filled:
            gate_filled = gf_trader.check_order_filled(gate_order.id)

        if bin_filled and gate_filled:
            # print('both binance and gate orders are filled')
            return True, bin_filled, gate_filled  # 都已fill
        time.sleep(5)

    print(f"binance order status is {bin_filled}, gate order status is {gate_filled}")
    return False, bin_filled, gate_filled  # 超时返回各平台的fill情况

# 主循环监控函数
def monitor_exit_loop(bdata_handler, gdata_handler, bf_trader, gf_trader, active_type_dict,
                      profit_threshold=0.001, exit_timeout=120, poll_interval=5):
    """
    :param active_type_dict: active_type1 或 active_type2 字典，包含所有持仓记录
    :param profit_threshold: 当最差盈亏比达到该阈值时，尝试平仓（例如 0.1% 收益）
    :param exit_timeout: 限价单等待时间（秒），超时则转市价
    :param poll_interval: 监控轮询间隔
    """
    while True:
        for symbol, record in list(active_type_dict.items()):
            pnl = evaluate_exit_profit(symbol, record, bdata_handler, gdata_handler)
            if pnl is None:
                print(f"[MONITOR] 无法获取 {symbol} 的深度信息，跳过")
                continue

            print(f"[MONITOR] {symbol} 当前worst-case pnl: {pnl:.5f}")

            # 满足条件（盈利超过阈值时）尝试平仓
            if pnl >= profit_threshold or pnl <= -profit_threshold:
                print(f"[EXIT] {symbol} 达到平仓条件，尝试以限价单平仓")

                trade_type = record.get('trade_type')
                gate_symbol = symbol.replace("USDT", "_USDT")

                # 根据 trade_type 发起对应的限价单
                if trade_type == 'type1':
                    # type1: gate 平空、binance 平多，用ask1/bid1
                    bin_best_bid = bdata_handler.get_binance_orderbook(symbol)['bids'][0][0]
                    gate_best_ask = gdata_handler.get_gate_orderbook(symbol)['asks'][0][0]
                    gate_close_order = gf_trader.close_future_limit_order(symbol=gate_symbol, price=gate_best_ask, direction='short')
                    binance_close_order = bf_trader.close_limit_long_order(symbol=symbol, quantity=record['bi_qty'], price=bin_best_bid)
                    print(gate_close_order)
                    print(binance_close_order)
                    if gate_close_order and binance_close_order:
                        print(f'[EXIT] {symbol} type1 平仓限价单下单成功')
                elif trade_type == 'type2':
                    # type2: gate 平多、binance 平空，用ask1/bid1
                    bin_best_ask = bdata_handler.get_binance_orderbook(symbol)['asks'][0][0]
                    gate_best_bid = gdata_handler.get_gate_orderbook(symbol)['bids'][0][0]
                    gate_close_order = gf_trader.close_future_limit_order(symbol=gate_symbol,price=gate_best_bid,direction='long')
                    binance_close_order = bf_trader.close_limit_short_order(symbol=symbol,quantity=record['bi_qty'],price=bin_best_ask)
                    print(gate_close_order)
                    print(binance_close_order)
                    if gate_close_order and binance_close_order:
                        print(f'[EXIT] {symbol} type2 平仓限价单下单成功')
                else:
                    print(f"[EXIT] {symbol} trade_type 未识别")
                    continue

                # 监控限价单是否成交
                success, bin_filled, gate_filled = monitor_limit_order(bf_trader, gf_trader, symbol,
                                              binance_close_order, gate_close_order, timeout=exit_timeout)
                if not success:
                    print(f"[EXIT] 限价单超时，{symbol} 尝试市价平仓")
                    # 依次调用市价单平仓接口补救
                    if not bin_filled:
                        bf_trader.client.futures_cancel_order(symbol=symbol, orderId=binance_close_order['orderId'])
                        if trade_type == 'type1':
                            binance_close_order = bf_trader.close_market_long_order(symbol, record['bi_qty'])
                        else:
                            binance_close_order = bf_trader.close_market_short_order(symbol, record['bi_qty'])
                        print(f"[EXIT] Binance市价平仓订单结果: {binance_close_order}")
                    else:
                        print("[EXIT] Binance限价单已成交，无需市价平仓")

                        # 如果Gate未成交，取消限价单并执行市价平仓
                    if not gate_filled:
                        gf_trader.cancel_futures_order(gate_close_order.order_id)
                        if trade_type == 'type1':
                            gate_close_order = gf_trader.close_future_market_order(gate_symbol, auto_size="close_short")
                        else:
                            gate_close_order = gf_trader.close_future_market_order(gate_symbol, auto_size="close_long")
                        print(f"[EXIT] Gate市价平仓订单结果: {gate_close_order}")
                    else:
                        print("[EXIT] Gate限价单已成交，无需市价平仓")
                else:
                    print(f"[EXIT] {symbol} 限价单完全成交，无需补救")

                # 无论何种方式平仓成功后，从 active 持仓中剔除
                del active_type_dict[symbol]
                print(active_type_dict)

        time.sleep(poll_interval)


if __name__ == '__main__':

    from active_positions import reinitialize_active_positions
    from future_trade import BFutureTrader, GateFuturesTrader
    bf_trader = BFutureTrader()
    gf_trader = GateFuturesTrader()

    active_type1, active_type2 = reinitialize_active_positions(bf_trader, gf_trader)
    print(active_type1, active_type2)

    bdata_handler = BinanceDataHandler()
    gdata_handler = GateDataHandler()

    # active_type1 = {'ADAUSDT': {'bi_qty': 30.0, 'gate_size': -3.0, 'funding_time': '2025-04-17 00:00:00',
    #              'bi_entry_price': 0.5959, 'gate_entry_price': 0.6087, 'trade_type': 'type1'}}
    #
    # pnl = evaluate_exit_profit(symbol='ADAUSDT', active_record=active_type1['ADAUSDT'], bdata_handler=bdata_handler, gdata_handler=gdata_handler)
    # print(pnl)

    # monitor_exit_loop(bdata_handler, gdata_handler, bf_trader, gf_trader, active_type2)



