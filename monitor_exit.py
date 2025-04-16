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

# 主循环监控函数（建议在单独的线程中运行）
def monitor_exit_loop(bdata_handler, gdata_handler, bf_trader, gf_trader, active_type_dict,
                      profit_threshold=0.001, exit_timeout=20, poll_interval=5):
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
                logging.error(f"[MONITOR] 无法获取 {symbol} 的深度信息，跳过")
                continue

            logging.info(f"[MONITOR] {symbol} 当前worst-case pnl: {pnl:.5f}")

            # 满足条件（盈利超过阈值时）尝试平仓
            if pnl >= profit_threshold:
                logging.info(f"[EXIT] {symbol} 达到平仓条件，尝试以限价单平仓")
                # 根据 trade_type 发起对应的限价单
                if record.get('trade_type') == 'type1':
                    # type1: gate 平空、binance 平多，用ask1/bid1
                    bin_best_bid = bdata_handler.get_binance_orderbook(symbol)['bids'][0][0]
                    gate_best_ask = gdata_handler.get_gate_orderbook(symbol)['asks'][0][0]
                    gate_close_order = gf_trader.close_future_limit_order(
                        symbol=symbol.replace("USDT", "_USDT"),
                        price=gate_best_ask,
                        direction='short'
                    )
                    binance_close_order = bf_trader.close_limit_long_order(
                        symbol=symbol,
                        quantity=record['bi_qty'],
                        price=bin_best_bid
                    )
                elif record.get('trade_type') == 'type2':
                    # type2: gate 平多、binance 平空，用ask1/bid1
                    bin_best_ask = bdata_handler.get_binance_orderbook(symbol)['asks'][0][0]
                    gate_best_bid = gdata_handler.get_gate_orderbook(symbol)['bids'][0][0]
                    gate_close_order = gf_trader.close_future_limit_order(
                        symbol=symbol.replace("USDT", "_USDT"),
                        price=gate_best_bid,
                        direction='long'
                    )
                    binance_close_order = bf_trader.close_limit_short_order(
                        symbol=symbol,
                        quantity=record['bi_qty'],
                        price=bin_best_ask
                    )
                else:
                    logging.error(f"[EXIT] {symbol} trade_type 未识别")
                    continue

                # 监控限价单是否成交
                success = monitor_limit_order(bf_trader, gf_trader, symbol,
                                              binance_close_order, gate_close_order, timeout=exit_timeout)
                if not success:
                    logging.warning(f"[EXIT] 限价单超时，{symbol} 尝试市价平仓")
                    # 依次调用市价单平仓接口补救
                    if record.get('trade_type') == 'type1':
                        gate_close_order = gf_trader.close_future_market_order(
                            symbol=symbol.replace("USDT", "_USDT"),
                            auto_size="close_short"
                        )
                        binance_close_order = bf_trader.close_market_long_order(
                            symbol=symbol,
                            quantity=record['bi_qty']
                        )
                    else:
                        gate_close_order = gf_trader.close_future_market_order(
                            symbol=symbol.replace("USDT", "_USDT"),
                            auto_size="close_long"
                        )
                        binance_close_order = bf_trader.close_market_short_order(
                            symbol=symbol,
                            quantity=record['bi_qty']
                        )
                    logging.info(f"[EXIT] 市价平仓订单：Gate: {gate_close_order}, Binance: {binance_close_order}")
                else:
                    logging.info(f"[EXIT] {symbol} 限价单平仓成功")
                # 无论何种方式平仓成功后，从 active 持仓中剔除
                del active_type_dict[symbol]

        time.sleep(poll_interval)

# 限价单监控函数
def monitor_limit_order(bf_trader, gf_trader, symbol, binance_order, gate_order, timeout=3600):
    start = time.time()
    while time.time() - start < timeout:
        bin_filled = bf_trader.check_order_filled(symbol, binance_order.get('orderId'))
        gate_filled = gf_trader.check_order_filled(gate_order.order_id)
        if bin_filled and gate_filled:
            return True
        time.sleep(10)

    return False

if __name__ == '__main__':
    pass