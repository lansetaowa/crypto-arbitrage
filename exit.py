"""
This module monitors depth data and captures good moments and prices to close positions.
"""
import time
from data import BinanceDataHandler, GateDataHandler, ArbitrageUtils
from config import *

from arbitrage import current_position

# 等待一个低点，orderbook挂单平空仓，计算对应的break-even价格后用该价格挂单平多仓，并设置止损
def wait_until_low_and_place_exit_orders(bdata_handler, gdata_handler, bf_trader, gf_trader):
    global current_position

    symbol = current_position['symbol']
    gate_symbol = symbol.replace("USDT", "_USDT")
    trade_type = current_position['trade_type']
    gate_size = current_position['gate_size']
    bi_qty = current_position['bi_qty']
    entry_price_gate = current_position['gate_entry_price']
    entry_price_bi = current_position['bi_entry_price']
    bi_tick_size = bdata_handler.bi_get_contract_info(symbol)['tick_size']

    if trade_type == 'type1': # Gate空，Binance多，伺机先平Gate
        gate_orderbook = gdata_handler.get_gate_orderbook(symbol)
        gate_low = gdata_handler.get_last_n_minutes_low(symbol)

        # Gate 做空，平空价为 ask[0]
        gate_exit_price = gate_orderbook['asks'][0][0]

        if gate_exit_price <= gate_low: # 当前价格在5分钟内最低

            break_even_price = entry_price_bi - entry_price_gate + gate_exit_price
            round_price = ArbitrageUtils.adjust_price_to_tick(break_even_price, bi_tick_size)
            gate_order = gf_trader.close_future_limit_order(gate_symbol, price=gate_exit_price, direction='short')
            bi_order = bf_trader.close_limit_long_order(symbol, quantity=bi_qty, price=round_price)
            print(f"[DEBUG] Gate order: {gate_order}")
            print(f"[DEBUG] Binance order: {bi_order}")
            print(f"[EXIT] {symbol} 限价平仓单已下: Gate={gate_exit_price}, Binance={round_price}")
            stop_loss_price = ArbitrageUtils.adjust_price_to_tick(entry_price_bi * STOP_LOSS, bi_tick_size)
            bi_sl = bf_trader.setup_long_order_stop_loss(symbol, quantity=bi_qty, stop_price=stop_loss_price)
            print(f"[DEBUG] Binance stop loss order: {bi_sl}")
            print(f"[EXIT] {symbol} Binance止损已下: 止损价格={stop_loss_price}")
            if gate_order and bi_order:
                print(f"[EXIT] type1 平仓限价单已下达 {symbol}")
                current_position['gate_order_id'] = gate_order.id
                current_position['bin_order_id'] = bi_order['orderId']
                current_position['sl_id'] = bi_sl['orderId']
                return True

    elif trade_type == 'type2': # Gate做多，Binance做空，伺机先平Binance

        bi_orderbook = bdata_handler.get_binance_orderbook(symbol)
        bi_low = bdata_handler.get_last_n_minutes_low(symbol)

        # Binance 做空，平空价为 ask[0]
        bi_exit_price = float(bi_orderbook['asks'][0][0])

        if bi_exit_price <= bi_low: # 当前价格在5分钟内最低
            break_even_price = - entry_price_bi + bi_exit_price + entry_price_gate
            gate_order = gf_trader.close_future_limit_order(gate_symbol, price=break_even_price, direction='long') # Gate平多
            bi_order = bf_trader.close_limit_short_order(symbol, quantity=bi_qty, price=bi_exit_price) # Binance平空
            print(f"[DEBUG] Gate order: {gate_order}")
            print(f"[DEBUG] Binance order: {bi_order}")
            print(f"[EXIT] {symbol} 限价平仓单已下: Gate={break_even_price}, Binance={bi_exit_price}")
            stop_loss_price = entry_price_gate * STOP_LOSS
            gate_sl = gf_trader.close_future_stop_loss_order(symbol=gate_symbol, trigger_price=stop_loss_price,
                                                   order_price="0", direction="long")
            print(f"[DEBUG] Gate stop loss order: {gate_sl}")
            print(f"[EXIT] {symbol} Gate止损已下: 止损价格={stop_loss_price}")
            if gate_order and bi_order:
                print(f"[EXIT] type2 平仓限价单已下达 {symbol}")
                current_position['gate_order_id'] = gate_order.id
                current_position['bin_order_id'] = bi_order['orderId']
                current_position['sl_id'] = gate_sl.id
                return True

    return False

# 监控实时pnl，判断是否超过阈值，并下限价单平仓
# def wait_until_pnl_and_place_orders(bdata_handler, gdata_handler, bf_trader, gf_trader):
#     global current_position
#
#     symbol = current_position['symbol']
#     gate_symbol = symbol.replace("USDT", "_USDT")
#     trade_type = current_position['trade_type']
#     gate_entry_price = float(current_position['gate_entry_price'])
#     bi_entry_price = float(current_position['bi_entry_price'])
#     bi_qty = current_position['bi_qty']
#
#     print(f"[MONITOR] 开始监控 {symbol} 实时 PnL")
#
#     while True:
#         gate_orderbook = gdata_handler.get_gate_orderbook(symbol)
#         bi_orderbook = bdata_handler.get_binance_orderbook(symbol)
#
#         if not gate_orderbook or not bi_orderbook:
#             print("[MONITOR] 无法获取 orderbook，等待后重试...")
#             time.sleep(3)
#             continue
#
#         normalized_pnl = ArbitrageUtils.calculate_worst_case_pnl(
#             entry_price_gate=gate_entry_price,
#             entry_price_binance=bi_entry_price,
#             trade_type=trade_type,
#             orderbook_gate=gate_orderbook,
#             orderbook_binance=bi_orderbook
#         )
#         print(f"[MONITOR] 当前 worst-case PnL: {normalized_pnl:.6f}")
#
#         if normalized_pnl >= MONITOR_PROFIT_THRESHOLD:
#             print(f"[MONITOR] 达到平仓阈值 {normalized_pnl:.6f}，准备下限价单")
#
#             gate_exit_price = float(gate_orderbook['asks'][0][0]) if trade_type == 'type1' else float(gate_orderbook['bids'][0][0])
#             binance_exit_price = float(bi_orderbook['bids'][0][0]) if trade_type == 'type1' else float(bi_orderbook['asks'][0][0])
#
#             try:
#                 if trade_type == 'type1':
#                     gate_order = gf_trader.close_future_limit_order(gate_symbol, price=gate_exit_price, direction='short')
#                     bi_order = bf_trader.close_limit_long_order(symbol, quantity=bi_qty, price=binance_exit_price)
#                     print(f"[DEBUG] Gate order: {gate_order}")
#                     print(f"[DEBUG] Binance order: {bi_order}")
#                     print(f"[EXIT] {symbol} 限价平仓单已下: Gate={gate_exit_price}, Binance={binance_exit_price}")
#                     if gate_order and bi_order:
#                         print(f"[EXIT] 平仓限价单已下达 {symbol}")
#                         current_position['exit_time'] = time.time()
#                         current_position['gate_order_id'] = gate_order.id
#                         current_position['bin_order_id'] = bi_order['orderId']
#                         return True
#
#                 elif trade_type == 'type2':
#                     gate_order = gf_trader.close_future_limit_order(gate_symbol, price=gate_exit_price, direction='long')
#                     bi_order = bf_trader.close_limit_short_order(symbol, quantity=bi_qty, price=binance_exit_price)
#                     print(f"[DEBUG] Gate order: {gate_order}")
#                     print(f"[DEBUG] Binance order: {bi_order}")
#                     print(f"[EXIT] {symbol} 限价平仓单已下: Gate={gate_exit_price}, Binance={binance_exit_price}")
#                     if gate_order and bi_order:
#                         print(f"[EXIT] 平仓限价单已下达 {symbol}")
#                         current_position['exit_time'] = time.time()
#                         current_position['gate_order_id'] = gate_order.id
#                         current_position['bin_order_id'] = bi_order['orderId']
#                         return True
#
#             except Exception as e:
#                 print(f"❌ Error placing limit close order: {e}")
#                 return False
#
#         time.sleep(3)

# 通过 gate 订单簿计算 binance 限价价并挂单
# def place_break_even_exit_orders(bdata_handler,gdata_handler, bf_trader, gf_trader):
#     global current_position
#
#     symbol = current_position['symbol']
#     gate_symbol = symbol.replace("USDT", "_USDT")
#     gate_orderbook = gdata_handler.get_gate_orderbook(symbol)
#     bi_orderbook = bdata_handler.get_binance_orderbook(symbol)
#
#     if not gate_orderbook:
#         print(f"[EXIT] 无法获取 Gate orderbook: {symbol}")
#         return False
#
#     bi_entry = float(current_position['bi_entry_price'])
#     gate_entry = float(current_position['gate_entry_price'])
#     direction = current_position['trade_type']
#     bi_qty = current_position['bi_qty']
#     bi_tick_size = bdata_handler.bi_get_contract_info(symbol)['tick_size']
#
#     if direction == 'type1': # type1: gate 平空、binance 平多，先用gate_orderbook平gate用ask1
#         gate_exit_price = float(gate_orderbook['asks'][0][0])
#         break_even_price = bi_entry - gate_entry + gate_exit_price
#         round_price = ArbitrageUtils.adjust_price_to_tick(break_even_price, bi_tick_size)
#         gate_order = gf_trader.close_future_limit_order(gate_symbol, price=gate_exit_price, direction='short')
#         bi_order = bf_trader.close_limit_long_order(symbol, quantity=bi_qty, price=round_price)
#         print(f"[DEBUG] Gate order: {gate_order}")
#         print(f"[DEBUG] Binance order: {bi_order}")
#         print(f"[EXIT] {symbol} 限价平仓单已下: Gate={gate_exit_price}, Binance={round_price}")
#         if gate_order and bi_order:
#             print(f"[EXIT] 平仓限价单已下达 {symbol}")
#             current_position['exit_time'] = time.time()
#             current_position['gate_order_id'] = gate_order.id
#             current_position['bin_order_id'] = bi_order['orderId']
#             return True
#
#     elif direction == 'type2': # type2: gate 平多、binance 平空，先用bi_orderbook平binance用ask1
#         bi_exit_price = float(bi_orderbook['asks'][0][0])
#         # gate_exit_price = float(gate_orderbook['bids'][0][0])
#         break_even_price = - bi_entry + bi_exit_price + gate_entry
#         round_price = ArbitrageUtils.adjust_price_to_tick(break_even_price, bi_tick_size)
#         gate_order = gf_trader.close_future_limit_order(gate_symbol, price=round_price, direction='long')
#         bi_order = bf_trader.close_limit_short_order(symbol, quantity=bi_qty, price=bi_exit_price)
#         print(f"[DEBUG] Gate order: {gate_order}")
#         print(f"[DEBUG] Binance order: {bi_order}")
#         print(f"[EXIT] {symbol} 限价平仓单已下: Gate={round_price}, Binance={bi_exit_price}")
#         if gate_order and bi_order:
#             print(f"[EXIT] 平仓限价单已下达 {symbol}")
#             current_position['exit_time'] = time.time()
#             current_position['gate_order_id'] = gate_order.id
#             current_position['bin_order_id'] = bi_order['orderId']
#             return True
#
#     return False

# 超时未成交强制市价平
# 超时强制市价平仓
# def force_market_exit(bf_trader, gf_trader):
#     global current_position
#
#     symbol = current_position['symbol']
#     gate_symbol = symbol.replace("USDT", "_USDT")
#     trade_type = current_position['trade_type']
#     bi_qty = current_position['bi_qty']
#
#     gate_exit_order_id = current_position['gate_order_id']
#     bi_exit_order_id = current_position['bin_order_id']
#
#     try:
#         gate_filled = gf_trader.check_order_filled(gate_exit_order_id)
#         bin_filled = bf_trader.check_order_filled(symbol, bi_exit_order_id)
#
#         if gate_filled and bin_filled:
#             print(f"[FORCE] {symbol} 限价单已全部成交，无需强平")
#             return True
#
#         print(f"[FORCE] {symbol} 超时未完成，执行强制市价平仓")
#
#         if not gate_filled:
#             print(f"[FORCE] {symbol} Gate 限价未成交，取消并市价平仓")
#             gf_trader.cancel_futures_order(gate_exit_order_id)
#             if trade_type == 'type1':
#                 gf_trader.close_future_market_order(gate_symbol, auto_size='close_short')
#             elif trade_type == 'type2':
#                 gf_trader.close_future_market_order(gate_symbol, auto_size='close_long')
#
#         if not bin_filled:
#             print(f"[FORCE] {symbol} Binance 限价未成交，取消并市价平仓")
#             bf_trader.cancel_futures_limit_order(symbol, bi_exit_order_id)
#             if trade_type == 'type1':
#                 bf_trader.close_market_long_order(symbol, bi_qty)
#             elif trade_type == 'type2':
#                 bf_trader.close_market_short_order(symbol, bi_qty)
#
#         current_position.clear()
#         return True
#
#     except Exception as e:
#         print(f"[FORCE] 强制市价平仓失败 {symbol}: {e}")

# exit主流程

def manage_exit(bdata_handler, gdata_handler, bf_trader, gf_trader):
    global current_position

    print(f"[MANAGE] Start manage_exit loop...")

    symbol = current_position['symbol']
    funding_time = current_position['funding_time']
    now = time.time()
    if now < funding_time.timestamp() + 30:
        print(f"[WAIT] 资金费率未发放，暂不平仓 {symbol}")
        return False

    print(f"[MANAGE] 资金费率已发放尝试限价平仓 {symbol}...")
    success = wait_until_low_and_place_exit_orders(bdata_handler, gdata_handler, bf_trader, gf_trader)
    if not success:
        print(f"[MANAGE] {symbol} 限价下单失败")
        return False

    gate_order_id = current_position['gate_order_id']
    bin_order_id = current_position['bin_order_id']
    sl_id = current_position['sl_id']
    trade_type = current_position['trade_type']

    print(f"[MONITOR] 开始监控 {symbol} 平仓订单和止损订单是否成交...")

    while True:
        try:
            gate_filled = gf_trader.check_order_filled(gate_order_id)
            bin_filled = bf_trader.check_order_filled(symbol, bin_order_id)
            print(f'[MANAGE] Binance status is {bin_filled}, and Gate status is {gate_filled}')
            if gate_filled and bin_filled:
                print(f"[MANAGE] {symbol} 平仓完成")
                current_position.clear()
                return True

            else:
                if trade_type == 'type1':
                    sl_filled = bf_trader.check_order_filled(symbol, sl_id)
                    print(f"[DEBUG] Binance stop loss status is {sl_filled}")
                    if sl_filled and gate_filled:
                        print(f"[STOP LOSS] {symbol} Binance止损触发，退出当前仓位 ❗")
                        current_position.clear()
                        return True
                elif trade_type == 'type2':
                    sl_filled = gf_trader.check_price_order_filled(sl_id)
                    print(f"[DEBUG] Gate stop loss status is {sl_filled}")
                    if sl_filled and bin_filled:
                        print(f"[STOP LOSS] {symbol} Gate止损触发，退出当前仓位 ❗")
                        current_position.clear()
                        return True

            time.sleep(MONITOR_FILL_INTERVAL)

        except Exception as e:
            print(f"[MANAGE] 查询成交状态失败 {symbol}: {e}")
            # break
            time.sleep(MONITOR_FILL_INTERVAL)


if __name__ == '__main__':

    from active_positions import reinitialize_active_positions
    from future_trade import BFutureTrader, GateFuturesTrader
    bf_trader = BFutureTrader()
    gf_trader = GateFuturesTrader()
    bdata_handler = BinanceDataHandler()
    gdata_handler = GateDataHandler()

    from arbitrage import current_position
    #
    # active_type1, active_type2 = reinitialize_active_positions(bf_trader, gf_trader)
    # print(active_type1, active_type2)
    #
    # current_position.update({
    #     'symbol': 'ALPACAUSDT',
    #     **active_type2['ALPACAUSDT']
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



