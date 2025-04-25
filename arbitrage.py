import time
import pandas as pd
import logging
from data import BinanceDataHandler, GateDataHandler, ArbitrageUtils
from future_trade import BFutureTrader, GateFuturesTrader
from config import *

# 设置日志记录
import os
from datetime import datetime

log_dir = "log"
log_filename = os.path.join(log_dir, f"arbitrage_log_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 记录在套利操作中的交易对与下单信息
# current_position = {symbol: x, 'bi_qty': x, 'gate_size': xx, 'funding_time': datetime, 'bi_entry_price': xx, 'gate_entry_price': xx, 'trade_type': type1 or type2}
current_position = {}

def log_initialization():
    logging.info("Initializing data handlers and traders.")

    bdata_handler = BinanceDataHandler()
    gdata_handler = GateDataHandler()
    bf_trader = BFutureTrader()
    gf_trader = GateFuturesTrader()

    return bdata_handler, gdata_handler, bf_trader, gf_trader

def fetch_funding_data(bdata_handler, gdata_handler):

    bi_df = bdata_handler.bi_get_funding_rates()
    gate_df = gdata_handler.gate_get_funding_rates()
    fr_combined = ArbitrageUtils.merge_funding_rates(bi_df, gate_df)
    logging.info("Merged funding rates data: %d rows", fr_combined.shape[0])

    return fr_combined

def calc_funding_time(fr_combined, now):

    next_funding_time = ArbitrageUtils.get_next_funding_time(fr_combined)
    current_time = pd.to_datetime(now, unit='s')
    till_next = (next_funding_time - current_time).total_seconds()
    logging.info("Next funding time: %s, time until funding: %.2f seconds", next_funding_time, till_next)

    filtered = ArbitrageUtils.filter_next_funding_symbols(merged_df=fr_combined, next_funding_time=next_funding_time)
    logging.info("Filtered symbols for next funding: %d rows", filtered.shape[0])

    return next_funding_time, till_next, filtered

# 选取fr_diff绝对值最大的symbol
def select_best_symbol(filtered_df):
    filtered_df = filtered_df.copy()
    filtered_df['abs_diff'] = filtered_df['fr_diff'].abs()
    best = filtered_df.sort_values('abs_diff', ascending=False).iloc[0]

    return best['symbol'], best['fr_diff']

# 开一个仓，选取最有利可图的
def open_new_position(symbol, fr_diff, next_funding_time, bdata_handler, gdata_handler, bf_trader, gf_trader):
    global current_position

    logging.info(f"[ENTRY] 开始开仓尝试 symbol={symbol}, fr_diff={fr_diff}")
    gate_symbol = symbol.replace("USDT", "_USDT")
    gate_balance = gf_trader.get_available_balance()
    binance_balance = bf_trader.get_available_balance()

    if gate_balance < TRADE_AMOUNT or binance_balance < TRADE_AMOUNT:
        print(f"[SKIP] {symbol} 资金不足，跳过")
        logging.warning(f"[SKIP] {symbol} 资金不足，跳过 - Gate余额: {gate_balance}, Binance余额: {binance_balance}")
        return False

    # 设置杠杆为1
    gf_trader.set_leverage(gate_symbol, leverage=1)
    bf_trader.set_leverage(symbol, leverage=1)

    # 计算qty/size
    gate_size, bi_quantity = ArbitrageUtils.calculate_trade_quantity(
        amount=TRADE_AMOUNT,
        symbol=symbol,
        binance_handler=bdata_handler,
        gate_handler=gdata_handler
    )

    if fr_diff > 0:
        # Gate 空，Binance 多
        logging.info(f"[ENTRY] 开仓方向：type1 - Gate做空, Binance做多")
        gate_order = gf_trader.place_future_market_order(gate_symbol, size=-gate_size)
        bi_order = bf_trader.place_market_long_order(symbol, quantity=bi_quantity)
        trade_type = "type1"
        print(f"[DEBUG] Gate order: {gate_order}")
        print(f"[DEBUG] Binance order: {bi_order}")

    else:
        # Gate 多，Binance 空
        logging.info(f"[ENTRY] 开仓方向：type2 - Gate做多, Binance做空")
        gate_order = gf_trader.place_future_market_order(gate_symbol, size=gate_size)
        bi_order = bf_trader.place_market_short_order(symbol, quantity=bi_quantity)
        trade_type = "type2"
        print(f"[DEBUG] Gate order: {gate_order}")
        print(f"[DEBUG] Binance order: {bi_order}")

    # 检查是否下单成功
    if gate_order and bi_order:
        bi_entry_price = bf_trader.check_fill_price(symbol=symbol, order_id=bi_order['orderId'])
        print(f"[ENTRY] 开仓成功 {symbol}, type={trade_type}, fr_diff={fr_diff}")
        logging.info(f"[ENTRY] 开仓成功 {symbol} | Gate价格={gate_order.fill_price}, Binance价格={bi_entry_price}, type={trade_type}")
        current_position = {
            'symbol': symbol,
            'trade_type': trade_type,
            'gate_size': gate_size,
            'bi_qty': bi_quantity,
            'gate_entry_price': float(gate_order.fill_price),
            'bi_entry_price': float(bi_entry_price),
            'funding_time': next_funding_time
        }
        return True

    else:
        print(f"[ERROR] 下单失败 {symbol}, 正在尝试回滚")
        logging.error(f"[ERROR] {symbol} 开仓失败，开始执行回滚操作")
        if gate_order and not bi_order:
            if trade_type == "type1":
                logging.info(f"[ROLLBACK] Gate做空订单回滚 {symbol}")
                gf_trader.close_future_market_order(gate_symbol, auto_size="close_short")
            elif trade_type == 'type2':
                logging.info(f"[ROLLBACK] Gate做多订单回滚 {symbol}")
                gf_trader.close_future_market_order(gate_symbol, auto_size="close_long")
        elif bi_order and not gate_order:
            if trade_type == "type1":
                logging.info(f"[ROLLBACK] Binance做多订单回滚 {symbol}")
                bf_trader.close_market_long_order(symbol, quantity=bi_quantity)
            elif trade_type == 'type2':
                logging.info(f"[ROLLBACK] Binance做空订单回滚 {symbol}")
                bf_trader.close_market_short_order(symbol, quantity=bi_quantity)

        return False



# 尝试开仓
# def open_new_positions(filtered_fr, next_funding_time, now, bdata_handler, gdata_handler, bf_trader, gf_trader):
#     """
#     根据筛选后的数据计算套利信号，并尝试开仓：
#       - 若满足资金费率差及价差条件则开仓
#       - 下单成功后记录持仓和资金费率生效时间
#       - 如果其中一边下单失败，回撤已成功的一边，并对回撤结果进行判断
#     """
#     global active_type1, active_type2
#
#     type1_list = list(filtered_fr[(filtered_fr['fr_diff'] > THRESHOLD)]['symbol'])
#     type2_list = list(filtered_fr[(filtered_fr['fr_diff'] < -THRESHOLD)]['symbol'])
#     logging.info("Type1 list length: %d; Type2 list length: %d", len(type1_list), len(type2_list))
#
#     # 开Type1仓位：gate-binance 为正
#     for s in type1_list:
#         if s not in active_type1:
#
#             time_to_funding = (next_funding_time - pd.to_datetime(now, unit='s')).total_seconds()
#             if time_to_funding < 30: # 如果距离发放funding的时间不足，就不进行下述步骤了
#                 logging.warning("[SKIP] Less than 60s to funding for %s, skipping remaining symbols", s)
#                 break
#
#             # 检查余额以及binance status
#             gate_balance = gf_trader.get_available_balance()
#             binance_balance = bf_trader.get_available_balance()
#             bi_status = bdata_handler.bi_get_contract_info(s)['status']
#             print(f"[DEBUG] Checking Type2 {s}: gate_balance={gate_balance}, binance_balance={binance_balance}, status={bi_status}")
#
#             if gate_balance >= TRADE_AMOUNT and binance_balance >= TRADE_AMOUNT and bi_status=='TRADING': # 余额足够且binance status valid
#                 logging.info("[ENTRY] Type1 open %s", s)
#                 # 设置杠杆
#                 bf_trader.set_leverage(symbol=s, leverage=1)
#                 gf_trader.set_leverage(symbol=s.replace("USDT", "_USDT"), leverage=1)
#                 # 基于trade_amount，计算开仓quantity和gate size
#                 gate_size, bi_quantity = ArbitrageUtils.calculate_trade_quantity(
#                     amount=TRADE_AMOUNT,
#                     symbol=s,
#                     binance_handler=bdata_handler,
#                     gate_handler=gdata_handler
#                 )
#                 # 下单
#                 gate_order = gf_trader.place_future_market_order(symbol=s.replace("USDT", "_USDT"), size=-gate_size) # gate开空
#                 bi_order = bf_trader.place_market_long_order(symbol=s, quantity=bi_quantity) # binance开多
#                 print(f"[DEBUG] Gate order: {gate_order}")
#                 print(f"[DEBUG] Binance order: {bi_order}")
#
#                 # 检查是否下单成功
#                 if gate_order is not None and bi_order is not None:
#                     try:
#                         bi_entry_price = bf_trader.check_fill_price(symbol=s, order_id=bi_order['orderId'])
#                         print(f"[DEBUG] Binance fill price: {bi_entry_price}")
#                         active_type1[s] = {
#                             'bi_qty': bi_quantity,
#                             'gate_size': gate_size,
#                             'funding_time': next_funding_time,
#                             'bi_entry_price': bi_entry_price,
#                             'gate_entry_price': float(gate_order.fill_price),
#                             'trade_type': 'type1'
#                         }
#                         logging.info("[CONFIRM] Type1 open success %s", s)
#                         print("[CONFIRM] Type1 open success %s", s)
#                     except Exception as e:
#                         logging.error(f"[ERROR] 更新 active_type1 失败 {s}: {e}")
#                         print(f"[ERROR] 更新 active_type1 失败 {s}: {e}")
#                 else:
#                     logging.warning("[FAIL] Type1 open failed for %s", s)
#
#                     if gate_order and not bi_order: # binance下单失败，回撤gate的空单
#                         rollback = gf_trader.close_future_market_order(symbol=s.replace("USDT", "_USDT"), auto_size="close_short")
#                         if rollback is None:
#                             raise Exception(f"Rollback failed for Gate position on {s} due to Binance failure")
#                         logging.info("[ROLLBACK] Gate position closed for %s due to Binance failure", s)
#                         print("[ROLLBACK] Gate position closed for %s due to Binance failure", s)
#                     if bi_order and not gate_order: # gate下单失败，回撤binance的多单
#                         rollback = bf_trader.close_market_long_order(symbol=s, quantity=bi_quantity)
#                         if rollback is None:
#                             raise Exception(f"Rollback failed for Binance position on {s} due to Gate failure")
#                         logging.info("[ROLLBACK] Binance position closed for %s due to Gate failure", s)
#                         print("[ROLLBACK] Binance position closed for %s due to Gate failure", s)
#             else: # 余额不够
#                 logging.warning("[SKIP] Insufficient balance to open type1 for %s: Gate=%s, Binance=%s", s, gate_balance, binance_balance)
#
#     # 开Type2仓位： gate-binance 为负
#     for s in type2_list:
#         if s not in active_type2:
#             time_to_funding = (next_funding_time - pd.to_datetime(now, unit='s')).total_seconds()
#             if time_to_funding < 30: # 如果距离发放funding的时间不足，就不进行下述步骤了
#                 logging.warning("[SKIP] Less than 60s to funding for %s, skipping remaining symbols", s)
#                 break
#
#             # 检查余额以及binance status
#             gate_balance = gf_trader.get_available_balance()
#             binance_balance = bf_trader.get_available_balance()
#             bi_status = bdata_handler.bi_get_contract_info(s)['status']
#             print(f"[DEBUG] Checking Type2 {s}: gate_balance={gate_balance}, binance_balance={binance_balance}, status={bi_status}")
#
#             if gate_balance >= TRADE_AMOUNT and binance_balance >= TRADE_AMOUNT and bi_status=='TRADING': # 余额足够且binance status valid
#                 logging.info("[ENTRY] Type2 open %s", s)
#                 # 设置杠杆
#                 bf_trader.set_leverage(symbol=s, leverage=1)
#                 gf_trader.set_leverage(symbol=s.replace("USDT", "_USDT"), leverage=1)
#                 # 基于trade_amount，计算开仓quantity和gate size
#                 gate_size, bi_quantity = ArbitrageUtils.calculate_trade_quantity(
#                     amount=TRADE_AMOUNT,
#                     symbol=s,
#                     binance_handler=bdata_handler,
#                     gate_handler=gdata_handler
#                 )
#                 # 下单
#                 gate_order = gf_trader.place_future_market_order(symbol=s.replace("USDT", "_USDT"), size=gate_size) # gate开多
#                 bi_order = bf_trader.place_market_short_order(symbol=s, quantity=bi_quantity) # binance开空
#                 print(f"[DEBUG] Gate order: {gate_order}")
#                 print(f"[DEBUG] Binance order: {bi_order}")
#                 # 检查是否下单成功
#                 if gate_order is not None and bi_order is not None:
#                     try:
#                         bi_entry_price = bf_trader.check_fill_price(symbol=s, order_id=bi_order['orderId'])
#                         print(f"[DEBUG] Binance fill price: {bi_entry_price}")
#                         active_type2[s] = {'bi_qty': bi_quantity,
#                                            'gate_size': gate_size,
#                                            'funding_time': next_funding_time,
#                                            'bi_entry_price': bi_entry_price,
#                                            'gate_entry_price': float(gate_order.fill_price),
#                                            'trade_type': 'type2'}
#                         logging.info("[CONFIRM] Type2 open success %s", s)
#                         print("[CONFIRM] Type2 open success %s", s)
#                     except Exception as e:
#                         logging.error(f"[ERROR] 更新 active_type2 失败 {s}: {e}")
#                         print(f"[ERROR] 更新 active_type2 失败 {s}: {e}")
#                 else:
#                     logging.warning("[FAIL] Type2 open failed for %s", s)
#                     if gate_order and not bi_order: # binance下单失败，回撤gate的多单
#                         rollback = gf_trader.close_future_market_order(symbol=s.replace("USDT", "_USDT"), auto_size="close_long")
#                         if rollback is None:
#                             raise Exception(f"Rollback failed for Gate position on {s} due to Binance failure")
#                         logging.info("[ROLLBACK] Gate position closed for %s due to Binance failure", s)
#                     if bi_order and not gate_order: # gate下单失败，回撤binance的空单
#                         rollback = bf_trader.close_market_short_order(symbol=s, quantity=bi_quantity)
#                         if rollback is None:
#                             raise Exception(f"Rollback failed for Binance position on {s} due to Gate failure")
#                         logging.info("[ROLLBACK] Binance position closed for %s due to Gate failure", s)
#             else:
#                 logging.warning("[SKIP] Insufficient balance to open type2 for %s: Gate=%s, Binance=%s", s, gate_balance, binance_balance)

# def run_arbitrage():
#     global active_type1, active_type2
#
#     # 初始化
#     bdata_handler, gdata_handler, bf_trader, gf_trader = log_initialization()
#     # 恢复已有对冲持仓
#     active_type1, active_type2 = reinitialize_active_positions(bf_trader, gf_trader)
#     print(active_type1)
#     print(active_type2)
#
#     while True:
#         now = time.time()
#         current_time = pd.to_datetime(now, unit='s')
#         logging.info("=== New iteration started at %s ===", current_time)
#
#         fr_combined = fetch_funding_data(bdata_handler, gdata_handler) # 获取并合并资金费率数据
#         next_funding_time, till_next, filtered_fr = calc_funding_time(fr_combined, now) # 计算下一次资金费率生效时间及筛选有效symbol
#
#         # 如果距离资金费率发放足够近，且有有效symbol，则执行后续逻辑
#         if till_next < TIME_BUFFER and filtered_fr.shape[0] > 0:
#             # 尝试开新仓
#             active_type_lock.acquire()
#             open_new_positions(filtered_fr, next_funding_time, now, bdata_handler, gdata_handler, bf_trader, gf_trader)
#             active_type_lock.release()
#
#             time.sleep(10)
#         else:
#             # logging.info("No funding event approaching, sleeping for 120 seconds.")
#             time.sleep(120)





