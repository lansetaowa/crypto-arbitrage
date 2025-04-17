import time
import pandas as pd
import logging
from data import BinanceDataHandler, GateDataHandler, ArbitrageUtils
from future_trade import BFutureTrader, GateFuturesTrader
from config import *
from records import record_trade
from active_positions import *

# 设置日志记录
logging.basicConfig(
    filename='arbitrage.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 记录在套利操作中的交易对与下单信息
# gate空 + binance多，格式：{symbol: {'bi_qty': x, 'gate_size': xx, 'funding_time': datetime, 'bi_entry_price': xx, 'gate_entry_price': xx}}
active_type1 = {}
# gate多 + binance空，格式：{symbol: {'bi_qty': x, 'gate_size': xx, 'funding_time': datetime, 'bi_entry_price': xx, 'gate_entry_price': xx}}
active_type2 = {}


def log_initialization():
    logging.info("Initializing data handlers and traders.")

    bdata_handler = BinanceDataHandler()
    gdata_handler = GateDataHandler()
    bf_trader = BFutureTrader(api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET)
    gf_trader = GateFuturesTrader(gate_key=GATEIO_API_KEY, gate_secret=GATEIO_API_SECRET)

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

# 检查active_type1和active_type2中是否存在资金费率间隔不匹配的symbol，
# 如果资金费率已收取（这里判断当前时间已超过funding_time），则自动平仓。
def check_interval_mismatch(current_time, gf_trader, bf_trader):

    global active_type1, active_type2

    with open("output/mismatch_symbols.txt", "r") as f:
        INTERVAL_MISMATCH_SYMBOLS = [line.strip() for line in f.readlines()]

    if any(s in INTERVAL_MISMATCH_SYMBOLS for s in active_type1.keys()) or \
       any(s in INTERVAL_MISMATCH_SYMBOLS for s in active_type2.keys()):
        for active_dict, pos_type in zip([active_type1, active_type2], ['type1', 'type2']):
            for s in list(active_dict):
                if s in INTERVAL_MISMATCH_SYMBOLS:
                    funding_time = active_dict[s]['funding_time']
                    # 当当前时间超过funding_time 60秒
                    if (current_time - funding_time).total_seconds() >= 60:
                        logging.info("[EXIT INTERVAL MISMATCH] %s close %s after funding received", pos_type.capitalize(), s)
                        bi_qty = active_dict[s]['bi_qty']
                        if pos_type == 'type1':
                            gate_close_order = gf_trader.close_future_market_order(symbol=s.replace("USDT", "_USDT"), auto_size="close_short") # gate平空
                            binance_close_order = bf_trader.close_market_long_order(symbol=s, quantity=bi_qty) # binance平多
                        else:
                            gate_close_order = gf_trader.close_future_market_order(symbol=s.replace("USDT", "_USDT"), auto_size="close_long") # gate平多
                            binance_close_order = bf_trader.close_market_short_order(symbol=s, quantity=bi_qty) # binance平空

                        # 如果两边平仓都成功，则记录订单并删除持仓
                        if gate_close_order and binance_close_order:
                            record_trade(platform="GateIO", order=gate_close_order)
                            record_trade(platform="Binance", order=binance_close_order)

                            del active_dict[s]
                        else:
                            # 如果有一边成功，则对成功一边回滚
                            if gate_close_order and not binance_close_order:
                                logging.error("Close order mismatch for %s: Gate closed successfully, Binance failed", s)
                                record_trade(platform="GateIO", order=gate_close_order)

                            if binance_close_order and not gate_close_order:
                                logging.error("Close order mismatch for %s: Binance closed successfully, Gate failed", s)
                                record_trade(platform="Binance", order=binance_close_order)

                            raise Exception(f"Not both successful to close order mismatch for {s} in check_interval_mismatch")


# 检查已持仓的symbol，当套利信号逆转时平仓。
def check_exit_positions(fr_combined, gf_trader, bf_trader):

    global active_type1, active_type2

    if active_type1:
        for s in list(active_type1):
            fr_diff = fr_combined.loc[fr_combined['symbol'] == s, 'fr_diff'].values[0]
            if fr_diff < 0.0001:
                logging.info("[EXIT] Type1 close %s - fr_diff: %.5f", s, fr_diff)
                gate_close_order = gf_trader.close_future_market_order(symbol=s.replace("USDT","_USDT"), auto_size="close_short") # gate平空
                binance_close_order = bf_trader.close_market_long_order(symbol=s, quantity=active_type1[s]['bi_qty']) # binance平多

                if gate_close_order and binance_close_order:
                    record_trade(platform="GateIO", order=gate_close_order)
                    record_trade(platform="Binance", order=binance_close_order)
                    del active_type1[s]

                else:
                    if gate_close_order and not binance_close_order:
                        record_trade(platform="GateIO", order=gate_close_order)
                        logging.error("Close order mismatch for %s: Gate closed successfully, Binance failed", s)
                        raise Exception(f"Close order mismatch for {s}: Binance failed")

                    elif binance_close_order and not gate_close_order:
                        record_trade(platform="Binance", order=binance_close_order)
                        logging.error("Close order mismatch for %s: Binance closed successfully, Gate failed", s)
                        raise Exception(f"Close order mismatch for {s}: Gate failed")

    if active_type2:
        for s in list(active_type2):
            fr_diff = fr_combined.loc[fr_combined['symbol'] == s, 'fr_diff'].values[0]
            if fr_diff > -0.0001:
                logging.info("[EXIT] Type2 close %s - fr_diff: %.5f", s, fr_diff)
                gate_close_order = gf_trader.close_future_market_order(symbol=s.replace("USDT","_USDT"), auto_size="close_long")
                binance_close_order = bf_trader.close_market_short_order(symbol=s, quantity=active_type2[s]['bi_qty'])

                if gate_close_order and binance_close_order:
                    record_trade(platform="GateIO", order=gate_close_order)
                    record_trade(platform="Binance", order=binance_close_order)
                    del active_type2[s]

                else:
                    if gate_close_order and not binance_close_order:
                        record_trade(platform="GateIO", order=gate_close_order)
                        logging.error("Close order mismatch for %s: Gate closed successfully, Binance failed", s)
                        raise Exception(f"Close order mismatch for {s}: Binance failed")

                    elif binance_close_order and not gate_close_order:
                        record_trade(platform="Binance", order=binance_close_order)
                        logging.error("Close order mismatch for %s: Binance closed successfully, Gate failed", s)
                        raise Exception(f"Close order mismatch for {s}: Gate failed")

# 尝试开仓
def open_new_positions(filtered_fr, next_funding_time, now, bdata_handler, gdata_handler, bf_trader, gf_trader):
    """
    根据筛选后的数据计算套利信号，并尝试开仓：
      - 若满足资金费率差及价差条件则开仓
      - 下单成功后记录持仓和资金费率生效时间
      - 如果其中一边下单失败，回撤已成功的一边，并对回撤结果进行判断
    """
    global active_type1, active_type2

    type1_list = list(filtered_fr[(filtered_fr['fr_diff'] > THRESHOLD)]['symbol'])
    type2_list = list(filtered_fr[(filtered_fr['fr_diff'] < -THRESHOLD)]['symbol'])
    logging.info("Type1 list length: %d; Type2 list length: %d", len(type1_list), len(type2_list))

    # 开Type1仓位：gate-binance 为正
    for s in type1_list:
        if s not in active_type1:

            time_to_funding = (next_funding_time - pd.to_datetime(now, unit='s')).total_seconds()
            if time_to_funding < 30: # 如果距离发放funding的时间不足，就不进行下述步骤了
                logging.warning("[SKIP] Less than 60s to funding for %s, skipping remaining symbols", s)
                break

            # 检查余额以及binance status
            gate_balance = gf_trader.get_available_balance()
            binance_balance = bf_trader.get_available_balance()
            bi_status = bdata_handler.bi_get_contract_info(s)['status']

            if gate_balance >= TRADE_AMOUNT and binance_balance >= TRADE_AMOUNT and bi_status=='TRADING': # 余额足够且binance status valid
                logging.info("[ENTRY] Type1 open %s", s)
                # 设置杠杆
                bf_trader.set_leverage(symbol=s, leverage=1)
                gf_trader.set_leverage(symbol=s.replace("USDT", "_USDT"), leverage=1)
                # 基于trade_amount，计算开仓quantity和gate size
                gate_size, bi_quantity = ArbitrageUtils.calculate_trade_quantity(
                    amount=TRADE_AMOUNT,
                    symbol=s,
                    binance_handler=bdata_handler,
                    gate_handler=gdata_handler
                )
                # 下单
                gate_order = gf_trader.place_future_market_order(symbol=s.replace("USDT", "_USDT"), size=-gate_size) # gate开空
                bi_order = bf_trader.place_market_long_order(symbol=s, quantity=bi_quantity) # binance开多
                # 检查是否下单成功
                if gate_order and bi_order:
                    bi_entry_price = bfuture_trader.check_fill_price(symbol=s, order_id=bi_order['orderId'])
                    active_type1[s] = {'bi_qty': bi_quantity,
                                       'gate_size': gate_size,
                                       'funding_time': next_funding_time,
                                       'bi_entry_price': bi_entry_price,
                                       'gate_entry_price': float(gate_order.fill_price),
                                       'trade_type': 'type1'}
                    logging.info("[CONFIRM] Type1 open success %s", s)
                    record_trade(platform="Binance", order=bi_order)
                    record_trade(platform="GateIO", order=gate_order)
                else:
                    logging.warning("[FAIL] Type1 open failed for %s", s)

                    if gate_order and not bi_order: # binance下单失败，回撤gate的空单
                        record_trade(platform="GateIO", order=gate_order)
                        rollback = gf_trader.close_future_market_order(symbol=s.replace("USDT", "_USDT"), auto_size="close_short")
                        if rollback is None:
                            raise Exception(f"Rollback failed for Gate position on {s} due to Binance failure")
                        logging.info("[ROLLBACK] Gate position closed for %s due to Binance failure", s)
                        record_trade(platform="GateIO", order=rollback)
                    if bi_order and not gate_order: # gate下单失败，回撤binance的多单
                        record_trade(platform="Binance", order=bi_order)
                        rollback = bf_trader.close_market_long_order(symbol=s, quantity=bi_quantity)
                        if rollback is None:
                            raise Exception(f"Rollback failed for Binance position on {s} due to Gate failure")
                        logging.info("[ROLLBACK] Binance position closed for %s due to Gate failure", s)
                        record_trade(platform="Binance", order=rollback)
            else: # 余额不够
                logging.warning("[SKIP] Insufficient balance to open type1 for %s: Gate=%s, Binance=%s", s, gate_balance, binance_balance)

    # 开Type2仓位： gate-binance 为负
    for s in type2_list:
        if s not in active_type2:
            time_to_funding = (next_funding_time - pd.to_datetime(now, unit='s')).total_seconds()
            if time_to_funding < 30: # 如果距离发放funding的时间不足，就不进行下述步骤了
                logging.warning("[SKIP] Less than 60s to funding for %s, skipping remaining symbols", s)
                break

            # 检查余额以及binance status
            gate_balance = gf_trader.get_available_balance()
            binance_balance = bf_trader.get_available_balance()
            bi_status = bdata_handler.bi_get_contract_info(s)['status']

            if gate_balance >= TRADE_AMOUNT and binance_balance >= TRADE_AMOUNT and bi_status=='TRADING': # 余额足够且binance status valid
                logging.info("[ENTRY] Type2 open %s", s)
                # 设置杠杆
                bf_trader.set_leverage(symbol=s, leverage=1)
                gf_trader.set_leverage(symbol=s.replace("USDT", "_USDT"), leverage=1)
                # 基于trade_amount，计算开仓quantity和gate size
                gate_size, bi_quantity = ArbitrageUtils.calculate_trade_quantity(
                    amount=TRADE_AMOUNT,
                    symbol=s,
                    binance_handler=bdata_handler,
                    gate_handler=gdata_handler
                )
                # 下单
                gate_order = gf_trader.place_future_market_order(symbol=s.replace("USDT", "_USDT"), size=gate_size) # gate开多
                bi_order = bf_trader.place_market_short_order(symbol=s, quantity=bi_quantity) # binance开空
                # 检查是否下单成功
                if gate_order and bi_order:
                    bi_entry_price = bfuture_trader.check_fill_price(symbol=s, order_id=bi_order['orderId'])
                    active_type2[s] = {'bi_qty': bi_quantity,
                                       'gate_size': gate_size,
                                       'funding_time': next_funding_time,
                                       'bi_entry_price': bi_entry_price,
                                       'gate_entry_price': float(gate_order.fill_price),
                                       'trade_type': 'type2'}
                    logging.info("[CONFIRM] Type2 open success %s", s)
                    record_trade(platform="Binance", order=bi_order)
                    record_trade(platform="GateIO", order=gate_order)
                else:
                    logging.warning("[FAIL] Type2 open failed for %s", s)
                    if gate_order and not bi_order: # binance下单失败，回撤gate的多单
                        record_trade(platform="GateIO", order=gate_order)
                        rollback = gf_trader.close_future_market_order(symbol=s.replace("USDT", "_USDT"), auto_size="close_long")
                        if rollback is None:
                            raise Exception(f"Rollback failed for Gate position on {s} due to Binance failure")
                        logging.info("[ROLLBACK] Gate position closed for %s due to Binance failure", s)
                        record_trade(platform="GateIO", order=rollback)
                    if bi_order and not gate_order: # gate下单失败，回撤binance的空单
                        record_trade(platform="Binance", order=bi_order)
                        rollback = bf_trader.close_market_short_order(symbol=s, quantity=bi_quantity)
                        if rollback is None:
                            raise Exception(f"Rollback failed for Binance position on {s} due to Gate failure")
                        logging.info("[ROLLBACK] Binance position closed for %s due to Gate failure", s)
                        record_trade(platform="Binance", order=rollback)
            else:
                logging.warning("[SKIP] Insufficient balance to open type2 for %s: Gate=%s, Binance=%s", s, gate_balance, binance_balance)

def run_arbitrage():
    global active_type1, active_type2

    # 初始化
    bdata_handler, gdata_handler, bf_trader, gf_trader = log_initialization()
    # 恢复已有对冲持仓
    active_type1, active_type2 = reinitialize_active_positions(bf_trader, gf_trader)
    print(active_type1)
    print(active_type2)

    while True:
        now = time.time()
        current_time = pd.to_datetime(now, unit='s')
        logging.info("=== New iteration started at %s ===", current_time)

        # 获取并合并资金费率数据
        fr_combined = fetch_funding_data(bdata_handler, gdata_handler)

        # 计算下一次资金费率生效时间及筛选有效symbol
        next_funding_time, till_next, filtered_fr = calc_funding_time(fr_combined, now)

        # 自动平仓funding rate interval mismatch的symbol的仓位
        check_interval_mismatch(current_time, gf_trader, bf_trader)

        # 如果距离资金费率发放足够近，且有有效symbol，则执行后续逻辑
        if till_next < TIME_BUFFER and filtered_fr.shape[0] > 0:

            # 检查反向仓位是否需要平仓
            check_exit_positions(fr_combined, gf_trader, bf_trader)
            # 尝试开新仓
            open_new_positions(filtered_fr, next_funding_time, now, bdata_handler, gdata_handler, bf_trader, gf_trader)

            time.sleep(10)
        else:
            logging.info("No funding event approaching, sleeping for 120 seconds.")
            time.sleep(120)





