import logging
import pandas as pd

# bf_trader 和 gf_trader 分别为 BFutureTrader 与 GateFuturesTrader 实例

# ------------------- 基础持仓获取 ------------------- #
def get_active_binance_positions(bf_trader):
    """从 Binance 获取当前持仓（非0仓位），返回列表（字典格式）。"""
    positions = bf_trader.client.futures_account().get('positions', [])
    active_positions = [p for p in positions if float(p.get("positionAmt", 0)) != 0]
    return active_positions

def get_active_gate_positions(gf_trader):
    """从 GateIO 获取当前持仓（非0仓位），返回列表（对象或字典格式）。"""
    gate_positions_raw = gf_trader.futures_api.list_positions(settle='usdt')
    active_positions = [p for p in gate_positions_raw if float(p.size) != 0]
    return active_positions

def get_union_active_symbols(bin_positions, gate_positions):
    """从 Binance 与 GateIO 的 active 持仓中，返回统一格式（例如 'BTCUSDT'）的 symbol 集合。"""
    bin_symbols = set([p["symbol"] for p in bin_positions])
    gate_symbols = set([p.contract.replace("_","") for p in gate_positions])
    union_symbols = bin_symbols.intersection(gate_symbols)

    return union_symbols

# ------------------- 订单与下单时间查询 ------------------- #
def get_recent_binance_order(bf_trader, symbol):
    """
    查询 Binance 某 symbol 最新订单（假设返回列表，取最后一笔）。
    返回订单字典，包含下单数量、side、订单时间等。
    """
    orders = bf_trader.client.futures_get_all_orders(symbol=symbol, limit=1)
    if orders:
        order = orders[-1] # 返回列表中最后一笔为最新
        return order
    else:
        return None

def get_recent_gate_order(gf_trader, symbol):
    """
    查询 GateIO 某 symbol 最新订单。由于 GateIO 的合约格式通常为 "BTC_USDT"，
    此处将 symbol 转换为合约格式，然后查询。
    """
    # 转换为 GateIO 格式：例如 "BTCUSDT" → "BTC_USDT"
    contract = symbol[:-4] + "_USDT" if symbol.endswith("USDT") else symbol
    try:
        orders = gf_trader.futures_api.list_futures_orders(settle='usdt',status='finished',contract=contract)
        if orders:
            order = orders[-1]
            return order
    except Exception as e:
        return None

# ------------------- Funding 时间查询 ------------------- #
def get_binance_funding_time(bf_trader, symbol, order):
    """
    直接从 Binance 订单对象中提取下单时间，然后查询 funding history，
    返回下单后最近一笔 funding 记录的 fundingTime（datetime 类型）。
    """
    ts = int(order.get("time", 0))
    order_time = pd.to_datetime(ts, unit='ms')
    start_ms = int(order_time.timestamp() * 1000)

    history = bf_trader.client.futures_funding_rate(symbol=symbol, startTime=start_ms, limit=1)
    if history:
        ft = int(history[0].get("fundingTime", 0))
        return pd.to_datetime(ft, unit='ms')

def get_gate_funding_time(gf_trader, contract, order):
    """
    直接从 GateIO 订单对象中提取下单时间，然后查询 funding history，
    返回下单后最近一笔 funding 记录的时间（datetime 类型）。
    """
    order_time = pd.to_datetime(order.create_time, unit='s')
    start_s = int(order_time.timestamp())

    history = gf_trader.futures_api.list_futures_funding_rate_history(settle='usdt', contract=contract)
    if history:
        filtered = [r for r in history if int(r.t) > start_s]
        if filtered:
            record = min(filtered, key=lambda r: int(r.t) - start_s)
            return pd.to_datetime(int(record.t), unit='s')
    else:
        return None

def choose_funding_time(bin_ft, gate_ft):
    if gate_ft == bin_ft:
        return gate_ft
    else:
        print('Unexpected funding time, Gate funding time is returned.')
        return gate_ft

# ------------------- 主逻辑函数 ------------------- #
def reinitialize_active_positions(bf_trader, gf_trader):
    """
    重新初始化 active 持仓：
    """
    active_type1 = {}
    active_type2 = {}

    bin_positions = get_active_binance_positions(bf_trader)
    gate_positions = get_active_gate_positions(gf_trader)
    union_symbols = get_union_active_symbols(bin_positions, gate_positions)

    for symbol in union_symbols:
        # 查询 Binance 最新订单，qty，和position side
        bin_order = get_recent_binance_order(bf_trader, symbol)
        bi_qty = float(bin_order.get("executedQty", 0))
        side = bin_order.get("positionSide")
        bi_entry_price = float(bin_order.get("avgPrice", bin_order.get("price", 0)))

        # 查询 GateIO 最新订单
        gate_order = get_recent_gate_order(gf_trader, symbol)
        gate_size = float(gate_order.size)
        gate_entry_price = float(gate_order.fill_price)

        # 查询 funding 时间
        bin_ft = get_binance_funding_time(bf_trader, symbol, bin_order)

        # 转换 symbol 为 GateIO 合约格式
        contract = symbol[:-4] + "_USDT" if symbol.endswith("USDT") else symbol.replace("USDT", "_USDT")
        gate_ft = get_gate_funding_time(gf_trader, contract, gate_order)
        funding_time = choose_funding_time(bin_ft, gate_ft)

        # 判断套利类型：以 Binance 订单 side 与 GateIO 的 size 判断
        if side == "LONG" and gate_size < 0:
            active_type1[symbol] = {
                'bi_qty': bi_qty,
                'gate_size': gate_size,
                'funding_time': funding_time,
                'bi_entry_price': bi_entry_price,
                'gate_entry_price': gate_entry_price,
            }

        elif side == "SHORT" and gate_size > 0:
            active_type2[symbol] = {
                'bi_qty': abs(bi_qty),
                'gate_size': gate_size,
                'funding_time': funding_time,
                'bi_entry_price': bi_entry_price,
                'gate_entry_price': gate_entry_price,
            }
        else:
            print(f"{symbol} orders do not match expected hedge structure")

    return active_type1, active_type2

if __name__ == '__main__':

    from dotenv import load_dotenv
    import os
    from future_trade import *

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
    gfuture_trader = GateFuturesTrader(gate_key=GATEIO_API_KEY, gate_secret=GATEIO_API_SECRET)
    #
    # g_order = get_recent_gate_order(gfuture_trader, 'FUNUSDT')
    # print(g_order)
    # print(pd.to_datetime(g_order.create_time, unit='s'))
    # print(g_order.size)
    # #
    # b_order = get_recent_binance_order(bfuture_trader, 'FUNUSDT')
    # print(b_order)
    # print(pd.to_datetime(b_order['time'], unit='ms'))
    # print(b_order['positionSide'])
    # #
    # b_funding_time = get_binance_funding_time(bfuture_trader, 'FUNUSDT', order=b_order)
    # print(b_funding_time)
    #
    # g_funding_time = get_gate_funding_time(gfuture_trader, 'FUN_USDT', g_order)
    # print(g_funding_time)

    # print(reinitialize_active_positions(bfuture_trader, gfuture_trader))
