import os
import csv
import json

def record_trade(platform, order):
    """
    将交易记录写入 records/trade_records.csv 文件中。
    记录两列：platform 和 order（以 JSON 格式存储）。
    """
    record_dir = "records"
    if not os.path.exists(record_dir):
        os.makedirs(record_dir)
    filename = os.path.join(record_dir, "trade_records.csv")

    fieldnames = ["platform", "order"]
    file_exists = os.path.exists(filename)

    with open(filename, mode="a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        # 将 order 对象转换成字符串
        order_str = str(order)

        writer.writerow({
            "platform": platform,
            "order": order_str
        })

if __name__ == '__main__':
    from dotenv import load_dotenv
    import os
    from future_trade import GateFuturesTrader, BFutureTrader

    # # load Gateio api
    # load_dotenv("gate_api.env")
    # GATEIO_API_KEY = os.getenv('G_KEY')
    # GATEIO_API_SECRET = os.getenv('G_SECRET')
    #
    # gfuture_trader = GateFuturesTrader(gate_key=GATEIO_API_KEY, gate_secret=GATEIO_API_SECRET)
    # g_order = gfuture_trader.place_future_limit_order('BTC_USDT', size=1, price=30000)
    # print(g_order)
    # print(type(g_order))
    # print(str(g_order))
    #
    #
    # # load Binance api
    # load_dotenv("binance_api.env")
    # BINANCE_API_KEY = os.getenv('B_KEY')
    # BINANCE_API_SECRET = os.getenv('B_SECRET')
    # #
    # bfuture_trader = BFutureTrader(api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET)
    # b_order = bfuture_trader.place_limit_long_order(symbol='BTCUSDT', quantity=0.002, order_price=50000)
    # print(b_order)
    # print(type(b_order))
    # print(str(b_order))
    # print(json.dumps(b_order, default=lambda o: o.__dict__, ensure_ascii=False))
