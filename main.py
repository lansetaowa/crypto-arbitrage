from arbitrage import run_arbitrage
import time
import datetime
import threading
from data import BinanceDataHandler, GateDataHandler, ArbitrageUtils
from arbitrage import run_arbitrage

# 每天更新funding rate interval mismatch列表
def schedule_daily_update():
    last_update_date = None

    while True:
        now = datetime.datetime.now()
        today = now.date()

        if now.hour >= 12 and today != last_update_date:
            ArbitrageUtils.update_interval_mismatch_list()
            last_update_date = today
            print(f"[SCHEDULE] Interval mismatch list updated at {now}")

        time.sleep(360)

if __name__ == "__main__":

    # 后台线程：每天更新funding rate interval mismatch列表
    update_thread = threading.Thread(target=schedule_daily_update, daemon=True)
    update_thread.start()

    print("🚀 Starting funding rate arbitrage monitor...")
    run_arbitrage()

