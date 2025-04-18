from arbitrage import run_arbitrage
import time
import datetime
import threading
from data import BinanceDataHandler, GateDataHandler, ArbitrageUtils
from arbitrage import run_arbitrage



if __name__ == "__main__":

    print("🚀 Starting funding rate arbitrage monitor...")
    run_arbitrage()

