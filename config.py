from dotenv import load_dotenv
import os

# load Gateio api
load_dotenv("gate_api.env")
GATEIO_API_KEY = os.getenv('G_KEY')
GATEIO_API_SECRET = os.getenv('G_SECRET')

# load Binance api
load_dotenv("binance_api.env")
BINANCE_API_KEY = os.getenv('B_KEY')
BINANCE_API_SECRET = os.getenv('B_SECRET')

# load proxies
load_dotenv('proxy.env')
BINANCE_PROXY = os.getenv('BINANCE_PROXY')
GATE_PROXY = os.getenv('GATE_PROXY')

THRESHOLD = 0.003  # 套利阈值
EXIT_THRESHOLD = 0.0001  # 平仓阈值
TRADE_AMOUNT = 20  # 单笔交易金额
SLEEP_TIME = 10  # 无套利机会时的等待时间（秒）
LEVERAGE = 1  # 杠杆倍数
SETTLE = "usdt"
TIME_BUFFER = 150

MONITOR_PROFIT_THRESHOLD = 0.0002 # 0.02%
MONITOR_EXIT_TIMEOUT = 4*60*60
MONITOR_POLL_INTERVAL = 30

# # 添加 interval mismatch symbols 列表
# def load_mismatch_symbols(file_path="output/mismatch_symbols.txt"):
#     try:
#         with open(file_path, "r") as f:
#             return [line.strip() for line in f.readlines()]
#     except FileNotFoundError:
#         return []
#
# INTERVAL_MISMATCH_SYMBOLS = load_mismatch_symbols()
# # print(INTERVAL_MISMATCH_SYMBOLS)

