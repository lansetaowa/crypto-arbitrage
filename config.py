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

# load Telegram bot token
load_dotenv('telegram_bot.env')
arbi_alarm = os.getenv("ArbiAlarmBot")
tele_chatid = os.getenv("chat_id")

THRESHOLD = 0.0005  # 套利阈值
TRADE_AMOUNT = 20  # 单边交易金额
LEVERAGE = 1  # 杠杆倍数
SETTLE = "usdt"
TIME_BUFFER = 90 # 里资金费率生效时间还剩多久开始套利流程
LOOP_INTERVAL = 120 # 无套利机会时的等待时间（秒）

MONITOR_PROFIT_THRESHOLD = 0.0005 # pnl判断平仓的阈值
# MONITOR_EXIT_TIMEOUT = 5*60 # 平仓的限价单多久没有fill就强制平仓
MONITOR_FILL_INTERVAL = 25

STOP_LOSS = 0.7


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

