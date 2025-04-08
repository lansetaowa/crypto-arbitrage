# Crypto Arbitrage Bot

This project is an automated crypto arbitrage bot designed to exploit funding rate differentials between **Binance Futures** and **Gate.io Perpetual Futures**. It identifies short-term funding rate opportunities and executes hedged long/short positions across the two platforms, aiming to earn the funding spread.

## 🚀 Features

- 📈 **Funding Rate Arbitrage** between Binance and Gate.io USDT-margined perpetual contracts
- 🧠 **Intelligent Symbol Filtering** based on price difference and funding intervals
- 📊 **Real-time funding rate tracking**
- ⚖️ **Dual exchange leverage control**
- ✅ **Order verification & rollback handling** in case of partial order failures
- 📁 **Auto-closing positions** for symbols with mismatched funding intervals
- 🔁 **Daily update** of mismatch symbols via background thread
- 📝 **Logging and trade recording**
- 🧪 Easy to extend with more strategies or exchanges

## ⚙️ Setup

1. **Install dependencies**
    ```bash
    pip install -r requirements.txt
    ```

2. **Set up `.env` files** for your API keys:
    - `binance_api.env`
    - `gate_api.env`

3. **Run the bot**
    ```bash
    python main.py
    ```

## 🧠 How It Works

1. The bot fetches real-time funding rates from both exchanges.
2. It checks if the funding rate difference exceeds a defined threshold.
3. If conditions are met and balances are sufficient, it opens hedged positions on both platforms.
4. If a symbol has **mismatched funding intervals**, it will auto-close the position after the funding fee is collected.
5. Symbols with mismatched intervals are **updated daily at 12:00 PM** and saved in `output/mismatch_symbols.txt`.

## ✅ Requirements

- Python 3.9+
- Binance API access
- Gate.io API access
- SOCKS5 proxy (configured for both exchanges)

## 📌 Notes

- Make sure to monitor available balances regularly to avoid failed orders.
- The bot assumes dual-position mode is enabled on Gate.io.
- All trades and logs are stored for future audit.

## 📜 License

MIT License

---

*This bot is for educational and research purposes only. Use it at your own risk.*
