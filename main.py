import time
from arbitrage import (
    current_position,
    log_initialization,
    fetch_funding_data,
    calc_funding_time,
    select_best_symbol,
    open_new_position
)
from exit import manage_exit
from config import *

STATE_WAITING_ENTRY = "WAITING_ENTRY"
STATE_WAITING_EXIT = "WAITING_EXIT"
LOOP_INTERVAL = 60  # seconds

if __name__ == "__main__":
    print("🚀 Starting arbitrage main loop...")

    bdata_handler, gdata_handler, bf_trader, gf_trader = log_initialization()
    state = STATE_WAITING_ENTRY

    while True:
        now = time.time()

        if state == STATE_WAITING_ENTRY:
            fr_combined = fetch_funding_data(bdata_handler, gdata_handler)
            next_funding_time, till_next, filtered = calc_funding_time(fr_combined, now)

            if till_next < TIME_BUFFER and not filtered.empty:
                symbol, fr_diff = select_best_symbol(filtered)
                if abs(fr_diff) >= THRESHOLD:
                    success = open_new_position(symbol, fr_diff, next_funding_time, bdata_handler, gdata_handler, bf_trader, gf_trader)
                    if success:
                        state = STATE_WAITING_EXIT
                        print(f"[DEBUG] current position is: {current_position}")
                        print(f"[DEBUG] state just changed, current state is {state}")

        elif state == STATE_WAITING_EXIT:
            if current_position and now > current_position['funding_time'].timestamp():
                print(f"[MAIN] Exiting position for {current_position['symbol']}")
                print(f"[DEBUG] current position is: {current_position}")
                finished = manage_exit(bdata_handler, gdata_handler, bf_trader, gf_trader)
                if finished:
                    state = STATE_WAITING_ENTRY
                    print(f"[DEBUG] state just changed, current state is {state}")

        time.sleep(LOOP_INTERVAL)
