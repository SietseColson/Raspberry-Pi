#!/usr/bin/env python3
"""Minimal ESP serial sniffer.

Run this on the Pi INSTEAD of automation_and_sensing.py to isolate whether
the ESP is actually streaming continuously over /dev/esp32. It does nothing
but read lines and print them with wall-clock timestamps, the gap since the
previous line, and a running count.

Stop the automation service first (so it doesn't fight for the port):
    sudo systemctl stop smart-coop-control       # if running via systemd
    # or just Ctrl+C the manual python run

Then:
    python3 Automation_testing/serial_sniffer.py
"""

import sys
import time
from datetime import datetime

import serial

PORT = "/dev/esp32"
BAUD = 115200


def main() -> None:
    try:
        ser = serial.Serial(PORT, BAUD, timeout=1)
    except Exception as exc:
        print(f"[sniffer] Failed to open {PORT}: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"[sniffer] Opened {PORT}@{BAUD}. Waiting 2s for ESP boot...")
    time.sleep(2)
    ser.reset_input_buffer()
    print("[sniffer] Reading lines. Ctrl+C to stop.\n")

    last_line_time = time.monotonic()
    start_time = last_line_time
    line_count = 0
    gap_max = 0.0
    gap_total = 0.0

    try:
        while True:
            line = ser.readline().decode("utf-8", errors="ignore").strip()
            if not line:
                # readline timeout (1s) with no data — surface it so we can
                # see when the ESP goes silent.
                gap = time.monotonic() - last_line_time
                if gap >= 2.0:
                    print(f"  >>> no data for {gap:.1f}s")
                continue

            now = time.monotonic()
            gap = now - last_line_time
            last_line_time = now
            line_count += 1
            gap_max = max(gap_max, gap)
            gap_total += gap

            ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            marker = " <-- LONG GAP" if gap > 2.0 else ""
            print(f"[{ts}] (+{gap:5.2f}s) #{line_count:5d}: {line}{marker}")
    except KeyboardInterrupt:
        elapsed = time.monotonic() - start_time
        avg_gap = gap_total / line_count if line_count else 0.0
        print(
            f"\n[sniffer] Stopped. {line_count} lines in {elapsed:.1f}s "
            f"(avg gap {avg_gap:.2f}s, max gap {gap_max:.2f}s)"
        )
    finally:
        ser.close()


if __name__ == "__main__":
    main()
