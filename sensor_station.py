import argparse
import glob
import json
import math
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import serial

from Volume_calculator import (
    feeder_status,
    drinker_status,
    feeder_status_label,
    drinker_status_label,
)
from risk_calculation import (
    vtt_original_step,
    VTTState,
    VTTOriginalParams,
    SensitivityLevel,
    DeclineMethod,
)

BAUDRATE = 115200
REPORT_EVERY_SECONDS = 300  # 5 minutes for production
SERIAL_READ_TIMEOUT_SECONDS = 1
SERIAL_SILENCE_RECONNECT_SECONDS = 20
DEFAULT_SERIAL_PORT = "/dev/esp32"

DEFAULT_ULTRASONIC_CM = 50.0

VTT_PARAMS = VTTOriginalParams(
    sensitivity=SensitivityLevel.VERY_SENSITIVE,
    sample_minutes=5,
    decline_method=DeclineMethod.WOOD,
)
vtt_state = VTTState(m=0.0, consecutive_unfavourable_minutes=0)


# ============================================================================
# CORE PIPELINE STATE
# ============================================================================


def _safe_float(value, default: float = 0.0) -> float:
    """Convert to float, returning *default* for None or non-numeric values."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


class WindowAccumulator:
    def __init__(self) -> None:
        self.t1: List[float] = []
        self.t2: List[float] = []
        self.h1: List[float] = []
        self.h2: List[float] = []
        self.u1: List[float] = []
        self.u2: List[float] = []
        self.h2s: List[float] = []
        self.co2: List[float] = []
        self.nh3: List[float] = []
        self.errors: set[str] = set()
        self.last_timestamp: datetime = datetime.now()

    def add(self, sample: Dict) -> None:
        self.t1.append(_safe_float(sample.get("temperature1_c"), 22.0))
        self.t2.append(_safe_float(sample.get("temperature2_c"), 22.0))
        self.h1.append(_safe_float(sample.get("humidity1_pct"), 60.0))
        self.h2.append(_safe_float(sample.get("humidity2_pct"), 60.0))
        self.u1.append(_safe_float(sample.get("ultrasonic1_cm"), 50.0))
        self.u2.append(_safe_float(sample.get("ultrasonic2_cm"), 50.0))
        self.h2s.append(_safe_float(sample.get("h2s_ppm"), 0.0))
        self.co2.append(_safe_float(sample.get("co2_ppm"), 400.0))
        self.nh3.append(_safe_float(sample.get("nh3_ppm"), 0.0))
        self.last_timestamp = sample["timestamp"]
        self.errors.update(sample.get("error", []))

    def has_data(self) -> bool:
        return len(self.t1) > 0

    def reset(self) -> None:
        self.__init__()


# ============================================================================
# CORE PIPELINE: DERIVED METRICS AND CLASSIFICATION
# ============================================================================


def mold_risk_from_m(m: float) -> Tuple[float, str]:
    score = round(min(m / 6.0, 1.0) * 100.0, 1)
    if score > 60:
        status = "critical"
    elif score > 30:
        status = "warning"
    else:
        status = "normal"
    return score, status


def heat_stress_status(temperature_c: float, humidity_pct: float) -> str:
    tw = (
        temperature_c * math.atan(0.151977 * math.sqrt(humidity_pct + 8.313659))
        + math.atan(temperature_c + humidity_pct)
        - math.atan(humidity_pct - 1.676331)
        + 0.00391838 * humidity_pct ** (3 / 2) * math.atan(0.023101 * humidity_pct)
        - 4.686035
    )
    hsi = 0.6 * temperature_c + 0.4 * tw

    if hsi > 76:
        return "critical"
    if hsi > 70:
        return "warning"
    return "normal"


def evaluate_levels(result: Dict) -> Dict:
    t = result["temperature_c"]
    rh = result["humidity_pct"]
    h = result["h2s_ppm"]
    f = result["feeder_pct"]
    w = result["waterer_pct"]

    result["temperature_status"] = (
        "critical" if t < -7 or t > 30 else "warning" if t < 4 or t > 27 else "normal"
    )
    result["humidity_status"] = (
        "critical" if rh <= 50 or rh >= 85 else "warning" if rh <= 55 or rh >= 80 else "normal"
    )
    result["h2s_level"] = "critical" if h > 1700 else "warning" if h > 12 else "normal"

    co2 = result.get("co2_ppm", 0)
    result["co2_level"] = "critical" if co2 > 3000 else "warning" if co2 > 2500 else "normal"

    nh3 = result.get("nh3_ppm", 0)
    result["nh3_level"] = "critical" if nh3 > 25 else "warning" if nh3 > 15 else "normal"

    result["feeder_status"] = feeder_status_label(f)
    result["waterer_status"] = drinker_status_label(w)
    result["heat_stress_index"] = heat_stress_status(t, rh)

    return result


# ============================================================================
# CORE PIPELINE: INPUT NORMALIZATION
# ============================================================================


def parse_timestamp(raw_timestamp) -> datetime:
    """Normalize ESP timestamp into Python datetime.

    Supports:
    - Unix epoch seconds (int/float)
    - ISO datetime string
    - Missing/invalid values -> current local time fallback
    """
    if isinstance(raw_timestamp, (int, float)):
        return datetime.fromtimestamp(raw_timestamp)
    if isinstance(raw_timestamp, str):
        try:
            return datetime.fromisoformat(raw_timestamp.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            return datetime.now()
    return datetime.now()


def read_serial_line(ser: serial.Serial) -> Optional[Dict]:
    """Read one JSON line from serial and validate required sensor keys."""
    try:
        if not ser.is_open:
            raise serial.SerialException("Attempting to use a port that is not open")
        raw = ser.readline()
        if not raw and ser.in_waiting:
            raw = ser.read(ser.in_waiting)
        line = raw.decode("utf-8", errors="ignore").strip()
    except (serial.SerialException, OSError):
        raise
    except Exception:
        return None

    if not line:
        return None

    if "\n" in line:
        line = line.splitlines()[-1].strip()

    try:
        payload = json.loads(line)
        print(f"[SERIAL] ← {payload}")
    except json.JSONDecodeError:
        print(f"[SERIAL] JSON decode failed: {line[:60]}")
        return None

    required_keys = {
        "temperature1_c",
        "temperature2_c",
        "humidity1_pct",
        "humidity2_pct",
        "ultrasonic1_cm",
        "ultrasonic2_cm",
        "h2s_ppm",
        "co2_ppm",
        "nh3_ppm",
    }
    if not required_keys.issubset(payload.keys()):
        print(f"[SERIAL] Missing keys, skipping")
        return None

    payload["timestamp"] = parse_timestamp(payload.get("timestamp"))
    payload.setdefault("error", [])
    return payload


# ============================================================================
# MAC/DEV-CONNECTION HELPERS (NOT CORE SENSOR PIPELINE LOGIC)
# These helpers are mainly to make local testing on macOS easier.
# On Raspberry Pi you can still pass --serial-port /dev/esp32 explicitly.
# ============================================================================


def list_serial_candidates() -> List[str]:
    """Discover likely USB serial device paths, especially useful on macOS."""
    patterns = [
        "/dev/cu.usb*",
        "/dev/cu.SLAB*",
        "/dev/cu.wch*",
        "/dev/tty.usb*",
        "/dev/tty.SLAB*",
        "/dev/tty.wch*",
    ]
    all_ports: List[str] = []
    for pattern in patterns:
        all_ports.extend(glob.glob(pattern))
    unique_sorted = sorted(set(all_ports))
    return unique_sorted


def resolve_serial_port(cli_port: Optional[str]) -> str:
    """Pick serial port priority: CLI arg > env var > default Pi path > auto-detect."""
    if cli_port:
        return cli_port

    env_port = os.getenv("COLSON_SERIAL_PORT")
    if env_port:
        return env_port

    if os.path.exists(DEFAULT_SERIAL_PORT):
        return DEFAULT_SERIAL_PORT

    candidates = list_serial_candidates()
    if candidates:
        print(f"[SERIAL] Auto-selected {candidates[0]}")
        return candidates[0]

    return DEFAULT_SERIAL_PORT


def safe_close_serial(ser: Optional[serial.Serial]) -> None:
    if ser is None:
        return
    try:
        if ser.is_open:
            ser.close()
    except Exception:
        pass


def open_serial_connection(serial_port_arg: Optional[str]) -> serial.Serial:
    while True:
        serial_port = resolve_serial_port(serial_port_arg)
        try:
            ser = serial.Serial(serial_port, BAUDRATE, timeout=SERIAL_READ_TIMEOUT_SECONDS)
            time.sleep(2.0)
            ser.reset_input_buffer()
            print(f"[SERIAL] Connected on {serial_port} @ {BAUDRATE}")
            return ser
        except serial.SerialException as exc:
            print(f"[SERIAL] Open failed on {serial_port}: {exc}. Retrying in 2s...")
            time.sleep(2)


# ============================================================================
# CORE PIPELINE: DATA CORRECTION + 5-MIN AGGREGATION
# ============================================================================


def correct_sample(sample: Dict, last_good_raw: Dict[str, float], previous_row: Optional[Dict]) -> Dict:
    s = dict(sample)

    fallback_temp = (previous_row.get("temperature_c") if previous_row else None) or 22.0
    fallback_hum = (previous_row.get("humidity_pct") if previous_row else None) or 60.0
    fallback_h2s = (previous_row.get("h2s_ppm") if previous_row else None) or 5.0
    fallback_co2 = (previous_row.get("co2_ppm") if previous_row else None) or 400.0
    fallback_nh3 = (previous_row.get("nh3_ppm") if previous_row else None) or 0.0

    if s.get("temperature1_c") is None:
        s["temperature1_c"] = s.get("temperature2_c", fallback_temp)
        s["error"].append("dht1")

    if s.get("temperature2_c") is None:
        s["temperature2_c"] = s.get("temperature1_c", fallback_temp)
        s["error"].append("dht2")

    if s.get("humidity1_pct") is None:
        s["humidity1_pct"] = s.get("humidity2_pct", fallback_hum)
        s["error"].append("humidity1")

    if s.get("humidity2_pct") is None:
        s["humidity2_pct"] = s.get("humidity1_pct", fallback_hum)
        s["error"].append("humidity2")

    if s.get("h2s_ppm") is None:
        s["h2s_ppm"] = fallback_h2s
        s["error"].append("h2s")

    if s.get("co2_ppm") is None:
        s["co2_ppm"] = fallback_co2
        s["error"].append("co2")

    if s.get("nh3_ppm") is None:
        s["nh3_ppm"] = fallback_nh3
        s["error"].append("nh3")

    if s.get("ultrasonic1_cm") is None:
        s["ultrasonic1_cm"] = last_good_raw.get("ultrasonic1_cm", DEFAULT_ULTRASONIC_CM)
        s["error"].append("ultrasonic1")

    if s.get("ultrasonic2_cm") is None:
        s["ultrasonic2_cm"] = last_good_raw.get("ultrasonic2_cm", DEFAULT_ULTRASONIC_CM)
        s["error"].append("ultrasonic2")

    last_good_raw["ultrasonic1_cm"] = float(s["ultrasonic1_cm"])
    last_good_raw["ultrasonic2_cm"] = float(s["ultrasonic2_cm"])

    return s


def build_window_result(window: WindowAccumulator) -> Dict:
    avg_t = round((sum(window.t1) + sum(window.t2)) / (2 * len(window.t1)), 2)
    avg_h = round((sum(window.h1) + sum(window.h2)) / (2 * len(window.h1)), 2)
    avg_h2s = round(sum(window.h2s) / len(window.h2s), 3)
    avg_co2 = round(sum(window.co2) / len(window.co2), 1)
    avg_nh3 = round(sum(window.nh3) / len(window.nh3), 3)

    avg_u1_cm = sum(window.u1) / len(window.u1)
    avg_u2_cm = sum(window.u2) / len(window.u2)

    feeder_pct = round(feeder_status(avg_u1_cm / 100.0), 2)
    waterer_pct = round(drinker_status(avg_u2_cm / 100.0), 2)

    result = {
        "timestamp": window.last_timestamp,
        "temperature_c": avg_t,
        "humidity_pct": avg_h,
        "feeder_pct": feeder_pct,
        "waterer_pct": waterer_pct,
        "h2s_ppm": avg_h2s,
        "co2_ppm": avg_co2,
        "nh3_ppm": avg_nh3,
        "door_open": False,
        "ventilation_on": False,
        "error": ",".join(sorted(window.errors)) if window.errors else None,
    }

    result = evaluate_levels(result)
    step = vtt_original_step(
        temp_c=result["temperature_c"],
        rh=result["humidity_pct"],
        state=vtt_state,
        params=VTT_PARAMS,
    )
    result["mold_risk_score"], result["mold_risk_status"] = mold_risk_from_m(step.m)

    return result


# ============================================================================
# DEV/SMOKE TEST ENTRYPOINT (NO SERIAL, NO DB)
# ============================================================================


def run_self_test() -> None:
    sample = {
        "timestamp": int(time.time()),
        "temperature1_c": 22.1,
        "temperature2_c": 22.6,
        "humidity1_pct": 58.0,
        "humidity2_pct": 59.0,
        "ultrasonic1_cm": 50,
        "ultrasonic2_cm": 52,
        "h2s_ppm": 4.6,
        "co2_ppm": 650.0,
        "nh3_ppm": 3.2,
        "error": [],
    }

    window = WindowAccumulator()
    last_good_raw: Dict[str, float] = {}
    for _ in range(10):
        fixed = correct_sample(sample, last_good_raw, None)
        window.add(fixed)

    result = build_window_result(window)
    print("[SELF-TEST] Aggregation output:")
    print(json.dumps({k: (str(v) if isinstance(v, datetime) else v) for k, v in result.items()}, indent=2))


# ============================================================================
# SERVICE RUNTIME ENTRYPOINT
# ============================================================================


def main(serial_port_arg: Optional[str]) -> None:
    from db_utils import (
        setup_database,
        insert_sensor_reading,
        get_latest_sensor_reading,
    )

    print("[SERVICE] Starting sensor_station_colson.py")
    setup_database()

    previous_row: Optional[Dict] = None
    try:
        previous_row = get_latest_sensor_reading()
        if previous_row and previous_row.get("mold_risk_score") is not None:
            vtt_state.m = float(previous_row["mold_risk_score"]) / 100.0 * 6.0
            print(f"[VTT] Recovered M={vtt_state.m:.3f}")
    except Exception as exc:
        print(f"[VTT] Could not recover previous state: {exc}")

    serial_port = resolve_serial_port(serial_port_arg)
    print(f"[SERIAL] Using port: {serial_port}")
    ser = open_serial_connection(serial_port_arg)

    last_data_time = time.monotonic()
    window = WindowAccumulator()
    next_flush_at = time.monotonic() + REPORT_EVERY_SECONDS
    last_good_raw: Dict[str, float] = {}

    while True:
        try:
            data = read_serial_line(ser)
            if data:
                last_data_time = time.monotonic()
                corrected = correct_sample(data, last_good_raw, previous_row)
                window.add(corrected)
                print(f"[WINDOW] Added sample ({len(window.t1)} readings so far)")

            if time.monotonic() - last_data_time > SERIAL_SILENCE_RECONNECT_SECONDS:
                print("[SERIAL] Silent too long. Reconnecting...")
                safe_close_serial(ser)
                ser = open_serial_connection(serial_port_arg)
                last_data_time = time.monotonic()

            if time.monotonic() >= next_flush_at:
                if window.has_data():
                    row = build_window_result(window)
                    row_id = insert_sensor_reading(row)
                    previous_row = row
                    print(
                        f"\n[FLUSH] {REPORT_EVERY_SECONDS}-second window complete! Uploading to DB..."
                    )
                    print(
                        f"[DB] Inserted row {row_id}: "
                        f"T={row['temperature_c']}C RH={row['humidity_pct']}% "
                        f"H2S={row['h2s_ppm']}ppm CO2={row['co2_ppm']}ppm NH3={row['nh3_ppm']}ppm"
                    )
                    print(
                        f"[DB] Status: Temp={row['temperature_status']} "
                        f"RH={row['humidity_status']} H2S={row['h2s_level']} "
                        f"Mold={row['mold_risk_status']}\n"
                    )
                else:
                    print("[DB] No samples in this window; skipping insert.")

                window.reset()
                next_flush_at += REPORT_EVERY_SECONDS

        except KeyboardInterrupt:
            print("[SERVICE] Stopped by user.")
            break
        except (serial.SerialException, OSError) as exc:
            print(f"[SERIAL] Error: {exc}. Reconnecting...")
            safe_close_serial(ser)
            time.sleep(2)
            ser = open_serial_connection(serial_port_arg)
        except Exception as exc:
            print(f"[SERVICE] Unexpected error: {exc}")
            time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Colson sensor station service (ESP32 -> DB)")
    parser.add_argument("--self-test", action="store_true", help="Run local logic test without serial or database")
    parser.add_argument("--list-ports", action="store_true", help="Print detected serial candidates and exit")
    parser.add_argument("--serial-port", type=str, default=None, help="Override serial port (e.g. /dev/cu.usbserial-xxx)")
    args = parser.parse_args()

    if args.list_ports:
        ports = list_serial_candidates()
        if ports:
            print("Detected serial ports:")
            for p in ports:
                print(f"  {p}")
        else:
            print("No common USB serial ports detected.")
    elif args.self_test:
        run_self_test()
    else:
        main(args.serial_port)
