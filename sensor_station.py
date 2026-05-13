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


def _safe_float(value, default: Optional[float] = None) -> Optional[float]:
    """Convert to float, preserving missing or invalid values as None."""
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
        self.t1.append(_safe_float(sample.get("temperature1_c")))
        self.t2.append(_safe_float(sample.get("temperature2_c")))
        self.h1.append(_safe_float(sample.get("humidity1_pct")))
        self.h2.append(_safe_float(sample.get("humidity2_pct")))
        self.u1.append(_safe_float(sample.get("ultrasonic1_cm")))
        self.u2.append(_safe_float(sample.get("ultrasonic2_cm")))
        self.h2s.append(_safe_float(sample.get("h2s_ppm")))
        self.co2.append(_safe_float(sample.get("co2_ppm")))
        self.nh3.append(_safe_float(sample.get("nh3_ppm")))
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
        "critical" if t is not None and (t < -7 or t > 30)
        else "warning" if t is not None and (t < 4 or t > 27)
        else "normal" if t is not None
        else "unknown"
    )
    result["humidity_status"] = (
        "critical" if rh is not None and (rh <= 50 or rh >= 85)
        else "warning" if rh is not None and (rh <= 55 or rh >= 80)
        else "normal" if rh is not None
        else "unknown"
    )
    result["h2s_level"] = (
        "critical" if h is not None and h > 1700
        else "warning" if h is not None and h > 12
        else "normal" if h is not None
        else None
    )

    co2 = result.get("co2_ppm")
    result["co2_level"] = (
        "critical" if co2 is not None and co2 > 3000
        else "warning" if co2 is not None and co2 > 2500
        else "normal" if co2 is not None
        else None
    )

    nh3 = result.get("nh3_ppm")
    result["nh3_level"] = (
        "critical" if nh3 is not None and nh3 > 25
        else "warning" if nh3 is not None and nh3 > 15
        else "normal" if nh3 is not None
        else None
    )

    result["feeder_status"] = feeder_status_label(f) if f is not None else "unknown"
    result["waterer_status"] = drinker_status_label(w) if w is not None else "unknown"
    result["heat_stress_index"] = (
        heat_stress_status(t, rh) if t is not None and rh is not None else None
    )

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


def correct_sample(sample: Dict) -> Dict:
    s = dict(sample)

    sensor_keys = [
        ("temperature1_c", "dht1"),
        ("temperature2_c", "dht2"),
        ("humidity1_pct", "humidity1"),
        ("humidity2_pct", "humidity2"),
        ("h2s_ppm", "h2s"),
        ("co2_ppm", "co2"),
        ("nh3_ppm", "nh3"),
        ("ultrasonic1_cm", "ultrasonic1"),
        ("ultrasonic2_cm", "ultrasonic2"),
    ]

    for key, error_code in sensor_keys:
        value = _safe_float(s.get(key))
        if value is None:
            s[key] = None
            s["error"].append(error_code)
        else:
            s[key] = value

    return s


def build_window_result(window: WindowAccumulator) -> Dict:
    def mean_or_none(values: List[Optional[float]]) -> Optional[float]:
        valid = [v for v in values if v is not None]
        if not valid:
            return None
        return round(sum(valid) / len(valid), 2)

    avg_t = mean_or_none(window.t1)
    avg_h = mean_or_none(window.h1 + window.h2)
    avg_h2s = mean_or_none(window.h2s)
    avg_co2 = mean_or_none(window.co2)
    avg_nh3 = mean_or_none(window.nh3)
    avg_u1_cm = mean_or_none(window.u1)
    avg_u2_cm = mean_or_none(window.u2)

    feeder_pct = round(feeder_status(avg_u1_cm / 100.0), 2) if avg_u1_cm is not None else None
    waterer_pct = round(drinker_status(avg_u2_cm / 100.0), 2) if avg_u2_cm is not None else None

    result = {
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
    if result["temperature_c"] is None or result["humidity_pct"] is None:
        result["mold_risk_score"] = None
        result["mold_risk_status"] = None
    else:
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
    for _ in range(10):
        fixed = correct_sample(sample)
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

    while True:
        try:
            data = read_serial_line(ser)
            if data:
                last_data_time = time.monotonic()
                corrected = correct_sample(data)
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
