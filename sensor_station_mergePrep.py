import json
import time
from typing import Dict, List, Optional
import serial

BAUDRATE = 115200
REPORT_EVERY_SECONDS = 300  # 5 minutes for production
SERIAL_READ_TIMEOUT_SECONDS = 0.1
SERIAL_SILENCE_RECONNECT_SECONDS = 20

FEEDER_EMPTY_CM = 28.0
FEEDER_FULL_CM = 5.0

WATERER_EMPTY_CM = 22.0
WATERER_FULL_CM = 4.0

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
        self.errors.update(sample.get("error", []))

    def has_data(self) -> bool:
        return len(self.t1) > 0

    def reset(self) -> None:
        self.__init__()


# ============================================================================
# CORE PIPELINE: DERIVED METRICS AND CLASSIFICATION
# ============================================================================


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
        "critical" if h is not None and h > 10
        else "warning" if h is not None and h > 2
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

    result["feeder_status"] = fill_level_label(f)
    result["waterer_status"] = fill_level_label(w)

    return result


def level_to_pct(distance_cm: Optional[float],
                 empty_cm: float,
                 full_cm: float) -> Optional[float]:
    """
    Convert ultrasonic distance to fill percentage.
    
    Empty = 0%
    Full = 100%
    """
    if distance_cm is None:
        return None

    pct = 100.0 * (empty_cm - distance_cm) / (empty_cm - full_cm)

    return round(
        max(0.0, min(100.0, pct)),
        1
    )

# ============================================================================
# CORE PIPELINE: INPUT NORMALIZATION
# ============================================================================

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

    payload.setdefault("error", [])
    return payload


def fill_level_label(pct: Optional[float]) -> Optional[str]:
    if pct is None:
        return None
    elif pct > 70:
        return "full"
    elif pct > 30:
        return "normal"
    elif pct > 10:
        return "low"
    else:
        return "empty"


# ============================================================================
# Serial port utilities
# ============================================================================


def safe_close_serial(ser: Optional[serial.Serial]) -> None:
    if ser is None:
        return
    try:
        if ser.is_open:
            ser.close()
    except Exception:
        pass


def open_serial_connection() -> serial.Serial:
    while True:
        try:
            ser = serial.Serial(
                "/dev/esp32",
                BAUDRATE,
                timeout=SERIAL_READ_TIMEOUT_SECONDS
            )

            time.sleep(2)
            ser.reset_input_buffer()

            print("[SERIAL] Connected to ESP32")
            return ser

        except serial.SerialException as exc:
            print(f"[SERIAL] Connection failed: {exc}")
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

    feeder_pct = level_to_pct(
        avg_u1_cm,
        FEEDER_EMPTY_CM,
        FEEDER_FULL_CM
    ) if avg_u1_cm is not None else None

    waterer_pct = level_to_pct(
        avg_u2_cm,
        WATERER_EMPTY_CM,
        WATERER_FULL_CM
    ) if avg_u2_cm is not None else None

    result = {
        "temperature_c": avg_t,
        "humidity_pct": avg_h,
        "feeder_pct": feeder_pct,
        "waterer_pct": waterer_pct,
        "h2s_ppm": avg_h2s,
        "co2_ppm": avg_co2,
        "nh3_ppm": avg_nh3,
        "error": ",".join(sorted(window.errors)) if window.errors else None,
    }

    result = evaluate_levels(result)
    return result


# ============================================================================
# SERVICE RUNTIME ENTRYPOINT
# ============================================================================


def main() -> None:
    from db_utils import (
        setup_database,
        insert_sensor_reading,
    )

    print("[SERVICE] Starting...")
    setup_database()

    ser = open_serial_connection()

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
                ser = open_serial_connection()
                last_data_time = time.monotonic()

            if time.monotonic() >= next_flush_at:
                if window.has_data():
                    row = build_window_result(window)
                    row_id = insert_sensor_reading(row)
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
            ser = open_serial_connection()
        except Exception as exc:
            print(f"[SERVICE] Unexpected error: {exc}")
            time.sleep(1)


if __name__ == "__main__":
    main()
