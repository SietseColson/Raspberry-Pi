#!/usr/bin/env python3
"""
Unified automation and sensing service for chicken coop.

This is a single service that owns the USB serial port to the ESP32, reads sensor telemetry,
aggregates data into 5-minute windows, computes derived metrics (fill %, status labels),
and runs automation logic (door, feeder, fan control) based on sunrise/sunset scheduling.

The service integrates:
The service integrates:
- Sensor ingestion and aggregation (from sensor_station_mergePrep.py)
- Automation control (from automation_ESP_Takeover.py)

Single process, exclusive serial port ownership, auto-reconnect on failure.
"""

import json
import queue
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import threading
import pytz
import requests
import serial

import db_utils

# =============================================================================
# CONFIGURATION
# =============================================================================

# Serial communication
ESP_SERIAL_PORT = "/dev/esp32"
ESP_SERIAL_BAUD = 115200
ESP_SERIAL_TIMEOUT = 0.1

# Sensor aggregation
REPORT_EVERY_SECONDS = 300  # 5 minutes for production
SERIAL_SILENCE_RECONNECT_SECONDS = 20

# Fill level constants (ultrasonic distances in cm)
FEEDER_EMPTY_CM = 28.0
FEEDER_FULL_CM = 5.0
WATERER_EMPTY_CM = 16.0
WATERER_FULL_CM = 1.0

# Sunrise/sunset scheduling
LATITUDE = 50.88
LONGITUDE = 4.70
LOCAL_TZ = pytz.timezone("Europe/Brussels")

# Polling interval (seconds)
POLL_SECONDS = 5

# Schedule offsets (minutes relative to sunrise/sunset)
DOOR_OPEN_OFFSET_MIN = -10      # Open 10 min before sunrise
DOOR_CLOSE_OFFSET_MIN = 30      # Close 30 min after sunset

FEEDER_OPEN_OFFSET_MIN = 0      # Open at sunrise
FEEDER_CLOSE_OFFSET_MIN = 0     # Close at sunset

# Backup times (used if API fails)
BACKUP_SUNRISE_HOUR = 6
BACKUP_SUNRISE_MINUTE = 30
BACKUP_SUNSET_HOUR = 20
BACKUP_SUNSET_MINUTE = 50

# Validation sets
VALID_DOOR_TARGETS = {"open", "closed"}
VALID_FEEDER_TARGETS = {"open", "closed"}
VALID_DOOR_STATUSES = {"open", "closed", "moving", "inbetween", "timeout", "error"}
VALID_FEEDER_STATUSES = {"open", "closed", "moving", "error"}

# Required sensor keys for validation
REQUIRED_SENSOR_KEYS = {
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

# Cache for sun times (to avoid repeated API calls)
LAST_VALID_SUN_TIMES = {
    "sunrise": None,
    "sunset": None,
    "fetch_date": None,
    "last_fetch_time": None,
    "last_retry": None,
}

# Threading locks for database and serial access
DB_LOCK = threading.Lock()
SERIAL_LOCK = threading.RLock()

# Queue for telemetry handed off from the serial reader thread to the main loop.
# Decouples serial draining from automation/DB work so a slow DB call cannot
# starve the serial buffer.
TELEMETRY_QUEUE: "queue.Queue[Dict]" = queue.Queue()

# Global state
esp_port: Optional[serial.Serial] = None
esp_door_state = "moving_or_unknown"
esp_command_pending = False
last_fan_pct = None
last_data_time = None

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================


def _safe_float(value, default: Optional[float] = None) -> Optional[float]:
    """Convert to float, preserving missing or invalid values as None."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


# =============================================================================
# SERIAL COMMUNICATION WITH ESP32
# =============================================================================


def open_esp_serial() -> None:
    """Open connection to ESP32 via USB serial."""
    global esp_port, last_fan_pct, last_data_time
    with SERIAL_LOCK:
        last_fan_pct = None
        # Keep last_data_time untouched until the port is actually open.

        if esp_port is not None and esp_port.is_open:
            return
        try:
            esp_port = serial.Serial(
                ESP_SERIAL_PORT,
                ESP_SERIAL_BAUD,
                timeout=ESP_SERIAL_TIMEOUT,
            )
            time.sleep(2)
            try:
                esp_port.reset_input_buffer()
            except Exception:
                pass
            last_data_time = time.monotonic()
            print(f"[SERIAL] Opened ESP serial on {ESP_SERIAL_PORT}@{ESP_SERIAL_BAUD}")
        except Exception as exc:
            esp_port = None
            print(f"[SERIAL] Failed to open ESP serial: {exc}")


def close_esp_serial() -> None:
    """Close connection to ESP32."""
    global esp_port
    with SERIAL_LOCK:
        if esp_port is not None:
            try:
                esp_port.close()
                print("[SERIAL] Closed ESP serial")
            except Exception as exc:
                print(f"[SERIAL] Error closing ESP serial: {exc}")
            esp_port = None


def send_esp_command(command: dict) -> bool:
    """Send a JSON command to ESP32 via serial."""
    with SERIAL_LOCK:
        if esp_port is None or not esp_port.is_open:
            open_esp_serial()
        if esp_port is None or not esp_port.is_open:
            return False

        payload = json.dumps(command)
        try:
            esp_port.write((payload + "\n").encode("utf-8"))
            esp_port.flush()
            print(f"[SERIAL] → {command}")
            return True
        except Exception as exc:
            print(f"[SERIAL] Failed to send command: {exc}")
            close_esp_serial()
            return False


def parse_esp_line(line: str) -> Optional[Dict]:
    """Parse ESP telemetry/event JSON line. Returns telemetry dict for sensor aggregation."""
    global esp_door_state, esp_command_pending

    if not line.strip():
        return None

    try:
        payload = json.loads(line)
    except json.JSONDecodeError:
        print(f"[SERIAL] JSON decode failed: {line[:60]}")
        return None

    print(f"[SERIAL] ← {payload}")

    msg_type = payload.get("type")

    # Handle switch events (door feedback)
    if msg_type == "event":
        device = payload.get("device")
        status = payload.get("status")
        if device == "bottom_switch":
            if status == "CLOSED":
                esp_door_state = "fully_closed"
                with DB_LOCK:
                    db_utils.update_device_control(door_status="closed")
                esp_command_pending = False
            elif status == "RELEASED":
                esp_door_state = "moving_or_unknown"
        elif device == "top_switch":
            if status == "OPEN":
                esp_door_state = "fully_open"
                with DB_LOCK:
                    db_utils.update_device_control(door_status="open")
                esp_command_pending = False
            elif status == "RELEASED":
                esp_door_state = "moving_or_unknown"
        return None

    # Handle periodic telemetry (sensor data)
    elif msg_type == "telemetry":
        # Ensure the error field is always a list for downstream aggregation.
        if not isinstance(payload.get("error"), list):
            payload["error"] = []

        # Sync door state from telemetry. Only hit the DB when the state
        # actually changes — otherwise every 1 Hz telemetry tick would issue
        # a Supabase write from the reader thread and starve serial reads.
        door_state = payload.get("door_state")
        if door_state == "fully_closed":
            state_changed = esp_door_state != "fully_closed"
            esp_door_state = "fully_closed"
            esp_command_pending = False
            if state_changed:
                with DB_LOCK:
                    db_utils.update_device_control(door_status="closed")
        elif door_state == "fully_open":
            state_changed = esp_door_state != "fully_open"
            esp_door_state = "fully_open"
            esp_command_pending = False
            if state_changed:
                with DB_LOCK:
                    db_utils.update_device_control(door_status="open")
        elif door_state == "moving_or_unknown":
            esp_door_state = "moving_or_unknown"

        # Validate required sensor keys
        if not REQUIRED_SENSOR_KEYS.issubset(payload.keys()):
            print(f"[SERIAL] Telemetry missing required keys, skipping")
            return None

        payload.setdefault("error", [])
        return payload

    return None


def drain_esp_serial() -> List[Dict]:
    """Drain the ESP serial buffer in one pass, returning all telemetry dicts.

    Switch events are processed inside parse_esp_line for their side effects
    (door state, esp_command_pending) and are not returned here.
    """
    global last_data_time
    results: List[Dict] = []
    with SERIAL_LOCK:
        if esp_port is None or not esp_port.is_open:
            open_esp_serial()
        if esp_port is None or not esp_port.is_open:
            return results

        try:
            while esp_port.in_waiting:
                line = esp_port.readline().decode("utf-8", errors="ignore").strip()
                if line:
                    data = parse_esp_line(line)
                    if data:
                        last_data_time = time.monotonic()
                        results.append(data)
        except Exception as exc:
            print(f"[SERIAL] Read error: {exc}")
            close_esp_serial()

    return results


def serial_reader_loop() -> None:
    """Daemon thread that continuously drains the ESP serial buffer.

    Telemetry is pushed onto TELEMETRY_QUEUE so the main loop can stay
    responsive even when DB/network calls in the automation block stall.
    """
    while True:
        try:
            for data in drain_esp_serial():
                TELEMETRY_QUEUE.put(data)
        except Exception as exc:
            print(f"[SERIAL] Reader thread error: {exc}")
        time.sleep(0.05)


def check_serial_silence() -> bool:
    """Check if we've had no data for too long, return True if reconnect needed."""
    global last_data_time
    if last_data_time is None:
        return False
    if time.monotonic() - last_data_time > SERIAL_SILENCE_RECONNECT_SECONDS:
        return True
    return False


# =============================================================================
# SENSOR AGGREGATION (5-MINUTE WINDOW)
# =============================================================================


class WindowAccumulator:
    """Accumulates sensor readings into 5-minute window."""

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
        """Add a sensor sample to the window."""
        self.t1.append(_safe_float(sample.get("temperature1_c")))
        self.t2.append(_safe_float(sample.get("temperature2_c")))
        self.h1.append(_safe_float(sample.get("humidity1_pct")))
        self.h2.append(_safe_float(sample.get("humidity2_pct")))
        self.u1.append(_safe_float(sample.get("ultrasonic1_cm")))
        self.u2.append(_safe_float(sample.get("ultrasonic2_cm")))
        self.h2s.append(_safe_float(sample.get("h2s_ppm")))
        self.co2.append(_safe_float(sample.get("co2_ppm")))
        self.nh3.append(_safe_float(sample.get("nh3_ppm")))
        self.errors.update(sample.get("error") or [])

    def has_data(self) -> bool:
        """Check if window has any readings."""
        return len(self.t1) > 0

    def reset(self) -> None:
        """Clear all readings from window."""
        self.__init__()


def correct_sample(sample: Dict) -> Dict:
    """Apply sensor error correction and validation."""
    s = dict(sample)

    if not isinstance(s.get("error"), list):
        s["error"] = []

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
            if error_code not in s["error"]:
                s["error"].append(error_code)
        else:
            s[key] = value

    return s


def level_to_pct(distance_cm: Optional[float],
                 empty_cm: float,
                 full_cm: float) -> Optional[float]:
    """
    Convert ultrasonic distance to fill percentage using linear interpolation.

    Empty (0%) = empty_cm
    Full (100%) = full_cm
    """
    if distance_cm is None:
        return None

    pct = 100.0 * (empty_cm - distance_cm) / (empty_cm - full_cm)
    return round(max(0.0, min(100.0, pct)), 1)


def fill_level_label(pct: Optional[float]) -> Optional[str]:
    """Convert fill percentage to status label."""
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


def evaluate_levels(result: Dict) -> Dict:
    """Evaluate sensor readings and assign status labels (critical/warning/normal)."""
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


def build_window_result(window: WindowAccumulator) -> Dict:
    """Compute aggregated metrics from 5-minute window."""
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


# =============================================================================
# SUNRISE/SUNSET SCHEDULING
# =============================================================================


def get_backup_sun_times() -> Tuple[datetime, datetime]:
    """Get fixed backup sunrise/sunset times for today."""
    now = datetime.now(LOCAL_TZ)
    sunrise = now.replace(
        hour=BACKUP_SUNRISE_HOUR,
        minute=BACKUP_SUNRISE_MINUTE,
        second=0,
        microsecond=0,
    )
    sunset = now.replace(
        hour=BACKUP_SUNSET_HOUR,
        minute=BACKUP_SUNSET_MINUTE,
        second=0,
        microsecond=0,
    )
    return sunrise, sunset


def get_sun_times() -> Tuple[datetime, datetime]:
    """
    Fetch sunrise/sunset times from the API.
    Falls back to cached times or fixed backup times on failure.
    Retries every hour if API fails, fetches once per day if successful.
    """
    today = datetime.now(LOCAL_TZ).date()
    now = datetime.now(LOCAL_TZ)

    # Use cached times if we already fetched successfully today
    if LAST_VALID_SUN_TIMES["fetch_date"] == today:
        if LAST_VALID_SUN_TIMES["last_fetch_time"] is not None:
            # Successful fetch today, use cached times
            return LAST_VALID_SUN_TIMES["sunrise"], LAST_VALID_SUN_TIMES["sunset"]
        # Failed fetch today, check if 1 hour has passed to retry
        last_retry = LAST_VALID_SUN_TIMES.get("last_retry")
        if last_retry is not None:
            time_since_last = now - last_retry
            if time_since_last < timedelta(hours=1):
                # Less than 1 hour since last retry, use cached/fallback
                if LAST_VALID_SUN_TIMES["sunrise"] is not None:
                    return LAST_VALID_SUN_TIMES["sunrise"], LAST_VALID_SUN_TIMES["sunset"]
        # Time to retry the API

    try:
        url = (
            f"https://api.sunrise-sunset.org/json"
            f"?lat={LATITUDE}&lng={LONGITUDE}&formatted=0"
        )
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("status") != "OK":
            raise ValueError(f"API returned status: {data.get('status')}")

        results = data["results"]
        sunrise = (
            datetime.fromisoformat(results["sunrise"].replace("Z", "+00:00"))
            .astimezone(LOCAL_TZ)
            .replace(
                year=today.year,
                month=today.month,
                day=today.day
            )
        )
        sunset = (
            datetime.fromisoformat(results["sunset"].replace("Z", "+00:00"))
            .astimezone(LOCAL_TZ)
            .replace(
                year=today.year,
                month=today.month,
                day=today.day
            )
        )

        # Cache the result
        LAST_VALID_SUN_TIMES["sunrise"] = sunrise
        LAST_VALID_SUN_TIMES["sunset"] = sunset
        LAST_VALID_SUN_TIMES["fetch_date"] = today
        LAST_VALID_SUN_TIMES["last_fetch_time"] = now  # Mark as successful fetch

        print(f"[SUN] Fetched: rise={sunrise.strftime('%H:%M')} set={sunset.strftime('%H:%M')}")
        return sunrise, sunset

    except Exception as exc:
        print(f"[SUN] API error (will retry in 1 hour): {exc}")
        LAST_VALID_SUN_TIMES["last_retry"] = now  # Mark retry time for hourly retry

        # Use cached times if available
        if LAST_VALID_SUN_TIMES["sunrise"] is not None:
            print("[SUN] Using cached times")
            return LAST_VALID_SUN_TIMES["sunrise"], LAST_VALID_SUN_TIMES["sunset"]

        # Fall back to fixed times
        print("[SUN] Using backup times")
        backup_rise, backup_set = get_backup_sun_times()
        LAST_VALID_SUN_TIMES["sunrise"] = backup_rise
        LAST_VALID_SUN_TIMES["sunset"] = backup_set
        LAST_VALID_SUN_TIMES["fetch_date"] = today
        LAST_VALID_SUN_TIMES["last_fetch_time"] = None  # Mark as failed fetch
        return backup_rise, backup_set


def sync_door_status_from_esp() -> Optional[str]:
    """Sync door status in database using ESP-reported switch and door state."""
    if esp_door_state == "fully_open":
        with DB_LOCK:
            db_utils.update_device_control(door_status="open")
        return "open"

    if esp_door_state == "fully_closed":
        with DB_LOCK:
            db_utils.update_device_control(door_status="closed")
        return "closed"

    with DB_LOCK:
        current = db_utils.fetch_device_control() or {}
    if current.get("door_status") == "timeout":
        return "timeout"

    if esp_command_pending:
        with DB_LOCK:
            db_utils.update_device_control(door_status="moving")
        return "moving"

    with DB_LOCK:
        db_utils.update_device_control(door_status="inbetween")
    return "inbetween"


# =============================================================================
# AUTOMATION CONTROL
# =============================================================================

def fan_off() -> None:
    """Turn the fan off via ESP."""
    send_esp_command({"type": "command", "device": "fan", "speed_pct": 0})


def set_fan_pct(pct: float) -> None:
    """Set fan speed via ESP serial command."""
    global last_fan_pct

    try:
        pct = round(max(0.0, min(100.0, float(pct))), 1)
    except (ValueError, TypeError):
        pct = 0.0

    if pct == last_fan_pct:
        return

    last_fan_pct = pct

    if pct <= 0:
        print(f"[FAN] OFF")
    else:
        print(f"[FAN] Set fan speed to {pct:.1f}%")

    send_esp_command({
        "type": "command",
        "device": "fan",
        "speed_pct": pct
    })

    with DB_LOCK:
        db_utils.update_device_control(fan_status_pct=pct)


def open_door() -> bool:
    """Send the ESP command to open the door."""
    print("[DOOR] Sending open command")
    success = send_esp_command({"type": "command", "device": "door", "action": "open"})
    if success:
        with DB_LOCK:
            db_utils.update_device_control(door_status="moving")
        global esp_command_pending
        esp_command_pending = True
    return success


def open_door_thread() -> None:
    """Open door in background thread."""
    threading.Thread(
        target=open_door,
        daemon=True,
        name="door_open"
    ).start()


def close_door() -> bool:
    """Send the ESP command to close the door."""
    print("[DOOR] Sending close command")
    success = send_esp_command({"type": "command", "device": "door", "action": "close"})
    if success:
        with DB_LOCK:
            db_utils.update_device_control(door_status="moving")
        global esp_command_pending
        esp_command_pending = True
    return success


def close_door_thread() -> None:
    """Close door in background thread."""
    threading.Thread(
        target=close_door,
        daemon=True,
        name="door_close"
    ).start()


def stop_door() -> None:
    """Stop the door motor via ESP."""
    send_esp_command({"type": "command", "device": "door", "action": "stop"})


def feeder_open() -> bool:
    """Send the ESP command to open the feeder."""
    print("[FEEDER] Sending open command")
    success = send_esp_command({"type": "command", "device": "feeder", "action": "open"})
    if success:
        with DB_LOCK:
            db_utils.update_device_control(feeder_status="open")
    return success


def feeder_open_thread() -> None:
    """Open feeder in background thread."""
    threading.Thread(
        target=feeder_open,
        daemon=True,
        name="feeder_open"
    ).start()


def feeder_close() -> bool:
    """Send the ESP command to close the feeder."""
    print("[FEEDER] Sending close command")
    success = send_esp_command({"type": "command", "device": "feeder", "action": "close"})
    if success:
        with DB_LOCK:
            db_utils.update_device_control(feeder_status="closed")
    return success


def feeder_close_thread() -> None:
    """Close feeder in background thread."""
    threading.Thread(
        target=feeder_close,
        daemon=True,
        name="feeder_close"
    ).start()


def stop_feeder() -> None:
    """Stop the feeder motor via ESP."""
    send_esp_command({"type": "command", "device": "feeder", "action": "stop"})


# =============================================================================
# STATE MACHINES FOR AUTOMATION
# =============================================================================


def validate_control_row(row: Optional[dict]) -> bool:
    """Validate the device_control row for consistency."""
    if row is None:
        print("[MAIN] No control row found in database")
        return False

    if row.get("door_target") not in VALID_DOOR_TARGETS:
        print(f"[MAIN] Invalid door_target: {row.get('door_target')}")
        with DB_LOCK:
            db_utils.update_device_control(door_status="error")
        return False

    if row.get("feeder_target") not in VALID_FEEDER_TARGETS:
        print(f"[MAIN] Invalid feeder_target: {row.get('feeder_target')}")
        with DB_LOCK:
            db_utils.update_device_control(feeder_status="error")
        return False

    if row.get("door_status") not in VALID_DOOR_STATUSES:
        print(f"[MAIN] Invalid door_status: {row.get('door_status')}")
        with DB_LOCK:
            db_utils.update_device_control(door_status="error")
        return False

    if row.get("feeder_status") not in VALID_FEEDER_STATUSES:
        print(f"[MAIN] Invalid feeder_status: {row.get('feeder_status')}")
        with DB_LOCK:
            db_utils.update_device_control(feeder_status="error")
        return False

    return True


def compute_door_target(row: dict, door_open_time: datetime, door_close_time: datetime) -> str:
    """Determine the target door state based on automation mode and time."""
    now = datetime.now(LOCAL_TZ)

    if row["door_auto"]:
        # Auto mode: schedule based on sunrise/sunset
        if now >= door_close_time:
            return "closed"
        elif now >= door_open_time:
            return "open"
        else:
            return "closed"
    else:
        # Manual mode: use user-set target
        return row["door_target"]


def compute_feeder_target(row: dict, feeder_open_time: datetime, feeder_close_time: datetime) -> str:
    """Determine the target feeder state based on automation mode and time."""
    now = datetime.now(LOCAL_TZ)

    if row["feeder_auto"]:
        # Auto mode: schedule based on sunrise/sunset
        if feeder_open_time <= now < feeder_close_time:
            return "open"
        return "closed"
    else:
        # Manual mode: use user-set target
        return row["feeder_target"]


def compute_fan_target(row: dict) -> float:
    """Determine the fan target speed (0-100%) based on automation mode."""
    if row["fan_auto"]:
        # Auto mode: use computed fan speed from smart_coop_control
        print(f"[FAN] Auto mode enabled, fan_speed_pct={row.get('fan_speed_pct')}%, fan_override_pct={row.get('fan_override_pct')}")
        return float(row.get("fan_speed_pct") or 0)
    else:
        # Manual mode: use override value
        print(f"[FAN] Manual override mode, fan_override_pct={row.get('fan_override_pct')}")
        return float(row.get("fan_override_pct") or 0)


def apply_door_state(row: dict, door_open_time: datetime, door_close_time: datetime) -> None:
    """Apply door state machine: update target if needed, then move motor if mismatch."""
    desired = compute_door_target(row, door_open_time, door_close_time)

    print(f"[DOOR] current={row['door_status']} target={row['door_target']} desired={desired} auto={row['door_auto']}")

    # Update target in DB if auto mode changed the computed state
    if row["door_auto"] and row["door_target"] != desired:
        with DB_LOCK:
            db_utils.update_device_control(door_target=desired)

    status = row["door_status"]

    # Don't move if in error state or already in desired state
    if status == "error":
        print("[DOOR] Status is 'error' - no automatic action")
        return

    if desired == status:
        return

    if status == "moving":
        return

    # Move motor to reach desired state
    if desired == "open":
        open_door_thread()
    elif desired == "closed":
        close_door_thread()


def apply_feeder_state(row: dict, feeder_open_time: datetime, feeder_close_time: datetime) -> None:
    """Apply feeder state machine: update target if needed, then move motor if mismatch."""
    desired = compute_feeder_target(row, feeder_open_time, feeder_close_time)

    print(f"[FEEDER] current={row['feeder_status']} target={row['feeder_target']} desired={desired} auto={row['feeder_auto']}")

    # Update target in DB if auto mode changed the computed state
    if row["feeder_auto"] and row["feeder_target"] != desired:
        with DB_LOCK:
            db_utils.update_device_control(feeder_target=desired)

    status = row["feeder_status"]

    # Don't move if in error state or already in desired state
    if status == "error":
        print("[FEEDER] Status is 'error' - no automatic action")
        return

    if desired == status:
        return

    if status == "moving":
        return

    # Move motor to reach desired state
    if desired == "open":
        feeder_open_thread()
    elif desired == "closed":
        feeder_close_thread()


def apply_predator_alarm(is_dark: bool) -> None:
    """Control predator alarm (placeholder - no GPIO control in ESP takeover mode)."""
    print(f"[PREDATOR] is_dark={is_dark} (ESP32 handles LED directly)")


# =============================================================================
# MAIN SERVICE LOOP
# =============================================================================


def cleanup():
    """Safe shutdown of the automation service."""
    print("[MAIN] Cleanup: shutting down ESP-controlled outputs...")
    try:
        send_esp_command({"type": "command", "device": "fan", "speed_pct": 0})
    except Exception:
        pass
    try:
        send_esp_command({"type": "command", "device": "door", "action": "stop"})
    except Exception:
        pass
    try:
        send_esp_command({"type": "command", "device": "feeder", "action": "stop"})
    except Exception:
        pass
    close_esp_serial()


def main_loop() -> None:
    """Main unified loop. Serial reads happen in a daemon thread that feeds
    TELEMETRY_QUEUE; this loop drains the queue, runs the 5-minute aggregation,
    and runs automation logic every POLL_SECONDS."""
    print("[MAIN] Starting unified automation and sensing service...")

    db_utils.setup_database()
    open_esp_serial()

    threading.Thread(
        target=serial_reader_loop,
        daemon=True,
        name="serial_reader",
    ).start()

    window = WindowAccumulator()
    next_flush_at = time.monotonic() + REPORT_EVERY_SECONDS
    next_automation_at = time.monotonic()

    while True:
        try:
            # ===== SENSOR INGESTION (drain queue produced by reader thread) =====
            while True:
                try:
                    data = TELEMETRY_QUEUE.get_nowait()
                except queue.Empty:
                    break
                corrected = correct_sample(data)
                window.add(corrected)
                print(f"[WINDOW] Added sample ({len(window.t1)} readings so far)")

            # Check for serial silence
            if check_serial_silence():
                print("[SERIAL] Silent too long. Reconnecting...")
                close_esp_serial()
                open_esp_serial()

            # ===== 5-MINUTE AGGREGATION =====
            if time.monotonic() >= next_flush_at:
                if window.has_data():
                    row = build_window_result(window)
                    with DB_LOCK:
                        row_id = db_utils.insert_sensor_reading(row)
                    print(
                        f"\n[FLUSH] {REPORT_EVERY_SECONDS}s window complete! Uploading to DB..."
                    )
                    print(
                        f"[DB] Inserted row {row_id}: "
                        f"T={row['temperature_c']}C RH={row['humidity_pct']}% "
                        f"Feeder={row['feeder_pct']}% Waterer={row['waterer_pct']}% "
                        f"H2S={row['h2s_ppm']}ppm CO2={row['co2_ppm']}ppm NH3={row['nh3_ppm']}ppm"
                    )
                    print(
                        f"[DB] Status: Temp={row['temperature_status']} "
                        f"RH={row['humidity_status']} H2S={row['h2s_level']} "
                        f"CO2={row['co2_level']} NH3={row['nh3_level']} "
                        f"Feeder={row['feeder_status']} Waterer={row['waterer_status']}"
                    )
                else:
                    print("[DB] No samples in this window; skipping insert.")

                window.reset()
                next_flush_at += REPORT_EVERY_SECONDS

            # ===== AUTOMATION CONTROL (every POLL_SECONDS) =====
            if time.monotonic() >= next_automation_at:
                try:
                    # Read ESP serial for door state updates (events)
                    door_status = sync_door_status_from_esp()
                    print(f"[DOOR] Detected physical state: {door_status}")

                    # Fetch current state from database
                    with DB_LOCK:
                        row = db_utils.fetch_device_control()
                    print(f"[MAIN] Current control row: {row}")
                    if not validate_control_row(row):
                        next_automation_at += POLL_SECONDS
                        continue

                    # Get sunrise/sunset (cached daily)
                    sunrise, sunset = get_sun_times()

                    # Compute scheduled times with offsets
                    door_open_time = sunrise + timedelta(minutes=DOOR_OPEN_OFFSET_MIN)
                    door_close_time = sunset + timedelta(minutes=DOOR_CLOSE_OFFSET_MIN)

                    feeder_open_time = sunrise + timedelta(minutes=FEEDER_OPEN_OFFSET_MIN)
                    feeder_close_time = sunset + timedelta(minutes=FEEDER_CLOSE_OFFSET_MIN)

                    print(
                        f"[SCHEDULE] door_open={door_open_time.strftime('%H:%M')} "
                        f"door_close={door_close_time.strftime('%H:%M')} "
                        f"feeder_open={feeder_open_time.strftime('%H:%M')} "
                        f"feeder_close={feeder_close_time.strftime('%H:%M')}"
                    )

                    # Apply state machines
                    apply_door_state(row, door_open_time, door_close_time)
                    apply_feeder_state(row, feeder_open_time, feeder_close_time)

                    # Apply fan control
                    fan_target = compute_fan_target(row)
                    set_fan_pct(fan_target)

                    # Apply predator alarm
                    now = datetime.now(LOCAL_TZ)
                    is_dark = now >= door_close_time or now < door_open_time
                    apply_predator_alarm(is_dark)

                    # Log current state
                    print(
                        f"[STATE] door={row['door_status']:7} "
                        f"feeder={row['feeder_status']:7} "
                        f"fan={fan_target:5.0f}% "
                        f"auto(d/f/fa)={row['door_auto']}/{row['feeder_auto']}/{row['fan_auto']} "
                        f"dark={is_dark}"
                    )

                except Exception as exc:
                    print(f"[MAIN] Automation error: {exc}")

                next_automation_at += POLL_SECONDS

            # Avoid busy looping when no data or automation steps are due.
            time.sleep(0.1)

        except KeyboardInterrupt:
            print("[MAIN] Stopped by user")
            break
        except (serial.SerialException, OSError) as exc:
            print(f"[SERIAL] Connection error: {exc}. Reconnecting...")
            close_esp_serial()
            time.sleep(2)
            open_esp_serial()
        except Exception as exc:
            print(f"[MAIN] Unexpected error: {exc}")
            time.sleep(1)


def main() -> None:
    """Setup and run the unified automation and sensing service."""
    try:
        main_loop()
    except KeyboardInterrupt:
        print("[MAIN] Stopped by user")
    except Exception as exc:
        print(f"[MAIN] Fatal error: {exc}")
        raise
    finally:
        cleanup()


if __name__ == "__main__":
    main()
