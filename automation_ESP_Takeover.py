#!/usr/bin/env python3
"""
Automation control for chicken coop: door, feeder, fan, and predator alarm.

Sends serial commands to the ESP32 to control hardware and reads automation state from the device_control
table in the database. Scheduling is based on sunrise/sunset times.

This script is designed to run continuously as a systemctl service.
"""

import atexit
import json
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pytz
import requests
import serial

import db_utils

# =============================================================================
# CONFIGURATION
# =============================================================================

LATITUDE = 50.88
LONGITUDE = 4.70
LOCAL_TZ = pytz.timezone("Europe/Brussels")

# Polling interval (seconds)
POLL_SECONDS = 5

 # Motor timing (seconds)
DOOR_TIMEOUT_SECONDS = 100
FEEDER_OPEN_SECONDS = 14
FEEDER_CLOSE_SECONDS = 14

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

# Cache for sun times (to avoid repeated API calls)
LAST_VALID_SUN_TIMES = {
    "sunrise": None,
    "sunset": None,
    "fetch_date": None,
    "last_fetch_time": None,
    "last_retry": None,
}

# Threading lock for database operations
DB_LOCK = threading.Lock()

# =============================================================================
# GPIO SETUP
# =============================================================================

ESP_SERIAL_PORT = "/dev/esp32"
ESP_SERIAL_BAUD = 115200
ESP_SERIAL_TIMEOUT = 0.1 
ESP_SERIAL_RECONNECT_SECONDS = 5

esp_port: Optional[serial.Serial] = None
esp_door_state = "moving_or_unknown"
esp_command_pending = False
esp_command_lock = threading.Lock()
esp_last_serial_line = ""
esp_last_telemetry: dict = {}

def open_esp_serial() -> None:
    global esp_port
    if esp_port is not None and esp_port.is_open:
        return
    try:
        esp_port = serial.Serial(
            ESP_SERIAL_PORT,
            ESP_SERIAL_BAUD,
            timeout=ESP_SERIAL_TIMEOUT,
        )
        print(f"[SERIAL] Opened ESP serial on {ESP_SERIAL_PORT}@{ESP_SERIAL_BAUD}")
    except Exception as exc:
        esp_port = None
        print(f"[SERIAL] Failed to open ESP serial: {exc}")


def close_esp_serial() -> None:
    global esp_port
    if esp_port is not None:
        try:
            esp_port.close()
            print("[SERIAL] Closed ESP serial")
        except Exception as exc:
            print(f"[SERIAL] Error closing ESP serial: {exc}")
        esp_port = None


def send_esp_command(command: dict) -> bool:
    if esp_port is None or not esp_port.is_open:
        open_esp_serial()
    if esp_port is None or not esp_port.is_open:
        return False

    payload = json.dumps(command)
    try:
        esp_port.write((payload + "\n").encode("utf-8"))
        esp_port.flush()
        return True
    except Exception as exc:
        print(f"[SERIAL] Failed to send command: {exc}")
        close_esp_serial()
        return False


def parse_esp_line(line: str) -> None:
    global esp_door_state, esp_last_telemetry, esp_command_pending
    if not line.strip():
        return
    try:
        payload = json.loads(line)
    except json.JSONDecodeError:
        return

    msg_type = payload.get("type")
    if msg_type == "event":
        device = payload.get("device")
        status = payload.get("status")
        if device == "bottom_switch":
            if status == "CLOSED":
                esp_door_state = "fully_closed"
                db_utils.update_device_control(door_status="closed")
                esp_command_pending = False
            elif status == "RELEASED":
                esp_door_state = "moving_or_unknown"
        elif device == "top_switch":
            if status == "OPEN":
                esp_door_state = "fully_open"
                db_utils.update_device_control(door_status="open")
                esp_command_pending = False
            elif status == "RELEASED":
                esp_door_state = "moving_or_unknown"
    elif msg_type == "telemetry":
        esp_last_telemetry = payload
        door_state = payload.get("door_state")
        if door_state == "fully_closed":
            esp_door_state = "fully_closed"
            db_utils.update_device_control(door_status="closed")
            esp_command_pending = False
        elif door_state == "fully_open":
            esp_door_state = "fully_open"
            db_utils.update_device_control(door_status="open")
            esp_command_pending = False
        elif door_state == "moving_or_unknown":
            esp_door_state = "moving_or_unknown"


def read_esp_serial() -> None:
    if esp_port is None or not esp_port.is_open:
        open_esp_serial()
    if esp_port is None or not esp_port.is_open:
        return
    try:
        while esp_port.in_waiting:
            line = esp_port.readline().decode("utf-8", errors="ignore").strip()
            if line:
                parse_esp_line(line)
    except Exception as exc:
        print(f"[SERIAL] Read error: {exc}")
        close_esp_serial()


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


def stop_door() -> None:
    """Stop the door motor via ESP."""
    send_esp_command({"type": "command", "device": "door", "action": "stop"})


def stop_feeder() -> None:
    """Stop the feeder motor via ESP."""
    send_esp_command({"type": "command", "device": "feeder", "action": "stop"})


def fan_off() -> None:
    """Turn the fan off via ESP."""
    send_esp_command({"type": "command", "device": "fan", "speed_pct": 0})


def set_fan_pct(pct: float) -> None:
    """Set fan speed percentage (0-100) on the ESP32."""
    try:
        pct = max(0.0, min(100.0, float(pct)))
    except (ValueError, TypeError):
        pct = 0.0

    if pct <= 0:
        print(f"[FAN] OFF (requested {pct:.1f}%)")
    else:
        print(f"[FAN] Set fan speed to {pct:.1f}%")

    send_esp_command({"type": "command", "device": "fan", "speed_pct": pct})
    db_utils.update_device_control(fan_status_pct=pct)


def open_door() -> bool:
    """Send the ESP command to open the door."""
    print("[DOOR] Sending open command")
    with DB_LOCK:
        db_utils.update_device_control(door_status="moving")
    global esp_command_pending
    esp_command_pending = True
    return send_esp_command({"type": "command", "device": "door", "action": "open"})


def open_door_thread() -> None:
    """Send the ESP door open command in a background thread."""
    thread = threading.Thread(target=open_door, name="door_open")
    thread.daemon = True
    thread.start()


def close_door() -> bool:
    """Send the ESP command to close the door."""
    print("[DOOR] Sending close command")
    with DB_LOCK:
        db_utils.update_device_control(door_status="moving")
    global esp_command_pending
    esp_command_pending = True
    return send_esp_command({"type": "command", "device": "door", "action": "close"})


def close_door_thread() -> None:
    """Send the ESP door close command in a background thread."""
    thread = threading.Thread(target=close_door, name="door_close")
    thread.daemon = True
    thread.start()


def feeder_open() -> bool:
    """Send the ESP command to open the feeder."""
    print("[FEEDER] Sending open command")
    with DB_LOCK:
        db_utils.update_device_control(feeder_status="moving")
    return send_esp_command({"type": "command", "device": "feeder", "action": "open"})


def feeder_open_thread() -> None:
    """Send the ESP feeder open command in a background thread."""
    thread = threading.Thread(target=feeder_open, name="feeder_open")
    thread.daemon = True
    thread.start()


def feeder_close() -> bool:
    """Send the ESP command to close the feeder."""
    print("[FEEDER] Sending close command")
    with DB_LOCK:
        db_utils.update_device_control(feeder_status="moving")
    return send_esp_command({"type": "command", "device": "feeder", "action": "close"})


def feeder_close_thread() -> None:
    """Send the ESP feeder close command in a background thread."""
    thread = threading.Thread(target=feeder_close, name="feeder_close")
    thread.daemon = True
    thread.start()

# =============================================================================
# SUN TIMES & SCHEDULING
# =============================================================================
def open_door() -> bool:
    """Send the ESP command to open the door."""
    print("[DOOR] Sending open command")
    with DB_LOCK:
        db_utils.update_device_control(door_status="moving")
    return send_esp_command({"type": "command", "device": "door", "action": "open"})


def open_door_thread() -> None:
    """Send the ESP door open command in a background thread."""
    thread = threading.Thread(target=open_door, name="door_open")
    thread.daemon = True
    thread.start()


def close_door() -> bool:
    """Send the ESP command to close the door."""
    print("[DOOR] Sending close command")
    with DB_LOCK:
        db_utils.update_device_control(door_status="moving")
    return send_esp_command({"type": "command", "device": "door", "action": "close"})


def close_door_thread() -> None:
    """Send the ESP door close command in a background thread."""
    thread = threading.Thread(target=close_door, name="door_close")
    thread.daemon = True
    thread.start()


def feeder_open() -> bool:
    """Send the ESP command to open the feeder."""
    print("[FEEDER] Sending open command")
    with DB_LOCK:
        db_utils.update_device_control(feeder_status="moving")
    return send_esp_command({"type": "command", "device": "feeder", "action": "open"})


def feeder_open_thread() -> None:
    """Send the ESP feeder open command in a background thread."""
    thread = threading.Thread(target=feeder_open, name="feeder_open")
    thread.daemon = True
    thread.start()


def feeder_close() -> bool:
    """Send the ESP command to close the feeder."""
    print("[FEEDER] Sending close command")
    with DB_LOCK:
        db_utils.update_device_control(feeder_status="moving")
    return send_esp_command({"type": "command", "device": "feeder", "action": "close"})


def feeder_close_thread() -> None:
    """Send the ESP feeder close command in a background thread."""
    thread = threading.Thread(target=feeder_close, name="feeder_close")
    thread.daemon = True
    thread.start()


# =============================================================================
# SUN TIMES & SCHEDULING
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
            .replace(date=today)
        )
        sunset = (
            datetime.fromisoformat(results["sunset"].replace("Z", "+00:00"))
            .astimezone(LOCAL_TZ)
            .replace(date=today)
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


def is_door_motor_running() -> bool:
    """
    Check if the door motor is currently running.
    Returns True if either forward or backward pin is active.
    """
    return esp_command_pending


def sync_door_status_from_esp() -> Optional[str]:
    """Sync door status in database using ESP-reported switch and door state."""
    if esp_door_state == "fully_open":
        db_utils.update_device_control(door_status="open")
        return "open"

    if esp_door_state == "fully_closed":
        db_utils.update_device_control(door_status="closed")
        return "closed"

    current = db_utils.fetch_device_control() or {}
    if current.get("door_status") == "timeout":
        return "timeout"

    if esp_command_pending:
        db_utils.update_device_control(door_status="moving")
        return "moving"

    db_utils.update_device_control(door_status="inbetween")
    return "inbetween"


# =============================================================================
# STATE MACHINES
# =============================================================================


def validate_control_row(row: Optional[dict]) -> bool:
    """Validate the device_control row for consistency."""
    if row is None:
        print("[MAIN] No control row found in database")
        return False

    if row.get("door_target") not in VALID_DOOR_TARGETS:
        print(f"[MAIN] Invalid door_target: {row.get('door_target')}")
        db_utils.update_device_control(door_status="error")
        return False

    if row.get("feeder_target") not in VALID_FEEDER_TARGETS:
        print(f"[MAIN] Invalid feeder_target: {row.get('feeder_target')}")
        db_utils.update_device_control(feeder_status="error")
        return False

    if row.get("door_status") not in VALID_DOOR_STATUSES:
        print(f"[MAIN] Invalid door_status: {row.get('door_status')}")
        db_utils.update_device_control(door_status="error")
        return False

    if row.get("feeder_status") not in VALID_FEEDER_STATUSES:
        print(f"[MAIN] Invalid feeder_status: {row.get('feeder_status')}")
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
        # Auto mode: use computed fan speed from smart_coop_control or default
        print(f"[FAN] Auto mode enabled, fan_speed_pct={row.get('fan_speed_pct')}%, fan_override_pct={row.get('fan_override_pct')}")
        return float(row.get("fan_speed_pct") or 0)
    else:
        print(f"[FAN] Manual override mode, fan_override_pct={row.get('fan_override_pct')}")
        # Manual mode: use override value
        return float(row.get("fan_override_pct") or 0)


def apply_door_state(row: dict, door_open_time: datetime, door_close_time: datetime) -> None:
    """Apply door state machine: update target if needed, then move motor if mismatch."""
    desired = compute_door_target(row, door_open_time, door_close_time)

    print(f"[DOOR] current={row['door_status']} target={row['door_target']} desired={desired} auto={row['door_auto']}")

    # Update target in DB if auto mode changed the computed state
    if row["door_auto"] and row["door_target"] != desired:
        db_utils.update_device_control(door_target=desired)

    status = row["door_status"]

    # Don't move if in error state or already in desired state
    if status == "error":
        print("[DOOR] Status is 'error' - no automatic action")
        return

    if desired == status:
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
        db_utils.update_device_control(feeder_target=desired)

    status = row["feeder_status"]

    # Don't move if in error state or already in desired state
    if status == "error":
        print("[FEEDER] Status is 'error' - no automatic action")
        return

    if desired == status:
        return

    # Move motor to reach desired state
    if desired == "open":
        feeder_open_thread()
    elif desired == "closed":
        feeder_close_thread()


def apply_predator_alarm(is_dark: bool) -> None:
    """Control predator alarm LED based on darkness."""
    print(f"[PREDATOR] is_dark={is_dark} (no Pi GPIO control in ESP takeover mode)")


# =============================================================================
# MAIN LOOP
# =============================================================================


def main_loop() -> None:
    """Main control loop: runs every POLL_SECONDS."""
    print("[MAIN] Automation loop started")

    while True:
        try:
            # Read ESP serial for door state updates
            read_esp_serial()
            door_status = sync_door_status_from_esp()
            print(f"[DOOR] Detected physical state: {door_status}")

            # Fetch current state from database
            row = db_utils.fetch_device_control()
            print(f"[MAIN] Current control row: {row}")
            if not validate_control_row(row):
                time.sleep(POLL_SECONDS)
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
            print(f"[MAIN] Unexpected error in loop: {exc}")
            time.sleep(2)

        time.sleep(POLL_SECONDS)


def run() -> None:
    """Setup and run the automation service."""
    try:
        print("[MAIN] Starting automation service...")
        db_utils.setup_database()
        main_loop()
    except KeyboardInterrupt:
        print("[MAIN] Stopped by user")
    except Exception as exc:
        print(f"[MAIN] Fatal error: {exc}")
        raise
    finally:
        cleanup()


if __name__ == "__main__":
    run()
