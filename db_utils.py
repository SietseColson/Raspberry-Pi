import atexit
import os
from datetime import datetime
from typing import Dict, Optional, Tuple

import psycopg2.pool
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres.qdwofrcncjnhstbqegnj:Vg4Zc6Z!_tLKtMj@aws-1-eu-west-1.pooler.supabase.com:6543/postgres",
)
DB_CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "10"))

_pool = None


def _get_pool() -> psycopg2.pool.SimpleConnectionPool:
    global _pool
    if _pool is None:
        try:
            _pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=5,
                dsn=DATABASE_URL,
                connect_timeout=DB_CONNECT_TIMEOUT,
            )
            print("[DB] Connected")
        except Exception as exc:
            print(f"[DB] Connection failed after {DB_CONNECT_TIMEOUT}s: {exc}")
            raise
    return _pool


atexit.register(lambda: _pool.closeall() if _pool and not _pool.closed else None)


def get_db_connection():
    return _get_pool().getconn()


def release_db_connection(conn):
    _pool.putconn(conn)


CREATE_SENSOR_READINGS_SQL = """
CREATE TABLE IF NOT EXISTS sensor_readings_colson (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    temperature_c FLOAT,
    temperature_status TEXT DEFAULT 'normal',
    humidity_pct FLOAT,
    humidity_status TEXT DEFAULT 'normal',
    heat_stress_index TEXT DEFAULT 'normal',
    feeder_status TEXT DEFAULT 'full',
    waterer_status TEXT DEFAULT 'full',
    feeder_pct FLOAT,
    waterer_pct FLOAT,
    h2s_ppm FLOAT,
    h2s_level TEXT DEFAULT 'normal',
    co2_ppm FLOAT,
    co2_level TEXT DEFAULT 'normal',
    nh3_ppm FLOAT,
    nh3_level TEXT DEFAULT 'normal',
    mold_risk_score FLOAT,
    mold_risk_status TEXT DEFAULT 'normal',
    door_open BOOLEAN DEFAULT FALSE,
    ventilation_on BOOLEAN DEFAULT FALSE,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_sensor_colson_timestamp ON sensor_readings_colson(timestamp DESC);
"""


# Device control table (for automation: door, feeder, fan, etc.)
CREATE_DEVICE_CONTROL_SQL = """
CREATE TABLE IF NOT EXISTS device_control (
    id INTEGER PRIMARY KEY,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fan_auto BOOLEAN DEFAULT TRUE,
    fan_speed_pct FLOAT DEFAULT 0,
    fan_override_pct FLOAT,
    fan_status_pct FLOAT DEFAULT 0,
    door_auto BOOLEAN DEFAULT TRUE,
    door_target TEXT DEFAULT 'open',
    door_status TEXT DEFAULT 'closed',
    feeder_auto BOOLEAN DEFAULT TRUE,
    feeder_target TEXT DEFAULT 'open',
    feeder_status TEXT DEFAULT 'closed'
);
"""

# Future table (not used by sensor pipeline yet)
CREATE_CV_COUNTS_SQL = """
CREATE TABLE IF NOT EXISTS cv_counts_colson (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    number_of_chickens INT,
    egg_count INT
);

CREATE INDEX IF NOT EXISTS idx_cv_colson_timestamp ON cv_counts_colson(timestamp DESC);
"""

CREATE_HEATMAP_REPORTS_SQL = """
CREATE TABLE IF NOT EXISTS heatmap_reports_colson (
    id               SERIAL PRIMARY KEY,
    timestamp        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    crowding_verdict TEXT
);

CREATE INDEX IF NOT EXISTS idx_heatmap_colson_timestamp ON heatmap_reports_colson(timestamp DESC);
"""


def setup_database():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(CREATE_SENSOR_READINGS_SQL)
        cursor.execute(CREATE_CV_COUNTS_SQL)
        cursor.execute(CREATE_HEATMAP_REPORTS_SQL)
        cursor.execute(CREATE_DEVICE_CONTROL_SQL)
        conn.commit()
        cursor.close()
        print("[DB] Tables ready: sensor_readings_colson + cv_counts_colson + device_control")
        
        # Initialize device_control with default row if not present
        init_device_control()
    finally:
        release_db_connection(conn)


def init_device_control():
    """Initialize the device_control table with default row if empty.
    Uses INSERT...ON CONFLICT to be safely idempotent even with concurrent calls."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO device_control (
                fan_auto, fan_speed_pct, fan_override_pct, fan_status_pct,
                door_auto, door_target, door_status,
                feeder_auto, feeder_target, feeder_status
            )
            SELECT TRUE, 0, NULL, 0, TRUE, 'open', 'closed', TRUE, 'open', 'closed'
            WHERE NOT EXISTS (SELECT 1 FROM device_control)
        """)
        cursor.close()
        conn.commit()
    except Exception as exc:
        conn.rollback()
        print(f"[DB] Error initializing device_control: {exc}")
    finally:
        release_db_connection(conn)


def insert_sensor_reading(sensor_data: Dict) -> int:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO sensor_readings_colson (
                timestamp,
                temperature_c, temperature_status,
                humidity_pct, humidity_status,
                heat_stress_index,
                feeder_status, waterer_status,
                feeder_pct, waterer_pct,
                h2s_ppm, h2s_level,
                co2_ppm, co2_level,
                nh3_ppm, nh3_level,
                mold_risk_score, mold_risk_status,
                door_open, ventilation_on,
                error
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) RETURNING id
            """,
            (
                sensor_data.get("timestamp", datetime.now()),
                sensor_data.get("temperature_c"),
                sensor_data.get("temperature_status", "normal"),
                sensor_data.get("humidity_pct"),
                sensor_data.get("humidity_status", "normal"),
                sensor_data.get("heat_stress_index", "normal"),
                sensor_data.get("feeder_status", "full"),
                sensor_data.get("waterer_status", "full"),
                sensor_data.get("feeder_pct"),
                sensor_data.get("waterer_pct"),
                sensor_data.get("h2s_ppm"),
                sensor_data.get("h2s_level", "normal"),
                sensor_data.get("co2_ppm"),
                sensor_data.get("co2_level", "normal"),
                sensor_data.get("nh3_ppm"),
                sensor_data.get("nh3_level", "normal"),
                sensor_data.get("mold_risk_score"),
                sensor_data.get("mold_risk_status", "normal"),
                sensor_data.get("door_open", False),
                sensor_data.get("ventilation_on", False),
                sensor_data.get("error"),
            ),
        )
        inserted_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        return inserted_id
    except Exception:
        conn.rollback()
        raise
    finally:
        release_db_connection(conn)


# =============================================================================
# CV COUNTS
# =============================================================================

def insert_cv_count(chickens: int, eggs: int) -> int:
    """
    Insert a new row into cv_counts_colson.
    Returns the new row id.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO cv_counts_colson (timestamp, number_of_chickens, egg_count)
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            (datetime.now(), chickens, eggs),
        )
        row_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        return row_id
    except Exception:
        conn.rollback()
        raise
    finally:
        release_db_connection(conn)


def get_latest_cv_count() -> Optional[Tuple[int, int]]:
    """
    Return (number_of_chickens, egg_count) of the most recent row,
    or None if the table is empty.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            SELECT number_of_chickens, egg_count
            FROM cv_counts_colson
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            return None
        return (row["number_of_chickens"], row["egg_count"])
    finally:
        release_db_connection(conn)


def insert_heatmap_report(crowding_verdict: str) -> int:
    """
    Insert a new row into heatmap_reports_colson.
    Only writes the crowding verdict.
    Returns the new row id.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO heatmap_reports_colson (timestamp, crowding_verdict)
            VALUES (%s, %s)
            RETURNING id
            """,
            (datetime.now(), crowding_verdict),
        )
        row_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        return row_id
    except Exception:
        conn.rollback()
        raise
    finally:
        release_db_connection(conn)


def get_latest_heatmap_report() -> Optional[Dict]:
    """
    Return the most recent heatmap report row as a dict, or None if empty.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            SELECT *
            FROM heatmap_reports_colson
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        cursor.close()
        return dict(row) if row else None
    finally:
        release_db_connection(conn)


# =============================================================================
# SENSOR READINGS (READ)
# =============================================================================

def get_latest_sensor_reading() -> Optional[Dict]:
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            SELECT
                id, timestamp,
                temperature_c, temperature_status,
                humidity_pct, humidity_status,
                heat_stress_index,
                feeder_status, waterer_status,
                feeder_pct, waterer_pct,
                h2s_ppm, h2s_level,
                co2_ppm, co2_level,
                nh3_ppm, nh3_level,
                mold_risk_score, mold_risk_status,
                door_open, ventilation_on,
                error
            FROM sensor_readings_colson
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        cursor.close()
        return dict(row) if row else None
    finally:
        release_db_connection(conn)


# =============================================================================
# DEVICE CONTROL (Automation: door, feeder, fan state)
# =============================================================================

VALID_UPDATE_COLUMNS = {
    "fan_auto", "fan_speed_pct", "fan_override_pct", "fan_status_pct",
    "door_auto", "door_target", "door_status",
    "feeder_auto", "feeder_target", "feeder_status",
}


def fetch_device_control() -> Optional[Dict]:
    """Fetch the current device control state (row with id=1)."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT * FROM device_control WHERE id = 1")
        row = cursor.fetchone()
        cursor.close()
        return dict(row) if row else None
    finally:
        release_db_connection(conn)


def update_device_control(**kwargs) -> bool:
    """
    Update device control columns safely.
    Only allows updates to valid columns (defined in VALID_UPDATE_COLUMNS).
    Returns True on success, False on failure.
    """
    if not kwargs:
        return False

    # Validate column names
    invalid = [k for k in kwargs if k not in VALID_UPDATE_COLUMNS]
    if invalid:
        print(f"[DB] Invalid columns in update_device_control: {invalid}")
        return False

    cols = ", ".join([f"{k} = %s" for k in kwargs])
    vals = list(kwargs.values())
    vals.append(1)  # WHERE id = 1

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE device_control SET {cols}, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
            vals
        )
        cursor.close()
        conn.commit()
        return True
    except Exception as exc:
        conn.rollback()
        print(f"[DB] Error updating device_control: {exc}")
        return False
    finally:
        release_db_connection(conn)
