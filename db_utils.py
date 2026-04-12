import atexit
from datetime import datetime
from typing import Dict, Optional, Tuple

import psycopg2.pool
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://postgres.qdwofrcncjnhstbqegnj:Vg4Zc6Z!_tLKtMj@aws-1-eu-west-1.pooler.supabase.com:6543/postgres"

_pool = psycopg2.pool.SimpleConnectionPool(minconn=1, maxconn=5, dsn=DATABASE_URL)
print("[DB] Connected")

atexit.register(lambda: _pool.closeall() if _pool and not _pool.closed else None)


def get_db_connection():
    return _pool.getconn()


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


def setup_database():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(CREATE_SENSOR_READINGS_SQL)
        cursor.execute(CREATE_CV_COUNTS_SQL)
        conn.commit()
        cursor.close()
        print("[DB] Tables ready: sensor_readings_colson + cv_counts_colson")
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
