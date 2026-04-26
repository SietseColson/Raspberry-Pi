#!/usr/bin/env python3
"""
ventilation_simple.py — Simple ventilation rate calculator.

Calculates ventilation rate based on sensor data and writes PWM percentage
to device_control.fan_speed_pct. Designed to run every 5 minutes via systemd.
"""

import math
import os
import sys
from datetime import datetime
from typing import Dict, Optional

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import db_utils

# Configuration constants
COOP_LAT = 50.859660
COOP_LON = 4.696227

VENT_MAX = 150.0
VENT_MIN = 0.0
MIN_VENTILATION_THRESHOLD = 0.0

T_MAX = 24.0
RH_MAX = 0.70
CO2_TARGET = 2000.0
CO2_AMBIENT = 400.0

HEAT_RISK_BOOST_1 = 50.0
HEAT_RISK_BOOST_2 = 75.0
HEAT_RISK_BOOST_3 = 90.0

HEAT_RISK_MULT_LOW = 1.1
HEAT_RISK_MULT_MID = 1.2
HEAT_RISK_MULT_HIGH = 1.3

H2S_EMERG = 5.0
NH3_EMERG = 10.0  # Added NH3 emergency threshold constant

BIRD_WEIGHT_KG = 2.5
CO2_PER_BIRD_LD = 3.8


def get_latest_sensor_data() -> Optional[Dict]:
    """Get the most recent sensor reading from database."""
    return db_utils.get_latest_sensor_reading()


def get_latest_bird_count() -> int:
    """Get the most recent chicken count from database."""
    result = db_utils.get_latest_cv_count()
    return result[0] if result else 0


def get_weather_data() -> tuple[float, float]:
    """Get outdoor temperature and humidity. Returns fallback values on error."""
    try:
        import requests
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": COOP_LAT,
                "longitude": COOP_LON,
                "current": "temperature_2m,relative_humidity_2m",
                "forecast_days": 1,
            },
            timeout=10,
        )
        response.raise_for_status()
        current = response.json()["current"]
        t_amb = float(current["temperature_2m"])
        rh_amb = float(current["relative_humidity_2m"]) / 100.0
        return t_amb, rh_amb
    except Exception:
        # Fallback values
        return 10.0, 0.65


def absolute_humidity(T: float, RH: float) -> float:
    """Calculate absolute humidity [kg water / kg dry air]."""
    psat = 0.61121 * math.exp((18.678 - T / 234.5) * (T / (257.14 + T)))
    return 0.622 * (psat * RH / (101.325 - psat * RH))


def air_density(T: float) -> float:
    """Calculate dry air density [kg/m3]."""
    return 353.0 / (T + 273.15)


def latent_heat(T: float) -> float:
    """Get latent heat of vaporization [J/g] from lookup table."""
    points = [
        (0, 2500.9), (10, 2477.2), (20, 2453.5), (30, 2429.8),
        (40, 2406.4), (50, 2381.9), (60, 2357.7), (70, 2333.7),
        (80, 2308.0), (90, 2282.5), (96, 2266.9),
    ]
    if T <= points[0][0]:
        return points[0][1]
    if T >= points[-1][0]:
        return points[-1][1]

    for i in range(len(points) - 1):
        t0, l0 = points[i]
        t1, l1 = points[i + 1]
        if t0 <= T <= t1:
            return l0 + (l1 - l0) * (T - t0) / (t1 - t0)
    return points[-1][1]


def bird_heat_production(n: int, W: float, T: float) -> tuple[float, float]:
    """Calculate total sensible heat [W] and moisture [g/s] for n birds."""
    total = 10.62 * (W ** 0.75)
    sensible = (0.61 * (1000 + 20 * (20 - T) - 0.228 * T**2)) * (total / 1000)
    latent_w = total - sensible
    moisture = latent_w / latent_heat(T)
    return sensible * n, moisture * n


def co2_seed_rate(n_birds: int) -> float:
    """Calculate CO2-based ventilation rate when no CO2 sensor available."""
    q_m3h = (n_birds * CO2_PER_BIRD_LD) / (24 * 1000)
    delta = (CO2_TARGET - CO2_AMBIENT) * 1e-6
    if delta <= 0:
        return VENT_MAX
    rate = q_m3h / delta
    return rate


def can_cool(T_in: float, T_amb: float) -> bool:
    """Check if outdoor air is cooler than indoor air."""
    return T_amb < T_in + 1.0


def can_dry(T_in: float, RH_in: float, T_amb: float, RH_amb: float) -> bool:
    """Check if outdoor air is drier than indoor air."""
    ah_in = absolute_humidity(T_in, RH_in)
    ah_out = absolute_humidity(T_amb, RH_amb)
    return ah_out < ah_in + 0.001


def calculate_ventilation_rate(
    sensors: Dict,
    T_amb: float,
    RH_amb: float,
    n_birds: int,
    prev_rate: float = 75.0
) -> float:
    """
    Calculate ventilation rate in m3/h based on sensor data.

    Priority: NH3 emergency -> H2S emergency -> CO2 -> heat -> humidity
    """
    T_in = sensors["T_in"]
    RH_in = sensors["RH_in"]
    H2S_in = sensors.get("H2S_in", 0.0)
    NH3_in = sensors.get("NH3_in", 0.0)
    CO2_in = sensors.get("CO2_in")
    if NH3_in >= NH3_EMERG:
        return VENT_MAX

    # H2S emergency override
    if H2S_in >= H2S_EMERG:
        return VENT_MAX

    # CO2-based ventilation floor
    if CO2_in is not None:
        if CO2_in <= CO2_TARGET:
            vr_co2 = MIN_VENTILATION_THRESHOLD
        elif CO2_in >= 3500.0:
            vr_co2 = VENT_MAX
        else:
            co2_fraction = (CO2_in - CO2_TARGET) / (3500.0 - CO2_TARGET)
            vr_co2 = MIN_VENTILATION_THRESHOLD + co2_fraction * (VENT_MAX - MIN_VENTILATION_THRESHOLD)
    else:
        vr_co2 = co2_seed_rate(n_birds)

    target = max(VENT_MIN, min(vr_co2, VENT_MAX))

    # Heat stress override
    if T_in > T_MAX and can_cool(T_in, T_amb):
        Q_sen, _ = bird_heat_production(n_birds, BIRD_WEIGHT_KG, T_in)
        rho = air_density(T_amb)
        cp = 1005.0
        dT = T_MAX - T_amb
        if dT > 0:
            vr_temp = (Q_sen / (rho * cp * dT)) * 3600

            # Apply heat risk multiplier (simplified)
            heat_boost = HEAT_RISK_MULT_LOW  # Default low boost
            vr_temp_boosted = vr_temp * heat_boost

            if vr_temp_boosted > target:
                target = vr_temp_boosted

    # Humidity override
    if RH_in > RH_MAX and can_dry(T_in, RH_in, T_amb, RH_amb):
        ah_in = absolute_humidity(T_in, RH_in)
        ah_out = absolute_humidity(T_amb, RH_amb)
        ah_tgt = absolute_humidity(T_in, RH_MAX)
        num = ah_in - ah_out
        den = ah_tgt - ah_out
        if den > 0 and num > 0:
            vr_rh = prev_rate * (num / den)
            if vr_rh > target:
                target = vr_rh

    return max(VENT_MIN, min(target, VENT_MAX))


def map_rate_to_pwm_pct(rate_m3h: float) -> float:
    """
    Map ventilation rate to PWM fan percentage.
    - <= 50 m3/h -> 0%
    - 50-150 m3/h -> 30-100%
    - >= 150 m3/h -> 100%
    """
    if rate_m3h <= 50.0:
        return 0.0
    if rate_m3h >= 150.0:
        return 100.0

    # Linear mapping from 50-150 m3/h to 30-100%
    scaled = 30.0 + ((rate_m3h - 50.0) / (150.0 - 50.0)) * (100.0 - 30.0)
    return round(max(30.0, min(100.0, scaled)), 1)


def main():
    """Main function: calculate ventilation and update database."""
    try:
        # Setup database
        db_utils.setup_database()

        # Get sensor data
        sensor_data = get_latest_sensor_data()
        if not sensor_data:
            print("No sensor data available")
            return

        # Extract sensor values
        sensors = {
            "T_in": float(sensor_data["temperature_c"]),
            "RH_in": float(sensor_data["humidity_pct"]) / 100.0,
            "H2S_in": float(sensor_data.get("h2s_ppm") or 0.0),
            "NH3_in": float(sensor_data.get("nh3_ppm") or 0.0),
            "CO2_in": float(sensor_data["co2_ppm"]) if sensor_data.get("co2_ppm") else None,
        }

        # Get bird count and weather
        n_birds = get_latest_bird_count()
        T_amb, RH_amb = get_weather_data()

        print(f"Sensors: T={sensors['T_in']:.1f}C RH={sensors['RH_in']*100:.0f}% CO2={sensors['CO2_in'] or 'N/A'} H2S={sensors['H2S_in']:.1f} NH3={sensors['NH3_in']:.1f}")
        print(f"Birds: {n_birds}, Weather: T={T_amb:.1f}C RH={RH_amb*100:.0f}%")

        # Calculate ventilation rate
        rate_m3h = calculate_ventilation_rate(sensors, T_amb, RH_amb, n_birds)

        # Map to PWM percentage
        pwm_pct = map_rate_to_pwm_pct(rate_m3h)

        print(f"Calculated: {rate_m3h:.1f} m3/h -> {pwm_pct:.1f}% PWM")

        # Update database
        if db_utils.update_device_control(fan_speed_pct=pwm_pct):
            print(f"Updated device_control.fan_speed_pct to {pwm_pct:.1f}%")
        else:
            print("Failed to update device_control")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()