import json
import logging
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import requests
from supabase import Client, create_client

SCRIPT_DIR = Path(__file__).resolve().parent

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

SENSOR_TABLE = os.getenv("SENSOR_TABLE", "sensor_readings_colson")
RISK_SNAPSHOT_TABLE = os.getenv("RISK_SNAPSHOT_TABLE", "risk_snapshots")
CV_COUNT_TABLE = os.getenv("CV_COUNT_TABLE", "cv_counts_colson")

COOP_LAT = float(os.environ.get("COOP_LATITUDE", "50.864403"))
COOP_LON = float(os.environ.get("COOP_LONGITUDE", "4.686699"))

VENT_MAX = 150.0
VENT_MIN = 0.0
MIN_VENTILATION_THRESHOLD = 0.0  # user-defined baseline ventilation (m3/h)
MAX_SLEW = 50.0

T_MIN = 16.0
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

H2S_WARN = 1.0
H2S_EMERG = 5.0

BIRD_WEIGHT_KG = 2.5
CO2_PER_BIRD_LD = 3.8

STATE_FILE = SCRIPT_DIR / "vent_state.json"

Sensors = Dict[str, Any]


def connect() -> Client:
    """Create a Supabase client from environment variables."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise EnvironmentError(
            "SUPABASE_URL and SUPABASE_KEY must be set in your .env file."
        )
    return create_client(url, key)


def check_connection(client: Client) -> bool:
    """Ping required tables and return False if any query fails."""
    log.info("-- Supabase connection check ----------------")
    ok = True
    for table in [SENSOR_TABLE, CV_COUNT_TABLE]:
        try:
            client.table(table).select("id").limit(1).execute()
            log.info("  OK   %s", table)
        except Exception as exc:
            log.error("  FAIL %s -> %s", table, exc)
            ok = False
    log.info("-- %s --------------------------------------", "OK" if ok else "FAILED")
    return ok


def _build_sensor_payload(row: Dict[str, Any]) -> Sensors:
    """Map one raw sensor row to the controller input structure."""
    co2_raw = row.get("co2_ppm")
    return {
        "T_in": float(row["temperature_c"]),
        "RH_in": float(row["humidity_pct"]) / 100.0,
        "H2S_in": float(row.get("h2s_ppm") or 0.0),
        "CO2_in": float(co2_raw) if co2_raw is not None else None,
    }


def read_sensors(client: Client) -> Sensors:
    """Read the latest indoor sensor row from Supabase."""
    response = (
        client.table(SENSOR_TABLE)
        .select("*")
        .order("timestamp", desc=True)
        .limit(1)
        .execute()
    )
    if not response.data:
        raise ValueError(f"{SENSOR_TABLE} is empty.")

    sensors = _build_sensor_payload(response.data[0])
    log.info(
        "Sensors   T=%.1f C  RH=%.0f%%  H2S=%.2f ppm  CO2=%s",
        sensors["T_in"],
        sensors["RH_in"] * 100,
        sensors["H2S_in"],
        f"{sensors['CO2_in']:.0f} ppm"
        if sensors["CO2_in"] is not None
        else "no sensor yet",
    )
    return sensors


def read_bird_count(client: Client) -> int:
    """Read the latest chicken count. Return 0 if the table is empty."""
    response = (
        client.table(CV_COUNT_TABLE)
        .select("number_of_chickens, egg_count, timestamp")
        .order("timestamp", desc=True)
        .limit(1)
        .execute()
    )
    if not response.data:
        log.warning("%s empty - defaulting to 0 birds", CV_COUNT_TABLE)
        return 0

    row = response.data[0]
    n_birds = int(row.get("number_of_chickens") or 0)
    log.info("CV count  n_birds=%d  eggs=%d", n_birds, int(row.get("egg_count") or 0))
    return n_birds


def read_weather() -> tuple[float, float]:
    """Read outdoor temperature (C) and RH (0-1) from Open-Meteo."""
    try:
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
        log.info("Weather   T_amb=%.1f C  RH_amb=%.0f%%", t_amb, rh_amb * 100)
        return t_amb, rh_amb
    except Exception as exc:
        log.warning("Weather fetch failed (%s) - fallback T=10 C RH=65%%", exc)
        return 10.0, 0.65


def absolute_humidity(T: float, RH: float) -> float:
    """Absolute humidity [kg water / kg dry air] using the Buck equation."""
    psat = 0.61121 * math.exp((18.678 - T / 234.5) * (T / (257.14 + T)))
    return 0.622 * (psat * RH / (101.325 - psat * RH))


def air_density(T: float) -> float:
    """Dry air density [kg/m3]."""
    return 353.0 / (T + 273.15)


def latent_heat(T: float) -> float:
    """Latent heat of vaporization [J/g] from a lookup table."""
    points = [
        (0, 2500.9),
        (2, 2496.4),
        (4, 2491.2),
        (10, 2477.2),
        (14, 2467.7),
        (18, 2458.3),
        (20, 2453.5),
        (25, 2441.7),
        (30, 2429.8),
        (34, 2420.3),
        (40, 2406.4),
        (44, 2396.4),
        (50, 2381.9),
        (54, 2372.3),
        (60, 2357.7),
        (70, 2333.7),
        (80, 2308.0),
        (90, 2282.5),
        (96, 2266.9),
    ]
    if T <= points[0][0]:
        return points[0][1]
    if T >= points[-1][0]:
        return points[-1][1]

    for index in range(len(points) - 1):
        t0, l0 = points[index]
        t1, l1 = points[index + 1]
        if t0 <= T <= t1:
            return l0 + (l1 - l0) * (T - t0) / (t1 - t0)

    return points[-1][1]


def bird_heat_production(n: int, W: float, T: float) -> tuple[float, float]:
    """Return total sensible heat [W] and moisture [g/s] for n birds."""
    total = 10.62 * (W ** 0.75)
    sensible = (0.61 * (1000 + 20 * (20 - T) - 0.228 * T**2)) * (total / 1000)
    latent_w = total - sensible
    moisture = latent_w / latent_heat(T)
    return sensible * n, moisture * n


def co2_seed_rate(n_birds: int) -> float:
    """Model-based CO2 ventilation floor used when no CO2 sensor is present."""
    # TODO: Recheck the unit naming here after hardware tests; behavior is kept
    # unchanged for now because downstream control tuning depends on it.
    q_m3h = (n_birds * CO2_PER_BIRD_LD) / (24 * 1000)
    delta = (CO2_TARGET - CO2_AMBIENT) * 1e-6
    if delta <= 0:
        return VENT_MAX
    rate = q_m3h / delta
    log.info("CO2 seed  model-based (no sensor) -> %.0f m3/h", rate)
    return rate


def can_cool(T_in: float, T_amb: float) -> bool:
    """Return True only when outside air is cooler than inside air."""
    if T_amb < T_in + 1.0:  # add hysteresis to avoid rapid on/off when temperatures are close
        return True
    log.info(
        "Cooling   SKIP - T_amb=%.1f C >= T_in=%.1f C (outside warmer)",
        T_amb,
        T_in,
    )
    return False


def can_dry(T_in: float, RH_in: float, T_amb: float, RH_amb: float) -> bool:
    """Return True only when outdoor absolute humidity is lower."""
    ah_in = absolute_humidity(T_in, RH_in)
    ah_out = absolute_humidity(T_amb, RH_amb)
    if ah_out < ah_in + 0.001:  # add hysteresis to avoid rapid on/off when AH values are close
        return True
    log.info(
        "Drying    SKIP - outdoor AH=%.4f >= indoor AH=%.4f (outside too humid)",
        ah_out,
        ah_in,
    )
    return False


def compute_fan_rate(
    sensors: dict,
    heat_risk: dict,
    T_amb: float,
    RH_amb: float,
    n_birds: int,
    prev_rate: float,
    initialised: bool,
) -> tuple[float, str]:
    """
    Compute the fan rate [m3/h] for this cycle.

    Priority order:
    HARD H2S emergency -> CO2 floor (min = MIN_VENTILATION_THRESHOLD) -> heat -> humidity -> cold clamp.
    """
    T_in = sensors["T_in"]
    RH_in = sensors["RH_in"]
    H2S_in = sensors["H2S_in"]
    CO2_in = sensors["CO2_in"]
    heat_risk_score = float(heat_risk["risk_score"])

    notes = []

    log.info(
        "FAN DEBUG start T_in=%.2f RH_in=%.4f T_amb=%.2f RH_amb=%.4f "
        "heat_risk_score=%.1f prev_rate=%.2f initialised=%s",
        T_in,
        RH_in,
        T_amb,
        RH_amb,
        heat_risk_score,
        prev_rate,
        initialised,
    )

    if not initialised:
        prev_rate = VENT_MAX / 4.0
        notes.append(f"cold-start seed {prev_rate:.0f} m3/h")
        log.info("FAN DEBUG cold-start override prev_rate=%.2f", prev_rate)

    if H2S_in >= H2S_EMERG:
        log.info("FAN DEBUG H2S emergency H2S_in=%.2f -> rate=%.2f", H2S_in, VENT_MAX)
        return VENT_MAX, f"H2S EMERGENCY {H2S_in:.1f} ppm - fan at maximum"

    if CO2_in is not None:
        if CO2_in <= CO2_TARGET:
            vr_co2 = MIN_VENTILATION_THRESHOLD  # avoids absurdly low rates and influences the next-rate calculation
        elif CO2_in >= 3500.0:
            vr_co2 = VENT_MAX
        else:
            co2_fraction = (CO2_in - CO2_TARGET) / (3500.0 - CO2_TARGET)
            vr_co2 = MIN_VENTILATION_THRESHOLD + co2_fraction * (VENT_MAX - MIN_VENTILATION_THRESHOLD)
        notes.append(f"CO2={CO2_in:.0f} ppm -> floor {vr_co2:.0f} m3/h")
    else:
        vr_co2 = co2_seed_rate(n_birds)
        notes.append(f"CO2 model seed (no sensor) -> floor {vr_co2:.0f} m3/h")

    vr_co2 = max(VENT_MIN, min(vr_co2, VENT_MAX))
    target = vr_co2
    log.info(
        "FAN DEBUG CO2 branch CO2_in=%s vr_co2=%.2f target_after_co2=%.2f",
        f"{CO2_in:.2f}" if CO2_in is not None else "None",
        vr_co2,
        target,
    )

    heat_target_before = target
    heat_branch_active = T_in > T_MAX
    can_cool_result = False
    dT = T_MAX - T_amb
    vr_temp = None
    vr_temp_boosted = None
    heat_boost = None
    if T_in > T_MAX:
        can_cool_result = can_cool(T_in, T_amb)
        if can_cool_result:
            Q_sen, _ = bird_heat_production(n_birds, BIRD_WEIGHT_KG, T_in)
            rho = air_density(T_amb)
            cp = 1005.0
            if dT > 0:
                vr_temp = (Q_sen / (rho * cp * dT)) * 3600
                if heat_risk_score >= HEAT_RISK_BOOST_3:
                    heat_boost = HEAT_RISK_MULT_HIGH
                elif heat_risk_score >= HEAT_RISK_BOOST_2:
                    heat_boost = HEAT_RISK_MULT_MID
                elif heat_risk_score >= HEAT_RISK_BOOST_1:
                    heat_boost = HEAT_RISK_MULT_LOW
                else:
                    heat_boost = 1.0

                vr_temp_boosted = vr_temp * heat_boost
                if vr_temp_boosted > target:
                    notes.append(
                        f"heat stress T={T_in:.1f} C risk={heat_risk_score:.1f} "
                        f"-> {vr_temp_boosted:.0f} m3/h "
                        f"(base {vr_temp:.0f}, boost x{heat_boost:.2f}, "
                        f"T_amb={T_amb:.1f} C, can cool)"
                    )
                    target = vr_temp_boosted
    log.info(
        "FAN DEBUG heat branch active=%s can_cool=%s dT=%.2f "
        "target_before=%.2f vr_temp=%s heat_boost=%s vr_temp_boosted=%s target_after=%.2f",
        heat_branch_active,
        can_cool_result,
        dT,
        heat_target_before,
        f"{vr_temp:.2f}" if vr_temp is not None else "None",
        f"{heat_boost:.2f}" if heat_boost is not None else "None",
        f"{vr_temp_boosted:.2f}" if vr_temp_boosted is not None else "None",
        target,
    )

    humidity_target_before = target
    humidity_branch_active = RH_in > RH_MAX
    can_dry_result = False
    vr_rh = None
    if RH_in > RH_MAX:
        can_dry_result = can_dry(T_in, RH_in, T_amb, RH_amb)
        if can_dry_result:
            ah_in = absolute_humidity(T_in, RH_in)
            ah_out = absolute_humidity(T_amb, RH_amb)
            ah_tgt = absolute_humidity(T_in, RH_MAX)
            num = ah_in - ah_out
            den = ah_tgt - ah_out
            if den > 0 and num > 0:
                vr_rh = prev_rate * (num / den)
                if vr_rh > target:
                    notes.append(
                        f"humidity RH={RH_in * 100:.0f}% -> {vr_rh:.0f} m3/h "
                        f"(outdoor AH lower, can dry)"
                    )
                    target = vr_rh
    log.info(
        "FAN DEBUG humidity branch active=%s can_dry=%s "
        "target_before=%.2f vr_rh=%s target_after=%.2f",
        humidity_branch_active,
        can_dry_result,
        humidity_target_before,
        f"{vr_rh:.2f}" if vr_rh is not None else "None",
        target,
    )

    if H2S_WARN <= H2S_in < H2S_EMERG:
        boost = VENT_MAX * 0.15 * (H2S_in - H2S_WARN) / max(H2S_WARN, 1e-9)
        target = min(target + boost, VENT_MAX)
        notes.append(f"H2S warning {H2S_in:.1f} ppm -> boost +{boost:.0f} m3/h")
        log.info("FAN DEBUG H2S warning boost=%.2f target_after_h2s=%.2f", boost, target)

    if T_in < T_MIN and target > vr_co2:
        target = vr_co2
        notes.append(
            f"cold stress T={T_in:.1f} C - clamped to CO2 floor {vr_co2:.0f} m3/h"
        )
        log.info("FAN DEBUG cold clamp applied target=%.2f", target)

    delta = max(-MAX_SLEW, min(target - prev_rate, MAX_SLEW))
    rate = max(VENT_MIN, min(prev_rate + delta, VENT_MAX))
    log.info(
        "FAN DEBUG final target=%.2f prev_rate=%.2f delta=%.2f final_rate=%.2f notes=%s",
        target,
        prev_rate,
        delta,
        rate,
        " | ".join(notes) if notes else "baseline",
    )

    return rate, " | ".join(notes) if notes else "baseline"


def map_rate_to_pwm_pct(
    rate_m3h: float,
    off_threshold: float = 50.0,
    min_rate: float = 50.0,
    max_rate: float = 150.0,
    min_pct: float = 30.0,
    max_pct: float = 100.0,
) -> float:
    """Map a ventilation rate to a PWM fan percentage.

    - <= off_threshold -> 0%
    - 50-150 m3/h -> 30-100%
    - >= 150 m3/h -> 100%
    """
    if rate_m3h <= off_threshold:
        return 0.0
    if rate_m3h >= max_rate:
        return max_pct

    scaled = min_pct + (
        (rate_m3h - min_rate) / (max_rate - min_rate)
    ) * (max_pct - min_pct)
    return round(max(min_pct, min(max_pct, scaled)), 1)


def load_state() -> tuple[float, bool]:
    """Load the previous fan state from disk."""
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
            return float(state["prev_rate"]), bool(state["initialised"])
        except Exception:
            pass
    return 0.0, False


def save_state(rate: float) -> None:
    """Persist the current fan state to disk."""
    STATE_FILE.write_text(
        json.dumps({"prev_rate": rate, "initialised": True}, indent=2)
    )


class Fan:
    """Stub fan actuator. Replace set_rate() with the real hardware call."""

    def set_rate(self, rate: float) -> None:
        log.info("FAN COMMAND -> %.0f m3/h", rate)


def main() -> None:
    """Run one ventilation-only cycle."""
    log.info(
        "==== Ventilation cycle %s ====",
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    )

    client = connect()
    if not check_connection(client):
        log.error("Aborting - fix Supabase connection.")
        return

    try:
        sensors = read_sensors(client)
    except ValueError as exc:
        log.error("Sensor read failed: %s", exc)
        return

    n_birds = read_bird_count(client)
    T_amb, RH_amb = read_weather()

    prev_rate, initialised = load_state()
    rate, reason = compute_fan_rate(
        sensors=sensors,
        heat_risk={"risk_score": 0.0},
        T_amb=T_amb,
        RH_amb=RH_amb,
        n_birds=n_birds,
        prev_rate=prev_rate,
        initialised=initialised,
    )

    log.info("Result    rate=%.0f m3/h", rate)
    log.info("Reason    %s", reason)

    Fan().set_rate(rate)
    save_state(rate)

    log.info("==== Cycle complete ====")


if __name__ == "__main__":
    main()
