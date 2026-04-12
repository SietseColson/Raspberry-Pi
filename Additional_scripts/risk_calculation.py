from __future__ import annotations
import math
from typing import Any, Dict, List
from enum import Enum
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic.dataclasses import dataclass
from dataclasses import dataclass
from math import exp, log
from typing import Iterable, List, Optional
#=============================================================================
# ADAPTER LAYER — translating raw data to risk inputs
#=============================================================================

def _parse_timestamp(value: Any) -> Optional[datetime]:
    """Parse datetime/ISO timestamp and return timezone-aware UTC datetime."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        normalized = value.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError:
            return None
    else:
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _sort_readings_newest_first(readings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Sort readings by timestamp descending.
    Missing/invalid timestamps are treated as oldest.
    """
    oldest_utc = datetime.min.replace(tzinfo=timezone.utc)

    def sort_key(row: Dict[str, Any]) -> tuple[int, datetime]:
        ts = _parse_timestamp(row.get("timestamp"))
        if ts is None:
            return (0, oldest_utc)
        return (1, ts)

    return sorted(readings, key=sort_key, reverse=True)

def build_heat_risk_inputs_from_recent_readings(
    recent_readings: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Derive every `compute_heat_risk` input from rows returned by
    `db_utils.get_recent_readings()`.
    """
    if not recent_readings:
        raise ValueError("recent_readings is empty")

    # 1 hour window at 10-minute cadence -> 6 most recent rows.
    readings_sorted = _sort_readings_newest_first(recent_readings)
    window = readings_sorted[:6]

    temps = [float(r["temperature_c"]) for r in window if r.get("temperature_c") is not None]
    rhs = [float(r["humidity_pct"]) for r in window if r.get("humidity_pct") is not None]

    if not temps:
        raise ValueError("No temperature values found in recent_readings")
    if not rhs:
        raise ValueError("No humidity values found in recent_readings")

    thi_series: List[Dict[str, Any]] = []
    for row in window:
        t = row.get("temperature_c")
        rh = row.get("humidity_pct")
        if t is None or rh is None:
            continue
        t = float(t)
        rh = float(rh)
        twb = wet_bulb_temperature_c(t, rh)
        thi = 0.85 * t + 0.15 * twb
        thi_series.append(
            {
                "timestamp": _parse_timestamp(row.get("timestamp")),
                "thi": thi,
                "row": row,
            }
        )

    latest = window[0]
    feeder_status = str(latest.get("feeder_status", "")).lower()
    waterer_status = str(latest.get("waterer_status", "")).lower()

    feeder_pct = latest.get("feeder_pct")
    waterer_pct = latest.get("waterer_pct")

    feed_intake = "Normal"
    if feeder_status in {"low", "empty"}:
        feed_intake = "Reduced"
    elif feeder_pct is not None and float(feeder_pct) < 35:
        feed_intake = "Reduced"

    water_intake = "Normal"
    if waterer_status in {"low", "empty"}:
        water_intake = "High"
    elif waterer_pct is not None and float(waterer_pct) < 35:
        water_intake = "High"

    high_thi_threshold = 26.0
    sampling_interval = 10.0  # in minutes

    # Count consecutive high-THI points from newest backwards.
    # Stop on first below-threshold or missing temp/RH.
    count_consecutive_high = 0
    for row in window:
        t = row.get("temperature_c")
        rh = row.get("humidity_pct")
        if t is None or rh is None:
            break

        t_val = float(t)
        rh_val = float(rh)
        twb = wet_bulb_temperature_c(t_val, rh_val)
        thi = 0.85 * t_val + 0.15 * twb
        if thi >= high_thi_threshold:
            count_consecutive_high += 1
        else:
            break

    high_thi_streak_minutes = int(count_consecutive_high * sampling_interval)

    thi_slope_per_hour: Optional[float] = None
    thi_with_ts = [p for p in thi_series if p["timestamp"] is not None]
    if len(thi_with_ts) >= 2:
        thi_with_ts.sort(key=lambda x: x["timestamp"])
        first = thi_with_ts[0]
        last = thi_with_ts[-1]
        delta_hours = (last["timestamp"] - first["timestamp"]).total_seconds() / 3600.0
        if delta_hours > 0:
            thi_slope_per_hour = (last["thi"] - first["thi"]) / delta_hours

    expected_points = 6
    valid_points = sum(
        1
        for row in window
        if row.get("temperature_c") is not None and row.get("humidity_pct") is not None
    )
    data_coverage_last_hour = min(1.0, valid_points / expected_points)

    return {
        "temp_db_mean": sum(temps) / len(temps),
        "temp_db_max": max(temps),
        "rh_percent_mean": sum(rhs) / len(rhs),
        "high_thi_streak_minutes": high_thi_streak_minutes,
        "feed_intake": feed_intake,
        "water_intake": water_intake,
        "thi_slope_per_hour": thi_slope_per_hour,
        "data_coverage_last_hour": data_coverage_last_hour,
        "sensor_count_temp": 1,
    }


def build_mold_manager_inputs_from_recent_readings(
    recent_readings: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Derive minimal `mold_manager_step` inputs from recent DB rows.
    """
    if not recent_readings:
        raise ValueError("recent_readings is empty")

    readings_sorted = _sort_readings_newest_first(recent_readings)
    window = readings_sorted[:6]

    latest_valid: Optional[Dict[str, Any]] = None
    for row in window:
        if row.get("temperature_c") is not None and row.get("humidity_pct") is not None:
            latest_valid = row
            break

    if latest_valid is None:
        raise ValueError("No valid row with both temperature_c and humidity_pct in last hour window")

    expected_points = 6
    valid_points = sum(
        1
        for row in window
        if row.get("temperature_c") is not None and row.get("humidity_pct") is not None
    )
    coverage = min(1.0, valid_points / expected_points)

    return {
        "temp_c": float(latest_valid["temperature_c"]),
        "humidity_rh": float(latest_valid["humidity_pct"]),
        "data_coverage_last_hour": coverage,
    }

#=============================================================================
#WET BULP CALCULATION
#=============================================================================
def wet_bulb_temperature_c(t_db_c: float, rh_percent: float) -> float:
    """
    Approximate wet-bulb temperature (°C) from dry-bulb temperature (°C)
    and relative humidity (%) using Stull (2011) approximation.

    Valid for typical ambient conditions (roughly 0–50°C, 5–99% RH).
    Good enough for control/early warning use cases.
    """
    rh = max(1.0, min(rh_percent, 99.0))  # keep in a safe range
    t = t_db_c

    twb = (
        t * math.atan(0.151977 * math.sqrt(rh + 8.313659))
        + math.atan(t + rh)
        - math.atan(rh - 1.676331)
        + 0.00391838 * (rh ** 1.5) * math.atan(0.023101 * rh)
        - 4.686035
    )
    return twb
def compute_heat_risk(
    temp_db_mean: float,
    temp_db_max: float,
    rh_percent_mean: float,  # 0..100
    high_thi_streak_minutes: int,  # e.g. minutes above THI threshold
    feed_intake: str,
    water_intake: str,
    thi_slope_per_hour: Optional[float] = None,  # optional early warning
) -> Dict[str, Any]:
    score = 0.0
    contributing_factors: List[str] = []

    twb_mean = wet_bulb_temperature_c(temp_db_mean, rh_percent_mean)
    thi_mean = 0.85 * temp_db_mean + 0.15 * twb_mean

    twb_max = wet_bulb_temperature_c(temp_db_max, rh_percent_mean)
    thi_max = 0.85 * temp_db_max + 0.15 * twb_max

    # --- Base risk from THI (make high end steeper)
    if thi_mean < 19:
        score = 0.0
        contributing_factors.append("THI within safe range")
    elif thi_mean < 22:
        score = 0.2
        contributing_factors.append("THI elevated")
    elif thi_mean < 25:
        score = 0.5
        contributing_factors.append("THI moderately high")
    elif thi_mean < 29:
        score = 0.75
        contributing_factors.append("THI high")
    elif thi_mean < 31:
        score = 0.85
        contributing_factors.append("THI very high")
    elif thi_mean < 33:
        score = 0.95
        contributing_factors.append("THI critical")
    else:
        score = 1.0
        contributing_factors.append("THI extreme")

    # --- Peak risk (hotspots)
    if thi_max >= 33:
        score += 0.1
        contributing_factors.append("Critical peak conditions (hotspot)")

    # --- Prolonged exposure
    if high_thi_streak_minutes >= 30:
        score += 0.1
        contributing_factors.append("Sustained exposure (≥30 min)")

    # --- Resource tracking signals
    if feed_intake in {"Reduced", "Low"}:
        score += 0.1
        contributing_factors.append("Reduced feed intake")
    if water_intake in {"Increased", "High"}:
        score += 0.1
        contributing_factors.append("Increased water uptake")

    # --- Trend-based early warning (optional)
    if thi_slope_per_hour is not None and thi_slope_per_hour >= 2.0:
        score += 0.05
        contributing_factors.append("THI rising quickly")

    score = max(0.0, min(score, 1.0))

    # --- Risk level thresholds (keep simple)
    if score < 0.5:
        level = "LOW"
    elif score < 0.75:
        level = "MEDIUM"
    else:
        level = "HIGH"

    # --- Horizon (urgency)
    # Interpreting as: time before conditions likely become harmful if nothing changes
    time_horizon = 240
    if thi_mean >= 33:
        time_horizon = 10
    elif thi_mean >= 31:
        time_horizon = 30
    elif thi_mean >= 29:
        time_horizon = 60
    elif thi_mean >= 25:
        time_horizon = 120

    # If already sustained, shorten horizon ==> more urgent to act because animals are already stressed
    if high_thi_streak_minutes >= 60:
        time_horizon = min(time_horizon, 30)

    # If THI rising fast, shorten horizon ==> early warning that conditions may deteriorate quickly
    if thi_slope_per_hour is not None and thi_slope_per_hour >= 2.0:
        time_horizon = max(10, time_horizon // 2)

    return {
        "event_type": "HEAT_RISK",
        "risk_score": round(score * 100, 1),
        "risk_level": level,
        "time_horizon_minutes": int(time_horizon),
        "contributing_factors": contributing_factors,
        "thi_mean": round(thi_mean, 2),
        "thi_max": round(thi_max, 2),
    }

def decide_ventilation_action(
    risk: Dict[str, Any],
    current_fan_percent: int,
    min_fan_percent: int = 15,
    max_fan_percent: int = 100,
) -> Dict[str, Any]:
    """Zet risk-output om naar een concrete ventilatie-actie.

    Geeft een payload terug die je rechtstreeks kan loggen, publishen naar MQTT,
    of doorgeven aan je RAG-laag voor uitleg aan de gebruiker.
    """

    risk_level = risk["risk_level"]
    risk_score = float(risk["risk_score"])
    time_horizon = int(risk["time_horizon_minutes"])

    if risk_level == "LOW":
        target_fan = max(min_fan_percent, 20)
        action = "MAINTAIN"
    elif risk_level == "MEDIUM":
        target_fan = max(min_fan_percent, 45)
        action = "INCREASE"
    else:  # HIGH
        if risk_score >= 0.9 or time_horizon <= 10:
            target_fan = max_fan_percent
            action = "EMERGENCY_MAX"
        else:
            target_fan = min(max_fan_percent, 75)
            action = "INCREASE_STRONGLY"

    delta = target_fan - current_fan_percent

    return {
        "controller": "VENTILATION",
        "action": action,
        "current_fan_percent": current_fan_percent,
        "target_fan_percent": target_fan,
        "delta_percent": delta,
        "reason": {
            "risk_level": risk_level,
            "risk_score": risk_score,
            "time_horizon_minutes": time_horizon,
            "contributing_factors": risk["contributing_factors"],
        },
    }


def build_rag_context(risk: Dict[str, Any], ventilation_action: Dict[str, Any]) -> Dict[str, Any]:
    """Combineer detectie + actuatorbeslissing voor downstream RAG-advies."""

    return {
        "event_type": "HEAT_RISK_CONTROL",
        "risk": risk,
        "ventilation": ventilation_action,
        "operator_prompt": (
            "Leg in duidelijke taal uit waarom deze ventilatie-actie wordt uitgevoerd, "
            "welke bijkomende acties de pluimveehouder nu best doet (water, schaduw, dichtheid), "
            "en welke check over 10 minuten nodig is."
        ),
    }








class SensitivityLevel(str, Enum):
    VERY_SENSITIVE = "very_sensitive"
    SENSITIVE = "sensitive"
    MEDIUM_RESISTANT = "medium_resistant"
    RESISTANT = "resistant"

class DeclineMethod(str, Enum):
    WOOD = "WOOD"
    NON_WOOD = "NON_WOOD"


@dataclass(frozen=True)
class SensitivityParams:
    k11: float
    k12: float
    A: float
    B: float
    C: float
    rh_above_20: float

# Original sensitivity table from the paper
# Source paper table: very sensitive, sensitive, medium resistant, resistant
ORIGINAL_SENSITIVITIES = {
    SensitivityLevel.VERY_SENSITIVE: SensitivityParams(
        k11=1.0, k12=2.0, A=1.0, B=7.0, C=2.0, rh_above_20=70.0
    ),
    SensitivityLevel.SENSITIVE: SensitivityParams(
        k11=0.578, k12=0.386, A=0.3, B=6.0, C=1.0, rh_above_20=70.0
    ),
    SensitivityLevel.MEDIUM_RESISTANT: SensitivityParams(
        k11=0.072, k12=0.097, A=0.0, B=5.0, C=1.5, rh_above_20=70.0
    ),
    SensitivityLevel.RESISTANT: SensitivityParams(
        k11=0.033, k12=0.014, A=0.0, B=3.0, C=1.0, rh_above_20=70.0
    ),
}

# equations
@dataclass(frozen=True)
class VTTOriginalParams:
    """
    Originele VTT-parameters.

    Defaults:
    - sensitivity: VERY_SENSITIVE
    - wood_type_w: 0 = pine, 1 = spruce
    - surface_quality_sq: 0 = sawn, 1 = kiln dried
    - p_t, p_rh, p_c: originele waarden uit het model
    - decline_method: WOOD (originele toepassing is hout)
    - c_decline: 1.0 als standaard intensiteit
    - sample_minutes: numerieke timestep in minuten voor discrete integratie
    """
    sensitivity: SensitivityLevel = SensitivityLevel.VERY_SENSITIVE
    wood_type_w: int = 0 # 0 = pine, 1 = spruce
    surface_quality_sq: int = 0 # 0 = sawn, 1 = kiln dried
    p_t: float = 0.45
    p_rh: float = 9.0
    p_c: float = 58.0
    decline_method: DeclineMethod = DeclineMethod.WOOD
    c_decline: float = 1.0
    # VTT definieert dM/dt per 24 uur; sample_minutes bepaalt alleen de integratiestap.
    sample_minutes: int = 10
    m_min: float = 0.0
    m_max_hard: float = 6.0 # harde bovengrens voor M, ook al kan het model theoretisch hoger gaan. Dit helpt numerische stabiliteit.

    def __post_init__(self) -> None:
        if self.sample_minutes <= 0:
            raise ValueError("sample_minutes must be > 0")

@dataclass
class VTTState:
    m: float = 0.0
    consecutive_unfavourable_minutes: int = 0

@dataclass
class VTTStepResult:
    m: float
    dmdt_per_24h: float #slope van M per 24 uur
    rhcrit: float
    mmax: float
    favourable_for_growth: bool


def get_sensitivity_params(level: SensitivityLevel) -> SensitivityParams:
    return ORIGINAL_SENSITIVITIES[level]


def rh_crit_original(temp_c: float, rh_above_20: float) -> float:
    """
    RHcrit volgens het originele model, maar met de gecorrigeerde formule uit
    het corrigendum van de paper.

    Corrigendum:
    RHcrit = -0.00267*T^3 + 0.160*T^2 - 3.13*T + 100   when T <= 20
             RH>20                                     when T >= 20
    """
    if temp_c <= 20.0:
        return -0.00267 * (temp_c ** 3) + 0.160 * (temp_c ** 2) - 3.13 * temp_c + 100.0
    return rh_above_20


def compute_k1(m: float, sens: SensitivityParams) -> float:
    return sens.k11 if m < 1.0 else sens.k12


def compute_mmax(rh: float, rhcrit: float, sens: SensitivityParams) -> float: #mmax is de verzadigingswaarde van M bij gegeven RH
    denom = rhcrit - 100.0
    if abs(denom) < 1e-12:
        x = 0.0
    else:
        x = (rhcrit - rh) / denom # genormaliseerde afstand van RH tot RHcrit, geschaald naar [0, 1] waarbij 0 bij RH=100% is en 1 bij RH=RHcrit

    mmax = sens.A + sens.B * x - sens.C * (x ** 2)
    return max(0.0, mmax)


def compute_k2(m: float, mmax: float) -> float:
    return max(1.0 - exp(2.3 * (m - mmax)), 0.0)


def growth_rate_per_24h(
    *,
    temp_c: float,
    rh: float,
    m: float,
    params: VTTOriginalParams,
    sens: SensitivityParams,
    rhcrit: float,
) -> float:
    """
    Originele VTT groeivergelijking:
    dM/dt = k1*k2 / (7 * exp(-pT ln(T) - pRH ln(RH) + 0.14W - 0.33SQ + pC))

    dM/dt is uitgedrukt per 24 uur.
    """
    k1 = compute_k1(m, sens)
    mmax = compute_mmax(rh, rhcrit, sens)
    k2 = compute_k2(m, mmax)

    # Numerieke bescherming - log(0) is niet gedefinieerd, dus we zorgen dat T en RH binnen een redelijke range blijven voor de logaritme.
    t_for_log = max(temp_c, 0.1)
    rh_for_log = min(max(rh, 0.1), 100)
    
    exponent_term = (
        -params.p_t * log(t_for_log)
        -params.p_rh * log(rh_for_log)
        + 0.14 * params.wood_type_w
        - 0.33 * params.surface_quality_sq
        + params.p_c
    )

    denominator = 7.0 * exp(exponent_term)
    return (k1 * k2) / denominator


def decline_rate_per_24h(state: VTTState, params: VTTOriginalParams) -> float:
    """
    Originele decline-regel.
    """
    hours_below = state.consecutive_unfavourable_minutes / 60.0

    if params.decline_method == DeclineMethod.NON_WOOD:
        base_decline = -0.01
    else:
        if hours_below <= 6.0:
            base_decline = -0.01
        elif hours_below <= 24.0:
            base_decline = 0.0
        else:
            base_decline = -0.008

    return params.c_decline * base_decline


def vtt_original_step(
    *,
    temp_c: float,
    rh: float,
    state: VTTState,
    params: VTTOriginalParams,
) -> VTTStepResult:
    """
    Eén numerieke stap van het originele VTT-model.
    De toestand is M(t) = state.m.
    Let op: in VTT is dM/dt uitgedrukt per 24 uur, niet per timestep.
    Daarom wordt de increment geschaald met sample_minutes / (24 * 60).
    """
    rh = min(max(rh, 0.0), 100.0)
    sens = get_sensitivity_params(params.sensitivity)
    rhcrit = rh_crit_original(temp_c, sens.rh_above_20)

    favourable = rh >= rhcrit

    if favourable:
        state.consecutive_unfavourable_minutes = 0
        dmdt_24h = growth_rate_per_24h(
            temp_c=temp_c,
            rh=rh,
            m=state.m,
            params=params,
            sens=sens,
            rhcrit=rhcrit,
        )
    else:
        state.consecutive_unfavourable_minutes += params.sample_minutes
        # Physical guard: once M hits zero, it cannot decline below zero.
        # Keep M flat at 0 until favourable conditions create growth again.
        if state.m <= 0.0:
            dmdt_24h = 0.0
        else:
            dmdt_24h = decline_rate_per_24h(state, params)

    delta_m = dmdt_24h * (params.sample_minutes / (24.0 * 60.0))
    state.m += delta_m
    # Numerieke stabiliteit: houd M altijd binnen de modelgrenzen [0, 6].
    state.m = max(0.0, min(state.m, 6.0))

    mmax = compute_mmax(rh, rhcrit, sens)

    return VTTStepResult(
        m=state.m,
        dmdt_per_24h=dmdt_24h,
        rhcrit=rhcrit,
        mmax=mmax,
        favourable_for_growth=favourable,
    )


def run_vtt_original_series(
    *,
    temperatures_c: Iterable[float],
    rhs_percent: Iterable[float],
    params: VTTOriginalParams,
    initial_m: float = 0.0,
) -> List[VTTStepResult]:
    state = VTTState(m=initial_m, consecutive_unfavourable_minutes=0)
    results: List[VTTStepResult] = []

    for temp_c, rh in zip(temperatures_c, rhs_percent):
        results.append(
            vtt_original_step(
                temp_c=temp_c,
                rh=rh,
                state=state,
                params=params,
            )
        )

    return results


def mean_m(results: List[VTTStepResult]) -> float:
    if not results:
        return 0.0
    return sum(r.m for r in results) / len(results)

#--------------------------------------------------------------------------------
# simple simulation
#--------------------------------------------------------------------------------

# =========================================================
# 1. COMPLEX 1-WEEK SCENARIO MAKEN
# =========================================================

def generate_proof_of_concept_week(params: VTTOriginalParams):
    """
    Maakt een 1-week scenario met:
    - verschillende fasen
    - dag/nacht-variatie in temperatuur en RH
    - periodes van groei en decline
    """
    steps_per_hour = 60 // params.sample_minutes
    steps_per_day = 24 * steps_per_hour
    total_days = 7
    n_steps = total_days * steps_per_day

    temperatures = []
    rhs = []

    for i in range(n_steps):
        t_days = i * params.sample_minutes / (60 * 24)
        t_hours = i * params.sample_minutes / 60.0
        day_index = int(t_days)  # 0..6

        # ---------
        # BASISFASE PER DAG
        # ---------
        if day_index in [0, 1]:
            # Dag 1-2: gunstig -> sterke groei
            base_temp = 23.0
            base_rh = 92.0
            temp_amp = 2.0
            rh_amp = 3.0

        elif day_index == 2:
            # Dag 3: droger -> decline start
            base_temp = 24.0
            base_rh = 72.0
            temp_amp = 2.5
            rh_amp = 5.0

        elif day_index == 3:
            # Dag 4: nog droger en iets koeler
            base_temp = 19.0
            base_rh = 68.0
            temp_amp = 2.0
            rh_amp = 6.0

        elif day_index in [4, 5]:
            # Dag 5-6: opnieuw zeer gunstig
            base_temp = 22.0
            base_rh = 95.0
            temp_amp = 1.5
            rh_amp = 2.5

        else:
            # Dag 7: schommelt rond kritische grens
            base_temp = 21.0
            base_rh = 80.0
            temp_amp = 2.0
            rh_amp = 8.0

        # ---------
        # DAG/NACHT VARIATIE
        # ---------
        # Temperatuur piekt overdag
        temp_variation = temp_amp * math.sin(2 * math.pi * (t_hours - 6) / 24.0)

        # RH vaak omgekeerd aan temperatuur: hoger 's nachts / lager overdag
        rh_variation = -rh_amp * math.sin(2 * math.pi * (t_hours - 6) / 24.0)

        temp = base_temp + temp_variation
        rh = base_rh + rh_variation

        # clamp RH fysisch naar [0, 100]
        rh = max(0.0, min(100.0, rh))

        temperatures.append(temp)
        rhs.append(rh)

    return temperatures, rhs


if __name__ == "__main__":
    import matplotlib.pyplot as plt

    # =========================================================
    # 2. SIMULATIE UITVOEREN
    # =========================================================

    params = VTTOriginalParams(
        sensitivity=SensitivityLevel.VERY_SENSITIVE,
        sample_minutes=10,
        decline_method=DeclineMethod.WOOD,
        c_decline=1.0,
    )

    temperatures, rhs = generate_proof_of_concept_week(params)

    results = run_vtt_original_series(
        temperatures_c=temperatures,
        rhs_percent=rhs,
        params=params,
        initial_m=0.0,
    )

    time_days = [i * params.sample_minutes / (60 * 24) for i in range(len(results))]
    m_values = [r.m for r in results]
    mmax_values = [r.mmax for r in results]
    rhcrit_values = [r.rhcrit for r in results]
    growth_flags = [1 if r.favourable_for_growth else 0 for r in results]
    dmdt_values = [r.dmdt_per_24h for r in results]

    print(f"Aantal stappen: {len(results)}")
    print(f"Eindwaarde M: {results[-1].m:.3f}")
    print(f"Gemiddelde M: {mean_m(results):.3f}")
    print(f"Maximale M: {max(m_values):.3f}")

    plt.figure(figsize=(12, 5))
    plt.plot(time_days, m_values, label="Mould index M", linewidth=2)
    plt.plot(time_days, mmax_values, "--", label="Mmax", linewidth=2)
    plt.xlabel("Tijd [dagen]")
    plt.ylabel("Mould index")
    plt.title("VTT proof-of-concept simulatie over 1 week")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()

    fig, ax1 = plt.subplots(figsize=(12, 5))
    ax1.plot(time_days, rhs, label="RH [%]", linewidth=2)
    ax1.plot(time_days, rhcrit_values, "--", label="RHcrit [%]", linewidth=2)
    ax1.set_xlabel("Tijd [dagen]")
    ax1.set_ylabel("Relatieve vochtigheid [%]")
    ax1.grid(True)
    ax2 = ax1.twinx()
    ax2.plot(time_days, temperatures, label="Temperatuur [°C]", color="green", linewidth=2)
    ax2.set_ylabel("Temperatuur [°C]")
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right")
    plt.title("Omgevingscondities over 1 week")
    plt.tight_layout()
    plt.show()

    plt.figure(figsize=(12, 4))
    plt.plot(time_days, dmdt_values, label="dM/dt per 24h", linewidth=2)
    plt.axhline(0.0, linestyle="--")
    plt.xlabel("Tijd [dagen]")
    plt.ylabel("dM/dt [per 24h]")
    plt.title("Groei- en decline-snelheid")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()

    plt.figure(figsize=(12, 5))
    plt.plot(time_days, m_values, label="Mould index M", linewidth=2)
    for i in range(1, len(time_days)):
        if growth_flags[i] == 1:
            plt.axvspan(time_days[i-1], time_days[i], alpha=0.08)
    plt.xlabel("Tijd [dagen]")
    plt.ylabel("Mould index")
    plt.title("Mould index met gunstige groeiperioden")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()
