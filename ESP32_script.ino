#include <DHT.h>
#include <Wire.h>
#include <RTClib.h>
#include <ArduinoJson.h>

/************ PIN CONFIG ************/
#define DHTTYPE DHT22
#define DHT1_PIN 32
#define DHT2_PIN 33
#define MQ136_PIN 35
#define MHZ19C_PWM_PIN 25
#define MQ137_PIN 26
#define SDA_PIN 21
#define SCL_PIN 22

HardwareSerial ultrasonic1(2); // RX16 TX17
HardwareSerial ultrasonic2(1); // RX18 TX19

/************ OBJECTS ************/
DHT dht1(DHT1_PIN, DHTTYPE);
DHT dht2(DHT2_PIN, DHTTYPE);
RTC_DS1307 rtc;

/************ MQ136 CONSTANTS ************/
const float RLOAD = 10.0;
const float MQ136_VREF = 3.3;
const float MQ136_ADC_MAX = 4095.0;
float Ro = 20.0;
const float a_H2S = -3.21389;
const float b_H2S = -0.54744;

/************ MQ137 CONSTANTS (placeholder — recalibrate later) ************/
float Ro_nh3 = 20.0;
const float a_NH3 = -2.0;    // placeholder slope
const float b_NH3 = 0.8;    // placeholder intercept

/************ TIMING ************/
const uint32_t REPORT_INTERVAL_MS = 5000;
const uint32_t DHT_INTERVAL_MS = 2500;
const uint32_t MQ136_SAMPLE_INTERVAL_MS = 500;
const uint32_t MQ136_WARMUP_MS = 120000;
const uint32_t ULTRASONIC_STALE_MS = 1500;
const uint32_t MHZ19C_STALE_MS = 3000;
const uint32_t MQ137_SAMPLE_INTERVAL_MS = 500;
const uint32_t MQ137_WARMUP_MS = 120000;

/************ MQ136 CALIBRATION STATE ************/
static bool calibrated = false;
static bool calibration_error = false;
static uint32_t mq136WarmupStartMs = 0;

/************ MQ137 CALIBRATION STATE ************/
static bool mq137_calibrated = false;
static bool mq137_calibration_error = false;
static uint32_t mq137WarmupStartMs = 0;

/************ SENSOR CACHE ************/
struct UltrasonicState {
  int distance = -1;
  bool valid = false;
  uint8_t buf[4] = {0, 0, 0, 0};
  uint8_t idx = 0;
  uint32_t lastPacketMs = 0;
};

static float temperature1_c = NAN;
static float humidity1_pct = NAN;
static float temperature2_c = NAN;
static float humidity2_pct = NAN;
static bool dht1_error = true;
static bool dht2_error = true;
static float h2s_ppm = NAN;
static float co2_ppm = NAN;
static float nh3_ppm = NAN;
static UltrasonicState ultrasonicState1;
static UltrasonicState ultrasonicState2;
static bool co2_valid = false;
static uint32_t co2LastUpdateMs = 0;

static uint32_t nextReportMs = 0;
static uint32_t nextDhtReadMs = 0;
static uint32_t nextMq136SampleMs = 0;
static uint32_t nextMq137SampleMs = 0;

/************ MH-Z19C PWM STATE ************/
volatile uint32_t mhz19cLastRiseUs = 0;
volatile uint32_t mhz19cPendingHighUs = 0;
volatile uint32_t mhz19cSampleHighUs = 0;
volatile uint32_t mhz19cSamplePeriodUs = 0;
volatile bool mhz19cHighReady = false;
volatile bool mhz19cSampleReady = false;

/************ HELPERS ************/
float adcToVoltage(int adc) {
  return (adc * MQ136_VREF) / MQ136_ADC_MAX;
}

float voltageToRs(float voltage) {
  if (voltage <= 0.001) return NAN;
  return RLOAD * (MQ136_VREF / voltage - 1.0);
}

float computePpmFromRs(float Rs) {
  if (!calibrated || isnan(Rs) || isinf(Rs) || Ro <= 0.0) return NAN;
  float ratio = Rs / Ro;
  if (ratio <= 0.0) return NAN;
  return pow(10, (a_H2S * log10(ratio) + b_H2S));
}

void IRAM_ATTR handleMhz19cPwmChange() {
  uint32_t nowUs = micros();

  if (digitalRead(MHZ19C_PWM_PIN) == HIGH) {
    if (mhz19cLastRiseUs != 0 && mhz19cHighReady) {
      mhz19cSamplePeriodUs = nowUs - mhz19cLastRiseUs;
      mhz19cSampleHighUs = mhz19cPendingHighUs;
      mhz19cSampleReady = true;
      mhz19cHighReady = false;
    }
    mhz19cLastRiseUs = nowUs;
  } else {
    if (mhz19cLastRiseUs != 0) {
      mhz19cPendingHighUs = nowUs - mhz19cLastRiseUs;
      mhz19cHighReady = true;
    }
  }
}

void updateA02Stream(HardwareSerial &serial, UltrasonicState &state, uint32_t nowMs) {
  while (serial.available()) {
    uint8_t b = serial.read();

    if (state.idx == 0) {
      if (b != 0xFF) continue;
      state.buf[0] = b;
      state.idx = 1;
      continue;
    }

    state.buf[state.idx++] = b;
    if (state.idx == 4) {
      uint8_t checksum = (state.buf[0] + state.buf[1] + state.buf[2]) & 0xFF;
      if (checksum == state.buf[3]) {
        state.distance = (state.buf[1] << 8) | state.buf[2];
        state.valid = true;
        state.lastPacketMs = nowMs;
      }
      state.idx = 0;
    }
  }

  if (state.valid && nowMs - state.lastPacketMs > ULTRASONIC_STALE_MS) {
    state.valid = false;
  }
}

void updateDhtReadings() {
  float t1 = dht1.readTemperature();
  float h1 = dht1.readHumidity();
  float t2 = dht2.readTemperature();
  float h2 = dht2.readHumidity();

  dht1_error = isnan(t1) || isnan(h1);
  dht2_error = isnan(t2) || isnan(h2);

  if (!dht1_error) {
    temperature1_c = t1;
    humidity1_pct = h1;
  }

  if (!dht2_error) {
    temperature2_c = t2;
    humidity2_pct = h2;
  }
}

void updateMq136Reading() {
  if (!calibrated) {
    if (millis() - mq136WarmupStartMs < MQ136_WARMUP_MS) {
      return;
    }

    float Rs = 0.0;
    const int samples = 40;
    calibration_error = false;

    for (int i = 0; i < samples; i++) {
      int adc = analogRead(MQ136_PIN);
      float V = adcToVoltage(adc);
      if (V < 0.05 || V > 3.2) {
        calibration_error = true;
        break;
      }
      float RsSample = voltageToRs(V);
      if (isnan(RsSample) || isinf(RsSample)) {
        calibration_error = true;
        break;
      }
      Rs += RsSample;
      delay(25);
    }

    if (!calibration_error) {
      Ro = Rs / samples;
      calibrated = true;
    }
    return;
  }

  int adc = analogRead(MQ136_PIN);
  float V = adcToVoltage(adc);
  float Rs = voltageToRs(V);
  h2s_ppm = computePpmFromRs(Rs);
}

float computeNh3PpmFromRs(float Rs) {
  if (!mq137_calibrated || isnan(Rs) || isinf(Rs) || Ro_nh3 <= 0.0) return NAN;
  float ratio = Rs / Ro_nh3;
  if (ratio <= 0.0) return NAN;
  return pow(10, (a_NH3 * log10(ratio) + b_NH3));
}

void updateMq137Reading() {
  if (!mq137_calibrated) {
    if (millis() - mq137WarmupStartMs < MQ137_WARMUP_MS) {
      return;
    }

    float Rs = 0.0;
    const int samples = 40;
    mq137_calibration_error = false;

    for (int i = 0; i < samples; i++) {
      int adc = analogRead(MQ137_PIN);
      float V = adcToVoltage(adc);
      if (V < 0.05 || V > 3.2) {
        mq137_calibration_error = true;
        break;
      }
      float RsSample = voltageToRs(V);
      if (isnan(RsSample) || isinf(RsSample)) {
        mq137_calibration_error = true;
        break;
      }
      Rs += RsSample;
      delay(25);
    }

    if (!mq137_calibration_error) {
      Ro_nh3 = Rs / samples;
      mq137_calibrated = true;
    }
    return;
  }

  int adc = analogRead(MQ137_PIN);
  float V = adcToVoltage(adc);
  float Rs = voltageToRs(V);
  nh3_ppm = computeNh3PpmFromRs(Rs);
}

void updateCo2Reading(uint32_t nowMs) {
  uint32_t highUs = 0;
  uint32_t periodUs = 0;
  bool sampleReady = false;

  noInterrupts();
  if (mhz19cSampleReady) {
    highUs = mhz19cSampleHighUs;
    periodUs = mhz19cSamplePeriodUs;
    mhz19cSampleReady = false;
    sampleReady = true;
  }
  interrupts();

  if (sampleReady) {
    float highMs = highUs / 1000.0f;
    float periodMs = periodUs / 1000.0f;

    if (periodMs > 4.0f && highMs >= 2.0f) {
      co2_ppm = 2000.0f * (highMs - 2.0f) / (periodMs - 4.0f);
      if (co2_ppm < 0.0f) co2_ppm = 0.0f;
      co2_valid = true;
      co2LastUpdateMs = nowMs;
    }
  }

  if (co2_valid && nowMs - co2LastUpdateMs > MHZ19C_STALE_MS) {
    co2_valid = false;
  }
}

void emitJsonReport() {
  DateTime now = rtc.now();

  StaticJsonDocument<512> doc;
  doc["timestamp"] = now.timestamp(DateTime::TIMESTAMP_FULL);

  if (dht1_error) { doc["temperature1_c"] = nullptr; doc["humidity1_pct"] = nullptr; }
  else            { doc["temperature1_c"] = temperature1_c; doc["humidity1_pct"] = humidity1_pct; }

  if (dht2_error) { doc["temperature2_c"] = nullptr; doc["humidity2_pct"] = nullptr; }
  else            { doc["temperature2_c"] = temperature2_c; doc["humidity2_pct"] = humidity2_pct; }

  if (ultrasonicState1.valid) doc["ultrasonic1_cm"] = ultrasonicState1.distance / 10;
  else                        doc["ultrasonic1_cm"] = nullptr;

  if (ultrasonicState2.valid) doc["ultrasonic2_cm"] = ultrasonicState2.distance / 10;
  else                        doc["ultrasonic2_cm"] = nullptr;

  if (isnan(h2s_ppm)) doc["h2s_ppm"] = nullptr;
  else                doc["h2s_ppm"] = h2s_ppm;

  // H2S sensor status
  if (calibrated) {
    doc["h2s_status"] = "ok";
  } else if (calibration_error) {
    doc["h2s_status"] = "cal_error";
  } else {
    int secsLeft = (int)((MQ136_WARMUP_MS - (millis() - mq136WarmupStartMs)) / 1000);
    if (secsLeft < 0) secsLeft = 0;
    char buf136[24];
    snprintf(buf136, sizeof(buf136), "warmup_%ds", secsLeft);
    doc["h2s_status"] = buf136;
  }

  if (co2_valid) doc["co2_ppm"] = co2_ppm;
  else           doc["co2_ppm"] = nullptr;

  if (isnan(nh3_ppm)) doc["nh3_ppm"] = nullptr;
  else                doc["nh3_ppm"] = nh3_ppm;

  // NH3 sensor status
  if (mq137_calibrated) {
    doc["nh3_status"] = "ok";
  } else if (mq137_calibration_error) {
    doc["nh3_status"] = "cal_error";
  } else {
    int secsLeft = (int)((MQ137_WARMUP_MS - (millis() - mq137WarmupStartMs)) / 1000);
    if (secsLeft < 0) secsLeft = 0;
    char buf137[24];
    snprintf(buf137, sizeof(buf137), "warmup_%ds", secsLeft);
    doc["nh3_status"] = buf137;
  }

  serializeJson(doc, Serial);
  Serial.println();
}

/************ SETUP ************/

void setup() {
  Serial.begin(115200);
  pinMode(MHZ19C_PWM_PIN, INPUT);
  pinMode(MQ136_PIN, INPUT);
  pinMode(MQ137_PIN, INPUT);

  analogReadResolution(12);
  analogSetPinAttenuation(MQ136_PIN, ADC_11db);
  analogSetPinAttenuation(MQ137_PIN, ADC_11db);

  dht1.begin();
  dht2.begin();
  Wire.begin(SDA_PIN, SCL_PIN);
  rtc.begin();
  if (!rtc.isrunning()) {
    rtc.adjust(DateTime(F(__DATE__), F(__TIME__)));
  }
  ultrasonic1.begin(9600, SERIAL_8N1, 17, 16);
  ultrasonic2.begin(9600, SERIAL_8N1, 19, 18);

  // Let sensors stabilize before first read
  delay(500);

  attachInterrupt(digitalPinToInterrupt(MHZ19C_PWM_PIN), handleMhz19cPwmChange, CHANGE);

  uint32_t nowMs = millis();
  mq136WarmupStartMs = nowMs;
  mq137WarmupStartMs = nowMs;
  nextReportMs = nowMs + REPORT_INTERVAL_MS;
  nextDhtReadMs = nowMs;
  nextMq136SampleMs = nowMs;
  nextMq137SampleMs = nowMs;
}

/************ LOOP ************/

void loop() {
  uint32_t now_ms = millis();

  updateA02Stream(ultrasonic1, ultrasonicState1, now_ms);
  updateA02Stream(ultrasonic2, ultrasonicState2, now_ms);

  if ((int32_t)(now_ms - nextDhtReadMs) >= 0) {
    updateDhtReadings();
    nextDhtReadMs += DHT_INTERVAL_MS;
  }

  if ((int32_t)(now_ms - nextMq136SampleMs) >= 0) {
    updateMq136Reading();
    nextMq136SampleMs += MQ136_SAMPLE_INTERVAL_MS;
  }

  if ((int32_t)(now_ms - nextMq137SampleMs) >= 0) {
    updateMq137Reading();
    nextMq137SampleMs += MQ137_SAMPLE_INTERVAL_MS;
  }

  updateCo2Reading(now_ms);

  if ((int32_t)(now_ms - nextReportMs) >= 0) {
    emitJsonReport();
    nextReportMs += REPORT_INTERVAL_MS;
  }
}
