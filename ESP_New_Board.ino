#include <DHT.h>
#include <ArduinoJson.h>
#include <SoftwareSerial.h>
#include <math.h>

/************ PIN CONFIG ************/
#define DHTTYPE DHT22
#define DHT1_PIN 21
#define DHT2_PIN 22

#define AO2YYUW1_RX 36
#define AO2YYUW1_TX 18
#define AO2YYUW2_RX 39
#define AO2YYUW2_TX 19

#define MQ136_PIN 34
#define MQ137_PIN 35

#define MHZ19C_RX 32
#define MHZ19C_TX 23

#define uSWITCH1_PIN 13
#define uSWITCH2_PIN 14

#define M1_IN1_PIN 27     //motor 1 (DOOR)
#define M1_IN2_PIN 26     //motor 1 (DOOR)
#define M2_IN1_PIN 25     //motor 2 (FEEDER)
#define M2_IN2_PIN 33     //motor 2 (FEEDER)
#define FAN_IN1_PIN 17    //fan=ventilator
#define FAN_IN2_PIN 16    //fan=ventilator
#define FAN_EN_PIN 4      //fan PWM control for ventilation rate

HardwareSerial ultrasonic1(1);
HardwareSerial ultrasonic2(2);
SoftwareSerial swSerialCO2;

/************ OBJECTS ************/
DHT dht1(DHT1_PIN, DHTTYPE);
DHT dht2(DHT2_PIN, DHTTYPE);

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
const uint32_t MHZ19C_STALE_MS = 10000;
static uint32_t nextCo2ReadMs = 0;
const float H2S_MAX_PPM = 500.0;
const float NH3_MAX_PPM = 200.0;
const int CO2_MAX_PPM = 5500;
const uint32_t MQ137_SAMPLE_INTERVAL_MS = 500;
const uint32_t MQ137_WARMUP_MS = 120000;
const uint32_t SWITCH_DEBOUNCE_MS = 20;
const uint32_t DOOR_TIMEOUT_MS = 60000;
const uint32_t FEEDER_RUN_MS = 8000;
const int FAN_PWM_FREQ = 5000;
const int FAN_PWM_RESOLUTION = 8;


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
static bool h2s_valid = false;
static bool nh3_valid = false;
static uint32_t co2LastUpdateMs = 0;
static bool bottom_switch_state = HIGH;
static bool top_switch_state = HIGH;
static uint32_t bottom_switch_last_debounce_ms = 0;
static uint32_t top_switch_last_debounce_ms = 0;
char serialCommandBuffer[257] = {0};
uint16_t serialCommandIndex = 0;
enum DoorCommandState { DOOR_IDLE, DOOR_OPENING, DOOR_CLOSING };
static DoorCommandState doorCommandState = DOOR_IDLE;
static uint32_t doorCommandStartMs = 0;
enum FeederCommandState { FEEDER_IDLE, FEEDER_OPENING, FEEDER_CLOSING };
static FeederCommandState feederCommandState = FEEDER_IDLE;
static uint32_t feederCommandStartMs = 0;
static int currentFanPct = 0;

static uint32_t nextReportMs = 0;
static uint32_t nextDhtReadMs = 0;
static uint32_t nextMq136SampleMs = 0;
static uint32_t nextMq137SampleMs = 0;

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
      h2s_valid = false;
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
    h2s_valid = false;
    return;
  }

  int adc = analogRead(MQ136_PIN);
  float V = adcToVoltage(adc);
  float Rs = voltageToRs(V);
  h2s_ppm = computePpmFromRs(Rs);
  if (isnan(h2s_ppm) || h2s_ppm < 0.0 || h2s_ppm > H2S_MAX_PPM) {
    h2s_valid = false;
    h2s_ppm = NAN;
  } else {
    h2s_valid = true;
  }
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
      nh3_valid = false;
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
    nh3_valid = false;
    return;
  }

  int adc = analogRead(MQ137_PIN);
  float V = adcToVoltage(adc);
  float Rs = voltageToRs(V);
  nh3_ppm = computeNh3PpmFromRs(Rs);
  if (isnan(nh3_ppm) || nh3_ppm < 0.0 || nh3_ppm > NH3_MAX_PPM) {
    nh3_valid = false;
    nh3_ppm = NAN;
  } else {
    nh3_valid = true;
  }
}

void setDoorMotorOpen() {
  digitalWrite(M1_IN1_PIN, HIGH);
  digitalWrite(M1_IN2_PIN, LOW);
  doorCommandState = DOOR_OPENING;
  doorCommandStartMs = millis();
}

void setDoorMotorClose() {
  digitalWrite(M1_IN1_PIN, LOW);
  digitalWrite(M1_IN2_PIN, HIGH);
  doorCommandState = DOOR_CLOSING;
  doorCommandStartMs = millis();
}

void stopDoorMotor() {
  digitalWrite(M1_IN1_PIN, LOW);
  digitalWrite(M1_IN2_PIN, LOW);
  doorCommandState = DOOR_IDLE;
}

void setFeederMotorOpen() {
  digitalWrite(M2_IN1_PIN, HIGH);
  digitalWrite(M2_IN2_PIN, LOW);
  feederCommandState = FEEDER_OPENING;
  feederCommandStartMs = millis();
}

void setFeederMotorClose() {
  digitalWrite(M2_IN1_PIN, LOW);
  digitalWrite(M2_IN2_PIN, HIGH);
  feederCommandState = FEEDER_CLOSING;
  feederCommandStartMs = millis();
}

void stopFeederMotor() {
  digitalWrite(M2_IN1_PIN, LOW);
  digitalWrite(M2_IN2_PIN, LOW);
  feederCommandState = FEEDER_IDLE;
}

void setFanPct(int pct) {
  pct = constrain(pct, 0, 100);
  currentFanPct = pct;
  if (pct == 0) {
    digitalWrite(FAN_IN1_PIN, LOW);
    digitalWrite(FAN_IN2_PIN, LOW);
    ledcWrite(FAN_EN_PIN, 0);
  } else {
    digitalWrite(FAN_IN1_PIN, HIGH);
    digitalWrite(FAN_IN2_PIN, LOW);
    int duty = map(pct, 0, 100, 0, 255);
    ledcWrite(FAN_EN_PIN, duty);
  }
}

void stopAllMotors() {
  stopDoorMotor();
  stopFeederMotor();
  setFanPct(0);
}

void handleDoorCommandState(uint32_t nowMs) {
  if (doorCommandState == DOOR_IDLE) {
    return;
  }

  if (doorCommandState == DOOR_OPENING) {
    if (top_switch_state == LOW) {
      stopDoorMotor();
      return;
    }
    if (nowMs - doorCommandStartMs >= DOOR_TIMEOUT_MS) {
      stopDoorMotor();
      return;
    }
  }

  if (doorCommandState == DOOR_CLOSING) {
    if (bottom_switch_state == LOW) {
      stopDoorMotor();
      return;
    }
    if (nowMs - doorCommandStartMs >= DOOR_TIMEOUT_MS) {
      stopDoorMotor();
      return;
    }
  }
}

void handleFeederCommandState(uint32_t nowMs) {
  if (feederCommandState == FEEDER_OPENING || feederCommandState == FEEDER_CLOSING) {
    if (nowMs - feederCommandStartMs >= FEEDER_RUN_MS) {
      stopFeederMotor();
    }
  }
}

void publishSwitchEvent(const char *device, const char *status) {
  StaticJsonDocument<128> evt;
  evt["type"] = "event";
  evt["device"] = device;
  evt["status"] = status;
  serializeJson(evt, Serial);
  Serial.println();
}

void updateSwitchState(uint8_t pin, bool &stableState, uint32_t &lastDebounceMs, const char *device, bool isTopSwitch) {
  bool rawState = digitalRead(pin);
  uint32_t nowMs = millis();

  if (rawState != stableState) {
    if ((int32_t)(nowMs - lastDebounceMs) >= 0) {
      if (nowMs - lastDebounceMs >= SWITCH_DEBOUNCE_MS) {
        stableState = rawState;
        lastDebounceMs = nowMs;
        if (stableState == LOW) {
          publishSwitchEvent(device, isTopSwitch ? "OPEN" : "CLOSED");
        } else {
          publishSwitchEvent(device, "RELEASED");
        }
      }
    }
  }
}

void parseSerialCommand(const char *line) {
  StaticJsonDocument<256> cmd;
  DeserializationError err = deserializeJson(cmd, line);
  if (err) {
    return;
  }

  if (cmd["type"].isNull() || strcmp(cmd["type"], "command") != 0) {
    return;
  }

  const char *device = cmd["device"];
  if (device == nullptr) {
    return;
  }

  if (strcmp(device, "door") == 0) {
    const char *action = cmd["action"];
    if (action == nullptr) {
      return;
    }
    if (strcmp(action, "open") == 0) {
      if (top_switch_state == LOW) {
        stopDoorMotor();
      } else {
        setDoorMotorOpen();
      }
    } else if (strcmp(action, "close") == 0) {
      if (bottom_switch_state == LOW) {
        stopDoorMotor();
      } else {
        setDoorMotorClose();
      }
    } else if (strcmp(action, "stop") == 0) {
      stopDoorMotor();
    }
  } else if (strcmp(device, "feeder") == 0) {
    const char *action = cmd["action"];
    if (action == nullptr) {
      return;
    }
    if (strcmp(action, "open") == 0) {
      setFeederMotorOpen();
    } else if (strcmp(action, "close") == 0) {
      setFeederMotorClose();
    } else if (strcmp(action, "stop") == 0) {
      stopFeederMotor();
    }
  } else if (strcmp(device, "fan") == 0) {
    if (!cmd["speed_pct"].isNull()) {
      int pct = cmd["speed_pct"];
      setFanPct(pct);
    }
  }
}

void handleSerialCommands() {
  while (Serial.available()) {
    char c = Serial.read();
    if (c == '\r') {
      continue;
    }
    if (c == '\n') {
      if (serialCommandIndex > 0) {
        serialCommandBuffer[serialCommandIndex] = '\0';
        parseSerialCommand(serialCommandBuffer);
        serialCommandIndex = 0;
        serialCommandBuffer[0] = '\0';
      }
      continue;
    }
    if (serialCommandIndex < sizeof(serialCommandBuffer) - 1) {
      serialCommandBuffer[serialCommandIndex++] = c;
    }
  }
}


void emitJsonReport(uint32_t nowMs) {

  StaticJsonDocument<768> doc;
  doc["type"] = "telemetry";
  doc["timestamp_ms"] = nowMs;

  if (dht1_error) { doc["temperature1_c"] = nullptr; doc["humidity1_pct"] = nullptr; }
  else            { doc["temperature1_c"] = temperature1_c; doc["humidity1_pct"] = humidity1_pct; }

  if (dht2_error) { doc["temperature2_c"] = nullptr; doc["humidity2_pct"] = nullptr; }
  else            { doc["temperature2_c"] = temperature2_c; doc["humidity2_pct"] = humidity2_pct; }

  if (ultrasonicState1.valid) doc["ultrasonic1_cm"] = ultrasonicState1.distance / 10;
  else                        doc["ultrasonic1_cm"] = nullptr;

  if (ultrasonicState2.valid) doc["ultrasonic2_cm"] = ultrasonicState2.distance / 10;
  else                        doc["ultrasonic2_cm"] = nullptr;

  if (h2s_valid) doc["h2s_ppm"] = h2s_ppm;
  else           doc["h2s_ppm"] = nullptr;

  // H2S sensor status
  if (h2s_valid && calibrated) {
    doc["h2s_status"] = "ok";
  } else if (!calibrated && !calibration_error) {
    int secsLeft = (int)((MQ136_WARMUP_MS - (millis() - mq136WarmupStartMs)) / 1000);
    if (secsLeft < 0) secsLeft = 0;
    char buf136[24];
    snprintf(buf136, sizeof(buf136), "warmup_%ds", secsLeft);
    doc["h2s_status"] = buf136;
  } else {
    doc["h2s_status"] = nullptr;
  }

  if (co2_valid) {
    doc["co2_ppm"] = co2_ppm;
    doc["co2_status"] = "ok";
  } else {
    doc["co2_ppm"] = nullptr;
    doc["co2_status"] = nullptr;
  }

  if (nh3_valid) doc["nh3_ppm"] = nh3_ppm;
  else           doc["nh3_ppm"] = nullptr;

  // NH3 sensor status
  if (nh3_valid && mq137_calibrated) {
    doc["nh3_status"] = "ok";
  } else if (!mq137_calibrated && !mq137_calibration_error) {
    int secsLeft = (int)((MQ137_WARMUP_MS - (millis() - mq137WarmupStartMs)) / 1000);
    if (secsLeft < 0) secsLeft = 0;
    char buf137[24];
    snprintf(buf137, sizeof(buf137), "warmup_%ds", secsLeft);
    doc["nh3_status"] = buf137;
  } else {
    doc["nh3_status"] = nullptr;
  }

  if (bottom_switch_state == LOW) {
    doc["door_state"] = "fully_closed";
  } else if (top_switch_state == LOW) {
    doc["door_state"] = "fully_open";
  } else {
    doc["door_state"] = "moving_or_unknown";
  }

  serializeJson(doc, Serial);
  Serial.println();
}


int readCO2() {

  byte command[9] = {
    0xFF, 0x01, 0x86,
    0x00, 0x00, 0x00,
    0x00, 0x00, 0x79
  };

  byte response[9];

  while (swSerialCO2.available()) {
    swSerialCO2.read();
    }
    swSerialCO2.write(command, 9);

    delay(100);   // allow MH-Z19C enough time to respond

    if (swSerialCO2.available() < 9) {
        return -1;
    }

    for (int i = 0; i < 9; i++) {
        response[i] = swSerialCO2.read();
    }

    if (response[0] != 0xFF || response[1] != 0x86) {
        return -1;
    }
    uint8_t checksum = 0;
    for (int i = 1; i < 8; i++) {
        checksum += response[i];
    }
    checksum = 0xFF - checksum + 1;
    if (checksum != response[8]) {
        return -1;
    }
    return (response[2] << 8) + response[3];
}


/************ SETUP ************/

void setup() {
    Serial.begin(115200);

    pinMode(uSWITCH1_PIN, INPUT_PULLUP);
    pinMode(uSWITCH2_PIN, INPUT_PULLUP);

    pinMode(M1_IN1_PIN, OUTPUT);
    pinMode(M1_IN2_PIN, OUTPUT);
    pinMode(M2_IN1_PIN, OUTPUT);
    pinMode(M2_IN2_PIN, OUTPUT);
    pinMode(FAN_IN1_PIN, OUTPUT);
    pinMode(FAN_IN2_PIN, OUTPUT);
    pinMode(FAN_EN_PIN, OUTPUT);
    digitalWrite(M1_IN1_PIN, LOW);
    digitalWrite(M1_IN2_PIN, LOW);
    digitalWrite(M2_IN1_PIN, LOW);
    digitalWrite(M2_IN2_PIN, LOW);
    digitalWrite(FAN_IN1_PIN, LOW);
    digitalWrite(FAN_IN2_PIN, LOW);
    ledcAttach(FAN_EN_PIN, FAN_PWM_FREQ, FAN_PWM_RESOLUTION);
    ledcWrite(FAN_EN_PIN, 0);

    bottom_switch_state = digitalRead(uSWITCH1_PIN);
    top_switch_state = digitalRead(uSWITCH2_PIN);
    bottom_switch_last_debounce_ms = millis();
    top_switch_last_debounce_ms = millis();

    pinMode(MQ136_PIN, INPUT);
    pinMode(MQ137_PIN, INPUT);

    analogReadResolution(12);
    analogSetPinAttenuation(MQ136_PIN, ADC_11db);
    analogSetPinAttenuation(MQ137_PIN, ADC_11db);

    dht1.begin();
    dht2.begin();

    ultrasonic1.begin(9600, SERIAL_8N1, AO2YYUW1_RX, AO2YYUW1_TX);
    ultrasonic2.begin(9600, SERIAL_8N1, AO2YYUW2_RX, AO2YYUW2_TX);
    swSerialCO2.begin(9600, SWSERIAL_8N1, MHZ19C_RX, MHZ19C_TX);

    // Let sensors stabilize before first read
    delay(500);

    uint32_t nowMs = millis();
    mq136WarmupStartMs = nowMs;
    mq137WarmupStartMs = nowMs;
    nextReportMs = nowMs + REPORT_INTERVAL_MS;
    nextDhtReadMs = nowMs;
    nextMq136SampleMs = nowMs;
    nextMq137SampleMs = nowMs;
    nextCo2ReadMs = nowMs;
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

    handleSerialCommands();
    handleDoorCommandState(now_ms);
    handleFeederCommandState(now_ms);
    updateSwitchState(uSWITCH1_PIN, bottom_switch_state, bottom_switch_last_debounce_ms, "bottom_switch", false);
    updateSwitchState(uSWITCH2_PIN, top_switch_state, top_switch_last_debounce_ms, "top_switch", true);

    if ((int32_t)(now_ms - nextCo2ReadMs) >= 0) {
        int co2Reading = readCO2();

        if (co2Reading >= 0 && co2Reading <= CO2_MAX_PPM) {
            co2_ppm = co2Reading;
            co2_valid = true;
            co2LastUpdateMs = now_ms;
        } else {
            co2_ppm = NAN;
            co2_valid = false;
        }
        nextCo2ReadMs += 3000;
    }

    if (co2_valid && (now_ms - co2LastUpdateMs > MHZ19C_STALE_MS)) {
        co2_valid = false;
        co2_ppm = NAN;
    }

    if ((int32_t)(now_ms - nextReportMs) >= 0) {
        emitJsonReport(now_ms);
        nextReportMs += REPORT_INTERVAL_MS;
    }
}
