#include <MeComAPI.h>
#include <SPI.h>
#include <Wire.h>

// =======================
// LDD / MeCom (RS485 on Serial1)
// =======================
#define RS485_EN_PIN     4
#define RS485_TX_LEVEL   HIGH
#define RS485_RX_LEVEL   LOW

const uint8_t  DEV_ADDRESS = 1;
const uint8_t  INST_MON    = 1;
const uint8_t  INST_COMMON = 1;
const uint32_t MECOM_BAUD  = 57600;

// MeCom uses Serial1 (Nano Every)
Stream* ArduinoSerial = &Serial1;

// --- MeCom helpers ---
static float readFloat(uint16_t parId, uint8_t inst, bool &okOut) {
  MeParFloatFields f;
  uint8_t ok = MeCom_ParValuef(DEV_ADDRESS, parId, inst, &f, MeGet);
  okOut = (ok == 1);
  return okOut ? f.Value : NAN;
}

static long readLong(uint16_t parId, uint8_t inst, bool &okOut) {
  MeParLongFields l;
  uint8_t ok = MeCom_ParValuel(DEV_ADDRESS, parId, inst, &l, MeGet);
  okOut = (ok == 1);
  return okOut ? (long)l.Value : -1;
}

static bool writeFloat(uint16_t parId, uint8_t inst, float value) {
  MeParFloatFields f; f.Value = value;
  uint8_t ok = MeCom_ParValuef(DEV_ADDRESS, parId, inst, &f, MeSet);
  return (ok == 1);
}

static bool writeLong(uint16_t parId, uint8_t inst, long value) {
  MeParLongFields l; l.Value = value;
  uint8_t ok = MeCom_ParValuel(DEV_ADDRESS, parId, inst, &l, MeSet);
  return (ok == 1);
}

static bool identTest() {
  int8_t buf[64] = {0};
  uint8_t ok = MeCom_GetIdentString(DEV_ADDRESS, buf);
  if (ok != 1) return false;
  Serial.print(F("IDENT addr="));
  Serial.print(DEV_ADDRESS);
  Serial.print(F(": "));
  Serial.println((char*)buf);
  return true;
}

// error text (optional depending on your MeComAPI)
static void printErrorTextField() {
#if defined(MeCom_GetErrorString)
  int8_t buf[80] = {0};
  uint8_t ok = MeCom_GetErrorString(DEV_ADDRESS, buf);
  if (ok == 1) Serial.print((char*)buf);
#elif defined(MeCom_GetErrorText)
  int8_t buf[80] = {0};
  uint8_t ok = MeCom_GetErrorText(DEV_ADDRESS, buf);
  if (ok == 1) Serial.print((char*)buf);
#else
  // leave blank
#endif
}

// =======================
// Pump / Pressure / HDC2022
// =======================
const int pressureSS = 10;
const float P_min   = 0.0;
const float P_max   = 1.6;
const float Out_min = 1638.0;
const float Out_max = 14746.0;

const int pumpPwmPin = 9;
const int fgPin      = 2;
const int PULSES_PER_REV = 6;

#define HDC2022_ADDR    0x41
#define HDC_TEMP_LOW    0x00
#define HDC_RESET_REG   0x0E
#define HDC_MEAS_CONFIG 0x0F

volatile uint32_t fgPulses = 0;

int currentSpeedPercent = 40;
float lastTemperature = -40.0;
float lastHumidity    = 0.0;

static void setPumpSpeed(int speedPercent) {
  speedPercent = constrain(speedPercent, 0, 100);
  // Inverse PWM: 0% = 255 (Stop), 100% = 0 (Full)
  int pwmValue = map(speedPercent, 0, 100, 255, 0);
  analogWrite(pumpPwmPin, pwmValue);
}

void ISR_fg() {
  fgPulses++;
}

static void configureSensorAuto() {
  Wire.beginTransmission(HDC2022_ADDR);
  Wire.write(HDC_RESET_REG);
  Wire.write(0x40); // 1 Hz Auto Mode
  Wire.endTransmission();
  delay(10);

  Wire.beginTransmission(HDC2022_ADDR);
  Wire.write(HDC_MEAS_CONFIG);
  Wire.write(0x01); // Start auto mode
  Wire.endTransmission();
}

static bool readHDCData(float &temp, float &hum) {
  Wire.beginTransmission(HDC2022_ADDR);
  Wire.write(HDC_TEMP_LOW);
  if (Wire.endTransmission() != 0) return false;

  uint8_t got = Wire.requestFrom(HDC2022_ADDR, (uint8_t)4);
  if (got != 4) return false;

  uint16_t tRaw = Wire.read() | (Wire.read() << 8);
  uint16_t hRaw = Wire.read() | (Wire.read() << 8);

  temp = (tRaw / 65536.0) * 165.0 - 40.0;
  hum  = (hRaw / 65536.0) * 100.0;
  return true;
}

// Pressure SPI read -> pressure_mb, status (0=OK)
static void readPressure(float &pressure_mb, uint8_t &status) {
  pressure_mb = NAN;

  SPI.beginTransaction(SPISettings(800000, MSBFIRST, SPI_MODE0));
  digitalWrite(pressureSS, LOW);
  byte byte1 = SPI.transfer(0x00);
  byte byte2 = SPI.transfer(0x00);
  digitalWrite(pressureSS, HIGH);
  SPI.endTransaction();

  unsigned int rawCounts = ((byte1 & 0x3F) << 8) | byte2;
  status = (byte1 >> 6) & 0x03;

  if (status == 0) {
    float pressure_bar =
      ((rawCounts - Out_min) * (P_max - P_min)) / (Out_max - Out_min) + P_min;
    pressure_mb = pressure_bar * 1000.0;
  }
}

// =======================
// Unified command parser (no String fragmentation)
// =======================
static char cmdBuf[128];
static uint8_t cmdLen = 0;

static void handleCommandLine(const char* line) {
  while (*line == ' ' || *line == '\t') line++;
  if (*line == 0) return;

  // tokenise first word
  char tmp[128];
  strncpy(tmp, line, sizeof(tmp)-1);
  tmp[sizeof(tmp)-1] = 0;

  char* sp = strchr(tmp, ' ');
  if (sp) *sp = 0;

  for (char* p = tmp; *p; p++) *p = toupper(*p);

  const char* arg = nullptr;
  if (sp) {
    // find arg in original line, not tmp
    arg = line + (sp - tmp) + 1;
    while (*arg == ' ' || *arg == '\t') arg++;
  }

  // ---- generic ----
  if (strcmp(tmp, "PING") == 0) {
    Serial.println(F("OK PONG"));
    return;
  }

  // ---- LDD commands ----
  if (strcmp(tmp, "GET") == 0) {
    // One-shot full row will be printed on next 1Hz tick; still ack immediately
    Serial.println(F("OK GET"));
    return;
  }

  if (strcmp(tmp, "RESET") == 0) {
    bool ok = writeLong(111, INST_COMMON, 1);
    Serial.println(ok ? F("OK RESET") : F("ERR RESET"));
    return;
  }

  if (strcmp(tmp, "SETC") == 0) {
    if (!arg || !*arg) { Serial.println(F("ERR SETC missing_value")); return; }
    float amps = atof(arg);
    bool ok = writeFloat(2102, INST_MON, amps);
    if (ok) { Serial.print(F("OK SETC ")); Serial.println(amps, 3); }
    else    { Serial.println(F("ERR SETC")); }
    return;
  }

  if (strcmp(tmp, "SETT") == 0) {
    if (!arg || !*arg) { Serial.println(F("ERR SETT missing_value")); return; }
    float degC = atof(arg);
    bool ok = writeFloat(4000, INST_MON, degC);
    if (ok) { Serial.print(F("OK SETT ")); Serial.println(degC, 2); }
    else    { Serial.println(F("ERR SETT")); }
    return;
  }

  // ---- Pump power ----
  if (strcmp(tmp, "SETPWR") == 0 || strcmp(tmp, "SETPOWER") == 0) {
    if (!arg || !*arg) { Serial.println(F("ERR SETPWR missing_value")); return; }
    int pct = atoi(arg);
    pct = constrain(pct, 0, 100);
    currentSpeedPercent = pct;
    setPumpSpeed(currentSpeedPercent);
    Serial.print(F("OK SETPWR "));
    Serial.println(currentSpeedPercent);
    return;
  }

  Serial.print(F("ERR unknown_cmd "));
  Serial.println(tmp);
}

static void pollUsbCommands() {
  while (Serial.available() > 0) {
    char c = (char)Serial.read();
    if (c == '\r') continue;
    if (c == '\n') {
      cmdBuf[cmdLen] = 0;
      handleCommandLine(cmdBuf);
      cmdLen = 0;
    } else {
      if (cmdLen < sizeof(cmdBuf)-1) cmdBuf[cmdLen++] = c;
    }
  }
}

// =======================
// CSV header (full LDD + pump)
// =======================
static void printMergedHeader() {
  Serial.println(
    F("ms,"
      "ErrorNumber,ErrorInstance,ErrorParameter,ErrorText,"
      "LDD_ActualOutputCurrent,LDD_ActualOutputVoltage,LDD_ActualOutputCurrentRaw,"
      "LDD_ActualAnodeVoltage,LDD_ActualCathodeVoltage,"
      "LDD_NominalOutputCurrentRamp,"
      "TEC_TargetObjectTemperature,TEC_NominalObjectTemperatureRamp,TEC_ThermalPowerModelCurrent,"
      "TEC_ActualOutputCurrent,TEC_ActualOutputVoltage,"
      "TEC_ObjectTemperature,TEC_SinkTemperature,"
      "AnalogVoltageInputRawADC,AnalogVoltageInput,"
      "LaserPower,OutputLevel,"
      "DriverInputVoltage,Internal8V,Internal5V,Internal3V3,InternalMinus3V3,"
      "DeviceTemperature,PowerstageTemperature,"
      "pump_rpm,pressure_mb,temp_c,humidity_pct,power_pct,pressure_status")
  );
}

// =======================
// Main
// =======================
void setup() {
  // One USB serial baud for the merged sketch
  Serial.begin(115200);
  unsigned long tStart = millis();
  while (!Serial && millis() - tStart < 2000) { delay(10); }

  // MeCom / RS485
  Serial1.begin(MECOM_BAUD);
  pinMode(RS485_EN_PIN, OUTPUT);
  digitalWrite(RS485_EN_PIN, RS485_RX_LEVEL);

  delay(200);

  Serial.println(F("Merged LDD + Pump controller (1Hz merged CSV)"));
  bool ok = identTest();
  Serial.print(F("IDENT ok="));
  Serial.println(ok ? 1 : 0);

  Serial.println(F("OK COMMANDS: PING, RESET, SETC <amps>, SETT <degC>, SETPWR <0-100>"));

  // Pump/pressure/HDC init
  SPI.begin();
  pinMode(pressureSS, OUTPUT);
  digitalWrite(pressureSS, HIGH);

  pinMode(pumpPwmPin, OUTPUT);
  pinMode(fgPin, INPUT_PULLUP);
  attachInterrupt(digitalPinToInterrupt(fgPin), ISR_fg, FALLING);

  setPumpSpeed(currentSpeedPercent);

  Wire.begin();
  delay(100);
  configureSensorAuto();

  // Print CSV header once
  printMergedHeader();
}

void loop() {
  pollUsbCommands();

  static uint32_t lastPrint = 0;
  uint32_t now = millis();

  if (now - lastPrint >= 1000) {
    lastPrint = now;

    // ---------- Pump RPM (non-blocking) ----------
    static uint32_t lastPulses = 0;
    uint32_t pulses;
    noInterrupts();
    pulses = fgPulses;
    interrupts();

    uint32_t dp = pulses - lastPulses;
    lastPulses = pulses;

    float pump_rpm = (dp * 60.0f) / (float)PULSES_PER_REV;

    // ---------- Pressure ----------
    float pressure_mb;
    uint8_t pstat;
    readPressure(pressure_mb, pstat);

    // ---------- HDC ----------
    float temperature = lastTemperature;
    float humidity = lastHumidity;
    bool hOk = readHDCData(temperature, humidity);
    if (hOk) {
      lastTemperature = temperature;
      lastHumidity = humidity;
    }

    // ---------- Full LDD read (same field set as your full telemetry) ----------
    bool okL;

    long errNo   = readLong(105, INST_COMMON, okL);
    long errInst = readLong(106, INST_COMMON, okL);
    long errPar  = readLong(107, INST_COMMON, okL);

    float ldd_i      = readFloat(1100, INST_MON, okL);
    float ldd_v      = readFloat(1101, INST_MON, okL);
    long  ldd_i_raw  = (long)readFloat(1102, INST_MON, okL);
    float ldd_va     = readFloat(1104, INST_MON, okL);
    float ldd_vc     = readFloat(1105, INST_MON, okL);

    float ldd_i_ramp = readFloat(1402, INST_MON, okL);

    float tec_target = readFloat(1010, INST_MON, okL);
    float tec_nom_r  = readFloat(1011, INST_MON, okL);
    float tec_modeli = readFloat(1012, INST_MON, okL);

    float tec_i      = readFloat(1020, INST_MON, okL);
    float tec_v      = readFloat(1021, INST_MON, okL);

    float obj_t      = readFloat(1000, INST_MON, okL);
    float sink_t     = readFloat(1001, INST_MON, okL);

    long  ain_adc    = (long)readFloat(1502, INST_MON, okL);
    float ain_v      = readFloat(1500, INST_MON, okL);

    float laser_p    = readFloat(1600, INST_MON, okL);
    float out_level  = readFloat(1601, INST_MON, okL);

    float vin        = readFloat(1200, INST_MON, okL);
    float v8         = readFloat(1201, INST_MON, okL);
    float v5         = readFloat(1202, INST_MON, okL);
    float v33        = readFloat(1203, INST_MON, okL);
    float vm33       = readFloat(1204, INST_MON, okL);

    float dev_t      = readFloat(1300, INST_MON, okL);
    float pstage_t   = readFloat(1301, INST_MON, okL);

    // ---------- Print one merged CSV row ----------

    Serial.print(errNo);   Serial.print(',');
    Serial.print(errInst); Serial.print(',');
    Serial.print(errPar);  Serial.print(',');
    printErrorTextField(); Serial.print(',');

    Serial.print(ldd_i, 6);     Serial.print(',');
    Serial.print(ldd_v, 6);     Serial.print(',');
    Serial.print(ldd_i_raw);    Serial.print(',');
    Serial.print(ldd_va, 6);    Serial.print(',');
    Serial.print(ldd_vc, 6);    Serial.print(',');

    Serial.print(ldd_i_ramp, 6); Serial.print(',');

    Serial.print(tec_target, 3); Serial.print(',');
    Serial.print(tec_nom_r, 3);  Serial.print(',');
    Serial.print(tec_modeli, 6); Serial.print(',');

    Serial.print(tec_i, 6); Serial.print(',');
    Serial.print(tec_v, 6); Serial.print(',');

    Serial.print(obj_t, 3);  Serial.print(',');
    Serial.print(sink_t, 3); Serial.print(',');

    Serial.print(ain_adc); Serial.print(',');
    Serial.print(ain_v, 6); Serial.print(',');

    Serial.print(laser_p, 6); Serial.print(',');
    Serial.print(out_level, 6); Serial.print(',');

    Serial.print(vin, 3);  Serial.print(',');
    Serial.print(v8, 3);   Serial.print(',');
    Serial.print(v5, 3);   Serial.print(',');
    Serial.print(v33, 3);  Serial.print(',');
    Serial.print(vm33, 3); Serial.print(',');

    Serial.print(dev_t, 3);    Serial.print(',');
    Serial.print(pstage_t, 3); Serial.print(',');

    // Pump block
    Serial.print(pump_rpm, 0); Serial.print(',');
    if (pstat == 0) Serial.print(pressure_mb, 1);
    else            Serial.print(F("nan"));
    Serial.print(',');
    Serial.print(temperature, 1); Serial.print(',');
    Serial.print(humidity, 1); Serial.print(',');
    Serial.print(currentSpeedPercent); Serial.print(',');
    Serial.println((int)pstat);
  }
}
