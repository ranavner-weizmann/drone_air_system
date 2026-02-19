#include <MeComAPI.h>

// ---- RS485 direction control (matches your working setup) ----
#define RS485_EN_PIN     4
#define RS485_TX_LEVEL   HIGH
#define RS485_RX_LEVEL   LOW

// -------- Device config --------
const uint8_t DEV_ADDRESS = 1;

const uint8_t INST_MON    = 1;
const uint8_t INST_COMMON = 1;

const uint32_t MECOM_BAUD = 57600;

const uint32_t TELEMETRY_PERIOD_MS = 100;

Stream* ArduinoSerial = &Serial1;

// -------- Minimal helpers --------
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
  MeParFloatFields f;
  f.Value = value;
  uint8_t ok = MeCom_ParValuef(DEV_ADDRESS, parId, inst, &f, MeSet);
  return (ok == 1);
}

static bool writeLong(uint16_t parId, uint8_t inst, long value) {
  MeParLongFields l;
  l.Value = value;
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

static void printHeader() {
  Serial.println(
    F("ErrorNumber,ErrorInstance,ErrorParameter,ErrorText,"
      "LDD_ActualOutputCurrent,LDD_ActualOutputVoltage,LDD_ActualOutputCurrentRaw,"
      "LDD_ActualAnodeVoltage,LDD_ActualCathodeVoltage,"
      "LDD_NominalOutputCurrentRamp,"
      "TEC_TargetObjectTemperature,TEC_NominalObjectTemperatureRamp,TEC_ThermalPowerModelCurrent,"
      "TEC_ActualOutputCurrent,TEC_ActualOutputVoltage,"
      "TEC_ObjectTemperature,TEC_SinkTemperature,"
      "AnalogVoltageInputRawADC,AnalogVoltageInput,"
      "LaserPower,OutputLevel,"
      "DriverInputVoltage,Internal8V,Internal5V,Internal3V3,InternalMinus3V3,"
      "DeviceTemperature,PowerstageTemperature")
  );
}

// If your library exposes error text getter, enable it; otherwise keep blank.
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
  // blank
#endif
}

// -------- Telemetry function (so GET can call it) --------
static void printTelemetryLine() {
  bool ok;

  long errNo   = readLong(105, INST_COMMON, ok);
  long errInst = readLong(106, INST_COMMON, ok);
  long errPar  = readLong(107, INST_COMMON, ok);

  float ldd_i      = readFloat(1100, INST_MON, ok);
  float ldd_v      = readFloat(1101, INST_MON, ok);
  long  ldd_i_raw  = readFloat(1102, INST_MON, ok);
  float ldd_va     = readFloat(1104, INST_MON, ok);
  float ldd_vc     = readFloat(1105, INST_MON, ok);

  float ldd_i_ramp = readFloat(1402, INST_MON, ok);

  float tec_target = readFloat(1010, INST_MON, ok);
  float tec_nom_r  = readFloat(1011, INST_MON, ok);
  float tec_modeli = readFloat(1012, INST_MON, ok);

  float teca_i      = readFloat(1020, INST_MON, ok);
  float tec_v      = readFloat(1021, INST_MON, ok);

  float obj_t      = readFloat(1000, INST_MON, ok);
  float sink_t     = readFloat(1001, INST_MON, ok);

  long  ain_adc    = readFloat(1502, INST_MON, ok);
  float ain_v      = readFloat(1500, INST_MON, ok);

  float laser_p    = readFloat(1600, INST_MON, ok);
  float out_level  = readFloat(1601, INST_MON, ok);

  float vin        = readFloat(1060, INST_MON, ok);
  float v8         = readFloat(1061, INST_MON, ok);
  float v5         = readFloat(1062, INST_MON, ok);
  float v3_3       = readFloat(1063, INST_MON, ok);
  float vm3_3      = readFloat(1064, INST_MON, ok);
  float t_dev      = readFloat(1065, INST_MON, ok);
  float t_pstage   = readFloat(1066, INST_MON, ok);

  Serial.print(errNo);   Serial.print(',');
  Serial.print(errInst); Serial.print(',');
  Serial.print(errPar);  Serial.print(',');

  printErrorTextField(); Serial.print(',');

  Serial.print(ldd_i);      Serial.print(',');
  Serial.print(ldd_v);      Serial.print(',');
  Serial.print(ldd_i_raw);  Serial.print(',');

  Serial.print(ldd_va);     Serial.print(',');
  Serial.print(ldd_vc);     Serial.print(',');
  Serial.print(ldd_i_ramp); Serial.print(',');

  Serial.print(tec_target); Serial.print(',');
  Serial.print(tec_nom_r);  Serial.print(',');
  Serial.print(tec_modeli); Serial.print(',');

  Serial.print(tec_i);      Serial.print(',');
  Serial.print(tec_v);      Serial.print(',');

  Serial.print(obj_t);      Serial.print(',');
  Serial.print(sink_t);     Serial.print(',');

  Serial.print(ain_adc);    Serial.print(',');
  Serial.print(ain_v);      Serial.print(',');

  Serial.print(laser_p);    Serial.print(',');
  Serial.print(out_level);  Serial.print(',');

  Serial.print(vin);        Serial.print(',');
  Serial.print(v8);         Serial.print(',');
  Serial.print(v5);         Serial.print(',');
  Serial.print(v3_3);       Serial.print(',');
  Serial.print(vm3_3);      Serial.print(',');

  Serial.print(t_dev);      Serial.print(',');
  Serial.print(t_pstage);

  Serial.println();
}

// -------- Command parsing (USB Serial) --------
static String cmdLine;

static void handleCommandLine(const String &line) {
  String s = line;
  s.trim();
  if (s.length() == 0) return;

  // Split first token
  int sp = s.indexOf(' ');
  String cmd = (sp < 0) ? s : s.substring(0, sp);
  cmd.toUpperCase();

  if (cmd == "PING") {
    Serial.println(F("OK PONG"));
    return;
  }

  if (cmd == "GET") {
    // immediate telemetry snapshot
    printTelemetryLine();
    Serial.println(F("OK GET"));
    return;
  }

  if (cmd == "RESET") {
    // Soft reset = write 1 to parId 111
    bool ok = writeLong(111, INST_COMMON, 1);
    Serial.println(ok ? F("OK RESET") : F("ERR RESET"));
    return;
  }

  if (cmd == "SETC") {
    // LDD set current = parId 2102 (float)
    if (sp < 0) { Serial.println(F("ERR SETC missing_value")); return; }
    float amps = s.substring(sp + 1).toFloat();
    bool ok = writeFloat(2102, INST_MON, amps);
    if (ok) {
      Serial.print(F("OK SETC "));
      Serial.println(amps, 3);
    } else {
      Serial.println(F("ERR SETC"));
    }
    return;
  }

  if (cmd == "SETT") {
    // TEC target object temp = parId 4000 (float)
    if (sp < 0) { Serial.println(F("ERR SETT missing_value")); return; }
    float degC = s.substring(sp + 1).toFloat();
    bool ok = writeFloat(4000, INST_MON, degC);
    if (ok) {
      Serial.print(F("OK SETT "));
      Serial.println(degC, 2);
    } else {
      Serial.println(F("ERR SETT"));
    }
    return;
  }

  Serial.print(F("ERR unknown_cmd "));
  Serial.println(cmd);
}

static void pollUsbCommands() {
  while (Serial.available() > 0) {
    char c = (char)Serial.read();
    if (c == '\r') continue;
    if (c == '\n') {
      handleCommandLine(cmdLine);
      cmdLine = "";
    } else {
      if (cmdLine.length() < 120) cmdLine += c; // prevent runaway lines
    }
  }
}

// -------- Setup / loop --------
void setup() {
  Serial.begin(57600);
  while (!Serial) {}

  Serial1.begin(MECOM_BAUD);

  pinMode(RS485_EN_PIN, OUTPUT);
  digitalWrite(RS485_EN_PIN, RS485_RX_LEVEL);

  delay(200);

  Serial.println(F("MeCom + Pi command interface"));
  bool ok = identTest();
  Serial.print(F("IDENT ok="));
  Serial.println(ok ? 1 : 0);

  // Tell Pi what commands exist
  Serial.println(F("OK COMMANDS: PING, GET, RESET, SETC <amps>, SETT <degC>"));

  printHeader();
}

void loop() {
  static uint32_t lastT = 0;

  pollUsbCommands();

  uint32_t now = millis();
  if (now - lastT >= TELEMETRY_PERIOD_MS) {
    lastT = now;
    printTelemetryLine();
  }
}
