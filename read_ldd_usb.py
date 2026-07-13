#!/usr/bin/env python3
"""Read the same LDD telemetry the uri_aplogger cavity process logs,
but directly over the LDD's USB (MeCom) port instead of the Arduino."""
import struct
import sys
import time

import serial

USB_LINK_BAUD = 57600
CANDIDATE_PORTS = ["/dev/ttyUSB0", "/dev/ttyUSB2", "/dev/ttyUSB1"]


def crc16(data: bytes) -> int:
    n = 0
    for b in data:
        n ^= b << 8
        for _ in range(8):
            n = ((n << 1) ^ 0x1021) if n & 0x8000 else (n << 1)
        n &= 0xFFFF
    return n


_seq = int(time.time()) & 0x7FFF


def transact(ser, addr: int, payload: str, timeout=0.5):
    global _seq
    _seq = (_seq + 1) & 0xFFFF
    body = f"#{addr:02X}{_seq:04X}{payload}".encode("ascii")
    sent_crc = crc16(body)
    frame = body + f"{sent_crc:04X}".encode("ascii") + b"\r"
    ser.reset_input_buffer()
    ser.write(frame)
    ser.flush()

    buf = b""
    t0 = time.time()
    while time.time() - t0 < timeout:
        c = ser.read(1)
        if not c:
            continue
        if c == b"!":
            buf = c
        elif buf:
            if c == b"\r":
                s = buf.decode("ascii", errors="replace")
                if len(s) < 11:
                    return None
                expect = sent_crc if len(s) == 11 else crc16(buf[:-4])
                if expect != int(s[-4:], 16):
                    return None
                if int(s[3:7], 16) != _seq:
                    buf = b""
                    continue
                return s[7:-4]
            buf += c
    return None


def read_i32(ser, par_id, inst=1):
    p = transact(ser, 0, f"?VR{par_id:04X}{inst:02X}")
    if p is None or p.startswith("+"):
        return None
    v = int(p, 16)
    return v - (1 << 32) if v >= (1 << 31) else v


def read_f32(ser, par_id, inst=1):
    p = transact(ser, 0, f"?VR{par_id:04X}{inst:02X}")
    if p is None or p.startswith("+"):
        return None
    return struct.unpack(">f", bytes.fromhex(p))[0]


# (name, par_id, type)  -- same set the cavity Arduino firmware queries
PARAMS = [
    ("ErrorNumber", 105, "i"),
    ("ErrorInstance", 106, "i"),
    ("ErrorParameter", 107, "i"),
    ("LDD_ActualOutputCurrent", 1100, "f"),
    ("LDD_ActualOutputVoltage", 1101, "f"),
    ("LDD_ActualOutputCurrentRaw", 1102, "fi"),
    ("LDD_ActualAnodeVoltage", 1104, "f"),
    ("LDD_ActualCathodeVoltage", 1105, "f"),
    ("LDD_NominalOutputCurrentRamp", 1402, "f"),
    ("TEC_TargetObjectTemperature", 1010, "f"),
    ("TEC_NominalObjectTemperatureRamp", 1011, "f"),
    ("TEC_ThermalPowerModelCurrent", 1012, "f"),
    ("TEC_ActualOutputCurrent", 1020, "f"),
    ("TEC_ActualOutputVoltage", 1021, "f"),
    ("TEC_ObjectTemperature", 1000, "f"),
    ("TEC_SinkTemperature", 1001, "f"),
    ("AnalogVoltageInputRawADC", 1502, "fi"),
    ("AnalogVoltageInput", 1500, "f"),
    ("LaserPower", 1600, "f"),
    ("OutputLevel", 1601, "f"),
    ("DriverInputVoltage", 1200, "f"),
    ("Internal8V", 1201, "f"),
    ("Internal5V", 1202, "f"),
    ("Internal3V3", 1203, "f"),
    ("InternalMinus3V3", 1204, "f"),
    ("DeviceTemperature", 1300, "f"),
    ("PowerstageTemperature", 1301, "f"),
]


def find_ldd():
    for port in CANDIDATE_PORTS:
        try:
            ser = serial.Serial(port, USB_LINK_BAUD, timeout=0.1)
        except Exception as e:
            print(f"{port}: cannot open ({e})", file=sys.stderr)
            continue
        time.sleep(0.2)
        ident = transact(ser, 0, "?IF")
        if ident is not None:
            print(f"LDD found on {port}: {ident.strip()}", file=sys.stderr)
            return ser
        ser.close()
        print(f"{port}: no MeCom reply", file=sys.stderr)
    return None


def main():
    ser = find_ldd()
    if ser is None:
        sys.exit("No LDD found on any candidate port.")
    with ser:
        for name, pid, typ in PARAMS:
            if typ == "i":
                v = read_i32(ser, pid)
                print(f"{name:34s} = {v}")
            elif typ == "fi":
                v = read_f32(ser, pid)
                print(f"{name:34s} = {int(v) if v is not None else None}")
            else:
                v = read_f32(ser, pid)
                print(f"{name:34s} = {v:.4f}" if v is not None else f"{name:34s} = None")


if __name__ == "__main__":
    main()
