#!/usr/bin/env python3
"""Read or set the RS485 baud rate of the Meerstetter LDD over its USB port.

Usage:
    python set_ldd_rs485_baud.py                    # read current settings only
    python set_ldd_rs485_baud.py 57600              # set RS485 baud to 57600
    python set_ldd_rs485_baud.py 9600               # revert RS485 baud to 9600
    python set_ldd_rs485_baud.py --port /dev/ttyUSB1 57600

Talks MeCom (Meerstetter communication protocol) on the LDD's USB serial
port (FTDI FT230X). The USB link itself always works at 57600 regardless
of the RS485 setting, so this tool can always reach the device to revert.

Parameter changed: 2050 "RS485 Base Baud Rate" (INT32, instance 1).
"""
import argparse
import struct
import sys
import time

import serial

USB_LINK_BAUD = 57600      # baud of the USB/FTDI link (not the RS485 setting)
PAR_RS485_BAUD = 2050      # RS485 Base Baud Rate
PAR_DEVICE_ADDR = 2051     # Device Address
PAR_SAVE_TO_FLASH = 108    # Save Data to Flash (0 = auto-save enabled)
ALLOWED_BAUDS = {9600, 19200, 38400, 57600, 115200, 230400}


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
    """Send one MeCom frame, return (seq, reply_payload) or None on timeout."""
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
                    print(f"  ! short reply: {buf!r}")
                    return None
                # An 11-char frame is a set-ACK: its CRC field echoes the CRC
                # of the request frame (see MeFrame.cpp). Longer frames carry
                # a payload and a CRC computed over the reply itself.
                expect = sent_crc if len(s) == 11 else crc16(buf[:-4])
                if expect != int(s[-4:], 16):
                    print(f"  ! bad CRC in reply: {buf!r}")
                    return None
                if int(s[3:7], 16) != _seq:
                    buf = b""   # stale frame, keep listening
                    continue
                return (_seq, s[7:-4])
            buf += c
    return None


def read_i32(ser, par_id, inst=1):
    r = transact(ser, 0, f"?VR{par_id:04X}{inst:02X}")
    if r is None:
        return None
    payload = r[1]
    if payload.startswith("+"):
        return f"device NACK {payload}"
    v = int(payload, 16)
    return v - (1 << 32) if v >= (1 << 31) else v


def write_i32(ser, par_id, value, inst=1):
    """Returns True if the device ACKed the set command."""
    r = transact(ser, 0, f"VS{par_id:04X}{inst:02X}{value & 0xFFFFFFFF:08X}")
    if r is None:
        return False
    if r[1].startswith("+"):
        print(f"  ! device NACK: {r[1]}")
        return False
    return True   # empty payload = ACK


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("baud", nargs="?", type=int,
                    help="target RS485 baud rate; omit to only read")
    ap.add_argument("--port", default="/dev/ttyUSB1")
    args = ap.parse_args()

    if args.baud is not None and args.baud not in ALLOWED_BAUDS:
        sys.exit(f"refusing unusual baud {args.baud}; allowed: {sorted(ALLOWED_BAUDS)}")

    with serial.Serial(args.port, USB_LINK_BAUD, timeout=0.1) as ser:
        time.sleep(0.2)

        ident = transact(ser, 0, "?IF")
        if ident is None:
            sys.exit(f"No MeCom reply on {args.port} — is the LDD connected via USB?")
        print(f"Device       : {ident[1].strip()}")
        print(f"Device Addr  : {read_i32(ser, PAR_DEVICE_ADDR)}")
        print(f"SaveToFlash  : {read_i32(ser, PAR_SAVE_TO_FLASH)} (0 = auto-save enabled)")
        before = read_i32(ser, PAR_RS485_BAUD)
        print(f"RS485 baud   : {before}   <-- current value")

        if args.baud is None:
            return

        if before == args.baud:
            print(f"Already {args.baud}, nothing to do.")
            return

        print(f"\nWriting parameter {PAR_RS485_BAUD} = {args.baud} ...")
        if not write_i32(ser, PAR_RS485_BAUD, args.baud):
            sys.exit("Set command was not acknowledged — nothing changed.")

        time.sleep(0.3)
        after = read_i32(ser, PAR_RS485_BAUD)
        print(f"Read-back    : {after}")
        if after == args.baud:
            print(f"SUCCESS: RS485 baud changed {before} -> {after}")
            print("Power-cycle the LDD and run this script with no argument "
                  "to confirm the value persisted.")
        else:
            print(f"WARNING: read-back is {after}, expected {args.baud}")


if __name__ == "__main__":
    main()
