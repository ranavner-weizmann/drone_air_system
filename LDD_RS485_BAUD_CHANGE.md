# LDD RS485 baud rate change — 2026-07-12

## The problem

The cavity Arduino (Nano Every, `arduino/sensor/sensor.ino`) got **no response at all**
from the LDD over RS485/MeCom. Every telemetry query timed out, so all `LDD_*` /
`TEC_*` columns in `cavity_data_*.csv` were `nan` / `-1` / `-2147483648`, and rows
came every ~8.36 s instead of 1 s (30 queries x 3 retries x ~93 ms timeout).
Wiring, USB, Arduino firmware, pump/pressure/humidity sensors: all verified OK.

## Root cause

Probing the LDD directly over its USB port (`/dev/ttyUSB1`, FTDI FT230X serial
`DQ01F1MH`) found the device healthy but with a **baud mismatch**:

| | |
|---|---|
| Device | `8157-LDD-AN-LIN G01`, type **LDD-1321**, HW 1.20, FW 1.30, S/N 60 |
| Device Status | 2 (Run), no errors |
| Device Address (par 2051) | 1  — matches firmware, OK |
| **RS485 Base Baud Rate (par 2050)** | **9600** — firmware expects **57600** |

`arduino/sensor/sensor.ino` line 15: `const uint32_t MECOM_BAUD = 57600;`

## The change that was made

Exactly ONE device parameter was written, over USB, using
`set_ldd_rs485_baud.py` (in this directory):

| Parameter | Instance | Old value | New value |
|-----------|----------|-----------|-----------|
| 2050 "RS485 Base Baud Rate" | 1 | **9600** | **57600** |

Nothing else was changed: not the device address, not the firmware on the
Arduino, not any Python logger code.

**Result (2026-07-12 ~19:00):** write acknowledged and verified — parameter
2050 read back **57600**. Device still healthy (address 1, status Run,
no errors).

**Update (2026-07-12 ~19:15): the write did NOT persist.** After the LDD was
power-cycled and tested on the drone (still all-NaN), reading it over USB
showed 2050 back at **9600**. MeCom `VS` writes on this device are RAM-only;
parameter 108 ("Save Data to Flash", used by the TEC family) does not exist
on the LDD-1321 (`+05` NACK). So the ~19:04 drone test ran at 9600 — the
57600 fix was never actually active on the RS485 bus and remains untested.

**Current device state: 9600 — identical to before any changes were made.**

To make a persistent change, use the Meerstetter service software on the PC
(set "RS485 Base Baud Rate", then use its explicit save-to-flash function,
power-cycle, and verify). A RAM-only write with `set_ldd_rs485_baud.py`
is still useful for a live test: it takes effect immediately and
self-reverts on the next power-cycle.

## How to revert

The USB link to the LDD always runs at 57600 regardless of this parameter,
so you can always reach the device to undo this:

```bash
# LDD connected to the Pi via USB (shows up as /dev/ttyUSB1):
cd /home/rsp/drone_air_system
venv/bin/python set_ldd_rs485_baud.py          # show current value
venv/bin/python set_ldd_rs485_baud.py 9600     # revert to the old value
```

Or in the Meerstetter service software on a PC: Communication settings ->
"RS485 Base Baud Rate" -> set 9600 -> save to flash.

## After changing (either direction)

1. Power-cycle the LDD, then run `set_ldd_rs485_baud.py` with no argument
   to confirm the value persisted in flash.
2. Reconnect the LDD to the drone's RS485 bus.
3. Restart the logger and check `output/<run>/csv/cavity_data_*.csv`:
   rows should now arrive every ~1 s with real numbers in the LDD/TEC columns.
