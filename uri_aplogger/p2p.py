#!/usr/bin/env python3
import sys
import time
import serial
import pyudev
############################# PARTECTOR 2 PRO TESTING 
######################################### IT'S BROKEN

def find_device_port(vendor_id, model_id, serial_short=None):
    target_vendor = str(vendor_id).lower()
    target_model = str(model_id).lower()
    target_serial = str(serial_short).lower() if serial_short else None

    context = pyudev.Context()
    matches = []

    for device in context.list_devices(subsystem="tty"):
        vid = device.get("ID_VENDOR_ID", "").lower()
        mid = device.get("ID_MODEL_ID", "").lower()
        ser = device.get("ID_SERIAL_SHORT", "").lower()
        node = device.device_node

        if vid == target_vendor and mid == target_model:
            if target_serial and ser != target_serial:
                continue
            matches.append((node, ser))

    if len(matches) == 1:
        return matches[0][0]

    if len(matches) > 1:
        print("ERROR: multiple matching devices:", matches)
        return None

    print("ERROR: no device found")
    return None


def main():

    # Use the same IDs from your config
    VENDOR_ID = "ffff"
    MODEL_ID = "0005"

    port = find_device_port(VENDOR_ID, MODEL_ID)

    if not port:
        sys.exit(1)

    print("Connecting to", port)

    ser = serial.Serial(
        port=port,
        baudrate=115200,
        bytesize=serial.EIGHTBITS,
        parity=serial.PARITY_NONE,
        stopbits=serial.STOPBITS_ONE,
        timeout=2
    )

    time.sleep(2)

    # Stop any existing streaming
    ser.write(b"X0000!\r\n")
    time.sleep(0.5)

    # Enable Pro mode 6
    ser.write(b"X0006!\r\n")
    time.sleep(1)

    ser.reset_input_buffer()

    print("Streaming mode 6. Press Ctrl+C to stop.\n")

    try:
        while True:
            line = ser.readline()
            if not line:
                continue

            decoded = line.decode("utf-8", errors="ignore").strip()

            print(decoded)

    except KeyboardInterrupt:
        pass

    ser.close()


if __name__ == "__main__":
    main()