import serial
import time

# CHANGE THIS if needed
PORT = "/dev/ttyUSB0"      # or "/dev/aeth"
BAUDRATE = 1000000
TIMEOUT = 1

def main():
    print(f"Opening serial port {PORT} @ {BAUDRATE} baud...")
    ser = serial.Serial(
        port=PORT,
        baudrate=BAUDRATE,
        bytesize=serial.EIGHTBITS,
        parity=serial.PARITY_NONE,
        stopbits=serial.STOPBITS_ONE,
        timeout=TIMEOUT
    )

    time.sleep(2)
    print("Connected. Listening...\n")

    # Try a status command
    print(">>> sending 'cs'")
    ser.write(b"cs\r")
    time.sleep(0.5)

    # Read whatever comes back
    for _ in range(5):
        line = ser.readline()
        if line:
            print("<<<", line.decode(errors="replace").strip())

    # Try data request command
    print("\n>>> sending 'dr'")
    ser.write(b"dr\r")
    time.sleep(0.5)

    for _ in range(10):
        line = ser.readline()
        if line:
            print("<<<", line.decode(errors="replace").strip())

    print("\nNow dumping raw incoming data for 10 seconds...\n")
    end_time = time.time() + 10

    while time.time() < end_time:
        line = ser.readline()
        if line:
            print("<<<", line.decode(errors="replace").strip())

    ser.close()
    print("\nSerial connection closed.")

if __name__ == "__main__":
    main()
