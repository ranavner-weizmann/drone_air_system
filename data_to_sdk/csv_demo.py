'''
This is a simple script that creates a real-time csv to test the PSDK.
'''


import csv
import time
import random
import os  # Added this library to find the path

filename = "vitals.csv"
headers = ["Sensor_A", "Sensor_B", "Sensor_C", "Sensor_D", "Sensor_E"]

# Get the absolute path of where the file will be created
full_path = os.path.abspath(filename)

print(f"--- STARTING ---")
print(f"Saving file to: {full_path}")  # <--- LOOK AT THIS LINE IN YOUR OUTPUT
print(f"----------------")

# 1. Create file and write headers
with open(filename, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(headers)

print("Headers written. Press Ctrl+C to stop.")

try:
    while True:
        # Generate random data
        row_data = [random.randint(0, 100) for _ in range(5)]
        
        # Append data
        with open(filename, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(row_data)
        
        print(f"Added row: {row_data}")
        time.sleep(1)

except KeyboardInterrupt:
    print(f"\nStopped. You can find your file here:\n{full_path}")