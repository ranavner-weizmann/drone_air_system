import csv
import time
import os
import glob
from datetime import datetime

def check_sensor(sensor_name, timeout=5):
    """Check if sensor is alive based on file modification time."""
    pattern = f'output/{sensor_name}/{sensor_name}_data_*.csv'
    files = glob.glob(pattern)
    
    if not files:
        return 'X'
    
    latest_file = max(files, key=os.path.getmtime)
    file_age = time.time() - os.path.getmtime(latest_file)
    
    return 'V' if file_age < timeout else 'X'

def main():
    # Sensor list and timeouts (POM gets 15s, others 6s)
    sensors = {
        'imet': 6,
        'pom': 15,
        'trisonica': 6,
        'spectro': 6,
        'partector2pro': 8
    }
    
    # Create output file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'output/minimal_status_{timestamp}.csv'
    
    os.makedirs('output', exist_ok=True)
    
    # Write header
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp'] + list(sensors.keys()))
    
    # Main loop
    try:
        while True:
            # Get current timestamp
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Check each sensor
            status_row = [current_time]
            for sensor, timeout in sensors.items():
                status = check_sensor(sensor, timeout)
                status_row.append(status)
            
            # Write to file
            with open(output_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(status_row)
            
            # Wait 1 second
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopped.")

if __name__ == "__main__":
    main()