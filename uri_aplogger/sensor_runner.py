# sensor_runner.py
"""
Universal sensor runner - fixed version
"""

import sys
import json
from sensor_implementations import create_sensor

def main():
    if len(sys.argv) < 2:
        print("Usage: python sensor_runner.py <sensor_name>")
        print("Available sensors: imet, pom, trisonica, partector2pro")
        sys.exit(1)
    
    sensor_name = sys.argv[1].lower()  # Use lowercase for consistency
    
    # Load configuration
    try:
        with open('sensor_config.json', 'r') as f:
            config = json.load(f)
    except Exception as e:
        print(f"Error loading config: {e}")
        sys.exit(1)
    
    if sensor_name not in config['sensors']:
        print(f"Unknown sensor: {sensor_name}")
        print(f"Available: {list(config['sensors'].keys())}")
        sys.exit(1)
    
    global_logging = config.get("logging", {})
    sensor_config = config["sensors"][sensor_name]

    # Merge: per-sensor overrides global
    merged_logging = {**global_logging, **sensor_config.get("logging", {})}
    sensor_config["logging"] = merged_logging
    
    # Create and run sensor
    try:
        sensor = create_sensor(sensor_config['type'], sensor_name, sensor_config)
        sensor.run()
    except Exception as e:
        print(f"Error running sensor {sensor_name}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()