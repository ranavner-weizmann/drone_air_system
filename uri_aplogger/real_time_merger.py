# real_time_merger.py
"""
Real-time data merger that continuously monitors and merges the latest data from all sensors
"""

import csv
import time
import json
import glob
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
import threading
from collections import defaultdict

class RealTimeMerger:
    def __init__(self, config_file='sensor_config.json', output_interval=1.0):
        """
        Initialize the real-time merger
        
        Args:
            config_file: Path to sensor configuration file
            output_interval: How often to output merged data (seconds)
        """
        self.config = self.load_config(config_file)
        self.output_interval = output_interval
        self.running = True
        
        # File tracking for each sensor
        self.sensor_files = {}
        self.sensor_handles = {}
        self.sensor_positions = {}
        self.latest_data = {}
        self.last_read_times = {}
        self.has_new_data = {}
        
        # Set up merged output
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.merged_file = f'output/merged_data_{timestamp}.csv'
        
        self.setup_logging()
        self.initialize_sensor_tracking()
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - Merger - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('merger.log')
            ]
        )
        self.logger = logging.getLogger('RealTimeMerger')
        
    def load_config(self, config_file):
        """Load sensor configuration"""
        try:
            with open(config_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading config: {e}")
            return {'sensors': {}}
    
    def initialize_sensor_tracking(self):
        """Initialize tracking for all enabled sensors"""
        for sensor_name, sensor_config in self.config['sensors'].items():
            if sensor_config.get('enabled', True):
                # Determine file pattern based on sensor type
                if sensor_name == 'spectro':
                    pattern = sensor_config.get('output_file_pattern', 'output/spectro_data_*.csv')
                else:
                    pattern = f'output/{sensor_name}_data_*.csv'
                
                self.sensor_files[sensor_name] = pattern
                self.latest_data[sensor_name] = None
                self.has_new_data[sensor_name] = False  # Initialize as False
                self.last_read_times[sensor_name] = datetime.now()
                self.logger.info(f"Tracking {sensor_name} with pattern: {pattern}")
    
    def find_latest_file(self, pattern):
        """Find the latest file matching the pattern"""
        try:
            files = glob.glob(pattern)
            if not files:
                return None
            # Get the most recently modified file
            latest_file = max(files, key=os.path.getmtime)
            return latest_file
        except Exception as e:
            self.logger.error(f"Error finding files for pattern {pattern}: {e}")
            return None
    
    def read_new_lines(self, sensor_name, file_path):
        """Read new lines from a sensor file since last read"""
        new_data = []
        
        try:
            # If we haven't read this file before, start from the beginning
            if sensor_name not in self.sensor_positions or self.sensor_positions[sensor_name].get('file') != file_path:
                self.sensor_positions[sensor_name] = {'file': file_path, 'position': 0}
            
            position = self.sensor_positions[sensor_name]['position']
            
            with open(file_path, 'r') as f:
                # Move to last known position
                f.seek(0, 2)  # Go to end to check file size
                file_size = f.tell()
                
                if position > file_size:
                    # File was probably rotated/truncated, start from beginning
                    position = 0
                
                f.seek(position)
                
                reader = csv.reader(f)
                try:
                    # Skip header if we're at position 0
                    if position == 0:
                        next(reader, None)
                    
                    for row in reader:
                        if row:  # Skip empty rows
                            new_data.append(row)
                    
                    # Update position
                    self.sensor_positions[sensor_name]['position'] = f.tell()
                    
                except StopIteration:
                    pass
                    
        except Exception as e:
            self.logger.error(f"Error reading {sensor_name} file: {e}")
        
        return new_data
    
    def update_sensor_data(self):
        """Update data from all sensors"""
        for sensor_name, pattern in self.sensor_files.items():
            try:
                # Find the latest file
                latest_file = self.find_latest_file(pattern)
                if not latest_file:
                    continue
                
                # Read new data
                new_lines = self.read_new_lines(sensor_name, latest_file)
                
                if new_lines:
                    # Store the latest data point
                    self.latest_data[sensor_name] = new_lines[-1]
                    self.last_read_times[sensor_name] = datetime.now()
                    
                    # Log if we got multiple new lines
                    if len(new_lines) > 1:
                        self.logger.debug(f"Got {len(new_lines)} new lines from {sensor_name}")
                        
                    # IMPORTANT: Mark that this sensor has new data for this cycle
                    self.has_new_data[sensor_name] = True
                else:
                    # No new data for this sensor
                    self.has_new_data[sensor_name] = False
                        
            except Exception as e:
                self.logger.error(f"Error updating {sensor_name}: {e}")
                self.has_new_data[sensor_name] = False

    def create_merged_row(self):
        """Create a merged row from the latest data of all sensors"""
        merged_row = {
            'merge_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Add data from each sensor ONLY if it has new data
        for sensor_name, data in self.latest_data.items():
            # Check if this sensor has new data for this cycle
            if data and self.has_new_data.get(sensor_name, False):
                # Add sensor name prefix to each column
                config = self.config['sensors'][sensor_name]
                column_names = config.get('column_names', [])
                
                # Create dictionary of column name -> value
                if len(data) == len(column_names):
                    for i, col_name in enumerate(column_names):
                        if col_name.lower() == 'timestamp':
                            merged_row[f'{sensor_name}_{col_name}'] = data[i]
                        else:
                            merged_row[f'{sensor_name}_{col_name}'] = data[i]
                else:
                    # Fallback: just use the data as-is
                    for i, value in enumerate(data):
                        merged_row[f'{sensor_name}_col{i}'] = value
            else:
                # No new data from this sensor - leave columns blank
                config = self.config['sensors'][sensor_name]
                column_names = config.get('column_names', [])
                
                # Add blank columns for this sensor
                for col_name in column_names:
                    merged_row[f'{sensor_name}_{col_name}'] = ''
        
        return merged_row
    
    def get_merged_headers(self):
        """Generate headers for the merged CSV file"""
        headers = ['merge_timestamp']
        
        for sensor_name, sensor_config in self.config['sensors'].items():
            if sensor_config.get('enabled', True):
                column_names = sensor_config.get('column_names', [])
                for col_name in column_names:
                    headers.append(f'{sensor_name}_{col_name}')
        
        return headers
    
    def write_merged_data(self):
        """Write merged data to CSV file"""
        # Ensure output directory exists
        Path('output').mkdir(exist_ok=True)
        
        # Initialize CSV file if it doesn't exist
        if not os.path.exists(self.merged_file):
            with open(self.merged_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.get_merged_headers())
                writer.writeheader()
            self.logger.info(f"Created merged output file: {self.merged_file}")
        
        # Open file in append mode
        with open(self.merged_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.get_merged_headers())
            
            while self.running:
                try:
                    # Update data from all sensors
                    self.update_sensor_data()
                    
                    # Create and write merged row
                    merged_row = self.create_merged_row()
                    
                    # Only write if we have data from at least one sensor
                    if any(self.latest_data.values()):
                        writer.writerow(merged_row)
                        f.flush()  # Ensure data is written immediately
                        
                        # Log periodically
                        if time.time() % 10 < 0.1:  # Every ~10 seconds
                            active_sensors = [name for name, data in self.latest_data.items() if data]
                            self.logger.info(f"Merged data from {len(active_sensors)} sensors: {', '.join(active_sensors)}")
                    
                    time.sleep(self.output_interval)
                    
                except KeyboardInterrupt:
                    self.logger.info("Keyboard interrupt received")
                    self.running = False
                    break
                except Exception as e:
                    self.logger.error(f"Error in write loop: {e}")
                    time.sleep(1)
    
    def monitor_sensor_health(self):
        """Monitor sensor health and log warnings for stale data"""
        while self.running:
            try:
                current_time = datetime.now()
                for sensor_name, last_read in self.last_read_times.items():
                    time_diff = (current_time - last_read).total_seconds()
                    if time_diff > 30 and self.latest_data[sensor_name] is not None:
                        self.logger.warning(f"No new data from {sensor_name} for {time_diff:.0f} seconds")
                    elif time_diff > 120:
                        self.latest_data[sensor_name] = None  # Mark as stale
                
                time.sleep(10)  # Check every 10 seconds
            except Exception as e:
                self.logger.error(f"Error in health monitor: {e}")
                time.sleep(10)
    
    def run(self):
        """Run the real-time merger"""
        self.logger.info("Starting real-time data merger")
        self.logger.info(f"Output file: {self.merged_file}")
        self.logger.info(f"Output interval: {self.output_interval}s")
        
        # Start health monitoring in a separate thread
        health_thread = threading.Thread(target=self.monitor_sensor_health, daemon=True)
        health_thread.start()
        
        # Start the main write loop
        self.write_merged_data()
        
        self.logger.info("Real-time merger stopped")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Real-time data merger for sensor data')
    parser.add_argument('--config', default='sensor_config.json', help='Path to sensor configuration file')
    parser.add_argument('--interval', type=float, default=1.0, help='Output interval in seconds')
    
    args = parser.parse_args()
    
    merger = RealTimeMerger(config_file=args.config, output_interval=args.interval)
    
    try:
        merger.run()
    except KeyboardInterrupt:
        merger.logger.info("Shutting down...")
    except Exception as e:
        merger.logger.error(f"Fatal error: {e}")

if __name__ == "__main__":
    main()
