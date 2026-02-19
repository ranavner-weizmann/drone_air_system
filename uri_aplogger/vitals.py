# vitals_exporter.py
"""
Real-time vitals exporter - Extracts and saves only the most critical data columns from all sensors
"""

import csv
import time
import json
import glob
import os
import logging
from datetime import datetime
from pathlib import Path
import threading
import argparse

class VitalsExporter:
    def __init__(self, config_file='sensor_config.json', output_interval=1.0):
        """
        Initialize the vitals exporter
        
        Args:
            config_file: Path to sensor configuration file
            output_interval: How often to output vitals data (seconds)
        """
        self.config = self.load_config(config_file)
        self.output_interval = output_interval
        self.running = True
        
        # Define vital columns for each sensor (most important data only)
        self.vital_columns = {
            'imet': {
                'columns': ['Timestamp', 'temp', 'pressure', 'altitude', 'latitude', 'longitude'],
                'aliases': ['iMet_Temp_C', 'iMet_Pressure_hPa', 'iMet_Altitude_m', 'iMet_Lat', 'iMet_Lon']
            },
            'pom': {
                'columns': ['Timestamp', 'Ozone_ppb', 'Cell_Temperature_K'],
                'aliases': ['POM_Ozone_ppb', 'POM_CellTemp_K']
            },
            'trisonica': {
                'columns': ['Timestamp', 'Wind_Speed', 'Wind_Direction', 'Temperature'],
                'aliases': ['Wind_Speed_m_s', 'Wind_Direction_deg', 'Air_Temp_C']
            },
            'spectro': {
                'columns': ['Timestamp', 'peak_wavelength', 'max_intensity'],
                'aliases': ['Spectro_Peak_nm', 'Spectro_MaxIntensity']
            },
            'partector2pro': {
                'columns': ['Timestamp', 'LDSA_um2_cm3', 'diameter_nm', 'number_1_cm3'],
                'aliases': ['Particle_LDSA', 'Particle_Size_nm', 'Particle_Count_#/cm3']
            }
        }
        
        # File tracking for each sensor
        self.sensor_files = {}
        self.sensor_positions = {}
        self.latest_data = {}
        self.latest_timestamps = {}
        
        # Set up vitals output file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.vitals_file = f'output/vitals_summary_{timestamp}.csv'
        
        self.setup_logging()
        self.initialize_sensor_tracking()
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - Vitals - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('vitals_exporter.log')
            ]
        )
        self.logger = logging.getLogger('VitalsExporter')
        
    def load_config(self, config_file):
        """Load sensor configuration"""
        try:
            with open(config_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading config: {e}")
            return {'sensors': {}}
    
    def initialize_sensor_tracking(self):
        """Initialize tracking for all enabled sensors"""
        for sensor_name, sensor_config in self.config['sensors'].items():
            if sensor_config.get('enabled', True) and sensor_name in self.vital_columns:
                # Determine file pattern based on sensor type
                if sensor_name == 'spectro':
                    pattern = sensor_config.get('output_file_pattern', 'output/spectro/spectro_data_*.csv')
                else:
                    pattern = f'output/{sensor_name}/{sensor_name}_data_*.csv'
                
                self.sensor_files[sensor_name] = pattern
                self.latest_data[sensor_name] = {}
                self.latest_timestamps[sensor_name] = None
                self.logger.info(f"Tracking vitals from {sensor_name} with pattern: {pattern}")
    
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
                    # Read header
                    if position == 0:
                        header = next(reader, None)
                        if header:
                            self.logger.debug(f"{sensor_name} header: {header}")
                    
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
    
    def extract_vitals(self, sensor_name, row):
        """Extract only the vital columns from a sensor data row"""
        if sensor_name not in self.vital_columns:
            return None
        
        config = self.config['sensors'][sensor_name]
        column_names = config.get('column_names', [])
        vital_def = self.vital_columns[sensor_name]
        vital_cols = vital_def['columns']
        
        # Find indices of vital columns
        vital_indices = []
        for col in vital_cols:
            try:
                idx = column_names.index(col)
                vital_indices.append(idx)
            except ValueError:
                # Column not found in this sensor's data
                self.logger.warning(f"Vital column '{col}' not found in {sensor_name} data")
                continue
        
        # Extract values for vital columns
        vitals = {}
        timestamp = None
        
        for i, col_idx in enumerate(vital_indices):
            if col_idx < len(row):
                col_name = vital_cols[i]
                value = row[col_idx]
                
                if col_name == 'Timestamp':
                    timestamp = value
                else:
                    # Use alias if available, otherwise use original column name
                    alias_idx = i - 1 if col_name == 'Timestamp' else i
                    if 'aliases' in vital_def and alias_idx < len(vital_def['aliases']):
                        display_name = vital_def['aliases'][alias_idx]
                    else:
                        display_name = f"{sensor_name}_{col_name}"
                    
                    vitals[display_name] = value
        
        return timestamp, vitals
    
    def update_sensor_data(self):
        """Update vitals data from all sensors"""
        for sensor_name, pattern in self.sensor_files.items():
            try:
                # Find the latest file
                latest_file = self.find_latest_file(pattern)
                if not latest_file:
                    continue
                
                # Read new data
                new_lines = self.read_new_lines(sensor_name, latest_file)
                
                if new_lines:
                    # Process each new line
                    for row in new_lines:
                        timestamp, vitals = self.extract_vitals(sensor_name, row)
                        
                        if timestamp and vitals:
                            # Update latest data for this sensor
                            self.latest_data[sensor_name] = vitals
                            self.latest_timestamps[sensor_name] = timestamp
                            
                            # Log first data point
                            if len(new_lines) == 1:
                                self.logger.debug(f"Updated vitals from {sensor_name}: {list(vitals.keys())}")
                
            except Exception as e:
                self.logger.error(f"Error updating {sensor_name}: {e}")
    
    def get_vitals_row(self):
        """Create a vitals row from the latest data of all sensors"""
        # Use current time as primary timestamp
        row = {
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'Export_Time': datetime.now().isoformat()
        }
        
        # Add vitals from each sensor
        for sensor_name, vitals in self.latest_data.items():
            if vitals:
                row.update(vitals)
        
        # Add sensor-specific timestamps
        for sensor_name, timestamp in self.latest_timestamps.items():
            if timestamp:
                row[f'{sensor_name}_Timestamp'] = timestamp
        
        return row
    
    def get_vitals_headers(self):
        """Generate headers for the vitals CSV file"""
        headers = ['Timestamp', 'Export_Time']
        
        # Add sensor timestamp headers
        for sensor_name in self.sensor_files.keys():
            headers.append(f'{sensor_name}_Timestamp')
        
        # Add all vital data headers
        for sensor_name, vital_def in self.vital_columns.items():
            if sensor_name in self.sensor_files:
                # Skip timestamp column (already added above)
                vital_cols = vital_def['columns'][1:]  # Exclude 'Timestamp'
                
                for i, col in enumerate(vital_cols):
                    # Use alias if available
                    if 'aliases' in vital_def and i < len(vital_def['aliases']):
                        headers.append(vital_def['aliases'][i])
                    else:
                        headers.append(f"{sensor_name}_{col}")
        
        return headers
    
    def write_vitals_data(self):
        """Write vitals data to CSV file"""
        # Ensure output directory exists
        Path('output').mkdir(exist_ok=True)
        
        # Initialize CSV file if it doesn't exist
        if not os.path.exists(self.vitals_file):
            with open(self.vitals_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.get_vitals_headers())
                writer.writeheader()
            self.logger.info(f"Created vitals output file: {self.vitals_file}")
            self.logger.info(f"Vitals columns: {', '.join(self.get_vitals_headers())}")
        
        # Open file in append mode
        with open(self.vitals_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.get_vitals_headers())
            
            last_status_time = time.time()
            
            while self.running:
                try:
                    # Update data from all sensors
                    self.update_sensor_data()
                    
                    # Create and write vitals row
                    vitals_row = self.get_vitals_row()
                    writer.writerow(vitals_row)
                    f.flush()  # Ensure data is written immediately
                    
                    # Log status every 30 seconds
                    current_time = time.time()
                    if current_time - last_status_time >= 30:
                        active_sensors = [name for name, data in self.latest_data.items() if data]
                        self.logger.info(f"Exporting vitals from {len(active_sensors)} sensors: {', '.join(active_sensors)}")
                        last_status_time = current_time
                    
                    time.sleep(self.output_interval)
                    
                except KeyboardInterrupt:
                    self.logger.info("Keyboard interrupt received")
                    self.running = False
                    break
                except Exception as e:
                    self.logger.error(f"Error in vitals export loop: {e}")
                    time.sleep(1)
    
    def run(self):
        """Run the vitals exporter"""
        self.logger.info("Starting vitals data exporter")
        self.logger.info(f"Output file: {self.vitals_file}")
        self.logger.info(f"Output interval: {self.output_interval}s")
        self.logger.info("Tracking vital parameters:")
        
        for sensor_name, vital_def in self.vital_columns.items():
            if sensor_name in self.sensor_files:
                vital_list = vital_def['columns'][1:]  # Exclude 'Timestamp'
                self.logger.info(f"  {sensor_name}: {', '.join(vital_list)}")
        
        # Start the main write loop
        self.write_vitals_data()
        
        self.logger.info("Vitals exporter stopped")

def main():
    parser = argparse.ArgumentParser(description='Real-time vitals exporter for critical sensor data')
    parser.add_argument('--config', default='sensor_config.json', help='Path to sensor configuration file')
    parser.add_argument('--interval', type=float, default=1.0, help='Export interval in seconds')
    parser.add_argument('--output', help='Custom output file path')
    
    args = parser.parse_args()
    
    exporter = VitalsExporter(config_file=args.config, output_interval=args.interval)
    
    if args.output:
        exporter.vitals_file = args.output
    
    try:
        exporter.run()
    except KeyboardInterrupt:
        exporter.logger.info("Shutting down...")
    except Exception as e:
        exporter.logger.error(f"Fatal error: {e}")

if __name__ == "__main__":
    main()