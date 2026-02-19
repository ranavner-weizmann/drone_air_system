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
                'columns': ['temp', 'pressure', 'rel_hum'],
                'aliases': ['iMet_Temp_C', 'iMet_Pressure_hPa', 'iMet_Relative_Humidity']
            },
            'pom': {
                'columns': ['Ozone_ppb'],
                'aliases': ['POM_Ozone_ppb']
            },
            'trisonica': {
                'columns': ['U_Vector', 'V_Vector', 'W_Vector'],
                'aliases': ['Tri_Wind_U', 'Tri_Wind_V', 'Tri_Wind_W']
            },
            'spectro': {
                'columns': ['peak_wavelength', 'max_intensity'],
                'aliases': ['Spectro_Peak_nm', 'Spectro_MaxIntensity']
            },
            'partector2pro': {
                'columns': ['number_1_cm3', 'battery_voltage_V'],
                'aliases': ['Partector_Particle_Count_#/cm3', 'Partector_Battery_Voltage_V']
            },
            'miniaeth': {
                'columns': ['blue_BCc'],
                'aliases': ['Aeth_Blue_BlackCarbon']
            },
            'pops': {
                'columns': ['b4', 'b8', 'b15'],
                'aliases': ['POPS_Bin_4', 'POPS_Bin_8', 'POPS_Bin_15']
            },
            'cavity': {
                'columns': ['TEC_ActualOutputCurrent', 'TEC_ActualOutputVoltage', 'TEC_TargetObjectTemperature', 'LDD_ActualOutputCurrent', 'TEC_ObjectTemperature', 'temp_c', 'humidity_pct', 'pressure_mb', 'pump_rpm'],
                'aliases': ['TEC_Output_Current', 'TEC_Output_Voltage', 'TEC_Target_Temperature', 'LDD_Output_Current', 'TEC_Object_Temperature', 'Inline_Temp', 'Inline_Relative_Humidity', 'Inline_Pressure_mbar', 'Pump_RPM']
            }
        }
        
        # File tracking for each sensor
        self.sensor_files = {}
        self.sensor_positions = {}
        self.latest_data = {}
        
        # Set up vitals output file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.vitals_file = f'output/vitals_summary_{timestamp}.csv'
        self.vitals_live = f'../data_to_sdk/vitals.csv'
        
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

        vitals = {}

        for i, col in enumerate(vital_cols):
            try:
                col_idx = column_names.index(col)
            except ValueError:
                self.logger.warning(f"Vital column '{col}' not found in {sensor_name} data")
                continue

            if col_idx < len(row):
                value = row[col_idx]

                # Use alias if available
                if 'aliases' in vital_def and i < len(vital_def['aliases']):
                    display_name = vital_def['aliases'][i]
                else:
                    display_name = f"{sensor_name}_{col}"

                vitals[display_name] = value

        return vitals
    
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
                        vitals = self.extract_vitals(sensor_name, row)
                        
                        if vitals:
                            # Update latest data for this sensor
                            self.latest_data[sensor_name] = vitals
                            
                            # Log first data point
                            if len(new_lines) == 1:
                                self.logger.debug(f"Updated vitals from {sensor_name}: {list(vitals.keys())}")
                
            except Exception as e:
                self.logger.error(f"Error updating {sensor_name}: {e}")
    
    def get_vitals_row(self):
        """Create a vitals row from the latest data of all sensors"""
        headers = self.get_vitals_headers()

        row = {
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        # Initialize all expected headers with empty values
        for header in headers:
            if header != "Timestamp":
                row[header] = ""

        # Fill in available vitals
        for vitals in self.latest_data.values():
            if vitals:
                for key, value in vitals.items():
                    if key in row:
                        row[key] = value

        return row
    
    def get_vitals_headers(self):
        """Generate headers for the vitals CSV file"""
        headers = ['Timestamp']

        for sensor_name, vital_def in self.vital_columns.items():
            if sensor_name in self.sensor_files:
                vital_cols = vital_def['columns']

                for i, col in enumerate(vital_cols):
                    if 'aliases' in vital_def and i < len(vital_def['aliases']):
                        headers.append(vital_def['aliases'][i])
                    else:
                        headers.append(f"{sensor_name}_{col}")

        return headers
    
    def write_vitals_data(self):
        """Write vitals data to CSV files"""
        Path('output').mkdir(exist_ok=True)
        Path('./data_to_sdk').mkdir(exist_ok=True)

        headers = self.get_vitals_headers()

        # Initialize files if needed
        if not os.path.exists(self.vitals_file):
            with open(self.vitals_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()

        if not os.path.exists(self.vitals_live):
            with open(self.vitals_live, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()

        # Open BOTH files at once
        with open(self.vitals_file, 'a', newline='') as f_main, \
            open(self.vitals_live, 'w', newline='') as f_live:

            writer_main = csv.DictWriter(f_main, fieldnames=headers)
            writer_live = csv.DictWriter(f_live, fieldnames=headers)

            while self.running:
                try:
                    self.update_sensor_data()
                    vitals_row = self.get_vitals_row()

                    # Append to historical file
                    writer_main.writerow(vitals_row)
                    f_main.flush()

                    # Overwrite live file with latest row only
                    f_live.seek(0)
                    writer_live.writeheader()
                    writer_live.writerow(vitals_row)
                    f_live.truncate()
                    f_live.flush()

                    time.sleep(self.output_interval)

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
                vital_list = vital_def['columns']
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