# generic_sensor.py
"""
Generic Sensor Base Class - Fixed version
"""

import serial
import csv
import signal
import sys
import time
from datetime import datetime
import logging
from pathlib import Path

try:
    import pyudev
except ImportError:
    print("ERROR: pyudev not installed. Install with: pip install pyudev")
    sys.exit(1)

class GenericSensor:
    """Generic base class for all USB sensors"""
    
    def __init__(self, name, config):
        self.name = name
        self.config = config
        self.running = True
        self.serial_conn = None
        self.consecutive_failures = 0
        self.max_failures = config.get('max_failures', 5)
        self.reconnect_delay = config.get('reconnect_delay', 5)
        
        # Common settings with defaults
        self.baudrate = config.get('baudrate', 9600)
        self.timeout = config.get('timeout', 2)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_file = config.get('output_file', f'output/{name.lower()}_data_{timestamp}.csv')
        
        self.setup_logging()
        signal.signal(signal.SIGINT, self.signal_handler)
        
        # Create output directory
        Path('output').mkdir(exist_ok=True)

    def setup_logging(self):
        """Setup common logging format"""
        logging.basicConfig(
            format=f"%(asctime)s {self.name}: %(message)s",
            level=logging.INFO,
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger(self.name)

    def signal_handler(self, sig, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Stopping {self.name} data collection...")
        self.running = False

    def find_device_port(self):
        """Find device port using vendor/model IDs - Fixed to handle multiple formats"""
        try:
            context = pyudev.Context()
            for device in context.list_devices(subsystem='tty'):
                vendor_id = device.get('ID_VENDOR_ID', '')
                model_id = device.get('ID_MODEL_ID', '')
                
                # Try exact match first
                if (vendor_id == self.config['identifiers']['vendor_id'] and 
                    model_id == self.config['identifiers']['model_id']):
                    self.logger.info(f"Found {self.name} at: {device.device_node}")
                    return device.device_node
                    
            # If not found, try case-insensitive and partial matches
            self.logger.warning(f"{self.name} device not found with exact IDs, trying broader search...")
            return self._fallback_find_device()
            
        except Exception as e:
            self.logger.error(f"Error finding {self.name} port: {e}")
            return None

    def _fallback_find_device(self):
        """Fallback device discovery"""
        try:
            context = pyudev.Context()
            target_vendor = self.config['identifiers']['vendor_id'].lower()
            target_model = self.config['identifiers']['model_id'].lower()
            
            for device in context.list_devices(subsystem='tty'):
                vendor_id = device.get('ID_VENDOR_ID', '').lower()
                model_id = device.get('ID_MODEL_ID', '').lower()
                
                if target_vendor in vendor_id and target_model in model_id:
                    self.logger.info(f"Found {self.name} at: {device.device_node} (fallback match)")
                    return device.device_node
            
            self.logger.error(f"No {self.name} device found even with fallback search")
            return None
        except Exception as e:
            self.logger.error(f"Fallback search failed: {e}")
            return None

    def init_serial(self):
        """Initialize serial connection - Common for all sensors"""
        if self.serial_conn and self.serial_conn.is_open:
            self.serial_conn.close()
            
        port = self.find_device_port()
        if not port:
            self.logger.error(f"No port found for {self.name}")
            return False
            
        try:
            self.serial_conn = serial.Serial(
                port=port,
                baudrate=self.baudrate,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=self.timeout
            )
            
            time.sleep(2)  # Device initialization
            self.logger.info(f"Connected to {self.name} on {port}")
            self.consecutive_failures = 0
            return True
            
        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            self.consecutive_failures += 1
            return False

    # generic_sensor.py - Updated read_serial_data method
    def read_serial_data(self):
        """Read data from serial port - Common for all sensors"""
        if not self.serial_conn or not self.serial_conn.is_open:
            if not self.init_serial():
                return None

        try:
            # Read available data
            if self.serial_conn.in_waiting > 0:
                line = self.serial_conn.readline()
                if line:
                    decoded = line.decode('utf-8', errors='ignore').strip()
                    if decoded:
                        self.logger.debug(f"Raw data: {decoded}")
                        return decoded
            
            # For some sensors like Partector 2 Pro, we might need to read anyway
            # Try a non-blocking read with timeout
            try:
                line = self.serial_conn.readline()
                if line:
                    decoded = line.decode('utf-8', errors='ignore').strip()
                    if decoded:
                        self.logger.debug(f"Raw data (direct read): {decoded}")
                        return decoded
            except:
                pass
                
            return None
            
        except Exception as e:
            self.logger.error(f"Read error: {e}")
            self.consecutive_failures += 1
            if self.serial_conn:
                self.serial_conn.close()
                self.serial_conn = None
            return None

    def parse_data(self, data):
        """Parse sensor-specific data - To be implemented by child classes"""
        raise NotImplementedError("Child classes must implement parse_data")

    def is_valid_data(self, parsed_data):
        """Validate parsed data - Can be overridden by child classes"""
        return parsed_data is not None and len(parsed_data) > 1  # At least timestamp + one data field

    def write_data(self, writer, parsed_data):
        """Write data to CSV - Common for all sensors"""
        try:
            writer.writerow(parsed_data)
            
            # Log sample of written data (first few non-timestamp fields)
            sample_fields = [str(field) for field in parsed_data[1:4] if field]  # First 3 data fields
            if sample_fields:
                self.logger.info(f"Written: {', '.join(sample_fields)}")
            else:
                self.logger.debug("Written: [placeholder data]")
        except Exception as e:
            self.logger.error(f"Write error: {e}")

    def run(self):
        """Main data collection loop - Common for all sensors"""
        self.logger.info(f"Starting {self.name} data collection")
        
        # Initialize CSV file with headers
        try:
            with open(self.output_file, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(self.config['column_names'])
            self.logger.info(f"Output file: {self.output_file}")
        except Exception as e:
            self.logger.error(f"Failed to create output file: {e}")
            return

        last_reconnect = time.time()
        last_data_time = time.time()
        data_count = 0
        
        with open(self.output_file, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            while self.running:
                current_time = time.time()
                
                # Reconnection logic
                if not self.serial_conn and current_time - last_reconnect >= self.reconnect_delay:
                    self.logger.info("Attempting to reconnect...")
                    if self.init_serial():
                        self.logger.info("Reconnected successfully!")
                    last_reconnect = current_time

                # Data reading and processing
                if self.serial_conn and self.serial_conn.is_open:
                    raw_data = self.read_serial_data()
                    if raw_data:
                        parsed_data = self.parse_data(raw_data)
                        if self.is_valid_data(parsed_data):
                            self.write_data(writer, parsed_data)
                            csvfile.flush()
                            self.consecutive_failures = 0
                            data_count += 1
                            last_data_time = current_time
                
                # If no data for a while, try to read anyway (some devices don't show in_waiting properly)
                elif current_time - last_data_time > 5 and self.serial_conn:
                    try:
                        # Force a read attempt
                        raw_data = self.serial_conn.readline().decode('utf-8', errors='ignore').strip()
                        if raw_data:
                            parsed_data = self.parse_data(raw_data)
                            if self.is_valid_data(parsed_data):
                                self.write_data(writer, parsed_data)
                                csvfile.flush()
                                data_count += 1
                                last_data_time = current_time
                    except:
                        pass

                # Failure handling
                if self.consecutive_failures >= self.max_failures:
                    self.logger.warning(f"Multiple failures, reconnecting in {self.reconnect_delay}s")
                    if self.serial_conn:
                        self.serial_conn.close()
                    self.serial_conn = None
                    self.consecutive_failures = 0
                    last_reconnect = current_time
                
                time.sleep(0.1)
        
        # Cleanup
        if self.serial_conn:
            self.serial_conn.close()
        self.logger.info(f"{self.name} data collection stopped. Total data points: {data_count}")