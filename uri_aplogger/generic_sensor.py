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
        self.output_file = config.get('output_file', f'output/{name.lower()}/{name.lower()}_data_{timestamp}.csv')
        self.setup_logging()
        print(f"{self.logger.name} logger initialized.")
        print(f"{self.logger.info}")
        signal.signal(signal.SIGINT, self.signal_handler)
        
        # Create output directory
        Path(f'output/{self.name}').mkdir(parents=True, exist_ok=True)

    def setup_logging(self):
        """Setup logging based on config verbosity."""
        log_cfg = (self.config or {}).get("logging", {})
        verbosity = int(log_cfg.get("verbosity", 2))
        to_console = bool(log_cfg.get("console", True))
        to_file = bool(log_cfg.get("file", True))

        # Map verbosity -> logging level
        level_map = {
            0: logging.CRITICAL,  # we'll disable below anyway
            1: logging.WARNING,
            2: logging.INFO,
            3: logging.DEBUG,
        }
        level = level_map.get(verbosity, logging.INFO)

        # Create logger unique to this sensor
        self.logger = logging.getLogger(self.name)
        self.logger.handlers.clear()
        self.logger.propagate = False  # don't double-log via root logger

        if verbosity <= 0:
            # Completely silence this sensor's logger
            self.logger.addHandler(logging.NullHandler())
            self.logger.setLevel(logging.CRITICAL)
            return

        self.logger.setLevel(level)

        formatter = logging.Formatter(f"%(asctime)s {self.name}: %(message)s")

        # Ensure per-sensor output directory exists (important!)
        Path(f"output/{self.name}").mkdir(parents=True, exist_ok=True)

        if to_console:
            sh = logging.StreamHandler()
            sh.setLevel(level)
            sh.setFormatter(formatter)
            self.logger.addHandler(sh)

        if to_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            fh = logging.FileHandler(f"output/{self.name}/{self.name}_log_{timestamp}.log")
            fh.setLevel(level)
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)
        

    def signal_handler(self, sig, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Stopping {self.name} data collection...")
        self.running = False

    def find_device_port(self):
        """Find device port using vendor/model IDs and optional serial_short."""
        try:
            ids = self.config.get("identifiers")
            if not isinstance(ids, dict):
                self.logger.error(f"{self.name}: Missing or invalid 'identifiers' in config")
                return None

            target_vendor = str(ids.get("vendor_id", "")).lower()
            target_model = str(ids.get("model_id", "")).lower()
            target_serial = ids.get("serial_short")
            target_serial = str(target_serial).lower() if target_serial else None

            if not target_vendor or not target_model:
                self.logger.error(f"{self.name}: identifiers must include vendor_id and model_id")
                return None

            context = pyudev.Context()

            matches = []
            for device in context.list_devices(subsystem="tty"):
                vendor_id = device.get("ID_VENDOR_ID", "").lower()
                model_id = device.get("ID_MODEL_ID", "").lower()
                serial_s = device.get("ID_SERIAL_SHORT", "").lower()
                node = device.device_node

                if vendor_id == target_vendor and model_id == target_model:
                    # If serial_short is specified, enforce it
                    if target_serial and serial_s != target_serial:
                        continue
                    matches.append((node, serial_s))

            if len(matches) == 1:
                node, serial_s = matches[0]
                self.logger.info(f"Found {self.name} at: {node} (serial={serial_s})")
                return node

            if len(matches) > 1:
                self.logger.error(
                    f"{self.name}: Multiple devices match {target_vendor}:{target_model} "
                    f"{'(no serial_short specified)' if not target_serial else ''} -> {matches}"
                )
                return None

            # No matches
            if target_serial:
                self.logger.error(
                    f"{self.name}: No device found for {target_vendor}:{target_model} serial_short={target_serial}"
                )
            else:
                self.logger.error(
                    f"{self.name}: No device found for {target_vendor}:{target_model}"
                )
            return None

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

    def read_serial_data(self):
        """Read data from serial port - Common for all sensors"""
        if not self.serial_conn or not self.serial_conn.is_open:
            if not self.init_serial():
                return None

        try:
            # For fast data producers like TriSonica, read ALL available data
            data_chunks = []
            
            # Read multiple lines if available
            while self.serial_conn.in_waiting > 0:
                line = self.serial_conn.readline()
                if line:
                    decoded = line.decode('utf-8', errors='ignore').strip()
                    if decoded:
                        data_chunks.append(decoded)
                        self.logger.debug(f"Raw data chunk: {decoded}")
            
            if data_chunks:
                # For TriSonica, return the most recent complete line
                # This prevents buffer overflow
                return data_chunks[-1]
            
            # Fallback for other sensors
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
                    try:
                        raw_data = self.read_serial_data()
                        if raw_data:
                            parsed_data = self.parse_data(raw_data)
                            if self.is_valid_data(parsed_data):
                                self.write_data(writer, parsed_data)
                                csvfile.flush()
                                self.consecutive_failures = 0
                                data_count += 1
                                last_data_time = current_time
                    except Exception as e:
                        self.logger.error(f"Processing error: {e}")
                        self.consecutive_failures += 1
                    
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