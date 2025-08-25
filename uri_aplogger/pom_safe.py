
import serial
import csv
import signal
import sys
import time
from datetime import datetime
import logging

try:
    import pyudev
except ImportError:
    print("ERROR: pyudev not installed. Install with: pip install pyudev")
    sys.exit(1)

class OptimizedPOMReader:
    IDENTIFIERS = {
        "ID_VENDOR_ID": "067b",
        "ID_MODEL_ID": "23a3"
    }
    
    PV_NAMES = [
        'Timestamp', 'Log_Number', 'Ozone_ppb', 'Cell_Temperature_K', 'Cell_Pressure_torr',
        'Photodiode_Voltage_V', 'Power_Supply_V', 'Latitude', 'Longitude',
        'Altitude_m', 'GPS_Quality', 'Date', 'Time'
    ]

    def __init__(self):
        self.running = True
        self.serial = None
        self.consecutive_failures = 0
        self.max_failures = 5
        self.reconnect_delay = 5
        self.header_lines_skipped = 0
        self.max_header_lines = 10
        
        self.setup_logging()
        signal.signal(signal.SIGINT, self.signal_handler)

    def setup_logging(self):
        logging.basicConfig(
            format="%(asctime)s POM: %(message)s",
            level=logging.INFO,
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger('POM')

    def signal_handler(self, sig, frame):
        self.logger.info("Stopping POM data collection...")
        self.running = False

    def find_pom_port(self):
        """Find POM port using device identifiers"""
        try:
            context = pyudev.Context()
            for device in context.list_devices(subsystem='tty'):
                if (device.get('ID_VENDOR_ID') == self.IDENTIFIERS["ID_VENDOR_ID"] and 
                    device.get('ID_MODEL_ID') == self.IDENTIFIERS["ID_MODEL_ID"]):
                    self.logger.info(f"Found POM at: {device.device_node}")
                    return device.device_node
            self.logger.warning("POM device not found")
            return None
        except Exception as e:
            self.logger.error(f"Error finding POM port: {e}")
            return None

    def init_serial(self):
        """Initialize serial connection to POM"""
        if self.serial and self.serial.is_open:
            self.serial.close()
            
        port = self.find_pom_port()
        if not port:
            return False
            
        try:
            self.serial = serial.Serial(
                port=port,
                baudrate=19200,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=1
            )
            
            time.sleep(2)  # Device initialization
            self.logger.info(f"Connected to POM on {port}")
            self.consecutive_failures = 0
            self.header_lines_skipped = 0
            return True
            
        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            self.consecutive_failures += 1
            return False

    def read_pom_data(self):
        """Read and parse data from POM"""
        if not self.serial or not self.serial.is_open:
            if not self.init_serial():
                return None

        try:
            if self.serial.in_waiting > 0:
                line = self.serial.readline()
                decoded = line.decode('utf-8').strip()
                
                if not decoded:
                    return None
                    
                # Skip header lines
                if "Personal Ozone Monitor" in decoded or decoded.isdigit():
                    self.header_lines_skipped += 1
                    if self.header_lines_skipped <= 3:
                        self.logger.info(f"Skipping header: {decoded}")
                    return None
                
                self.logger.info(f"Raw data: {decoded}")
                return self.parse_pom_data(decoded)
            return None
                
        except Exception as e:
            self.logger.error(f"Read error: {e}")
            self.consecutive_failures += 1
            self.serial = None
            return None

    def parse_pom_data(self, data):
        """Parse POM data string into structured format"""
        try:
            data_list = data.split(',')
            
            # Handle both data formats (11 fields = real-time, 12 fields = logged)
            if len(data_list) == 11:
                data_list = [''] + data_list  # Add empty log number
            elif len(data_list) != 12:
                self.logger.warning(f"Unexpected data format: {len(data_list)} fields")
                return None
            
            # Add timestamp
            data_list.insert(0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            return data_list
            
        except Exception as e:
            self.logger.error(f"Parse error: {e}")
            return None

    def run(self):
        """Main data collection loop"""
        self.logger.info("Starting POM data collection")
        
        with open('output/pom_data.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(self.PV_NAMES)
            
            if not self.init_serial():
                self.logger.error("Failed initial connection. Will retry...")

            last_reconnect = 0
            
            while self.running:
                current_time = time.time()
                
                # Reconnection logic
                if not self.serial and current_time - last_reconnect >= self.reconnect_delay:
                    self.logger.info("Attempting to reconnect...")
                    if self.init_serial():
                        self.logger.info("Reconnection successful!")
                    last_reconnect = current_time

                # Data reading
                if self.serial and self.serial.is_open:
                    parsed_data = self.read_pom_data()
                    if parsed_data:
                        writer.writerow(parsed_data)
                        csvfile.flush()
                        self.logger.info(f"Written: Ozone: {parsed_data[1]} ppb, Temp: {parsed_data[2]} K")
                        self.consecutive_failures = 0

                # Failure handling
                if self.consecutive_failures >= self.max_failures:
                    self.logger.warning(f"Multiple failures, reconnecting in {self.reconnect_delay}s")
                    self.serial = None
                    self.consecutive_failures = 0
                    last_reconnect = current_time
                
                time.sleep(0.1)
        
        if self.serial:
            self.serial.close()
        self.logger.info("POM data collection stopped")

def main():
    reader = OptimizedPOMReader()
    reader.run()

if __name__ == "__main__":
    main()