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

class OptimizedTriSonicaReader:
    IDENTIFIERS = {
        "ID_VENDOR_ID": "10c4",
        "ID_MODEL_ID": "ea60"
    }
    
    PV_NAMES = [
        'Timestamp', 'Wind_Speed', 'Wind_Direction', 'U_Vector', 'V_Vector', 'W_Vector',
        'Temperature', 'Relative_Humidity', 'Pressure', 'Compass_Heading',
        'Pitch', 'Roll'
    ]

    def __init__(self):
        self.running = True
        self.serial = None
        self.consecutive_failures = 0
        self.max_failures = -1
        self.reconnect_delay = 5
        
        self.setup_logging()
        signal.signal(signal.SIGINT, self.signal_handler)

    def setup_logging(self):
        logging.basicConfig(
            format="%(asctime)s TriSonica: %(message)s",
            level=logging.INFO,
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger('TriSonica')

    def signal_handler(self, sig, frame):
        self.logger.info("Stopping TriSonica data collection...")
        self.running = False

    def find_trisonica_port(self):
        """Find TriSonica port using device identifiers"""
        try:
            context = pyudev.Context()
            for device in context.list_devices(subsystem='tty'):
                if (device.get('ID_VENDOR_ID') == self.IDENTIFIERS["ID_VENDOR_ID"] and 
                    device.get('ID_MODEL_ID') == self.IDENTIFIERS["ID_MODEL_ID"]):
                    self.logger.info(f"Found TriSonica at: {device.device_node}")
                    return device.device_node
            self.logger.warning("TriSonica device not connected")
            return None
        except Exception as e:
            self.logger.error(f"Error finding TriSonica port: {e}")
            return None

    def init_serial(self):
        """Initialize serial connection to TriSonica"""
        if self.serial and self.serial.is_open:
            self.serial.close()
            
        port = self.find_trisonica_port()
        if not port:
            return False
            
        try:
            self.serial = serial.Serial(
                port=port,
                baudrate=115200,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=1
            )
            
            time.sleep(2)  # Device initialization
            self.logger.info(f"Connected to TriSonica on {port}")
            self.consecutive_failures = 0
            return True
            
        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            self.consecutive_failures += 1
            return False

    def read_trisonica_data(self):
        """Read and parse data from TriSonica"""
        if not self.serial or not self.serial.is_open:
            if not self.init_serial():
                return None

        try:
            if self.serial.in_waiting > 0:
                line = self.serial.readline()
                decoded = line.decode('utf-8').strip()
                
                if not decoded:
                    return None
                
                self.logger.info(f"Raw data: {decoded}")
                return self.parse_trisonica_data(decoded)
            return None
                
        except Exception as e:
            self.logger.error(f"Read error: {e}")
            self.consecutive_failures += 1
            self.serial = None
            return None

    def parse_trisonica_data(self, data):
        """Parse TriSonica data string into structured format"""
        try:
            parts = data.strip().split()
            data_dict = {}
            
            # Parse key-value pairs
            for i in range(0, len(parts), 2):
                if i + 1 < len(parts):
                    key, value = parts[i], parts[i + 1]
                    try:
                        data_dict[key] = float(value)
                    except ValueError:
                        data_dict[key] = value
            
            # Map to output fields
            parsed_data = [
                data_dict.get('S', ''),  # Wind Speed
                data_dict.get('D', ''),  # Wind Direction
                data_dict.get('U', ''),  # U Vector
                data_dict.get('V', ''),  # V Vector
                data_dict.get('W', ''),  # W Vector
                data_dict.get('T', ''),  # Temperature
                data_dict.get('H', ''),  # Relative Humidity
                data_dict.get('P', ''),  # Pressure
                data_dict.get('PI', ''), # Compass Heading
                data_dict.get('RO', ''), # Pitch
                data_dict.get('MD', '')  # Roll
            ]
            
            # Add timestamp
            parsed_data.insert(0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            return parsed_data
            
        except Exception as e:
            self.logger.error(f"Parse error: {e}")
            return None

    def run(self):
        """Main data collection loop"""
        self.logger.info("Starting TriSonica data collection")
        Path('output').mkdir(exist_ok=True)
        
        with open('output/trisonica_data.csv', 'w', newline='') as csvfile:
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
                    parsed_data = self.read_trisonica_data()
                    if parsed_data:
                        writer.writerow(parsed_data)
                        csvfile.flush()
                        self.logger.info(f"Written: Wind Speed: {parsed_data[0]} m/s")
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
        self.logger.info("TriSonica data collection stopped")

def main():
    Path('output').mkdir(exist_ok=True)
    OptimizedTriSonicaReader().run()

if __name__ == "__main__":
    main()