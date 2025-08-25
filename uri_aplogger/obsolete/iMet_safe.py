import csv
import signal
import sys
import time
from datetime import datetime, timedelta
import logging
from pathlib import Path

try:
    import serial
    import pyudev
except ImportError as e:
    print(f"ERROR: Missing dependency: {e}")
    sys.exit(1)

class OptimizediMetReader:
    IDENTIFIERS = {"ID_VENDOR_ID": "0403", "ID_MODEL_ID": "6015"}
    PV_NAMES = ['Timestamp', 'pressure', 'temp', 'rel_hum', 'hum_temp', 'date', 'time', 
                'longitude', 'latitude', 'altitude', 'sat']
    
    def __init__(self):
        self.running = True
        self.serial_conn = None
        self.consecutive_failures = 0
        self.max_failures = 5
        self.reconnect_delay = 5
        self.last_successful_read = None
        
        self.setup_logging()
        signal.signal(signal.SIGINT, self.signal_handler)

    def setup_logging(self):
        logging.basicConfig(
            format="%(asctime)s iMet: %(message)s",
            level=logging.INFO,
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger('iMet')

    def signal_handler(self, sig, frame):
        self.logger.info("Stopping iMet data collection...")
        self.running = False

    def find_imet_port(self):
        """Find iMet port using device identifiers"""
        try:
            context = pyudev.Context()
            for device in context.list_devices(subsystem='tty'):
                if (device.get('ID_VENDOR_ID') == self.IDENTIFIERS["ID_VENDOR_ID"] and 
                    device.get('ID_MODEL_ID') == self.IDENTIFIERS["ID_MODEL_ID"]):
                    self.logger.info(f"Found iMet at: {device.device_node}")
                    return device.device_node
            self.logger.warning("iMet device not found")
            return None
        except Exception as e:
            self.logger.error(f"Error finding iMet port: {e}")
            return None

    def init_serial(self):
        """Initialize serial connection"""
        if self.serial_conn and self.serial_conn.is_open:
            self.serial_conn.close()
            
        port = self.find_imet_port()
        if not port:
            return False
            
        try:
            self.serial_conn = serial.Serial(
                port=port,
                baudrate=57600,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=2
            )
            time.sleep(2)  # Device initialization
            self.logger.info(f"Connected to iMet on {port}")
            self.consecutive_failures = 0
            self.last_successful_read = time.time()
            return True
        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            self.consecutive_failures += 1
            return False

    def read_imet_data(self):
        """Read and parse data from iMet"""
        if not self.serial_conn or not self.serial_conn.is_open:
            if not self.init_serial():
                return None

        try:
            if self.serial_conn.in_waiting > 0:
                line = self.serial_conn.readline()
                decoded = line.decode('utf-8', errors='ignore').strip()
                if decoded:
                    self.logger.info(f"Raw data: {decoded}")
                    self.last_successful_read = time.time()
                    return self.parse_imet_data(decoded)
            return None
        except Exception as e:
            self.logger.error(f"Read error: {e}")
            self.consecutive_failures += 1
            self.serial_conn = None
            return None

    def parse_imet_data(self, data):
        """Parse iMet data string into structured format"""
        try:
            data = data.strip().lstrip(',')
            data_list = data.split(',')
            
            if len(data_list) < 10:
                return None
            
            # Process temperatures (divide by 100)
            for idx in [1, 3]:  # temp and hum_temp indices
                if idx < len(data_list) and data_list[idx]:
                    data_list[idx] = str(float(data_list[idx]) / 100)
            
            # Adjust time by 2 hours
            if len(data_list) > 5 and data_list[5] and ':' in data_list[5]:
                time_obj = datetime.strptime(data_list[5], "%H:%M:%S")
                data_list[5] = (time_obj + timedelta(hours=2)).strftime("%H:%M:%S")
            
            # Add timestamp and remove first field (XQ)
            data_list = data_list[1:11]  # Take exactly 10 fields
            data_list.insert(0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            
            return data_list
        except Exception as e:
            self.logger.error(f"Parse error: {e}, data: {data}")
            return None

    def run(self):
        """Main data collection loop"""
        self.logger.info("Starting iMet data collection")
        Path('output').mkdir(exist_ok=True)
        
        output_file = 'output/imet_data.csv'
        with open(output_file, 'w', newline='') as csvfile:
            csv.writer(csvfile).writerow(self.PV_NAMES)
        
        self.logger.info(f"Output file: {output_file}")
        
        last_reconnect = last_placeholder = 0
        
        with open(output_file, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            while self.running:
                current_time = time.time()
                
                # Reconnect logic
                if not self.serial_conn and current_time - last_reconnect >= self.reconnect_delay:
                    if self.init_serial():
                        self.logger.info("Reconnected!")
                    last_reconnect = current_time
                
                # Data reading
                if self.serial_conn and self.serial_conn.is_open:
                    parsed_data = self.read_imet_data()
                    if parsed_data:
                        writer.writerow(parsed_data)
                        csvfile.flush()
                        self.logger.info(f"Written: P={parsed_data[0]} hPa, T={parsed_data[1]} °C")
                
                # Placeholder every 30 seconds
                if current_time - last_placeholder >= 30:
                    placeholder = [''] * (len(self.PV_NAMES) - 1) + [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
                    writer.writerow(placeholder)
                    csvfile.flush()
                    last_placeholder = current_time
                
                # Failure handling
                if self.consecutive_failures >= self.max_failures:
                    self.logger.warning(f"Multiple failures, reconnecting in {self.reconnect_delay}s")
                    self.serial_conn = None
                    self.consecutive_failures = 0
                    last_reconnect = current_time
                
                time.sleep(1)
        
        if self.serial_conn:
            self.serial_conn.close()
        self.logger.info("Data collection stopped")

def main():
    print("Starting iMet data collection...")
    OptimizediMetReader().run()

if __name__ == "__main__":
    main()