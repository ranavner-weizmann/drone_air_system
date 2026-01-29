# sensor_implementations.py
"""
Sensor-specific implementations
"""

from generic_sensor import GenericSensor
from datetime import datetime, timedelta
import time

class iMetSensor(GenericSensor):
    """iMet sensor implementation"""
    
    def parse_data(self, data):
        try:
            data = data.strip().lstrip(',')
            data_list = data.split(',')
            
            # Process temperatures (divide by 100)
            data_list[1] = float(data_list[1]) 
            data_list[1] /= 100
            data_list[3] = float(data_list[3]) 
            data_list[3] /= 100

            # Adjust time by 2 hours
            if len(data_list) > 5 and data_list[5] and ':' in data_list[5]:
                try:
                    time_obj = datetime.strptime(data_list[5], "%H:%M:%S")
                    data_list[5] = (time_obj + timedelta(hours=2)).strftime("%H:%M:%S")
                except ValueError:
                    pass
            
            # Add timestamp and remove first field (printed XQ)
            data_list = data_list[1:11]  # Take exactly 10 fields
            data_list.insert(0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            
            return data_list
        except Exception as e:
            self.logger.error(f"Parse error: {e}, data: {data}")
            return None

class POMSensor(GenericSensor):
    """POM sensor implementation - Fixed"""
    
    def __init__(self, name, config):
        super().__init__(name, config)
        self.header_lines_skipped = 0
        self.max_header_lines = 10
        self.skip_first_data_row = True  # Flag to skip the first data row
    
    def parse_data(self, data):
        try:
            # Skip header lines
            if "Personal Ozone Monitor" in data or data.isdigit():
                self.header_lines_skipped += 1
                if self.header_lines_skipped <= 3:
                    self.logger.info(f"Skipping header: {data}")
                return None
            
            data_list = data.split(',')
            
            # Handle both data formats (11 fields = real-time, 12 fields = logged)
            if len(data_list) > 12:
                self.logger.warning(f"Unexpected data format: {len(data_list)} fields, data: {data}")
                return None
            
            # Skip the first data row (which contains weird characters)
            if self.skip_first_data_row:
                self.logger.info("Skipping first data row with weird characters")
                self.skip_first_data_row = False
                return None

            # Add timestamp as first column
            data_list.insert(0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            return data_list
            
        except Exception as e:
            self.logger.error(f"Parse error: {e}, data: {data}")
            return None

class TriSonicaSensor(GenericSensor):
    """TriSonica sensor implementation"""
    
    def parse_data(self, data):
        try:
            parts = data.strip().split()
            data_dict = {}
            
            # Parse key-value pairs
            for i in range(0, len(parts), 2):
                if i + 1 < len(parts):
                    key = parts[i].strip()
                    value = parts[i + 1].strip()
                    data_dict[key] = value
            
            # Map to output fields in correct order
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
            
            # Add timestamp as first column
            parsed_data.insert(0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            return parsed_data
            
        except Exception as e:
            self.logger.error(f"Parse error: {e}, data: {data}")
            return None

class Partector2ProSensor(GenericSensor):
    """Partector 2 Pro sensor implementation"""
    
    def __init__(self, name, config):
        super().__init__(name, config)
        self.command_sent = False
        self.mode = config.get('mode', 1)  # Default to 1Hz mode
        # Override baudrate to 115200 for Partector 2 Pro
        self.baudrate = 115200
        self.timeout = config.get('timeout', 2)
        
    def init_serial(self):
        """Initialize serial connection and send start command"""
        success = super().init_serial()
        if success and self.serial_conn and self.serial_conn.is_open:
            # Send command to start data streaming
            try:
                command = f"X000{self.mode}!\r\n".encode('utf-8')
                self.serial_conn.write(command)
                self.logger.info(f"Sent start command: X000{self.mode}!")
                time.sleep(1)  # Wait for device to initialize
                
                # Clear any initial data
                self.serial_conn.flushInput()
                self.command_sent = True
            except Exception as e:
                self.logger.error(f"Error sending start command: {e}")
                return False
        return success
    
    def parse_data(self, data):
        """
        Parse Partector 2 Pro data.
        Data format is tab-separated values according to the PDF documentation.
        """
        try:
            # Clean up the data
            data = data.strip()
            
            # Skip empty lines or command responses
            if not data or data.startswith("X"):
                return None
            
            # Split by tabs (tsv format)
            data_list = data.split('\t')
            
            # Remove any empty strings
            data_list = [item for item in data_list if item]
            
            # Based on the PDF, we expect either:
            # - 18 fields for standard 1Hz mode (mode 1)
            # - 32 fields for size distribution mode (mode 6)
            
            # Convert numeric values where possible
            parsed_values = []
            for value in data_list:
                try:
                    # Try to convert to float if it looks like a number
                    if value.replace('.', '', 1).replace('-', '', 1).isdigit():
                        parsed_values.append(float(value))
                    else:
                        parsed_values.append(value)
                except:
                    parsed_values.append(value)
            
            # Add timestamp as first column
            parsed_values.insert(0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            
            return parsed_values
            
        except Exception as e:
            self.logger.error(f"Parse error: {e}, data: {data}")
            return None

class MiniaethMA200Sensor(GenericSensor):
    def __init__(self, name, config):
        super().__init__(name, config)
        self.baudrate = config.get("baudrate", 1000000)
        self.timeout = config.get("timeout", 1)

        # Poll rate (seconds) — IMPORTANT so we don’t spam dr
        self.poll_interval = float(config.get("poll_interval", 1.0))
        self._last_poll = 0.0

    def read_serial_data(self):
        # Ensure connection
        if not self.serial_conn or not self.serial_conn.is_open:
            if not self.init_serial():
                return None

        # Throttle polling (GenericSensor.run loops every 0.1s) :contentReference[oaicite:2]{index=2}
        now = time.time()
        if now - self._last_poll < self.poll_interval:
            return None
        self._last_poll = now

        try:
            # Clear any queued junk so we read the freshest response
            try:
                self.serial_conn.reset_input_buffer()
            except Exception:
                pass

            self.serial_conn.write(b"dr\r")

            # Read for up to ~timeout seconds; skip echo/blank lines
            deadline = time.time() + max(1.0, float(self.timeout))
            while time.time() < deadline:
                raw = self.serial_conn.readline()
                if not raw:
                    continue
                line = raw.decode("utf-8", errors="ignore").strip()

                # DEBUG: show everything we receive
                self.logger.debug(f"MA200 RX: {line!r}")

                if not line:
                    continue
                if line.lower() == "dr":
                    continue
                if line.startswith("MA200-") and "," in line:
                    return line

            self.logger.debug("MA200: no valid data line received this poll")
            return None

        except Exception as e:
            self.logger.error(f"MA200 read error: {e}")
            self.consecutive_failures += 1
            if self.serial_conn:
                self.serial_conn.close()
                self.serial_conn = None
            return None

    def parse_data(self, data):
        try:
            parts = [p.strip() for p in data.split(",")]
            parts.insert(0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            self.logger.debug(f"MA200 parsed fields: {len(parts)}")
            return parts
        except Exception as e:
            self.logger.error(f"Parse error: {e}, data: {data}")
            return None


# Factory function to create sensors
def create_sensor(sensor_type, name, config):
    """Factory function to create appropriate sensor instance"""
    sensor_classes = {
        'iMet': iMetSensor,
        'POM': POMSensor,
        'TriSonica': TriSonicaSensor,
        'Partector2Pro': Partector2ProSensor,
        'MiniaethMA200': MiniaethMA200Sensor,
        'Generic': GenericSensor  # Fallback
    }
    
    sensor_class = sensor_classes.get(sensor_type, GenericSensor)
    return sensor_class(name, config)