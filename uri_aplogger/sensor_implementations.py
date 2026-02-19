# sensor_implementations.py
"""
Sensor-specific implementations
"""

from generic_sensor import GenericSensor
from datetime import datetime, timedelta
import time
import re

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

import socket
import csv
from pathlib import Path

class POPSSensor(GenericSensor):
    """
    POPS UDP -> CSV sensor.
    Replaces legacy pops_class UDP behavior but uses the same CSV/merge conventions.
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        self.udp_ip = config.get("udp_ip", "0.0.0.0")
        self.udp_port = int(config.get("udp_port", 10080))
        self.buffer_size = int(config.get("buffer_size", 8192))
        # socket and control flags
        self._sock = None
        self._sock_timeout = float(config.get("socket_timeout", 0.5))
        # GenericSensor fields used for reconnect/failure handling
        self.reconnect_delay = config.get("reconnect_delay", self.reconnect_delay)
        self.max_failures = config.get("max_failures", self.max_failures)

    def _open_socket(self):
        if self._sock:
            return True
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._sock.bind((self.udp_ip, self.udp_port))
            self._sock.settimeout(self._sock_timeout)
            self.logger.info(f"POPS listening on UDP {self.udp_ip}:{self.udp_port}")
            self.consecutive_failures = 0
            return True
        except Exception as e:
            self.logger.error(f"Failed to open POPS UDP socket: {e}")
            self._sock = None
            self.consecutive_failures += 1
            return False

    def _close_socket(self):
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    def read_udp_packet(self):
        """Non-blocking-ish receive that returns decoded string or None."""
        if not self._sock:
            if not self._open_socket():
                return None

        try:
            data, addr = self._sock.recvfrom(self.buffer_size)
            if not data:
                return None
            msg = data.decode("utf-8", errors="ignore").strip("\x00\r\n ")
            self.logger.debug(f"POPS RX from {addr}: {msg[:200]!r}")
            return msg
        except socket.timeout:
            return None
        except Exception as e:
            self.logger.error(f"POPS read error: {e}")
            self.consecutive_failures += 1
            # close socket so next loop tries to reopen
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None
            return None

    def parse_data(self, data):
        """
        Legacy POPS payloads were comma-separated and code used message[3:].
        We follow that behavior: split, take fields from index 3 onward,
        then prepend a timestamp so CSV matches your other sensors.
        """
        try:
            parts = [p.strip() for p in data.split(",")]
            values = parts[3:] if len(parts) > 3 else []
            row = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] + values

            # Ensure exact column count (pad/truncate) to match config column_names
            expected = len(self.config.get("column_names", []))
            if expected:
                if len(row) < expected:
                    row += [""] * (expected - len(row))
                elif len(row) > expected:
                    row = row[:expected]
            return row
        except Exception as e:
            self.logger.error(f"POPS parse error: {e}, raw={data!r}")
            return None

    def run(self):
        """Own run loop (UDP needs its own flow, so we don't call GenericSensor.run)."""
        self.logger.info(f"Starting POPS UDP listener: {self.udp_ip}:{self.udp_port}")

        # Ensure output dir / file exist, then append rows
        Path(f'output/{self.name}').mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_file = self.config.get('output_file', f'output/{self.name}/{self.name}_data_{timestamp}.csv')

        try:
            # Initialize CSV with headers if needed
            if not Path(self.output_file).exists():
                with open(self.output_file, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(self.config.get('column_names', []))
                self.logger.info(f"Created POPS output file: {self.output_file}")
        except Exception as e:
            self.logger.error(f"Failed to create POPS output file: {e}")
            return

        last_reconnect = time.time()
        data_count = 0
        self.running = True

        with open(self.output_file, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)

            while self.running:
                now = time.time()
                # Try to ensure socket is open periodically (reconnect_delay from GenericSensor)
                if not self._sock and now - last_reconnect >= self.reconnect_delay:
                    self.logger.info("Attempting to (re)open POPS socket...")
                    self._open_socket()
                    last_reconnect = now

                # Read one UDP packet (if any)
                packet = self.read_udp_packet()
                if packet:
                    parsed = self.parse_data(packet)
                    if parsed:
                        try:
                            writer.writerow(parsed)
                            csvfile.flush()
                            data_count += 1
                            self.consecutive_failures = 0
                            # log a short sample
                            sample_fields = [str(f) for f in parsed[1:4] if f]
                            if sample_fields:
                                self.logger.info(f"Written: {', '.join(sample_fields)}")
                        except Exception as e:
                            self.logger.error(f"POPS write error: {e}")

                # Failure handling similar to GenericSensor
                if self.consecutive_failures >= self.max_failures:
                    self.logger.warning(f"POPS: consecutive failures >= {self.max_failures}, closing socket and retrying after {self.reconnect_delay}s")
                    self._close_socket()
                    self.consecutive_failures = 0
                    last_reconnect = now

                time.sleep(0.1)

        # cleanup
        self._close_socket()
        self.logger.info(f"POPS stopped. Total rows: {data_count}")

    def teardown(self):
        # Ensure socket closed if framework calls teardown
        try:
            self._close_socket()
        except Exception:
            pass
        super().signal_handler(None, None)

import os
import errno
from pathlib import Path

class LDDSensor(GenericSensor):
    """
    LDD Arduino (serial) sensor using pyudev identifiers (2341:0058).

    Commands (sent over serial with newline): PING, GET, RESET, SETC <amps>, SETT <degC>
    Telemetry is CSV lines; header starts with:
      ErrorNumber,ErrorInstance,ErrorParameter,...
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        self.baudrate = int(config.get("baudrate", 57600))
        self.timeout = float(config.get("timeout", 1))

        # Optional startup actions
        self.send_ping = bool(config.get("send_ping", True))
        self.setc = config.get("setc")  # float amps or None
        self.sett = config.get("sett")  # float degC or None
        self.do_reset = bool(config.get("do_reset", False))

        # Track whether we already did startup commands for the current connection
        self._did_startup_for_connection = False

        self.cmd_fifo = Path(f"output/{self.name}/cmd.fifo")
        self._fifo_fd = None
        self._fifo_buf = ""

    def init_serial(self):
        """
        Use the standard GenericSensor serial discovery (identifiers via pyudev),
        then send startup commands once after a successful connection.
        """
        ok = super().init_serial()
        if not ok or not self.serial_conn or not self.serial_conn.is_open:
            self._did_startup_for_connection = False
            return False

        # Only once per (re)connection
        if not self._did_startup_for_connection:
            try:
                time.sleep(2)  # Arduino settle time
                try:
                    self.serial_conn.reset_input_buffer()
                except Exception:
                    pass

                self._send_startup_commands()
                self._did_startup_for_connection = True
            except Exception as e:
                self.logger.warning(f"LDD startup commands failed: {e}")
                # connection is still OK; let it run anyway

        return True

    def _send_line(self, cmd: str):
        if not self.serial_conn or not self.serial_conn.is_open:
            return
        self.serial_conn.write((cmd.strip() + "\n").encode("utf-8"))
        self.logger.info(f"TX: {cmd.strip()}")

    def _send_startup_commands(self):
        if self.send_ping:
            self._send_line("PING")
            time.sleep(0.1)

        if self.setc is not None:
            self._send_line(f"SETC {float(self.setc):.3f}")
            time.sleep(0.1)

        if self.sett is not None:
            self._send_line(f"SETT {float(self.sett):.2f}")
            time.sleep(0.1)

        if self.do_reset:
            self._send_line("RESET")
            time.sleep(0.1)

    def parse_data(self, data: str):
        self._poll_cmd_fifo()
        try:
            s = data.strip()
            if not s:
                return None

            up = s.upper()
            # Skip command responses / banners
            if up.startswith("OK") or up.startswith("ERR") or "COMMANDS" in up or up.startswith("IDENT"):
                return None

            # Skip header
            if s.startswith("ErrorNumber,ErrorInstance,ErrorParameter"):
                return None

            # Telemetry lines are CSV
            if "," not in s:
                return None

            parts = [p.strip() for p in s.split(",")]

            # Prepend timestamp to match your framework
            parts.insert(0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            return parts

        except Exception as e:
            self.logger.error(f"Parse error: {e}, data: {data!r}")
            return None

    def _open_cmd_fifo(self):
        # Ensure dir exists
        self.cmd_fifo.parent.mkdir(parents=True, exist_ok=True)

        # Only open once
        if self._fifo_fd is not None:
            return

        # Only open if FIFO exists (user creates it with mkfifo)
        if not self.cmd_fifo.exists():
            return

        try:
            # Non-blocking read end; won't freeze your sensor loop
            self._fifo_fd = os.open(str(self.cmd_fifo), os.O_RDONLY | os.O_NONBLOCK)
            self.logger.info(f"LDD command FIFO opened: {self.cmd_fifo}")
        except Exception as e:
            self.logger.warning(f"Could not open command FIFO: {e}")
            self._fifo_fd = None

    def _poll_cmd_fifo(self):
        self._open_cmd_fifo()
        if self._fifo_fd is None:
            return

        try:
            chunk = os.read(self._fifo_fd, 4096)
            if not chunk:
                # Writer closed; keep FD open (or reopen if you prefer)
                return

            self._fifo_buf += chunk.decode("utf-8", errors="ignore")

            # Process complete lines only (Enter-delimited)
            while "\n" in self._fifo_buf:
                line, self._fifo_buf = self._fifo_buf.split("\n", 1)
                cmd = line.strip()
                if cmd:
                    self._send_line(cmd)

        except OSError as e:
            if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                return  # no data available this loop
            self.logger.warning(f"FIFO read error: {e}")

import re
import os
import errno
from pathlib import Path
from datetime import datetime
import time

class PumpSensor(GenericSensor):
    """
    Pump + Pressure + Temp + Hum sensor over Arduino serial.

    Accepts either:
      Pump: 2660 RPM | Pres: 1007.5 mb | Temp: 22.8 C | Hum: 48.5 %
    or:
      Pres: 1007.5 mb | Temp: 22.8 C | Hum: 48.5 % | Pump: 2660 RPM
    """

    LINE_RE = re.compile(
        r"^\s*(?:Pump:\s*(?P<rpm_a>[-+]?\d+(?:\.\d+)?)\s*RPM\s*\|\s*)?"
        r"Pres:\s*(?P<pres>(?:ERR|[-+]?\d+(?:\.\d+)?))\s*mb\s*\|\s*"
        r"Temp:\s*(?P<temp>[-+]?\d+(?:\.\d+)?)\s*C\s*\|\s*"
        r"Hum:\s*(?P<hum>[-+]?\d+(?:\.\d+)?)\s*%\s*"
        r"(?:\|\s*Pump:\s*(?P<rpm_b>[-+]?\d+(?:\.\d+)?)\s*RPM)?\s*$",
        re.IGNORECASE
    )

    def __init__(self, name, config):
        super().__init__(name, config)
        self.baudrate = int(config.get("baudrate", 115200))
        self.timeout = float(config.get("timeout", 2))

        self.power_setpoint = float(config.get("initial_power", 40.0))

        # FIFO like LDD
        self.power_fifo = Path(config.get("power_fifo", f"output/{self.name}/power.fifo"))
        self._fifo_fd = None
        self._fifo_buf = ""

        self._did_startup_for_connection = False

    def init_serial(self):
        ok = super().init_serial()
        if not ok or not self.serial_conn or not self.serial_conn.is_open:
            self._did_startup_for_connection = False
            return False

        if not self._did_startup_for_connection:
            time.sleep(2)
            try:
                self.serial_conn.reset_input_buffer()
            except Exception:
                pass

            # Apply initial power once per connection
            self._send_power(self.power_setpoint)
            self._did_startup_for_connection = True

        return True

    def _send_line(self, cmd: str):
        if not self.serial_conn or not self.serial_conn.is_open:
            return
        self.serial_conn.write((cmd.strip() + "\n").encode("utf-8"))
        self.logger.info(f"TX: {cmd.strip()}")

    def _send_power(self, percent: float):
        percent = max(0.0, min(100.0, float(percent)))
        self.power_setpoint = percent
        # Your updated sketch supports SETPWR (good)
        self._send_line(f"SETPWR {percent:.1f}")

    def _open_power_fifo(self):
        self.power_fifo.parent.mkdir(parents=True, exist_ok=True)
        if self._fifo_fd is not None:
            return
        if not self.power_fifo.exists():
            return
        try:
            self._fifo_fd = os.open(str(self.power_fifo), os.O_RDONLY | os.O_NONBLOCK)
            self.logger.info(f"Pump power FIFO opened: {self.power_fifo}")
        except Exception as e:
            self.logger.warning(f"Could not open pump power FIFO: {e}")
            self._fifo_fd = None

    def _poll_power_fifo(self):
        self._open_power_fifo()
        if self._fifo_fd is None:
            return

        try:
            chunk = os.read(self._fifo_fd, 4096)
            if not chunk:
                return

            self._fifo_buf += chunk.decode("utf-8", errors="ignore")

            while "\n" in self._fifo_buf:
                line, self._fifo_buf = self._fifo_buf.split("\n", 1)
                line = line.strip()
                if not line:
                    continue

                # You can type just: 55<enter>
                try:
                    val = float(line)
                except ValueError:
                    self.logger.warning(f"Ignoring FIFO input (not a number): {line!r}")
                    continue

                self._send_power(val)

        except OSError as e:
            if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                return
            self.logger.warning(f"Pump FIFO read error: {e}")

    def parse_data(self, data: str):
        # FIFO polling piggybacks on incoming lines (like your LDD)
        self._poll_power_fifo()

        s = data.strip()
        if not s:
            return None

        # Skip command replies
        up = s.upper()
        if up.startswith("OK") or up.startswith("ERR") or "SYSTEM STARTUP" in up or up.startswith("HDC "):
            return None

        m = self.LINE_RE.match(s)
        if not m:
            # turn this on temporarily if needed:
            # self.logger.debug(f"Unmatched pump line: {s!r}")
            return None

        rpm = m.group("rpm_a") or m.group("rpm_b")
        if rpm is None:
            return None

        pres_raw = m.group("pres")
        pres = "" if pres_raw.upper() == "ERR" else float(pres_raw)

        row = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            float(rpm),
            pres,
            float(m.group("temp")),
            float(m.group("hum")),
            self.power_setpoint
        ]
        return row

    
# Factory function to create sensors
def create_sensor(sensor_type, name, config):
    """Factory function to create appropriate sensor instance"""
    sensor_classes = {
        'iMet': iMetSensor,
        'POM': POMSensor,
        'TriSonica': TriSonicaSensor,
        'Partector2Pro': Partector2ProSensor,
        'MiniaethMA200': MiniaethMA200Sensor,
        'POPS': POPSSensor,
        'LDD': LDDSensor,
        'Pump': PumpSensor,
        'Generic': GenericSensor  # Fallback
    }
    
    sensor_class = sensor_classes.get(sensor_type, GenericSensor)
    return sensor_class(name, config)