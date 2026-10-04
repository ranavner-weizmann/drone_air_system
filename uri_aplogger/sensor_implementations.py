# sensor_implementations.py
"""
Sensor-specific implementations
"""

from generic_sensor import GenericSensor
from datetime import datetime, timedelta
import time
import re
from collections import deque

import numpy as np

from run_paths import get_csv_dir, get_run_dir
from derived import (to_float, fmt, PopsPMCalculator, PowerLawFit, pops_bin_edges_nm,
                     MA200_WAVELENGTHS_NM, MA200_MAC_M2_G, MA200_CHANNEL_ORDER,
                     POPS_FACTORY_LOGMIN, POPS_FACTORY_LOGMAX)


def _col_index(column_names, name):
    """Index of `name` in the configured header, or None."""
    try:
        return column_names.index(name)
    except ValueError:
        return None


def _derived_layout(column_names, derived):
    """
    (n_raw, append) for a sensor whose header may end with `derived` columns.
    If the configured header does not end with exactly those names (e.g. an
    older sensor_config.json), nothing is appended and the raw row is aligned
    to the full header, so a code/config mismatch can never shift columns.
    """
    k = len(derived)
    if k and len(column_names) >= k and tuple(column_names[-k:]) == tuple(derived):
        return len(column_names) - k, True
    return len(column_names), False


def _fit_row_length(row, n):
    """Pad with '' / truncate in place so that len(row) == n (n <= 0: no-op)."""
    if n <= 0:
        return row
    if len(row) < n:
        row.extend([""] * (n - len(row)))
    elif len(row) > n:
        del row[n:]
    return row


class iMetSensor(GenericSensor):
    """iMet sensor implementation"""

    # Derived columns appended after the device fields (see sensor_config.json):
    #   temp_C      - 'temp' / 100      (device sends hundredths of degC)
    #   hum_temp_C  - 'hum_temp' / 100  (same scaling, humidity-sensor temperature)
    DERIVED_COLUMNS = ("temp_C", "hum_temp_C")

    def __init__(self, name, config):
        super().__init__(name, config)
        cols = config.get("column_names", [])
        self._n_raw_cols, self._append_derived = _derived_layout(cols, self.DERIVED_COLUMNS)
        if not self._append_derived:
            self.logger.warning("iMet: column_names do not end with "
                                f"{list(self.DERIVED_COLUMNS)}; derived columns disabled")
        self._idx_temp = _col_index(cols, "temp")
        self._idx_hum_temp = _col_index(cols, "hum_temp")

    @staticmethod
    def _hundredths_to_degC(raw):
        v = to_float(raw)
        return "" if v is None else f"{v / 100.0:.2f}"

    def parse_data(self, data):
        try:
            data = data.strip().lstrip(',')
            data_list = data.split(',')
            
            # Process temperatures (divide by 100)
            data_list[1] = float(data_list[1])
            data_list[1] /= 100
            # Relative humidity is transmitted as %RH * 10, not * 100
            data_list[3] = float(data_list[3])
            data_list[3] /= 10

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

            # Derived: scaled temperatures. Raw device fields stay as they are;
            # the new values are appended so existing columns never move.
            _fit_row_length(data_list, self._n_raw_cols)
            if self._append_derived:
                temp_c = "" if self._idx_temp is None else self._hundredths_to_degC(data_list[self._idx_temp])
                hum_c = "" if self._idx_hum_temp is None else self._hundredths_to_degC(data_list[self._idx_hum_temp])
                data_list.append(temp_c)
                data_list.append(hum_c)

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
                data_dict.get('MD', ''), # Compass Heading
                data_dict.get('PI', ''), # Pitch
                data_dict.get('RO', '')  # Roll
            ]
            
            # Add timestamp as first column
            parsed_data.insert(0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            return parsed_data
            
        except Exception as e:
            self.logger.error(f"Parse error: {e}, data: {data}")
            return None

class Partector2ProSensor(GenericSensor):
    """
    Clean, stable Partector 2 Pro (USB, mode 6, size distribution)
    """

    def __init__(self, name, config):
        super().__init__(name, config)

        # naneos' own library uses 9600; the device's USB stack does not
        # stream at other line-coding settings (115200 silently fails).
        self.baudrate = 9600
        self.timeout = 2
        self.mode = 6  # FORCE mode 6 for Pro (size distribution)
        self._did_startup = False
        # Wall-clock time of the last received data line; used to detect a
        # device that went silent (e.g. power-cycled) while the serial
        # connection itself stayed open.
        self._last_rx = None
        # Mode-6 lines normally arrive every few seconds, so a minute of
        # silence means the device is no longer streaming.
        self._silence_timeout = 60

    # --------------------------------------------------
    # Serial Init
    # --------------------------------------------------

    def init_serial(self):
        ok = super().init_serial()
        if not ok or not self.serial_conn or not self.serial_conn.is_open:
            self._did_startup = False
            return False

        if not self._did_startup:
            try:
                time.sleep(2)
                self.serial_conn.reset_input_buffer()

                # Commands are terminated by '!' itself - no \r\n, matching
                # naneos' own library.
                # Stop streaming first (important)
                self.serial_conn.write(b"X0000!")
                time.sleep(0.5)

                # Start mode 6 (size distribution). M0004 selects the size
                # distribution output; the device may not remember it across
                # a power cycle, so always send it rather than relying on a
                # previously stored setting.
                self.serial_conn.write(b"X0006!")
                time.sleep(0.2)
                self.serial_conn.write(b"M0004!")
                self.logger.info("Partector Pro set to mode 6 (size distribution)")

                time.sleep(1)
                self.serial_conn.reset_input_buffer()

                self._did_startup = True

            except Exception as e:
                self.logger.error(f"P2Pro init error: {e}")
                return False

        self._last_rx = time.time()
        return True

    # --------------------------------------------------
    # Read raw line safely
    # --------------------------------------------------

    def read_serial_data(self):
        if not self.serial_conn or not self.serial_conn.is_open:
            if not self.init_serial():
                return None

        try:
            line = self.serial_conn.readline()
            if not line:
                # The connection can stay "open" while the device itself has
                # stopped streaming (e.g. after a power cycle). If it has
                # been silent too long, tear down and redo the full startup
                # (X0000/X0006/M0004) on the next call.
                if self._last_rx and time.time() - self._last_rx > self._silence_timeout:
                    self.logger.warning(
                        f"P2Pro silent for {self._silence_timeout}s - forcing full re-init"
                    )
                    try:
                        self.serial_conn.close()
                    except Exception:
                        pass
                    self.serial_conn = None
                    self._did_startup = False
                return None

            self._last_rx = time.time()
            decoded = line.decode("utf-8", errors="ignore").strip()

            # Ignore command echoes
            if decoded.startswith("X"):
                return None

            return decoded

        except Exception as e:
            self.logger.error(f"P2Pro read error: {e}")
            self.serial_conn = None
            self._did_startup = False
            return None

    # --------------------------------------------------
    # Parse mode 6 line (32 fields)
    # --------------------------------------------------

    def parse_data(self, data: str):
        try:
            if not data:
                return None

            parts = data.split("\t")

            # Accept >=32 fields (future firmware may append fields)
            if len(parts) < 32:
                self.logger.debug(f"P2Pro short frame ({len(parts)} fields)")
                return None

            # Truncate if firmware adds more fields
            parts = parts[:32]

            # Keep every field exactly as the device sent it (stripped of
            # padding only). No float() conversion: that would turn "0.500"
            # into 0.5 and "1531" into 1531.0. Note the device itself reports
            # RH as a whole percent in this output mode ("46" next to "25.2"
            # for temperature); naneos' own library types it as int.
            parsed = [v.strip() for v in parts]

            # Prepend wall clock timestamp
            row = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] + parsed

            return row

        except Exception as e:
            self.logger.error(f"P2Pro parse error: {e} | raw={data}")
            return None

class MiniaethMA200Sensor(GenericSensor):
    # Derived columns appended after the device fields (see sensor_config.json):
    #   AAE_fit      - absorption Angstrom exponent from a 5-wavelength
    #                  power-law fit  babs(lam) = A * (lam/lam_ref)^(-AAE)
    #   AAE_fit_amp  - A, the fitted absorption coefficient at lam_ref [Mm^-1]
    # babs(lam) = BC(lam) * MAC(lam); BCc is used when available, else BC1.
    DERIVED_COLUMNS = ("AAE_fit", "AAE_fit_amp")

    def __init__(self, name, config):
        super().__init__(name, config)
        self.baudrate = config.get("baudrate", 1000000)
        self.timeout = config.get("timeout", 1)

        cols = config.get("column_names", [])
        self._n_raw_cols, self._append_derived = _derived_layout(cols, self.DERIVED_COLUMNS)
        if not self._append_derived:
            self.logger.warning("MA200: column_names do not end with "
                                f"{list(self.DERIVED_COLUMNS)}; derived columns disabled")
        self._warned_len = False

        aae_cfg = config.get("aae_fit", {}) or {}
        ref_nm = float(aae_cfg.get("ref_wavelength_nm", 880.0))
        mac = dict(MA200_MAC_M2_G)
        mac.update(aae_cfg.get("mac_m2_g", {}) or {})
        # Per channel: (BCc index, BC1 index, MAC * 1e-3 -> Mm^-1 per ng/m3)
        self._aae_channels = tuple(
            (_col_index(cols, f"{ch}_BCc"), _col_index(cols, f"{ch}_BC1"), float(mac[ch]) * 1e-3)
            for ch in MA200_CHANNEL_ORDER
        )
        self._aae_fit = PowerLawFit(
            [MA200_WAVELENGTHS_NM[ch] for ch in MA200_CHANNEL_ORDER], ref_nm)
        self.logger.info(
            f"MA200 AAE fit: ref {ref_nm:g} nm, MAC m2/g = "
            + ", ".join(f"{ch} {mac[ch]:g}" for ch in MA200_CHANNEL_ORDER))

        # The MA200 streams continuously once told to start (see
        # obsolete/aeth_test.py: a single "dr" is followed by an
        # unprompted stream of lines). We only send "dr" once per
        # connection and then drain whatever the device pushes.
        self._stream_started = False
        self._line_queue = deque()

    def init_serial(self):
        ok = super().init_serial()
        if ok:
            self._stream_started = False
            self._line_queue.clear()
        return ok

    def read_serial_data(self):
        # Ensure connection
        if not self.serial_conn or not self.serial_conn.is_open:
            if not self.init_serial():
                return None

        try:
            if not self._stream_started:
                # Clear stale junk once, before kicking off the stream.
                try:
                    self.serial_conn.reset_input_buffer()
                except Exception:
                    pass
                self.serial_conn.write(b"dr\r")
                self._stream_started = True
                self.logger.debug("MA200: sent 'dr' to start data stream")

            # Pull in everything currently buffered so nothing queues up
            # and gets lost between calls (GenericSensor.run loops every
            # ~0.1s, faster than the MA200's own output rate).
            while self.serial_conn.in_waiting > 0:
                raw = self.serial_conn.readline()
                if not raw:
                    break
                line = raw.decode("utf-8", errors="ignore").strip()

                self.logger.debug(f"MA200 RX: {line!r}")

                if not line or line.lower() == "dr":
                    continue
                if line.startswith("MA200-") and "," in line:
                    self._line_queue.append(line)

            if self._line_queue:
                return self._line_queue.popleft()

            return None

        except Exception as e:
            self.logger.error(f"MA200 read error: {e}")
            self.consecutive_failures += 1
            if self.serial_conn:
                self.serial_conn.close()
                self.serial_conn = None
            self._stream_started = False
            return None

    def _absorption_Mm(self, parts):
        """babs per channel [Mm^-1] from the aligned row, None where unusable."""
        n = len(parts)
        out = []
        for idx_bcc, idx_bc1, mac_k in self._aae_channels:
            v = None
            for idx in (idx_bcc, idx_bc1):
                if idx is not None and idx < n:
                    f = to_float(parts[idx])
                    if f is not None and f > 0.0:
                        v = f * mac_k
                        break
            out.append(v)
        return out

    def parse_data(self, data):
        try:
            parts = [p.strip() for p in data.split(",")]
            parts.insert(0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            self.logger.debug(f"MA200 parsed fields: {len(parts)}")

            if self._n_raw_cols > 0 and len(parts) != self._n_raw_cols and not self._warned_len:
                self.logger.warning(
                    f"MA200 line has {len(parts)} fields incl. timestamp, header expects "
                    f"{self._n_raw_cols}; padding/truncating so derived columns stay aligned")
                self._warned_len = True
            _fit_row_length(parts, self._n_raw_cols)

            if self._append_derived:
                aae, amp = self._aae_fit.fit(self._absorption_Mm(parts))
                parts.append(fmt(aae, 4))
                parts.append(fmt(amp, 5))
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

    # Derived columns appended after the device fields (see sensor_config.json):
    #   PM1_ug_m3, PM2.5_ug_m3 - mass below 1000 / 2500 nm optical diameter,
    #   from the 1 s histogram (b0..b15), bin edges derived from the packet's
    #   nbins/logmin/logmax via the manual's amplitude->diameter table, an
    #   assumed particle density and the sampled volume (POPS_Flow [cm3/s] x 1 s).
    DERIVED_COLUMNS = ("PM1_ug_m3", "PM2.5_ug_m3")

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

        # PM derivation setup (indices resolved once from the configured header)
        cols = config.get("column_names", [])
        self._n_raw_cols, self._append_derived = _derived_layout(cols, self.DERIVED_COLUMNS)
        if not self._append_derived:
            self.logger.warning("POPS: column_names do not end with "
                                f"{list(self.DERIVED_COLUMNS)}; derived columns disabled")
        self._pm = PopsPMCalculator(density_g_cm3=config.get("pm_density_g_cm3", 1.65))
        self._idx_nbins = _col_index(cols, "nbins")
        self._idx_logmin = _col_index(cols, "logmin")
        self._idx_logmax = _col_index(cols, "logmax")
        self._idx_flow = _col_index(cols, "POPS_Flow")
        self._idx_b0 = _col_index(cols, "b0")
        self._pm_warned = set()
        self.logger.info(f"POPS PM: density {self._pm.density:g} g/cm3, "
                         f"cutoffs {self._pm.cutoffs} nm")

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

    def _warn_once(self, key, msg):
        if key not in self._pm_warned:
            self._pm_warned.add(key)
            self.logger.warning(msg)

    def _pm_from_row(self, row):
        """(PM1, PM2.5) in ug/m3 from an aligned row; (None, None) if not computable."""
        none = (None,) * len(self._pm.cutoffs)
        if self._idx_b0 is None or self._idx_flow is None or self._idx_nbins is None:
            self._warn_once("cols", "POPS PM: header lacks b0/POPS_Flow/nbins, cannot compute PM")
            return none

        nbins_f = to_float(row[self._idx_nbins])
        nbins = int(nbins_f) if nbins_f is not None else 0

        # Histogram edges depend on the unit's logmin/logmax (ours runs 1.0,
        # not the factory 1.6); fall back to factory values only if the packet
        # does not carry them.
        lmin = to_float(row[self._idx_logmin]) if self._idx_logmin is not None else None
        lmax = to_float(row[self._idx_logmax]) if self._idx_logmax is not None else None
        if lmin is None or lmax is None:
            self._warn_once("logdefaults", "POPS PM: logmin/logmax missing from packet, "
                            f"assuming factory {POPS_FACTORY_LOGMIN}/{POPS_FACTORY_LOGMAX}")
            lmin, lmax = POPS_FACTORY_LOGMIN, POPS_FACTORY_LOGMAX

        if self._pm.coefficients(nbins, lmin, lmax) is None:
            self._warn_once(("cfg", nbins, lmin, lmax),
                            f"POPS PM: unusable histogram config nbins={nbins} logmin={lmin} "
                            f"logmax={lmax}, PM left blank")
            return none
        self._warn_once(("edges", nbins, lmin, lmax),
                        f"POPS PM: nbins={nbins} logmin={lmin} logmax={lmax} -> bin edges (nm) "
                        + ", ".join(f"{e:.0f}" for e in pops_bin_edges_nm(nbins, lmin, lmax)))

        flow = to_float(row[self._idx_flow])
        counts = row[self._idx_b0:self._idx_b0 + nbins]
        return self._pm.compute(counts, nbins, flow, lmin, lmax)

    def parse_data(self, data):
        """
        Legacy POPS payloads were comma-separated and code used message[3:].
        We follow that behavior: split, take fields from index 3 onward,
        then prepend a timestamp so CSV matches your other sensors.
        Derived PM columns are appended after the device fields.
        """
        try:
            parts = [p.strip() for p in data.split(",")]
            values = parts[3:] if len(parts) > 3 else []
            row = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] + values

            # Ensure exact raw column count (pad/truncate) so derived columns
            # always land under their header names.
            _fit_row_length(row, self._n_raw_cols)

            if self._append_derived:
                pm1, pm25 = self._pm_from_row(row)
                row.append(fmt(pm1, 5))
                row.append(fmt(pm25, 5))
            return row
        except Exception as e:
            self.logger.error(f"POPS parse error: {e}, raw={data!r}")
            return None

    def run(self):
        """Own run loop (UDP needs its own flow, so we don't call GenericSensor.run)."""
        self.logger.info(f"Starting POPS UDP listener: {self.udp_ip}:{self.udp_port}")

        # Ensure output file path is set (csv dir already created by run_paths)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_file = self.config.get(
            'output_file',
            str(get_csv_dir() / f'{self.name}_data_{timestamp}.csv')
        )

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

        self.cmd_fifo = get_run_dir() / f"{self.name}_cmd.fifo"
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

import os
import errno
from pathlib import Path
from datetime import datetime
import time

class CavitySensor(GenericSensor):
    """
    Merged Arduino sensor (LDD + Pump) that outputs ONE CSV row per second.

    Expected Arduino output:
      - header: "ms,ErrorNumber,..."
      - rows:   <ms>,<ErrorNumber>,...,<PowerstageTemperature>,<pump_rpm>,<pressure_mb>,<temp_c>,<humidity_pct>,<power_pct>,<pressure_status>

    We also support a command FIFO for runtime control. The FIFO lives in the
    current run folder (output/<run_timestamp>/cavity_cmd.fifo):
      mkfifo "$RUN_DIR/cavity_cmd.fifo"
      echo "SETC 1.23"  > "$RUN_DIR/cavity_cmd.fifo"
      echo "SETT 35.0"  > "$RUN_DIR/cavity_cmd.fifo"
      echo "SETPWR 40"  > "$RUN_DIR/cavity_cmd.fifo"
      echo "RESET"      > "$RUN_DIR/cavity_cmd.fifo"
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        self.baudrate = int(config.get("baudrate", 115200))
        self.timeout = float(config.get("timeout", 1))

        # ms field from Arduino at the start of each row
        self._expect_ms_field = bool(config.get("expect_ms_field", True))

        # Optional startup actions (same idea as LDDSensor + Pump)
        self.send_ping = bool(config.get("send_ping", True))
        self.setc = config.get("setc")          # float amps or None
        self.sett = config.get("sett")          # float degC or None
        self.do_reset = bool(config.get("do_reset", False))
        self.initial_power = config.get("initial_power")  # pump power %

        self._did_startup_for_connection = False

        # Single command FIFO for all commands
        self.cmd_fifo = Path(
            config.get("cmd_fifo", str(get_run_dir() / f"{self.name}_cmd.fifo"))
        )
        self._fifo_fd = None
        self._fifo_buf = ""

    # --- serial / startup ---

    def init_serial(self):
        ok = super().init_serial()
        if not ok or not self.serial_conn or not self.serial_conn.is_open:
            self._did_startup_for_connection = False
            return False

        if not self._did_startup_for_connection:
            try:
                time.sleep(2)
                try:
                    self.serial_conn.reset_input_buffer()
                except Exception:
                    pass
                self._send_startup_commands()
                self._did_startup_for_connection = True
            except Exception as e:
                self.logger.warning(f"Cavity startup commands failed: {e}")

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

        if self.initial_power is not None:
            self._send_line(f"SETPWR {float(self.initial_power):.1f}")
            time.sleep(0.1)

        if self.do_reset:
            self._send_line("RESET")
            time.sleep(0.1)

    # --- FIFO handling (like LDDSensor) ---

    def _open_cmd_fifo(self):
        # Ensure dir exists
        self.cmd_fifo.parent.mkdir(parents=True, exist_ok=True)

        if self._fifo_fd is not None:
            return

        if not self.cmd_fifo.exists():
            return

        import os, errno
        try:
            self._fifo_fd = os.open(str(self.cmd_fifo), os.O_RDONLY | os.O_NONBLOCK)
            self.logger.info(f"Cavity command FIFO opened: {self.cmd_fifo}")
        except Exception as e:
            self.logger.warning(f"Could not open cavity command FIFO: {e}")
            self._fifo_fd = None

    def _poll_cmd_fifo(self):
        self._open_cmd_fifo()
        if self._fifo_fd is None:
            return

        import os, errno
        try:
            chunk = os.read(self._fifo_fd, 4096)
            if not chunk:
                # Writer closed, no data this time
                return

            self._fifo_buf += chunk.decode("utf-8", errors="ignore")

            while "\n" in self._fifo_buf:
                line, self._fifo_buf = self._fifo_buf.split("\n", 1)
                cmd = line.strip()
                if cmd:
                    self._send_line(cmd)

        except OSError as e:
            if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                return
            self.logger.warning(f"Cavity FIFO read error: {e}")

    # --- data parsing ---

    def parse_data(self, data: str):
        # Check for any new FIFO commands each time we get a line
        self._poll_cmd_fifo()

        try:
            s = data.strip()
            if not s:
                return None

            up = s.upper()

            # Skip non-data chatter / command acks
            if up.startswith("OK") or up.startswith("ERR") or up.startswith("IDENT") or "COMMANDS" in up:
                return None

            # Skip header line from Arduino
            if s.lower().startswith("ms,"):
                return None

            # Must be CSV-like
            if "," not in s:
                return None

            parts = [p.strip() for p in s.split(",")]

            # Expect first field to be ms
            if self._expect_ms_field:
                if not parts or not parts[0].isdigit():
                    return None

            # Add wall-clock timestamp
            row = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] + parts

            # Enforce column count if configured
            expected = len(self.config.get("column_names", []))
            if expected:
                if len(row) < expected:
                    row += [""] * (expected - len(row))
                elif len(row) > expected:
                    row = row[:expected]

            return row

        except Exception as e:
            self.logger.error(f"Cavity parse error: {e}, data: {data!r}")
            return None


import struct

class LDDUSBSensor(GenericSensor):
    """
    Meerstetter LDD read directly over its USB (MeCom) port — no Arduino.

    The LDD's FT230X USB link always runs at 57600 baud regardless of the
    RS485 setting. Once per poll_interval this sensor queries the same
    monitor parameters the cavity Arduino firmware reads over RS485
    (see arduino/sensor/sensor.ino) and writes one CSV row.

    Read-only by default. Optional startup setpoints, written once after
    each (re)connection — omit them from the config to leave the LDD alone:
      "setc": <amps>  -> parameter 2102 (laser current setpoint)
      "sett": <degC>  -> parameter 4000 (TEC target object temperature)

    Parameters 1203/1204/1300/1301 (3V3 rails, device/powerstage temp) are
    not available on the 8157-LDD-AN-LIN firmware (MeCom error +05), so they
    are not queried.
    """

    # (par_id, kind) per column, same order as column_names[1:]
    # kind: 'i' = INT32, 'f' = FLOAT32, 'fi' = FLOAT32 reported as int
    PARAMS = [
        (105, 'i'),    # ErrorNumber
        (106, 'i'),    # ErrorInstance
        (107, 'i'),    # ErrorParameter
        (1100, 'f'),   # LDD_ActualOutputCurrent
        (1101, 'f'),   # LDD_ActualOutputVoltage
        (1102, 'fi'),  # LDD_ActualOutputCurrentRaw
        (1104, 'f'),   # LDD_ActualAnodeVoltage
        (1105, 'f'),   # LDD_ActualCathodeVoltage
        (1402, 'f'),   # LDD_NominalOutputCurrentRamp
        (1010, 'f'),   # TEC_TargetObjectTemperature
        (1011, 'f'),   # TEC_NominalObjectTemperatureRamp
        (1012, 'f'),   # TEC_ThermalPowerModelCurrent
        (1020, 'f'),   # TEC_ActualOutputCurrent
        (1021, 'f'),   # TEC_ActualOutputVoltage
        (1000, 'f'),   # TEC_ObjectTemperature
        (1001, 'f'),   # TEC_SinkTemperature
        (1502, 'fi'),  # AnalogVoltageInputRawADC
        (1500, 'f'),   # AnalogVoltageInput
        (1600, 'f'),   # LaserPower
        (1601, 'f'),   # OutputLevel
        (1200, 'f'),   # DriverInputVoltage
        (1201, 'f'),   # Internal8V
        (1202, 'f'),   # Internal5V
    ]

    def __init__(self, name, config):
        super().__init__(name, config)
        self.baudrate = int(config.get("baudrate", 57600))
        self.timeout = float(config.get("timeout", 1))
        self.poll_interval = float(config.get("poll_interval", 1.0))
        self.mecom_address = int(config.get("mecom_address", 0))

        self.setc = config.get("setc")  # float amps or None
        self.sett = config.get("sett")  # float degC or None

        self._seq = int(time.time()) & 0x7FFF
        self._last_poll = 0.0
        self._did_startup_for_connection = False

    # --- MeCom framing ---

    @staticmethod
    def _crc16(data: bytes) -> int:
        n = 0
        for b in data:
            n ^= b << 8
            for _ in range(8):
                n = ((n << 1) ^ 0x1021) if n & 0x8000 else (n << 1)
            n &= 0xFFFF
        return n

    def _transact(self, payload: str, timeout=0.5):
        """Send one MeCom frame, return reply payload string or None."""
        self._seq = (self._seq + 1) & 0xFFFF
        body = f"#{self.mecom_address:02X}{self._seq:04X}{payload}".encode("ascii")
        sent_crc = self._crc16(body)
        self.serial_conn.reset_input_buffer()
        self.serial_conn.write(body + f"{sent_crc:04X}".encode("ascii") + b"\r")
        self.serial_conn.flush()

        buf = b""
        t0 = time.time()
        while time.time() - t0 < timeout:
            c = self.serial_conn.read(1)
            if not c:
                continue
            if c == b"!":
                buf = c
            elif buf:
                if c == b"\r":
                    s = buf.decode("ascii", errors="replace")
                    if len(s) < 11:
                        return None
                    # An 11-char frame is a set-ACK: its CRC echoes the
                    # request CRC. Longer frames carry their own CRC.
                    expect = sent_crc if len(s) == 11 else self._crc16(buf[:-4])
                    if expect != int(s[-4:], 16):
                        return None
                    if int(s[3:7], 16) != self._seq:
                        buf = b""  # stale frame, keep listening
                        continue
                    return s[7:-4]
                buf += c
        return None

    def _read_param(self, par_id, kind, inst=1):
        """Read one parameter; returns value or None on timeout/NACK."""
        p = self._transact(f"?VR{par_id:04X}{inst:02X}")
        if p is None or p.startswith("+") or len(p) != 8:
            return None
        if kind == 'i':
            v = int(p, 16)
            return v - (1 << 32) if v >= (1 << 31) else v
        f = struct.unpack(">f", bytes.fromhex(p))[0]
        # 'fi' parameters are integer-valued counts carried in a float32;
        # only drop the ".0" when the value really is integral.
        return int(f) if kind == 'fi' and f.is_integer() else f

    def _write_f32(self, par_id, value, inst=1):
        bits = struct.unpack(">I", struct.pack(">f", float(value)))[0]
        p = self._transact(f"VS{par_id:04X}{inst:02X}{bits:08X}")
        return p is not None and not p.startswith("+")

    # --- serial / startup ---

    def init_serial(self):
        ok = super().init_serial()
        if not ok or not self.serial_conn or not self.serial_conn.is_open:
            self._did_startup_for_connection = False
            return False

        if not self._did_startup_for_connection:
            try:
                ident = self._transact("?IF")
                if ident:
                    self.logger.info(f"LDD identified: {ident.strip()}")

                if self.setc is not None:
                    ok = self._write_f32(2102, float(self.setc))
                    self.logger.info(f"SETC {float(self.setc):.3f} -> {'OK' if ok else 'FAILED'}")

                if self.sett is not None:
                    ok = self._write_f32(4000, float(self.sett))
                    self.logger.info(f"SETT {float(self.sett):.2f} -> {'OK' if ok else 'FAILED'}")

                self._did_startup_for_connection = True
            except Exception as e:
                self.logger.warning(f"LDD startup failed: {e}")

        return True

    # --- polling ---

    def read_serial_data(self):
        """Poll all parameters once per poll_interval; returns a full row."""
        now = time.time()
        if now - self._last_poll < self.poll_interval:
            return None
        self._last_poll = now

        try:
            row = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
            got_any = False
            for par_id, kind in self.PARAMS:
                v = self._read_param(par_id, kind)
                if v is None:
                    row.append("nan")
                elif kind == 'i':
                    row.append(v)
                else:
                    # Shortest decimal that round-trips the device's float32
                    # exactly: no rounding, no spurious float64 digits.
                    row.append(str(np.float32(v)))
                if v is not None:
                    got_any = True

            if not got_any:
                self.logger.warning("LDD poll: no parameter answered")
                self.consecutive_failures += 1
                return None

            return row

        except Exception as e:
            self.logger.error(f"LDD poll error: {e}")
            self.consecutive_failures += 1
            try:
                if self.serial_conn:
                    self.serial_conn.close()
            except Exception:
                pass
            self.serial_conn = None
            return None

    def parse_data(self, data):
        # read_serial_data already returns a finished row
        return data if isinstance(data, list) else None


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
        'LDD_USB': LDDUSBSensor,
        'Cavity': CavitySensor,
        'Generic': GenericSensor  # Fallback
    }
    
    sensor_class = sensor_classes.get(sensor_type, GenericSensor)
    return sensor_class(name, config)