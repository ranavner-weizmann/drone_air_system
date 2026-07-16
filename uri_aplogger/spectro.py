"""
Spectrometer with CSV storage for full spectra and CSV for summary data
"""

import seabreeze
seabreeze.use('pyseabreeze')
import seabreeze.spectrometers as sb
import time
import csv
from datetime import datetime
import logging
import signal
from pathlib import Path
import numpy as np

from run_paths import get_csv_dir, get_log_dir


class CSVSpectrometer:
    def __init__(self, summary_interval=60):
        """
        Args:
            summary_interval: (kept for CLI compatibility; summary is written immediately in this script)
        """
        self.spec = None
        self.running = True
        self.consecutive_failures = 0
        self.max_failures = 3
        self.reconnect_delay = 10
        self.summary_interval = summary_interval

        # Resolve per-run output dirs (created by run_paths)
        csv_dir = get_csv_dir()

        # Create timestamp for all output files
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Output files
        self.summary_csv = str(csv_dir / f"spectro_summary_{self.timestamp}.csv")
        self.full_csv = str(csv_dir / f"spectro_full_{self.timestamp}.csv")

        # Data buffers
        self.wavelengths = None
        self.num_pixels = 0
        self.spectra_buffer = []       # list[np.ndarray]
        self.timestamps_buffer = []    # list[str]
        self.buffer_size = 100         # spectra to buffer before flushing to full CSV

        self.setup_logging()
        signal.signal(signal.SIGINT, self.signal_handler)

    def setup_logging(self):
        log_path = get_log_dir() / f"spectro_log_{self.timestamp}.log"
        logging.basicConfig(
            format="%(asctime)s Spectrometer: %(message)s",
            level=logging.INFO,
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(str(log_path)),
            ],
        )
        self.logger = logging.getLogger()

    def signal_handler(self, sig, frame):
        self.logger.info("Stopping Spectrometer data collection...")
        self.running = False

    def connect(self):
        """Connect to OceanSR6 and get wavelengths"""
        try:
            devices = sb.list_devices()
            if not devices:
                self.logger.warning("No spectrometers found!")
                return False

            # pyseabreeze resets the device as a side effect of list_devices()
            # (it opens/closes each device to read its serial number). Opening
            # again before the SR6 finishes recovering from that reset fails
            # with [Errno 19] No such device, so give it time to come back.
            time.sleep(2)

            self.spec = sb.Spectrometer(devices[0])
            self.spec.integration_time_micros(250000)  # 250ms

            # Get wavelengths once (they're fixed for the device)
            self.wavelengths = self.spec.wavelengths()
            self.num_pixels = len(self.wavelengths)

            # Initialize full spectra CSV with header
            self.init_full_csv()

            self.logger.info(f"Connected to: {self.spec.model}")
            self.logger.info(f"Number of pixels: {self.num_pixels}")
            self.logger.info(
                f"Wavelength range: {self.wavelengths[0]:.1f} nm to {self.wavelengths[-1]:.1f} nm"
            )
            self.logger.info(f"Full spectra CSV: {self.full_csv}")
            self.logger.info(f"Summary CSV: {self.summary_csv}")

            self.consecutive_failures = 0
            return True

        except Exception as e:
            self.logger.warning(f"Connection failed: {e}")
            self.consecutive_failures += 1
            return False

    def init_full_csv(self):
        """Initialize full spectra CSV with wavelength header columns"""
        # On reconnect the file already has header + data; don't overwrite it
        if Path(self.full_csv).exists():
            return

        # Header: Timestamp + one column per wavelength
        header = ["Timestamp"] + [f"{wl:.4f}" for wl in self.wavelengths]

        with open(self.full_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)

    def append_to_full_csv(self):
        """Append buffered spectra to full spectra CSV"""
        if not self.spectra_buffer:
            return

        try:
            with open(self.full_csv, "a", newline="") as f:
                writer = csv.writer(f)
                for ts, intensities in zip(self.timestamps_buffer, self.spectra_buffer):
                    # One row: timestamp + all intensities
                    writer.writerow([ts] + [f"{v:.6g}" for v in intensities])

            self.logger.info(
                f"Appended {len(self.spectra_buffer)} spectra to full CSV"
            )

            self.spectra_buffer.clear()
            self.timestamps_buffer.clear()

        except Exception as e:
            self.logger.error(f"Error writing to full CSV: {e}")
            import traceback
            self.logger.error(traceback.format_exc())

    def get_spectrum(self):
        """Get complete spectrum"""
        if not self.spec:
            return None

        try:
            intensities = self.spec.intensities()

            if len(intensities) == 0:
                self.logger.warning("Empty spectrum data")
                self.consecutive_failures += 1
                return None

            max_intensity = float(intensities.max())
            peak_wavelength = float(self.wavelengths[intensities.argmax()])

            return {
                "intensities": intensities.astype(np.float32),
                "peak_wavelength": peak_wavelength,
                "max_intensity": max_intensity,
                "timestamp": datetime.now(),
            }

        except Exception as e:
            self.logger.warning(f"Measurement failed: {e}")
            self.consecutive_failures += 1
            self.spec = None
            return None

    def init_summary_csv(self):
        """Initialize summary CSV file"""
        with open(self.summary_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "Timestamp",
                    "peak_wavelength",
                    "max_intensity",
                    "mean_intensity",
                    "std_intensity",
                    "total_points",
                    "status",
                ]
            )
        self.logger.info(f"Initialized summary CSV: {self.summary_csv}")

    def run(self):
        """Main data collection loop"""
        self.logger.info("Starting Spectrometer data collection")

        # Initialize summary CSV
        self.init_summary_csv()

        measurement_count = 0
        last_connection_attempt = time.time()
        last_full_csv_write = time.time()

        # Wait for initial connection
        while not self.spec and self.running:
            current_time = time.time()
            if current_time - last_connection_attempt >= 15:
                self.logger.info("Attempting to connect to spectrometer...")
                if self.connect():
                    break
                last_connection_attempt = current_time
            time.sleep(1)

        if not self.spec:
            self.logger.error("Failed to connect to spectrometer")
            return

        try:
            while self.running:
                current_time = time.time()

                spectrum = self.get_spectrum()

                if spectrum:
                    measurement_count += 1

                    # Buffer full spectrum for full CSV
                    self.spectra_buffer.append(spectrum["intensities"])
                    self.timestamps_buffer.append(spectrum["timestamp"].isoformat())

                    # Summary stats
                    mean_intensity = float(spectrum["intensities"].mean())
                    std_intensity = float(spectrum["intensities"].std())

                    # Write summary immediately
                    try:
                        with open(self.summary_csv, "a", newline="") as f:
                            writer = csv.writer(f)
                            writer.writerow(
                                [
                                    spectrum["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                                    f"{spectrum['peak_wavelength']:.4f}",
                                    f"{spectrum['max_intensity']:.2f}",
                                    f"{mean_intensity:.2f}",
                                    f"{std_intensity:.2f}",
                                    len(spectrum["intensities"]),
                                    "success",
                                ]
                            )
                    except Exception as e:
                        self.logger.error(f"Error writing to summary CSV: {e}")

                    if measurement_count % 10 == 0:
                        self.logger.info(
                            f"Scan {measurement_count}: Peak {spectrum['peak_wavelength']:.1f} nm, "
                            f"Max intensity {spectrum['max_intensity']:.0f}"
                        )

                    self.consecutive_failures = 0

                # Flush buffer to full CSV when full or every 10 seconds
                if (len(self.spectra_buffer) >= self.buffer_size) or (
                    current_time - last_full_csv_write >= 10
                ):
                    if self.spectra_buffer:
                        self.append_to_full_csv()
                        last_full_csv_write = current_time

                # Failure handling: reconnect if the device handle is gone
                # (a single I/O error clears self.spec) or failures pile up
                if self.spec is None or self.consecutive_failures >= self.max_failures:
                    self.logger.warning("Spectrometer disconnected or too many failures, attempting recovery...")
                    # Flush any remaining data before reconnecting
                    if self.spectra_buffer:
                        self.append_to_full_csv()

                    self.spec = None
                    time.sleep(self.reconnect_delay)

                    if self.connect():
                        self.consecutive_failures = 0

                time.sleep(0.5)

        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
        finally:
            self.logger.info("Shutting down, flushing remaining data...")
            if self.spectra_buffer:
                self.append_to_full_csv()

            if self.spec:
                try:
                    self.spec.close()
                    self.logger.info("Spectrometer closed")
                except Exception:
                    pass

            self.logger.info(f"Stopped after {measurement_count} measurements")
            self.logger.info(f"Full spectra CSV: {self.full_csv}")
            self.logger.info(f"Summary CSV: {self.summary_csv}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Spectrometer with CSV storage (full spectra + summary)")
    parser.add_argument(
        "--summary-interval",
        type=int,
        default=60,
        help="(kept for compatibility; summary is written immediately)",
    )
    parser.add_argument(
        "--buffer-size",
        type=int,
        default=100,
        help="Number of spectra to buffer before writing to full CSV",
    )

    args = parser.parse_args()

    spec = CSVSpectrometer(summary_interval=args.summary_interval)
    spec.buffer_size = args.buffer_size
    spec.run()


if __name__ == "__main__":
    main()