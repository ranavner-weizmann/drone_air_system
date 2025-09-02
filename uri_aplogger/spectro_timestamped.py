# spectro_timestamped.py
"""
Spectrometer with timestamped output files
"""

import seabreeze
seabreeze.use('pyseabreeze')
import seabreeze.spectrometers as sb
import time
import csv
from datetime import datetime
import logging
import signal
import sys
from pathlib import Path

class TimestampedOceanSR6Reader:
    def __init__(self):
        self.spec = None
        self.running = True
        self.consecutive_failures = 0
        self.max_failures = 3
        self.reconnect_delay = 2
        
        # Create timestamped output file
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_file = f'output/spectro_data_{self.timestamp}.csv'
        
        self.setup_logging()
        signal.signal(signal.SIGINT, self.signal_handler)
    
    def setup_logging(self):
        logging.basicConfig(
            format='%(asctime)s Spectrometer: %(message)s',
            level=logging.INFO
        )
        self.logger = logging.getLogger()
    
    def signal_handler(self, sig, frame):
        self.logger.info("Stopping Spectrometer data collection...")
        self.running = False
    
    def connect(self):
        """Connect to OceanSR6"""
        try:
            devices = sb.list_devices()
            if not devices:
                self.logger.warning("No spectrometers found!")
                return False
            
            self.spec = sb.Spectrometer(devices[0])
            self.spec.integration_time_micros(100000)  # 100ms
            self.logger.info(f"Connected to: {self.spec.model}")
            
            self.consecutive_failures = 0
            return True
            
        except Exception as e:
            self.logger.warning(f"Connection failed: {e}")
            self.consecutive_failures += 1
            return False
    
    def get_spectrum(self):
        """Get spectrum with error recovery"""
        if not self.spec:
            return None
        
        try:
            wavelengths = self.spec.wavelengths()
            intensities = self.spec.intensities()
            
            if len(wavelengths) == 0 or len(intensities) == 0:
                self.logger.warning("Empty spectrum data")
                self.consecutive_failures += 1
                return None
            
            return {
                'wavelengths': wavelengths,
                'intensities': intensities,
                'peak_wavelength': wavelengths[intensities.argmax()],
                'max_intensity': intensities.max(),
                'total_points': len(wavelengths),
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            self.logger.warning(f"Measurement failed: {e}")
            self.consecutive_failures += 1
            self.spec = None
            return None
    
    def run(self):
        """Main data collection loop"""
        self.logger.info(f"Starting Spectrometer data collection. Output: {self.output_file}")
        
        # Create output directory if needed
        Path('output').mkdir(exist_ok=True)
        
        # Setup CSV file with proper headers for merger
        try:
            csv_file = open(self.output_file, 'w', newline='')
            writer = csv.writer(csv_file)
            writer.writerow(['Timestamp', 'peak_wavelength', 'max_intensity', 'total_points', 'status'])
        except Exception as e:
            self.logger.error(f"Failed to create CSV file: {e}")
            return

        measurement_count = 0
        last_connection_attempt = time.time()
        
        try:
            while self.running:
                # Connection management
                if not self.spec:
                    current_time = time.time()
                    if current_time - last_connection_attempt >= 10:
                        self.logger.info("Attempting to connect to spectrometer...")
                        if self.connect():
                            self.logger.info("Connected to spectrometer")
                        last_connection_attempt = current_time
                    time.sleep(1)
                    continue
                
                # Data collection
                spectrum = self.get_spectrum()
                
                if spectrum:
                    measurement_count += 1
                    self.logger.info(
                        f"Scan {measurement_count}: Peak {spectrum['peak_wavelength']:.1f} nm, "
                        f"Intensity {spectrum['max_intensity']:.0f}"
                    )
                    
                    # Save to CSV in merger-compatible format
                    writer.writerow([
                        spectrum['timestamp'].strftime("%Y-%m-%d %H:%M:%S"),
                        f"{spectrum['peak_wavelength']:.4f}",
                        f"{spectrum['max_intensity']:.2f}",
                        spectrum['total_points'],
                        'success'
                    ])
                    csv_file.flush()
                    self.consecutive_failures = 0

                # Failure handling
                if self.consecutive_failures >= self.max_failures:
                    self.logger.warning("Too many failures, attempting recovery...")
                    self.spec = None
                    time.sleep(self.reconnect_delay)
                
                time.sleep(1)  # Collect data every second
                
        except KeyboardInterrupt:
            self.logger.info(f"Stopped by user after {measurement_count} measurements")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
        finally:
            if self.spec:
                try:
                    self.spec.close()
                except:
                    pass
            if csv_file:
                csv_file.close()

def main():
    TimestampedOceanSR6Reader().run()

if __name__ == "__main__":
    main()
