# spectro_hdf5.py
"""
Spectrometer with HDF5 storage for full spectra and CSV for summary data
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
import numpy as np
import h5py

class HDF5Spectrometer:
    def __init__(self, summary_interval=60):
        """
        Args:
            summary_interval: How often to save summary to CSV (seconds)
        """
        self.spec = None
        self.running = True
        self.consecutive_failures = 0
        self.max_failures = 3
        self.reconnect_delay = 2
        self.summary_interval = summary_interval
        
        # Create timestamp for all output files
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Output files
        self.summary_csv = f'output/spectro_summary_{self.timestamp}.csv'
        self.hdf5_file = f'output/spectro_full_{self.timestamp}.h5'
        
        # Data buffers
        self.wavelengths = None
        self.spectra_buffer = []  # Buffer for intensity arrays
        self.timestamps_buffer = []  # Buffer for timestamps
        self.buffer_size = 100  # Number of spectra to buffer before writing to HDF5
        
        # Summary statistics
        self.summary_data = []
        
        self.setup_logging()
        signal.signal(signal.SIGINT, self.signal_handler)
    
    def setup_logging(self):
        logging.basicConfig(
            format='%(asctime)s Spectrometer: %(message)s',
            level=logging.INFO,
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(f'output/spectro_log_{self.timestamp}.log')
            ]
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
            
            self.spec = sb.Spectrometer(devices[0])
            self.spec.integration_time_micros(100000)  # 100ms
            
            # Get wavelengths once (they're fixed for the device)
            self.wavelengths = self.spec.wavelengths()
            self.num_pixels = len(self.wavelengths)
            
            # Initialize HDF5 file with wavelengths dataset
            self.init_hdf5_file()
            
            self.logger.info(f"Connected to: {self.spec.model}")
            self.logger.info(f"Number of pixels: {self.num_pixels}")
            self.logger.info(f"Wavelength range: {self.wavelengths[0]:.1f} nm to {self.wavelengths[-1]:.1f} nm")
            self.logger.info(f"HDF5 file: {self.hdf5_file}")
            self.logger.info(f"Summary CSV: {self.summary_csv}")
            
            self.consecutive_failures = 0
            return True
            
        except Exception as e:
            self.logger.warning(f"Connection failed: {e}")
            self.consecutive_failures += 1
            return False
    
    def init_hdf5_file(self):
        """Initialize HDF5 file with proper structure"""
        with h5py.File(self.hdf5_file, 'w') as f:
            # Store wavelengths as a fixed dataset (these don't change)
            f.create_dataset('wavelengths', data=self.wavelengths, compression='gzip')
            f['wavelengths'].attrs['units'] = 'nanometers'
            f['wavelengths'].attrs['description'] = 'Wavelength values for each pixel'
            
            # Create resizable datasets for intensities and timestamps
            # We'll use maxshape=(None,) to allow unlimited growth
            f.create_dataset('intensities', 
                           shape=(0, self.num_pixels),
                           maxshape=(None, self.num_pixels),
                           dtype=np.float32,
                           compression='gzip',
                           chunks=(100, self.num_pixels))
            
            f.create_dataset('timestamps', 
                           shape=(0,),
                           maxshape=(None,),
                           dtype=h5py.special_dtype(vlen=str),
                           compression='gzip')
            
            # Add metadata
            f.attrs['creation_date'] = datetime.now().isoformat()
            f.attrs['instrument_model'] = str(self.spec.model) if self.spec else 'Unknown'
            f.attrs['integration_time'] = 100  # ms
            f.attrs['num_pixels'] = self.num_pixels
    
    def append_to_hdf5(self):
        """Append buffered spectra to HDF5 file"""
        if not self.spectra_buffer:
            return
        
        try:
            with h5py.File(self.hdf5_file, 'a') as f:
                # Get current sizes
                n_existing = f['intensities'].shape[0]
                n_new = len(self.spectra_buffer)
                
                # Resize datasets
                f['intensities'].resize((n_existing + n_new, self.num_pixels))
                f['timestamps'].resize((n_existing + n_new,))
                
                # Write new data
                f['intensities'][n_existing:n_existing + n_new] = self.spectra_buffer
                f['timestamps'][n_existing:n_existing + n_new] = self.timestamps_buffer
                
                # Update metadata
                f.attrs['last_update'] = datetime.now().isoformat()
                f.attrs['total_spectra'] = n_existing + n_new
            
            self.logger.debug(f"Appended {n_new} spectra to HDF5 (total: {n_existing + n_new})")
            
            # Clear buffers
            self.spectra_buffer.clear()
            self.timestamps_buffer.clear()
            
        except Exception as e:
            self.logger.error(f"Error writing to HDF5: {e}")
    
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
            
            # Calculate summary statistics
            max_intensity = intensities.max()
            peak_wavelength = self.wavelengths[intensities.argmax()]
            
            return {
                'intensities': intensities.astype(np.float32),
                'peak_wavelength': peak_wavelength,
                'max_intensity': max_intensity,
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            self.logger.warning(f"Measurement failed: {e}")
            self.consecutive_failures += 1
            self.spec = None
            return None
    
    def init_summary_csv(self):
        """Initialize summary CSV file"""
        Path('output').mkdir(exist_ok=True)
        
        with open(self.summary_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Timestamp', 'peak_wavelength', 'max_intensity', 
                'mean_intensity', 'std_intensity', 'total_points', 'status'
            ])
    
    def save_summary(self):
        """Save summary statistics to CSV"""
        if not self.summary_data:
            return
        
        with open(self.summary_csv, 'a', newline='') as f:
            writer = csv.writer(f)
            for row in self.summary_data:
                writer.writerow(row)
        
        self.logger.info(f"Saved {len(self.summary_data)} summary records to CSV")
        self.summary_data.clear()
    
    def run(self):
        """Main data collection loop"""
        self.logger.info(f"Starting Spectrometer data collection")
        
        # Initialize summary CSV
        self.init_summary_csv()
        
        # Initial connection
        measurement_count = 0
        last_connection_attempt = time.time()
        last_summary_save = time.time()
        last_buffer_flush = time.time()
        
        # Wait for initial connection
        while not self.spec and self.running:
            current_time = time.time()
            if current_time - last_connection_attempt >= 5:
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
                
                # Data collection
                spectrum = self.get_spectrum()
                
                if spectrum:
                    measurement_count += 1
                    
                    # Buffer spectrum for HDF5
                    self.spectra_buffer.append(spectrum['intensities'])
                    self.timestamps_buffer.append(spectrum['timestamp'].isoformat())
                    
                    # Calculate additional statistics
                    mean_intensity = spectrum['intensities'].mean()
                    std_intensity = spectrum['intensities'].std()
                    
                    # Add to summary buffer
                    self.summary_data.append([
                        spectrum['timestamp'].strftime("%Y-%m-%d %H:%M:%S"),
                        f"{spectrum['peak_wavelength']:.4f}",
                        f"{spectrum['max_intensity']:.2f}",
                        f"{mean_intensity:.2f}",
                        f"{std_intensity:.2f}",
                        len(spectrum['intensities']),
                        'success'
                    ])
                    
                    # Log every 10 measurements
                    if measurement_count % 10 == 0:
                        self.logger.info(
                            f"Scan {measurement_count}: Peak {spectrum['peak_wavelength']:.1f} nm, "
                            f"Max intensity {spectrum['max_intensity']:.0f}"
                        )
                    
                    self.consecutive_failures = 0
                
                # Periodic operations
                current_time = time.time()
                
                # Flush buffer to HDF5 when full or every 30 seconds
                if (len(self.spectra_buffer) >= self.buffer_size or 
                    current_time - last_buffer_flush >= 30):
                    self.append_to_hdf5()
                    last_buffer_flush = current_time
                
                # Save summary to CSV periodically
                if current_time - last_summary_save >= self.summary_interval:
                    self.save_summary()
                    last_summary_save = current_time
                
                # Failure handling
                if self.consecutive_failures >= self.max_failures:
                    self.logger.warning("Too many failures, attempting recovery...")
                    self.spec = None
                    
                    # Flush any remaining data before reconnecting
                    if self.spectra_buffer:
                        self.append_to_hdf5()
                    
                    time.sleep(self.reconnect_delay)
                
                time.sleep(1)  # Collect data every second
                
        except KeyboardInterrupt:
            self.logger.info(f"Keyboard interrupt received")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
        finally:
            # Final flush on shutdown
            if self.spectra_buffer:
                self.append_to_hdf5()
            if self.summary_data:
                self.save_summary()
            
            if self.spec:
                try:
                    self.spec.close()
                except:
                    pass
            
            self.logger.info(f"Stopped after {measurement_count} measurements")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Spectrometer with HDF5 storage')
    parser.add_argument('--summary-interval', type=int, default=60,
                       help='Interval for saving summary to CSV (seconds)')
    parser.add_argument('--buffer-size', type=int, default=100,
                       help='Number of spectra to buffer before writing to HDF5')
    
    args = parser.parse_args()
    
    spec = HDF5Spectrometer(summary_interval=args.summary_interval)
    spec.buffer_size = args.buffer_size
    spec.run()

if __name__ == "__main__":
    main()