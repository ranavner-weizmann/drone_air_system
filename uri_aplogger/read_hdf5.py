# read_spectro_hdf5.py
"""
Read and analyze HDF5 spectrometer data
"""

import h5py
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

def inspect_hdf5_file(filename):
    """Inspect HDF5 file structure"""
    with h5py.File(filename, 'r') as f:
        print("=== HDF5 File Structure ===")
        print(f"File: {filename}")
        print(f"Created: {f.attrs.get('creation_date', 'Unknown')}")
        print(f"Total spectra: {f.attrs.get('total_spectra', 0)}")
        
        def print_item(name, obj):
            if isinstance(obj, h5py.Dataset):
                print(f"  Dataset: {name}, shape: {obj.shape}, dtype: {obj.dtype}")
                for attr_name, attr_value in obj.attrs.items():
                    print(f"    {attr_name}: {attr_value}")
            elif isinstance(obj, h5py.Group):
                print(f"  Group: {name}")
        
        f.visititems(print_item)

def plot_spectrum(filename, spectrum_index=0):
    """Plot a specific spectrum from the HDF5 file"""
    with h5py.File(filename, 'r') as f:
        wavelengths = f['wavelengths'][:]
        intensities = f['intensities'][spectrum_index, :]
        timestamp = f['timestamps'][spectrum_index]
        
        plt.figure(figsize=(10, 6))
        plt.plot(wavelengths, intensities)
        plt.title(f"Spectrum {spectrum_index} - {timestamp}")
        plt.xlabel("Wavelength (nm)")
        plt.ylabel("Intensity")
        plt.grid(True)
        plt.show()
        
        print(f"Spectrum {spectrum_index}:")
        print(f"  Time: {timestamp}")
        print(f"  Max intensity: {intensities.max():.0f} at {wavelengths[intensities.argmax()]:.1f} nm")

def extract_time_series(filename, wavelength_index=1000):
    """Extract intensity time series at a specific wavelength"""
    with h5py.File(filename, 'r') as f:
        wavelengths = f['wavelengths'][:]
        intensities = f['intensities'][:, wavelength_index]
        timestamps = f['timestamps'][:]
        
        # Convert timestamps to datetime objects
        times = [datetime.fromisoformat(ts.decode() if isinstance(ts, bytes) else ts) 
                for ts in timestamps]
        
        plt.figure(figsize=(10, 6))
        plt.plot(times, intensities)
        plt.title(f"Intensity at {wavelengths[wavelength_index]:.1f} nm over time")
        plt.xlabel("Time")
        plt.ylabel("Intensity")
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()

def export_to_csv(filename, output_csv, max_spectra=100):
    """Export a subset of data to CSV"""
    with h5py.File(filename, 'r') as f:
        wavelengths = f['wavelengths'][:]
        intensities = f['intensities'][:max_spectra, :]
        timestamps = f['timestamps'][:max_spectra]
        
        # Create header
        header = ['timestamp'] + [f'wavelength_{i}_{w:.2f}' for i, w in enumerate(wavelengths)]
        
        # Write to CSV
        import csv
        with open(output_csv, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(header)
            
            for i, (ts, intensity_row) in enumerate(zip(timestamps, intensities)):
                row = [ts] + list(intensity_row)
                writer.writerow(row)
        
        print(f"Exported {len(intensities)} spectra to {output_csv}")

if __name__ == "__main__":
    # Example usage
    filename = "output/spectro_full_20251221_142542.h5"
    
    # 1. Inspect file
    inspect_hdf5_file(filename)
    
    # 2. Plot first spectrum
    plot_spectrum(filename, 0)
    
    # 3. Extract time series
    extract_time_series(filename, 1000)
    
    # 4. Export to CSV (first 100 spectra)
    export_to_csv(filename, "output/spectro/spectra_subset.csv", 100)