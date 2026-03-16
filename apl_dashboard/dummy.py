import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import os

OUTPUT_FILE = "merged_data_latest.csv"

def generate_row(t):
    return {
        "merge_timestamp": t.strftime("%Y-%m-%d %H:%M:%S"),

        # Spectro
        "spectro_peak_wavelength": np.random.normal(532, 2),
        "spectro_max_intensity": np.random.normal(5000, 500),
        "spectro_mean_intensity": np.random.normal(2000, 200),
        "spectro_std_intensity": np.random.normal(300, 50),

        # Partector
        "partector_temperature_C": np.random.normal(25, 1),
        "partector_relative_humidity_percent": np.random.normal(50, 5),
        "partector_LDSA_um2_cm3": np.random.normal(100, 20),
        "partector_diameter_nm": np.random.normal(80, 10),
        "partector_number_1_cm3": np.random.normal(10000, 2000),

        # Cavity (merged LDD + pump)
        "cavity_LaserPower": np.random.normal(1.5, 0.1),
        "cavity_DeviceTemperature": np.random.normal(30, 2),
        "cavity_pressure_mb": np.random.normal(1013, 5),
        "cavity_temp_c": np.random.normal(26, 1),
        "cavity_humidity_pct": np.random.normal(45, 5),
        "cavity_pump_rpm": np.random.normal(3000, 200),
    }

def main():
    print("Generating dummy data...")

    # Start from now
    t = datetime.now()

    df = pd.DataFrame()

    while True:
        row = generate_row(t)
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

        # Keep file from growing forever (optional)
        df = df.tail(1000)

        df.to_csv(OUTPUT_FILE, index=False)

        print(f"Generated row at {t}")

        # Increment time
        t += timedelta(seconds=1)

        time.sleep(1)

if __name__ == "__main__":
    main()
