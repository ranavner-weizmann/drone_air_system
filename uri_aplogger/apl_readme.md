File list:
- `runall.py` - Master script, handles running and stopping all other scripts.
- `sensor_runner.py` - Instantiates the sensor reader. Can be individually ran with the name of the sensor as an argument. "python sensor_runner.py [sensor name]"
- `sensor_implementations.py` - Sensor-specific settings for reading data from each sensor. Includes a class for every sensor (iMetSensor, POMSensor, TriSonicaSensor, Partector2ProSensor), which all extend GenericSensor.
- `generic_sensor.py` - Class that includes all shared aspects of reading data, such as connecting by serial or writing data to csv.
- `spectro_hdf5.py` and `read_hdf5.py` - Outlier sensor that reads data with a custom library, and not by serial, required a separate class. Outputs both to csv and HDF5 for lighter file sizes and better reading of spectra.
- `real_time_merger.py` - Finds all individual csvs from the `/output` folder, and merges them into a single csv.
- `sensor_config.json` - File that instructs `runall.py` on which sensors to run or not to run, with which headers, vendor/model ID, etc. 

Obsolete:
- Files in the `/obsolete` folder - Legacy files that are, titularly, obsolete. 

## Utilisation:
As long as everything is functional, the only file you need to run is `runall.py`.
For individual debugging, as stated before, run `sensor_runner.py` and pass the sensor name as an attribute.

