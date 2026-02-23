# drone_air_system

## General Idea:
Multiple services are operating to collect the connected sensors' data.    
The data is saved LOCALLY on the Pi.  
In addition, some of the data can be sent in real-time to the drone's RC using the PSDK.  
This README file explains how to set up the Pi for both tasks.   
There are 2 directories: drone_air_system and Payload-SDK.   
As you already guessed, the Payload-SDK contains the PSDK repo, and the drone_air_system contains the sensor collection services.  

## First Part - Setting up the Raspberry Pi  

1. Use the Raspberry Pi Imager to burn this image: one-drive link: https://weizmannacil-my.sharepoint.com/:u:/g/personal/ran_avner_weizmann_ac_il/IQCMBK04UkB3S5S6SL3bpX7cASmTtCo0KmFstAWR4B8dJWA?e=aaW2aQ (this is a pre-build img that DJI supplies, there is no guarantee that it will hold forever and ever)  
2. The password is *rsp*  
3. Set up the Pi's country and connect to Wi-Fi (ssh is already enabled).  
4. Get the IP using ifconfig and ssh the Pi from a remote pc.  
5. Generate a git public key by running: ssh-keygen -t ed25519 -C "your_email@example.com"  
6. go to /home/rsp/.ssh/id_ed25519.pub  
7. You will get something like:   

ssh-ed25519 asdfvsdflvjsdktuyshbekruvhbysev your_email@example.com  

Copy the public key, including the email, and ask Ran to add it to the SSH-Keys for Git access.  

6. git clone git@github.com:ranavner-weizmann/Payload-SDK.git  
   Note: This repo is forked from the DJI main branch. It has been modified so it'll work with the USB-BULK configuration,  
   and it has my (Ran) app credentials that were created through the DJI Developer Service.  
   In the future, if needed, this info needs to be updated in the app_config file.   
8. git clone this repo.  

## Second Part - Activating and operating the system  

### First Setup  
1. Inside the drone_air_system folder, there is a script called 'first_configuration.sh' - run it with sudo  
NEED TO ADD CMAKE INSTALLATION 
2. After running the script, the whole system should be transparent; it should start automatically when the Pi is connected to power.  
 
### Explanations  

The system works as follows:  

## File list:
- `runall.py` - Master script, handles running and stopping all other scripts.
- `sensor_runner.py` - Instantiates the sensor reader. Can be individually ran with the name of the sensor as an argument. "python sensor_runner.py [sensor name]"
- `sensor_implementations.py` - Sensor-specific settings for reading data from each sensor. Includes a class for every sensor (iMetSensor, POMSensor, TriSonicaSensor, Partector2ProSensor, POPSSensor, LDDSensor), which all extend GenericSensor.
- `generic_sensor.py` - Class that includes all shared aspects of reading data, such as connecting by serial or writing data to csv.
- `spectro_hdf5.py` and `read_hdf5.py` - Outlier sensor that reads data with a custom library, and not by serial, required a separate class. Outputs both to csv and HDF5 for lighter file sizes and better reading of spectra.
- `real_time_merger.py` - Finds all individual csvs from the `/output` folder, and merges them into a single csv.
- `sensor_config.json` - File that instructs `runall.py` on which sensors to run or not to run, with which headers, vendor/model ID, etc. 
- `vitals.py` - Writes the stripped down version of only the essential data from the drone in real time.
- `vx.py` - Checks if all drones are live or dead. (V/X)

## Obsolete:
- Files in the `/obsolete` folder - Legacy files that are, titularly, obsolete. 

## Utilisation:
As long as everything is functional, the only file you need to run is `runall.py`.
For individual debugging, as stated before, run `sensor_runner.py` and pass the sensor name as an attribute.

### Special case:
The Sensor allows for sending commands in real time.
To do that, first run `cat > output/ldd/cmd.fifo` in the terminal, while it's located in the logger directory.
Now, sending commands is as easy as typing them and pressing enter.
Valid commands:
- PING (Returns: `OK PONG`.)
- GET (Returns: all LDD values. Used by the script to create the csv. Also returns: `OK GET`.)
- RESET (Resets LDD. Returns: `OK RESET` if successful, `ERR RESET` if failed.)
- SETC <amps> (Expects: Float argument. Sets the current of the LDD. Returns: `OK SETC` if successful, `ERR SETC` if failed.)
- SETT <degC> (Expects: Float argument. Sets the target temperature of the object. Returns: `OK SETT` if successful, `ERR SETC` if failed.)

The data to be transmitted to the RC is located at: /home/rsp/drone_air_system/data_to_sdk/vitals.csv  


