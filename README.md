# drone_air_system

## General Idea:
Multiple services are operating to collect the connected sensors' data.  
The data is saved LOCALLY on the Pi.
In addition, some of the data can be sent in real-time to the drone's RC using the PSDK.
This README file explains how to set up the Pi for both tasks.  
There are 2 directories: drone_air_system and Payload-SDK.  
As you already guessed, the Payload-SDK contains the PSDK repo, and the drone_air_system contains the sensor collection services.

## First Part - Setting up the Raspberry Pi

1. Use the Raspberry Pi Imager to burn this image: one-drive link (this is a pre-build img that DJI supplies, there is no guarantee that it will hold forever and ever)
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
2. After running the script, the whole system should be transparent; it should start automatically when the Pi is connected to power.

### Explanations

The system works as follows:
to be filled by Uri
The data to be transmitted to the RC is located at: /home/rsp/drone_air_system/data_to_sdk/vitals.csv


