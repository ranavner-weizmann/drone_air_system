# drone_air_system

### First Part - Setting up the Raspberry Pi

1. Use the Raspberry Pi Imager to burn this image: one-drive link (this is a pre-build img that DJI supplies, there is no guarantee that it will hold forever and ever)
2. Set up the Pi's country and connect to Wi-Fi (ssh is already enabled).
3. Get the IP using ifconfig and ssh the Pi from a remote pc.
4. Generate a git public key by running: ssh-keygen -t ed25519 -C "your_email@example.com"
5. go to /home/rsp/.ssh/id_ed25519.pub
6. You will get something like: 

ssh-ed25519 asdfvsdflvjsdktuyshbekruvhbysev your_email@example.com

Copy the public key, including the email, and ask Ran to add it to the SSH-Keys for Git access.

6. git clone git@github.com:ranavner-weizmann/Payload-SDK.git
   Note: This repo is forked from the DJI main branch. It has been modified so it'll work with the USB-BULK configuration,
   and it has my (Ran) app credentials that were created through the DJI Developer Service.
   In the future, if needed, this info needs to be updated in the app_config file. 
8. git clone this repo.

### Second Part - Activating and operating the system

1. 
