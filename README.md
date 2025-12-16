# drone_air_system

1. Use the raspberry pi imager to burn this image: one-drive link
2. Set up the pi's country and connect to Wi-Fi (ssh is already enabled).
3. Get the IP using ifconfig and ssh the pi from remote.
4. Generate a git public key by running: ssh-keygen -t ed25519 -C "your_email@example.com"
5. go to /home/rsp/.ssh/id_ed25519.pub
6. You will get something like: 

ssh-ed25519 asdfvsdflvjsdktuyshbekruvhbysev your_email@example.com

Copy the public key, including the email, and ask Ran to add it to the SSH-Keys for Git access.

6. git clone git@github.com:ranavner-weizmann/Payload-SDK.git
7. git clone this repo.
