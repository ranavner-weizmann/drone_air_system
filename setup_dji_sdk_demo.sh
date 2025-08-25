#!/bin/bash

# setup_dji_sdk_demo.sh
# Creates, enables, and starts the dji_sdk_demo systemd service

set -e

SERVICE_NAME="dji_sdk_demo.service"
SERVICE_PATH="/etc/systemd/system/${SERVICE_NAME}"

echo "[*] Creating systemd service file at ${SERVICE_PATH}..."

sudo tee "${SERVICE_PATH}" > /dev/null <<EOF
[Unit]
Description=DJI SDK Demo on Raspberry Pi
After=multi-user.target

[Service]
Type=simple
User=rsp
WorkingDirectory=/home/rsp/Payload-SDK/build/bin
Environment="VIRTUAL_ENV=/home/rsp/.venv"
Environment="PATH=/home/rsp/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ExecStart=/home/rsp/Payload-SDK/build/bin/dji_sdk_demo_on_rpi
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

echo "[*] Reloading systemd daemon..."
sudo systemctl daemon-reload

echo "[*] Enabling ${SERVICE_NAME} to start on boot..."
sudo systemctl enable "${SERVICE_NAME}"

echo "[*] Starting ${SERVICE_NAME}..."
sudo systemctl start "${SERVICE_NAME}"

echo ""
echo "[✓] Service '${SERVICE_NAME}' has been installed, enabled, and started."
echo ""
echo "Useful commands:"
echo "  sudo systemctl status  ${SERVICE_NAME}   # Check service status"
echo "  sudo systemctl stop    ${SERVICE_NAME}   # Stop the service"
echo "  sudo systemctl restart ${SERVICE_NAME}   # Restart the service"
echo "  sudo journalctl -u     ${SERVICE_NAME} -f  # Follow live logs"