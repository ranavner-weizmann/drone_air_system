#!/usr/bin/env python3
"""
Complete Master Controller - Manages both sensors and merger
"""

import multiprocessing
import time
import logging
import signal
import sys
import os
from pathlib import Path
from datetime import datetime
import json
import subprocess

class CompleteSensorManager:
    """Manages all sensor processes and merger using generic approach"""
    
    def __init__(self, config_file='sensor_config.json'):
        self.config = self.load_config(config_file)
        self.sensor_processes = {}
        self.merger_process = None
        self.running = False
        
        signal.signal(signal.SIGINT, self.signal_handler)
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(), logging.FileHandler('sensor_controller.log')]
        )
        self.logger = logging.getLogger('SensorManager')

    def load_config(self, config_file):
        """Load sensor configuration"""
        try:
            with open(config_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading config: {e}")
            return {'sensors': {}, 'merger': {}}

    def start_sensor(self, name, config):
        """Start a single sensor process"""
        if not config.get('enabled', True):
            self.logger.info(f"Sensor {name} is disabled")
            return None
            
        script_path = config['script']
        
        # Use sensor_runner.py for generic sensors, specific script for others
        if script_path == 'sensor_runner.py':
            args = [sys.executable, script_path, name]
        else:
            args = [sys.executable, script_path]
        
        try:
            process = subprocess.Popen( # Start sensor process
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            self.logger.info(f"Started {name} (PID: {process.pid})")
            return process
        except Exception as e:
            self.logger.error(f"Failed to start {name}: {e}")
            return None

    def start_merger(self):
        """Start the data merger process"""
        merger_config = self.config.get('merger', {})
        if not merger_config.get('enabled', True):
            self.logger.info("Merger is disabled")
            return None
            
        # Use the new real-time merger
        script_path = merger_config.get('script', 'real_time_merger.py')
        
        try:
            # Get interval from config or use default
            interval = merger_config.get('interval', 1.0)
            
            process = subprocess.Popen(
                [sys.executable, script_path, '--interval', str(interval)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            self.logger.info(f"Started real-time merger (PID: {process.pid}) with interval {interval}s")
            return process
        except Exception as e:
            self.logger.error(f"Failed to start merger: {e}")
            return None
        
    def start_all(self):
        """Start all enabled sensors and merger"""
        self.logger.info("Starting all sensor processes...")
        
        # Start sensors first
        for name, config in self.config['sensors'].items():
            process = self.start_sensor(name, config)
            if process:
                self.sensor_processes[name] = process
                # Wait for sensor initialization
                startup_delay = config.get('startup_delay', 2)
                self.logger.info(f"Waiting {startup_delay}s for {name} to initialize...")
                time.sleep(startup_delay)
        
        # Start merger after sensors (with additional delay)
        merger_delay = self.config.get('merger', {}).get('startup_delay', 10)
        self.logger.info(f"Waiting {merger_delay}s before starting merger...")
        time.sleep(merger_delay)
        
        self.merger_process = self.start_merger()
        
        self.running = True
        self.logger.info("All processes started")

    def stop_all(self):
        """Stop all sensor processes and merger"""
        self.logger.info("Stopping all processes...")
        self.running = False
        
        # Stop merger first
        if self.merger_process and self.merger_process.poll() is None:
            self.logger.info("Stopping merger process...")
            self.merger_process.terminate()
            try:
                self.merger_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.merger_process.kill()
            self.logger.info("Merger stopped")
        
        # Stop sensors
        for name, process in self.sensor_processes.items():
            if process.poll() is None:  # Still running
                self.logger.info(f"Stopping {name}...")
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                self.logger.info(f"Stopped {name}")

    def monitor_processes(self):
        """Monitor and restart failed processes"""
        # Monitor sensors
        for name, process in list(self.sensor_processes.items()):
            if process.poll() is not None:  # Process ended
                exit_code = process.returncode
                self.logger.warning(f"Sensor {name} died (exit code: {exit_code}), restarting...")
                config = self.config['sensors'][name]
                new_process = self.start_sensor(name, config)
                if new_process:
                    self.sensor_processes[name] = new_process
        
        # Monitor merger
        if (self.merger_process and 
            self.merger_process.poll() is not None and 
            self.config.get('merger', {}).get('enabled', True)):
            
            exit_code = self.merger_process.returncode
            self.logger.warning(f"Merger died (exit code: {exit_code}), restarting...")
            new_merger = self.start_merger()
            if new_merger:
                self.merger_process = new_merger

    def get_status(self):
        """Get current status of all processes"""
        status = {
            'timestamp': datetime.now().isoformat(),
            'sensors': {},
            'merger': None
        }
        
        for name, process in self.sensor_processes.items():
            status['sensors'][name] = {
                'running': process.poll() is None,
                'pid': process.pid,
                'exit_code': process.returncode
            }
        
        if self.merger_process:
            status['merger'] = {
                'running': self.merger_process.poll() is None,
                'pid': self.merger_process.pid,
                'exit_code': self.merger_process.returncode
            }
            
        return status

    def signal_handler(self, sig, frame):
        """Handle shutdown signals"""
        self.logger.info("Shutting down...")
        self.stop_all()
        sys.exit(0)

    def run(self):
        """Main monitoring loop"""
        self.start_all()
        
        self.logger.info("Sensor controller running. Press Ctrl+C to stop.")
        
        last_status_report = time.time()
        
        try:
            while self.running:
                self.monitor_processes()
                
                # Print status every 60 seconds
                if time.time() - last_status_report >= 60:
                    status = self.get_status()
                    self.logger.info("=== Status Report ===")
                    
                    for sensor_name, sensor_status in status['sensors'].items():
                        state = "RUNNING" if sensor_status['running'] else "STOPPED"
                        exit_info = f"(Exit: {sensor_status.get('exit_code', 'N/A')})"
                        self.logger.info(f"  {sensor_name}: {state} {exit_info}")
                    
                    if status['merger']:
                        state = "RUNNING" if status['merger']['running'] else "STOPPED"
                        self.logger.info(f"  Merger: {state}")
                    
                    self.logger.info("=====================")
                    last_status_report = time.time()
                
                time.sleep(5)  # Check every 5 seconds
                
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received")
        except Exception as e:
            self.logger.error(f"Unexpected error in main loop: {e}")
        finally:
            self.stop_all()

if __name__ == "__main__":
    manager = CompleteSensorManager()
    manager.run()
