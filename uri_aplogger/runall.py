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
        # Store the original config filename
        self.config_filename = config_file
        self.sensor_processes = {}
        self.merger_process = None
        self.running = False
        
        signal.signal(signal.SIGINT, self.signal_handler)
        
        # Load config FIRST to get the path
        self.config = self.load_initial_config()
        
        # Now change to the base directory
        self.base_path = Path(self.config.get('path', '.')).resolve()
        os.chdir(self.base_path)
        print(f"Working directory changed to: {self.base_path}")
        
        # Now that we're in the right directory, setup logging
        self.setup_logging()

    def setup_logging(self):
        """Setup logging with absolute paths"""
        # Use base_path for log file
        log_file = self.base_path / 'sensor_controller.log'
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(str(log_file))
            ]
        )
        self.logger = logging.getLogger('SensorManager')
        self.logger.info(f"Logging to: {log_file}")

    def load_initial_config(self):
        """Load sensor configuration before changing directory"""
        # Try multiple locations for the config file
        possible_paths = [
            Path(self.config_filename).resolve(),  # Absolute path
            Path(self.config_filename),  # Relative to current directory
            Path.home() / 'drone_air_system' / 'uri_aplogger' / self.config_filename,  # Common location
        ]
        
        for config_path in possible_paths:
            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                print(f"Loaded config from: {config_path}")
                return config
            except (FileNotFoundError, json.JSONDecodeError) as e:
                continue
        
        # If no config found, create a minimal default with path
        print(f"ERROR: Config file '{self.config_filename}' not found in any location")
        print("Creating default config with current directory as path")
        return {
            'sensors': {}, 
            'merger': {}, 
            'path': str(Path.cwd())
        }

    def get_absolute_script_path(self, script_name):
        """Convert script name to absolute path relative to base directory"""
        script_path = Path(script_name)
        if script_path.is_absolute():
            return str(script_path)
        else:
            # Make it relative to the base path
            absolute_path = self.base_path / script_path
            return str(absolute_path.resolve())

    def start_sensor(self, name, config):
        """Start a single sensor process"""
        if not config.get('enabled', True):
            self.logger.info(f"Sensor {name} is disabled")
            return None
            
        script_path = config['script']
        absolute_script_path = self.get_absolute_script_path(script_path)
        
        # Check if script exists
        if not os.path.exists(absolute_script_path):
            self.logger.error(f"Script not found: {absolute_script_path}")
            return None
        
        # Use sensor_runner.py for generic sensors, specific script for others
        if script_path == 'sensor_runner.py':
            args = [sys.executable, absolute_script_path, name]
        else:
            args = [sys.executable, absolute_script_path]
        
        try:
            # Use base_path as the working directory for the process
            process = subprocess.Popen(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=self.base_path  # Run from base directory
            )
            self.logger.info(f"Started {name} (PID: {process.pid}) from {absolute_script_path}")
            return process
        except Exception as e:
            self.logger.error(f"Failed to start {name} from {absolute_script_path}: {e}")
            return None

    def start_merger(self):
        """Start the data merger process"""
        merger_config = self.config.get('merger', {})
        if not merger_config.get('enabled', True):
            self.logger.info("Merger is disabled")
            return None
            
        # Use the new real-time merger
        script_path = merger_config.get('script', 'real_time_merger.py')
        absolute_script_path = self.get_absolute_script_path(script_path)
        
        # Check if script exists
        if not os.path.exists(absolute_script_path):
            self.logger.error(f"Merger script not found: {absolute_script_path}")
            return None
        
        try:
            # Get interval from config or use default
            interval = merger_config.get('interval', 1.0)
            
            process = subprocess.Popen(
                [sys.executable, absolute_script_path, '--interval', str(interval)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=self.base_path  # Run from base directory
            )
            self.logger.info(f"Started real-time merger (PID: {process.pid}) from {absolute_script_path} with interval {interval}s")
            return process
        except Exception as e:
            self.logger.error(f"Failed to start merger from {absolute_script_path}: {e}")
            return None
        
    def start_all(self):
        """Start all enabled sensors and merger"""
        self.logger.info(f"Starting all sensor processes from directory: {self.base_path}")
        
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
            'base_directory': str(self.base_path),
            'sensors': {},
            'merger': None
        }
        
        for name, process in self.sensor_processes.items():
            status['sensors'][name] = {
                'running': process.poll() is None,
                'pid': process.pid,
                'exit_code': process.returncode,
                'script': self.config['sensors'][name]['script']
            }
        
        if self.merger_process:
            status['merger'] = {
                'running': self.merger_process.poll() is None,
                'pid': self.merger_process.pid,
                'exit_code': self.merger_process.returncode,
                'script': self.config.get('merger', {}).get('script', 'real_time_merger.py')
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
                    self.logger.info(f"Base directory: {status['base_directory']}")
                    
                    for sensor_name, sensor_status in status['sensors'].items():
                        state = "RUNNING" if sensor_status['running'] else "STOPPED"
                        exit_info = f"(Exit: {sensor_status.get('exit_code', 'N/A')})"
                        script_info = f"Script: {sensor_status.get('script', 'N/A')}"
                        self.logger.info(f"  {sensor_name}: {state} {exit_info} {script_info}")
                    
                    if status['merger']:
                        state = "RUNNING" if status['merger']['running'] else "STOPPED"
                        self.logger.info(f"  Merger: {state} (Script: {status['merger'].get('script', 'N/A')})")
                    
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
    # Get absolute path to config file if provided as argument
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = 'sensor_config.json'
    
    manager = CompleteSensorManager(config_file)
    manager.run()