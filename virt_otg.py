import subprocess
import shutil
import time
import sys
import os
import logging
from datetime import datetime
import xml.etree.ElementTree as ET

class VirtOTG:
    def __init__(self, domain, drive):
        self.drive = drive
        self.domain = domain

    def run_command(self, command):
        """Execute a shell command and return its output."""
        try:
            logging.info(f"Running command: {command}")
            result = subprocess.run(
                command,
                shell=True,
                check=True,
                capture_output=True,
                text=True
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            logging.error(f"Command failed: {command}")
            logging.error(f"Error output: {e.stderr}")
            raise

    def get_domain_xml(self):
        """Get domain XML using virsh command."""
        try:
            xml_output = self.run_command(f"virsh dumpxml {self.domain}")
            return xml_output
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to get domain XML: {e}")
            sys.exit(1)

    def get_disk_paths(self):
        """Parse domain XML to get disk paths."""
        try:
            xml = self.get_domain_xml()
            root = ET.fromstring(xml)
            disk_paths = []
            
            for disk in root.findall(".//disk"):
                if disk.get('device') == 'disk':
                    source = disk.find('source')
                    if source is not None:
                        path = source.get('file')
                        if path:
                            disk_paths.append(path)
            
            return disk_paths
        except Exception as e:
            logging.error(f"Failed to parse disk paths: {e}")
            sys.exit(1)

    def create_snapshot(self, disk_paths, suffix):
        """Create a diskonly snapshot using virsh."""
        snapshot_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Create snapshot XML
        snapshot_xml = f"""
        <domainsnapshot>
        <name>{snapshot_name}</name>
        <disks>
        """
        
        for disk_path in disk_paths:
            snapshot_xml += f"""
            <disk name='{disk_path}' snapshot='external'>
            <source file='{disk_path}.{suffix}'/>
            </disk>
            """
        
        snapshot_xml += """
        </disks>
        </domainsnapshot>
        """
        
        # Save snapshot XML to temporary file
        xml_path = f"/tmp/{snapshot_name}.xml"
        with open(xml_path, 'w') as f:
            f.write(snapshot_xml)
        
        try:
            # Create snapshot using virsh
            self.run_command(f"virsh snapshot-create {self.domain} {xml_path} --disk-only --quiesce")
            os.remove(xml_path)
            return snapshot_name
        except Exception as e:
            logging.error(f"Failed to create snapshot: {e}")
            if os.path.exists(xml_path):
                os.remove(xml_path)
            raise

    def perform_blockcommit(self, disk_paths, shallow=False, only_suffix=None):
        """Perform blockcommit using virsh command."""
        executed_disk_paths = []
        for disk_path in disk_paths:
            try:
                # Check if only_suffix is provided
                if only_suffix and not disk_path.endswith(only_suffix):
                    continue

                # Check if top and base are provided
                self.run_command(f"virsh blockcommit {self.domain} {disk_path} --active --verbose --pivot {'--shallow' if shallow else ''}")
                
                started_waiting = time.time()
                # Wait for blockcommit to complete
                while True:
                    output = self.run_command(f"virsh domblklist {self.domain} --details")
                    if 'block_commit' not in output:
                        break
                    if time.time() - started_waiting > 60:
                        logging.error(f"Blockcommit timed out on {disk_path}")
                        raise TimeoutError("Blockcommit timed out on {disk_path}")
                    time.sleep(1)
                
                executed_disk_paths.append(disk_path)

            except Exception as e:
                logging.error(f"Failed to perform blockcommit on {disk_path}: {e}")
                raise
            
        return executed_disk_paths


    def copy_file_with_progress(self, src_path, dst_path, chunk_size = 1024 * 1024) -> None:
        # Check if running interactively
        is_interactive = sys.stdout.isatty()
        
        # Get file size
        file_size = os.path.getsize(src_path)
        
        def format_size(bytes):
            """Convert bytes to human readable string"""
            for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                if bytes < 1024:
                    return f"{bytes:.1f}{unit}"
                bytes /= 1024
            return f"{bytes:.1f}PB"
        
        def print_progress(current, total, width=50):
            """Print progress bar to console"""
            filled = int(width * current // total)
            bar = '=' * filled + '-' * (width - filled)
            percentage = current / total * 100
            current_size = format_size(current)
            total_size = format_size(total)
            sys.stdout.write(f'\rProgress: [{bar}] {percentage:.1f}% {current_size}/{total_size}')
            sys.stdout.flush()
        
        copied = 0
        start_time = time.time()
        
        try:
            # Use different methods based on OS and file system
            if hasattr(os, 'sendfile') and os.path.exists(src_path):
                # sendfile is typically the fastest method on Linux
                with open(src_path, 'rb') as src, open(dst_path, 'wb') as dst:
                    while copied < file_size:
                        try:
                            sent = os.sendfile(dst.fileno(), src.fileno(), None, 
                                            min(chunk_size, file_size - copied))
                            if sent == 0:  # EOF reached
                                break
                            copied += sent
                            if is_interactive:
                                print_progress(copied, file_size)
                        except OSError:
                            # Fallback if sendfile fails (e.g., different filesystems)
                            remaining = file_size - copied
                            chunk = src.read(min(chunk_size, remaining))
                            if not chunk:
                                break
                            dst.write(chunk)
                            copied += len(chunk)
                            if is_interactive:
                                print_progress(copied, file_size)
            else:
                # Fallback for systems without sendfile
                with open(src_path, 'rb') as src, open(dst_path, 'wb') as dst:
                    while True:
                        chunk = src.read(chunk_size)
                        if not chunk:
                            break
                        dst.write(chunk)
                        copied += len(chunk)
                        if is_interactive:
                            print_progress(copied, file_size)
            
            if is_interactive:
                duration = time.time() - start_time
                speed = file_size / duration if duration > 0 else 0
                print(f"\nCompleted! Average speed: {format_size(speed)}/s")
        
        except Exception as e:
            # Clean up partial file on error
            if os.path.exists(dst_path):
                os.unlink(dst_path)
            raise e
        
        # Copy file permissions and timestamps
        shutil.copystat(src_path, dst_path)

    def backup_disks(self, disk_paths, intermediate_dir=None):
        """Copy disk files to external drive with progress tracking."""
        for disk_path in disk_paths:
            try:
                if intermediate_dir:
                    backup_dir = os.path.join(self.drive, intermediate_dir)
                    os.makedirs(backup_dir, exist_ok=True)
                else:
                    backup_dir = self.drive
                
                dest = os.path.join(backup_dir, os.path.basename(disk_path))
                logging.info(f"Backing up {disk_path} to {dest}")
                self.copy_file_with_progress(disk_path, dest)
            except Exception as e:
                logging.error(f"Failed to backup disk: {e}")
                raise

    def cleanup_disks(self, disk_paths):
        """Remove temporary disk snapshots."""
        for disk_path in disk_paths:
            try:
                if disk_path.endswith(".qcow2") and not disk_path.startswith(self.drive):
                    raise ValueError(f"You are trying to delete a disk that is not on the external drive: {disk_path}. Aborting!")

                self.run_command(f"rm -f {disk_path}")
            except Exception as e:
                logging.error(f"Failed to cleanup disk snapshot: {e}")
                raise

    def is_on_mounted_drive(self, directory = None):
        if directory is None:
            directory = self.drive
        # Ensure directory exists
        if not os.path.exists(directory):
            raise FileNotFoundError(f"Directory {directory} does not exist")
        
        # Get absolute path
        directory = os.path.abspath(directory)
        
        # Get list of mounted filesystems
        try:
            mount_output = self.run_command("mount").splitlines()
        except subprocess.CalledProcessError as e:
            raise subprocess.CalledProcessError(
                e.returncode,
                e.cmd,
                "Failed to get mount information"
            )
        
        # Parse mount points from output
        mount_points = []
        for line in mount_output:
            if line:
                # Mount point is typically the second item when split on spaces
                parts = line.split()
                if len(parts) >= 3:
                    mount_points.append(os.path.abspath(parts[2]))
        
        # Sort mount points by length descending to check most specific paths first
        mount_points.sort(key=len, reverse=True)
        
        # Check if directory is under any mount point
        for mount_point in mount_points:
            if directory == mount_point or directory.startswith(mount_point + '/'):
                return True, mount_point
                
        return False, None

    def destroy_domain(self):
        """Destroy the domain to release disk locks."""
        try: 
            # check if it is running
            dominfo = self.run_command(f"virsh dominfo {self.domain}")
            if "shut off" in dominfo:
                logging.info("Domain is already shut off")
                return

            self.run_command(f"virsh destroy {self.domain}")

            # Wait for the domain to be destroyed
            while True:
                try:
                    dominfo = self.run_command(f"virsh dominfo {self.domain}")
                    if "shut off" in dominfo:
                        break
                except subprocess.CalledProcessError:
                    break
                time.sleep(1)
                
        except Exception as e:
            logging.error(f"Failed to destroy domain: {e}")
            sys.exit(1)

    def disable_autostart(self):
        """Disable autostart for the domain.
        
        Returns:
            bool: True if successful, False if domain already has autostart disabled
        
        Raises:
            subprocess.CalledProcessError: If the virsh command fails
            Exception: For other errors during execution
        """
        try:
            # Check current autostart status
            status = self.run_command(f"virsh dominfo {self.domain}")
            if "Autostart:        disable" in status:
                logging.info(f"Autostart already disabled for domain {self.domain}")
                return False
                
            # Disable autostart
            self.run_command(f"virsh autostart --disable {self.domain}")
            logging.info(f"Successfully disabled autostart for domain {self.domain}")
            return True
            
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to disable autostart: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error while disabling autostart: {e}")
            raise

    def enable_autostart(self):
        """Enable autostart for the domain.
        
        Returns:
            bool: True if successful, False if domain already has autostart enabled
        
        Raises:
            subprocess.CalledProcessError: If the virsh command fails
            Exception: For other errors during execution
        """
        try:
            # Check current autostart status
            status = self.run_command(f"virsh dominfo {self.domain}")
            if "Autostart:        enable" in status:
                logging.info(f"Autostart already enabled for domain {self.domain}")
                return False
                
            # Enable autostart
            self.run_command(f"virsh autostart {self.domain}")
            logging.info(f"Successfully enabled autostart for domain {self.domain}")
            return True
            
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to enable autostart: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error while enabling autostart: {e}")
            raise


    def start_domain(self):
        """Start the domain if it's not already running.
        
        Returns:
            bool: True if domain was started, False if it was already running
            
        Raises:
            subprocess.CalledProcessError: If the virsh command fails
            TimeoutError: If domain fails to start within timeout period
            Exception: For other errors during execution
        """
        try:
            # Check current domain status
            dominfo = self.run_command(f"virsh dominfo {self.domain}")
            if "running" in dominfo:
                logging.info(f"Domain {self.domain} is already running")
                return False
                
            # Start the domain
            self.run_command(f"virsh start {self.domain}")
            
            # Wait for domain to be fully running
            timeout = 60  # seconds
            start_time = time.time()
            
            while True:
                try:
                    dominfo = self.run_command(f"virsh dominfo {self.domain}")
                    if "running" in dominfo:
                        logging.info(f"Successfully started domain {self.domain}")
                        return True
                        
                    if time.time() - start_time > timeout:
                        raise TimeoutError(f"Domain {self.domain} failed to start within {timeout} seconds")
                        
                    time.sleep(1)
                    
                except subprocess.CalledProcessError as e:
                    if time.time() - start_time > timeout:
                        raise TimeoutError(f"Domain {self.domain} failed to start within {timeout} seconds")
                    time.sleep(1)
                    
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to start domain: {e}")
            raise
        except TimeoutError as e:
            logging.error(str(e))
            raise
        except Exception as e:
            logging.error(f"Unexpected error while starting domain: {e}")
            raise