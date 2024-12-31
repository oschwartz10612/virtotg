#!/usr/bin/env python3

import sys
import os
import logging
import argparse
from virt_otg import VirtOTG

# Check if running as root
if os.geteuid() != 0:
    print("This script must be run as root")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/vm_transfer.log'),
        logging.StreamHandler()
    ]
)

parser = argparse.ArgumentParser(description='Backup VM disks to external drive')
parser.add_argument('--domain', type=str, help='Name of the domain to backup', required=True)
parser.add_argument('--drive', type=str, help='Path to the external drive', required=True)
args = parser.parse_args()

virtotg = VirtOTG(args.domain, args.drive)

def main():
    """Main execution function."""
    try:
        # Check if the backup directory exists
        is_mounted, mount_point = virtotg.is_on_mounted_drive()
        if not is_mounted:
            logging.error(f"External drive {args.drive} is not mounted")
            sys.exit(1)
        
        # Get disk paths
        disk_paths = virtotg.get_disk_paths()

        # Copy back in the disk files
        for disk_path in disk_paths:
            # move the original disk file to the disk path with the .bak extension to create a backup in case something is wrong with the transfer
            virtotg.run_command(f"mv {disk_path} {disk_path}.bak")

            # copy in the disk file from the external drive
            remote_disk = os.path.join(args.drive, os.path.basename(disk_path))
            if os.path.exists(remote_disk):
                virtotg.copy_file_with_progress(remote_disk, disk_path)

        # Re enable autostart
        virtotg.enable_autostart()
    
        # Start the domain
        virtotg.start_domain()

        logging.info("Transfer completed successfully!")

    except Exception as e:
        logging.error(f"Transfer failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
