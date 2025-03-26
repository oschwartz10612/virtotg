#!/usr/bin/env python3

import sys
import os
import logging
from datetime import datetime
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
        logging.FileHandler('/var/log/vm_backup.log'),
        logging.StreamHandler()
    ]
)

parser = argparse.ArgumentParser(description='Backup VM disks to external drive')
parser.add_argument('--domain', type=str, help='Name of the domain to backup', required=True)
parser.add_argument('--drive', type=str, help='Path to the external drive', required=True)
parser.add_argument('--full', action='store_true', help='Perform a full backup')
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
        snap_files = [disk_path for disk_path in disk_paths if disk_path.endswith(".snap")]
        
        if args.full:
            logging.info("Performing full backup routine")

            # Perform blockcommit
            disks_to_rm = virtotg.perform_blockcommit(disk_paths, only_suffix="snap")
            
            # Cleanup temporary disk snapshots
            virtotg.cleanup_disks(disks_to_rm)

            # Create new snapshot
            backing_disk_paths = virtotg.get_disk_paths()
            virtotg.create_snapshot(backing_disk_paths, "snap")
        
            # cleanup the disks on the external drive
            remote_disks = [os.path.join(args.drive, os.path.basename(backing_disk_path)) for backing_disk_path in backing_disk_paths]    
            virtotg.cleanup_disks(remote_disks)

            # Backup the backing files
            virtotg.backup_disks(backing_disk_paths)
            
        else:
            logging.info("Performing incremental backup routine")
            # cant perform incremental backup if there are not snap files in the backing
            if snap_files:
                virtotg.create_snapshot(disk_paths, "tmp") # snap disks

                # Backup the active disks
                backup_time = datetime.now().strftime('%Y%m%d_%H%M%S')
                virtotg.backup_disks(disk_paths, intermediate_dir=backup_time)

            # Perform blockcommit
            tmp_disk_paths = virtotg.get_disk_paths()
            disks_to_rm = virtotg.perform_blockcommit(tmp_disk_paths, shallow=True)

            # Cleanup temporary disk snapshots
            virtotg.cleanup_disks(disks_to_rm)
        
        logging.info("Backup completed successfully")
        
    except Exception as e:
        logging.error(f"Backup failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()