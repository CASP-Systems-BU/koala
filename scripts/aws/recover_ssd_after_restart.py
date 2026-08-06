import sys
import os

# Import utils from the parent directory
sys.path.append(os.path.abspath(".."))
import utils

# Import local util
if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from util import *

# Read instance.txt
# Get all remote IPs
ips = getRemoteIPs()
print(f"Found {len(ips)} instances: {ips}")


# Remount SSD on each instance
cmd_remount = "sudo mkfs -t ext4 -F /dev/nvme1n1 && \
sudo mount /dev/nvme1n1 /home/ubuntu/ssd && \
sudo chmod o+w /home/ubuntu/ssd && \
sudo chown -R ubuntu /home/ubuntu/ssd && \
df -h"

run_on_all_nodes(ips, cmd_remount, "Remounting SSD on remote instances")


# Move everything from /home/ubuntu/ebs/ to /home/ubuntu/ssd/
# We use rsync with --remove-source-files to mimic a move across filesystems
cmd_restore = "sudo rsync -a /home/ubuntu/ebs/ /home/ubuntu/ssd/ && sudo rm -rf /home/ubuntu/ebs/*"

run_on_all_nodes(ips, cmd_restore, "Restoring data back to SSD from EBS (moving files)")
