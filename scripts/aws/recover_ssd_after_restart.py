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
cmd_remount = "set -e; \
DEVS=$(lsblk -dno NAME,MODEL | grep -F \"Amazon EC2 NVMe Instance Storage\" | sed \"s/[[:space:]].*//\"); \
N=$(printf \"%s\\n\" \"$DEVS\" | grep -c . || true); \
if [ \"$N\" -ne 1 ]; then echo \"ABORT: expected exactly one instance-store device, found $N: $DEVS\"; exit 1; fi; \
DEV=/dev/$DEVS; \
ROOT=$(findmnt -no SOURCE /); \
case \"$ROOT\" in \"$DEV\"*) echo \"ABORT: $DEV backs the root filesystem ($ROOT)\"; exit 1;; esac; \
BUSY=$(lsblk -no MOUNTPOINT \"$DEV\" | grep . | grep -v \"^/home/ubuntu/ssd$\" || true); \
if [ -n \"$BUSY\" ]; then echo \"ABORT: $DEV has unexpected mounts: $BUSY\"; exit 1; fi; \
if [ \"$(findmnt -no SOURCE /home/ubuntu/ssd || true)\" = \"$DEV\" ]; \
then echo \"SKIP mkfs: $DEV is already mounted at /home/ubuntu/ssd\"; \
else sudo mkfs -t ext4 -F \"$DEV\"; sudo mount \"$DEV\" /home/ubuntu/ssd; fi; \
sudo chmod o+w /home/ubuntu/ssd; \
sudo chown -R ubuntu /home/ubuntu/ssd; \
df -h /home/ubuntu/ssd"

run_on_all_nodes(ips, cmd_remount, "Remounting SSD on remote instances")


# Copy everything from /home/ubuntu/ebs/ back to /home/ubuntu/ssd/.
#
# EBS is deliberately left untouched here. It is the only copy of the data that
# survives an instance stop (the NVMe SSD is wiped and re-mkfs'd above), so a
# recovery that has not been verified must not be the thing that destroys it.
# EBS is modified in exactly one place: persist_ssd_before_stop.py, which moves
# SSD -> EBS with --remove-source-files before the instances are stopped.
cmd_restore = "sudo rsync -a /home/ubuntu/ebs/ /home/ubuntu/ssd/"

run_on_all_nodes(ips, cmd_restore, "Restoring data back to SSD from EBS (copying files)")
