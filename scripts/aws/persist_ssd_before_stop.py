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


# Persist SSD -> EBS
# Move everything from /home/ubuntu/ssd/ to /home/ubuntu/ebs/ excluding lost+found
cmd_persist = "cd /home/ubuntu/ && \
sudo rsync -a --remove-source-files --exclude='lost+found' /home/ubuntu/ssd/ /home/ubuntu/ebs/ && \
sudo find /home/ubuntu/ssd -mindepth 1 -type d -empty ! -path '/home/ubuntu/ssd/lost+found' -delete"

run_on_all_nodes(ips, cmd_persist, "Persisting SSD data to EBS (moving files)")
