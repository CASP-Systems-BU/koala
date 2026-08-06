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
#
# This is the ONLY place that modifies EBS. The NVMe SSD is wiped and re-mkfs'd
# whenever an instance stops, so EBS holds the only surviving copy of the data;
# recover_ssd_after_restart.py therefore reads EBS without touching it, and can
# be re-run as many times as needed.
#
# EBS is mirrored, not merged: --delete drops whatever EBS still holds from an
# earlier cycle so it ends up matching the SSD exactly. Without it EBS would
# accumulate stale files forever and recovery would copy them back onto the SSD.
#
# 'lost+found' is excluded from the transfer, and rsync protects excluded paths
# from --delete, so each filesystem keeps its own.
#
# The guard is what makes --delete safe. If this ran while the SSD were empty -
# after a reboot but before recover_ssd_after_restart.py - the mirror would erase
# the only copy of the data. Refuse instead. (An SSD that is genuinely empty has
# nothing worth persisting, so aborting costs nothing.)
#
# Note on quoting: utils.runSSHCmdAsync() wraps this whole string in single
# quotes (ssh {ip} '{cmd}'), so avoid single quotes in anything added here - the
# two below are pre-existing and safe only because lost+found has no spaces.
cmd_persist = "cd /home/ubuntu/ && \
if [ -z \"$(sudo find /home/ubuntu/ssd -mindepth 1 -not -path /home/ubuntu/ssd/lost+found -print -quit)\" ]; \
then echo ABORT: /home/ubuntu/ssd is empty, refusing to overwrite EBS; exit 1; fi && \
sudo rsync -a --delete --remove-source-files --exclude='lost+found' /home/ubuntu/ssd/ /home/ubuntu/ebs/ && \
sudo find /home/ubuntu/ssd -mindepth 1 -type d -empty ! -path '/home/ubuntu/ssd/lost+found' -delete"

run_on_all_nodes(ips, cmd_persist, "Persisting SSD data to EBS (EBS is replaced with an exact mirror)")
