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


# 1. Configure firewall
cmd_ufw = "echo y | sudo ufw enable && \
sudo systemctl start ufw && \
sudo systemctl enable ufw && \
sudo ufw default deny incoming && \
sudo ufw default allow outgoing && \
sudo ufw allow from 128.197.29.0/24 && \
sudo ufw allow from 128.197.28.0/24 && \
sudo ufw allow from 10.10.1.0/24 && \
sudo ufw allow from 192.168.1.0/24 && \
sudo ufw allow from 173.76.191.218 && \
sudo ufw allow from 38.42.127.128 && \
sudo ufw allow from 38.42.98.142 && \
sudo ufw allow from 98.216.176.72 && \
sudo ufw status verbose"

run_on_all_nodes(ips, cmd_ufw, "Configuring firewall on remote instances")

# 2. Mount SSD on remote instances (m5d.2xlarge has an NVMe SSD at /dev/nvme1n1)
cmd_mount = "sudo mkdir -p /home/ubuntu/ssd && \
sudo mkdir -p /home/ubuntu/ebs && \
sudo mkfs -t ext4 -F /dev/nvme1n1 && \
sudo mount /dev/nvme1n1 /home/ubuntu/ssd && \
sudo chmod o+w /home/ubuntu/ssd && \
sudo chown -R ubuntu /home/ubuntu/ssd"

run_on_all_nodes(ips, cmd_mount, "Mounting SSD on remote instances")


# 3. Install dependencies
cmd_install = "sudo apt update && \
sudo apt install -y python3-pip openjdk-11-jdk-headless iperf3 htop nload && \
python3 -m pip install psutil kafka-python paramiko scp"

run_on_all_nodes(ips, cmd_install, "Installing dependencies on remote instances")
