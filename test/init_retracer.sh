#!/bin/sh
set -e
exec >/var/log/cloud-init.log 2>&1
PACKAGES="apport-retrace bzr python-pip nfs-common"
sudo apt-get update
sudo apt-get install -y $PACKAGES
sudo easy_install -U distribute
# For talking to the MQ.
sudo pip install pika
sudo pip install pycassa
# Core files.
sudo mkdir -p /srv/cores
# Retracer config.
h=/home/ubuntu
sudo -u ubuntu mkdir -p "$h/config/Ubuntu 12.04"
cat > "$h/config/Ubuntu 12.04/sources.list" << EOF
deb http://archive.ubuntu.com/ubuntu precise main restricted universe multiverse
deb http://archive.ubuntu.com/ubuntu precise-updates main restricted universe multiverse
deb http://archive.ubuntu.com/ubuntu precise-proposed main restricted universe multiverse
deb http://archive.ubuntu.com/ubuntu precise-security main restricted universe multiverse

deb http://ddebs.ubuntu.com precise main restricted universe multiverse
deb http://ddebs.ubuntu.com precise-updates main restricted universe multiverse
deb http://ddebs.ubuntu.com precise-proposed main restricted universe multiverse
deb http://ddebs.ubuntu.com precise-security main restricted universe multiverse
EOF
sudo -u ubuntu mkdir -p $h/cache
sudo -u ubuntu bzr branch lp:whoopsie-daisy $h/whoopsie-daisy
sudo -u ubuntu nohup $h/whoopsie-daisy/backend/process_core.py --config-dir $h/config --cache $h/cache > /var/log/retrace.log 2>&1 < /dev/null &
