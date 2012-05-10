#!/bin/sh
set -e
exec >/var/log/cloud-init.log 2>&1
PACKAGES="apport-retrace bzr python-pycassa python-amqplib nfs-common oops-repository"
echo "deb http://archive.admin.canonical.com lucid-cat main" >> /etc/apt/sources.list
sudo add-apt-repository ppa:ev/whoopsie-daisy
sudo apt-get update
sudo apt-get install -y $PACKAGES
# Core files.
sudo mkdir -p /srv/cores
# Retracer config.
h=/home/ubuntu
sudo -u ubuntu mkdir -p $h/cache
sudo -u ubuntu bzr branch lp:daisy $h/daisy
sudo -u ubuntu nohup $h/daisy/process_core.py --config-dir $h/daisy/retracer/config --cache $h/cache > /var/log/retrace.log 2>&1 < /dev/null &
