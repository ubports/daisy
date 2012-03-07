#!/bin/sh
set -e
exec >/var/log/cloud-init.log 2>&1
echo "deb http://archive.admin.canonical.com lucid-cat-proposed main" >> /etc/apt/sources.list
sudo apt-get update
packages="rabbitmq-server"
DEBCONF_FRONTEND=noninteractive sudo apt-get -y install $packages
