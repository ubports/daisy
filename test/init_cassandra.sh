#!/bin/sh
exec >/var/log/cloud-init.log 2>&1
echo "deb http://www.apache.org/dist/cassandra debian 10x main" >> /etc/apt/sources.list
gpg --keyserver pgp.mit.edu --recv-keys F758CE318D77295D
gpg --export --armor F758CE318D77295D | sudo apt-key add -
gpg --keyserver pgp.mit.edu --recv-keys 2B5C1B00
gpg --export --armor 2B5C1B00 | sudo apt-key add -
packages="cassandra"
sudo apt-get update
DEBCONF_FRONTEND=noninteractive sudo apt-get -y install $packages
bzr branch lp:~ev/oops-repository/whoopsie-daisy /tmp/oops-repository
(cd /tmp/oops-repository; python oopsrepository/schema.py)
