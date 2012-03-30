#!/bin/sh
set -e
exec >/var/log/cloud-init.log 2>&1
echo "deb http://www.apache.org/dist/cassandra/debian 10x main" >> /etc/apt/sources.list
echo "deb http://archive.admin.canonical.com lucid-cat main" >> /etc/apt/sources.list
gpg --keyserver pgp.mit.edu --recv-keys F758CE318D77295D
gpg --export --armor F758CE318D77295D | sudo apt-key add -
gpg --keyserver pgp.mit.edu --recv-keys 2B5C1B00
gpg --export --armor 2B5C1B00 | sudo apt-key add -
packages="cassandra bzr python-thrift python-pycassa oops-repository"
sudo add-apt-repository ppa:ev/whoopsie-daisy
sudo apt-get update
DEBCONF_FRONTEND=noninteractive sudo apt-get -y install $packages
sed -i 's/rpc_address:.*/rpc_address: 0.0.0.0/g' /etc/cassandra/cassandra.yaml
echo "connect localhost/9160; 
create keyspace crashdb;" | cassandra-cli -B
OOPS_KEYSPACE=crashdb python /usr/share/pyshared/oopsrepository/schema.py
bzr branch lp:whoopsie-daisy
python whoopsie-daisy/backend/schema.py
# To pick up the change to rpc_address
sudo /etc/init.d/cassandra restart
