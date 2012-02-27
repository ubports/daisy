#!/bin/sh
exec >/var/log/cloud-init.log 2>&1
echo "deb http://www.apache.org/dist/cassandra/debian 08x main" >> /etc/apt/sources.list
gpg --keyserver pgp.mit.edu --recv-keys F758CE318D77295D
gpg --export --armor F758CE318D77295D | sudo apt-key add -
gpg --keyserver pgp.mit.edu --recv-keys 2B5C1B00
gpg --export --armor 2B5C1B00 | sudo apt-key add -
packages="cassandra bzr python-pip"
sudo apt-get update
DEBCONF_FRONTEND=noninteractive sudo apt-get -y install $packages
sed -i 's/rpc_address:.*/rpc_address: 0.0.0.0/g' /etc/cassandra/cassandra.yaml
sudo /etc/init.d/cassandra restart
echo "connect localhost/9160; 
create keyspace crashdb;" | cassandra-cli -B
sudo easy_install -U distribute
sudo pip install pycassa
bzr branch lp:~ev/oops-repository/whoopsie-daisy /tmp/oops-repository
(cd /tmp/oops-repository; OOPS_KEYSPACE=crashdb PYTHONPATH=. python oopsrepository/schema.py)
