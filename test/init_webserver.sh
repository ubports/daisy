#!/bin/sh
exec >/var/log/cloud-init.log 2>&1
packages="apache2 python-pip libapache2-mod-wsgi bzr nfs-kernel-server"
sudo apt-get update
DEBCONF_FRONTEND=noninteractive sudo apt-get -y install $packages
# For bson.
sudo pip install pymongo
sudo easy_install -U distribute
sudo pip install pycassa
# For talking to the MQ.
sudo pip install pika
# Enable mod_rewrite.
sudo a2enmod rewrite
cat > /etc/exports << EOF
/srv/cores 10.55.60.0/24(rw,sync,no_subtree_check)
EOF
sudo exportfs -ra
cat > /etc/apache2/sites-enabled/000-default << EOF
WSGIPythonPath /var/www/whoopsie-daisy/backend
<VirtualHost *:80>
	ServerAdmin webmaster@localhost

	DocumentRoot /var/www/whoopsie-daisy/backend
	WSGIScriptAlias / /var/www/whoopsie-daisy/backend/submit.wsgi
	RewriteEngine on
	RewriteRule ^/([^/]+)/submit-core/([^/]+)/([^/]+) /submit_core.wsgi?uuid=\$1&arch=\$2&systemuuid=\$3 [L]
	<Directory /var/www/whoopsie-daisy/backend>
		SetHandler wsgi-script
	</Directory>
	ErrorLog /var/log/apache2/error.log
	LogLevel warn
	CustomLog /var/log/apache2/access.log combined
</VirtualHost>
EOF
bzr branch lp:whoopsie-daisy /var/www/whoopsie-daisy
bzr branch lp:~ev/oops-repository/whoopsie-daisy /tmp/oops-repository
(cd /tmp/oops-repository; python setup.py build; sudo python setup.py install)
sudo /etc/init.d/apache2 restart
