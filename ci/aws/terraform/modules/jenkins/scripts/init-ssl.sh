#!/bin/bash

a2enmod proxy proxy_http headers ssl
a2dissite 000-default.conf
# Comment all lines in ports.conf
sed -ie 's/^\([^#].*\)/# \1/g' /etc/apache2/ports.conf

tee /home/ubuntu/.renew_cert.sh <<EOF
service apache2 stop
certbot certonly -d SUBST_PUBLIC_HOSTNAME --non-interactive --agree-tos --standalone -m support@h2o.ai
cp /etc/letsencrypt/live/SUBST_PUBLIC_HOSTNAME/privkey.pem /etc/ssl/private/my-key.pem
cp /etc/letsencrypt/live/SUBST_PUBLIC_HOSTNAME/fullchain.pem /etc/ssl/certs/my-cert.pem
service apache2 start
EOF
chmod +x /home/ubuntu/.renew_cert.sh

# configure apache for ssl proxying
tee /etc/apache2/sites-enabled/ssl.conf <<EOF
LoadModule ssl_module modules/mod_ssl.so
LoadModule proxy_module modules/mod_proxy.so
Listen 443
<VirtualHost *:443>
  <Proxy "*">
    Order deny,allow
    Allow from all
  </Proxy>
  SSLEngine             on
  SSLCertificateFile    /etc/ssl/certs/my-cert.pem
  SSLCertificateKeyFile /etc/ssl/private/my-key.pem
  # this option is mandatory to force apache to forward the client cert data to tomcat
  SSLOptions +ExportCertData
  Header always set Strict-Transport-Security "max-age=63072000; includeSubdomains; preload"
  Header always set X-Frame-Options DENY
  Header always set X-Content-Type-Options nosniff
  ProxyPass / http://localhost:8080/ retry=0 nocanon
  ProxyPassReverse / http://localhost:8080/
  ProxyPreserveHost on
  AllowEncodedSlashes NoDecode
  RequestHeader set X-Forwarded-Proto "https"
  RequestHeader set X-Forwarded-Port "443"
  LogFormat "%h (%{X-Forwarded-For}i) %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\""
  ErrorLog /var/log/apache2/ssl-error_log
  TransferLog /var/log/apache2/ssl-access_log
</VirtualHost>
EOF

# Set up auto-renowal every week
echo '0 0 * * 0 root /bin/sh /home/ubuntu/.renew_cert.sh' > /etc/cron.d/cerbot
/home/ubuntu/.renew_cert.sh
