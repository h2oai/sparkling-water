#!/bin/bash


echo " Configure Default Proxy Server"
cd /usr/share/nginx/html
wget https://h2ostore.blob.core.windows.net/marketplacescripts/SparklingWater/usr/share/nginx/html/doc.html

cd /etc/nginx 
rm nginx.conf
wget https://h2ostore.blob.core.windows.net/marketplacescripts/SparklingWater/etc/nginx/nginx.conf
cd /etc/nginx/conf.d/
wget https://h2ostore.blob.core.windows.net/marketplacescripts/SparklingWater/etc/nginx/conf.d/h2o.conf

echo "Starting nginx ..."
nginx 
echo "Now to install Flask"
pip install virtualenv
pip install --upgrade Flask
wget https://h2ostore.blob.core.windows.net/marketplacescripts/SparklingWater/bin/EdgeNodeFlask.py
echo "Set RESTful api"
export FLASK_APP=EdgeNodeFlask.py
echo " Start the FLASK RESTful API"
nohup python EdgeNodeFlask.py >flask.log 2>&1 & 
