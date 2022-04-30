#!/bin/bash

apt-get update
apt-get -y install p7zip-full
apt-get -y install wget
apt-get -y install ftp

bash pipelines/bash_scripts/install_tor.sh

bash download.sh $1