#!/bin/bash

su -

apt-get update
apt-get -y install p7zip-full
apt-get -y install wget
apt-get -y install ftp
apt-get -y install gpg

bash pipelines/bash_scripts/install_tor.sh

source torsocks on

pwd

bash pipelines/datasets/br_me_novo_caged/bash_scripts/download.sh $1

source torsocks off
