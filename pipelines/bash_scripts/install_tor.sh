apt-get -y install apt-transport-https curl lsb-release systemd-sysv ubuntu-standard
echo "deb https://deb.torproject.org/torproject.org/ $(lsb_release -cs) main" > /etc/apt/sources.list.d/tor.list
curl https://deb.torproject.org/torproject.org/A3C4F0F979CAA22CDBA8F512EE8CBC9E886DDD89.asc | gpg --import
gpg --export A3C4F0F979CAA22CDBA8F512EE8CBC9E886DDD89 | apt-key add -
apt update
apt-get -y install tor tor-geoipdb torsocks deb.torproject.org-keyring
echo "ExitNodes {us}\nStrictNodes 1">>/etc/tor/torrc
systemctl reload tor
torify curl ipinfo.io