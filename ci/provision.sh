#!/usr/bin/env bash
set -x -e -o pipefail

MESOS_VERSION=$1

apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E56151BF
DISTRO=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
CODENAME=$(lsb_release -cs)

echo "deb http://repos.mesosphere.io/${DISTRO} ${CODENAME} main" | \
    sudo tee /etc/apt/sources.list.d/mesosphere.list
apt-get -y update

apt-get -y install mesos="$MESOS_VERSION-2.0.3" zookeeperd
