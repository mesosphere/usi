#!/usr/bin/env bash
set -x -e -o pipefail

MESOS_VERSION=$1

DISTRO=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
CODENAME=$(lsb_release -cs)

# Add Mesosphere repo to the list
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E56151BF
echo "deb http://repos.mesosphere.io/${DISTRO} ${CODENAME} main" | \
    sudo tee /etc/apt/sources.list.d/mesosphere.list

apt-get -y update

# Install Mesos
apt-get -y install mesos="$MESOS_VERSION-2.0.3"
