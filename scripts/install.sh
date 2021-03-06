#!/bin/bash

function install_edgenode()
{
    dhcpd=`which dhcpd`
    if [ "$dhcpd" != "" ]; then
        apt-get install -y isc-dhcp-server
    fi
    apt-get install -y fuse
    apt-get install -y bluez
    apt-get install -y bluetooth
    apt-get install -y bluez-tools
    apt-get install -y build-essential
    apt-get install -y libbluetooth-dev
    apt-get install -y libopencv-dev
    apt-get install -y librsync-dev
    apt-get install -y libfuse-dev
    apt-get install -y libzbar-dev
    apt-get install -y libzmq-dev
    apt-get install -y libxss1
    apt-get install -y lsof
    apt-get install -y n2n
    apt-get install -y python-all-dev
    apt-get install -y python-bluez
    apt-get install -y python-cffi
    apt-get install -y python-dbus
    apt-get install -y python-gevent
    apt-get install -y python-gi-dev
    apt-get install -y python-imaging
    apt-get install -y python-numpy
    apt-get install -y python-opencv
    apt-get install -y python-pil
    apt-get install -y python-pip
    apt-get install -y python-pygame
    apt-get install -y python-scipy
    apt-get install -y python-zbar
    apt-get install -y xvfb

    pip install fusepy
    pip install gensim
    pip install hash_ring
    pip install html2text
    pip install isodate
    pip install langid
    pip install netifaces
    pip install -U nltk
    pip install psutil
    pip install pycrypto
    pip install pynetlinux
    pip install pypng
    pip install pyqrcode
    pip install pyserial
    pip install python-librsync
    pip install python-rake
    pip install pytz
    pip install pyzmq
    pip install RestrictedPython
    pip install stop-words
    pip install textblob
    pip install tornado
    pip install websocket-client
    pip install wget
    pip install xattr
    pip install zerorpc
    pip install paho-mqtt

    ARCH=`arch`
    if [  $ARCH = "i386" ]; then
        echo "TensorFlow cannot be installed (i386 is not supported)"
        exit
    fi

    if [ $ARCH = "armv7l" ]; then
        TF_BINARY_URL=https://github.com/samjabrahams/tensorflow-on-raspberry-pi/raw/master/bin/tensorflow-0.9.0-cp27-none-linux_armv7l.whl
        pip install --upgrade $TF_BINARY_URL
    fi

    if [ $ARCH = "x86_64" ]; then
        TF_BINARY_URL=https://storage.googleapis.com/tensorflow/linux/cpu/tensorflow-0.9.0rc0-cp27-none-linux_x86_64.whl
        pip install --upgrade $TF_BINARY_URL
    fi
}

function install_supernode()
{
    dhcpd=`which dhcpd`
    if [ "$dhcpd" != "" ]; then
        apt-get install -y isc-dhcp-server
    fi
    apt-get install -y fuse
    apt-get install -y build-essential
    apt-get install -y libkrb5-dev
    apt-get install -y librsync-dev
    apt-get install -y libsasl2-dev
    apt-get install -y libfuse-dev
    apt-get install -y libzmq-dev
    apt-get install -y libxss1
    apt-get install -y lsof
    apt-get install -y memcached
    apt-get install -y mosquitto
    apt-get install -y mongodb
    apt-get install -y n2n
    apt-get install -y python-all-dev
    apt-get install -y python-cffi
    apt-get install -y python-dbus
    apt-get install -y python-gevent
    apt-get install -y python-gi-dev
    apt-get install -y python-memcache
    apt-get install -y python-numpy
    apt-get install -y python-pip
    apt-get install -y xvfb

    pip install fusepy
    pip install hdfs
    pip install happybase
    pip install hash_ring
    pip install isodate
    pip install netifaces
    pip install psutil
    pip install pycrypto
    pip install pymongo
    pip install pynetlinux
    pip install python-librsync
    pip install python-memcached
    pip install python-inotify==0.6-test
    pip install pytz
    pip install pyzmq
    pip install RestrictedPython
    pip install snakebite
    pip install sophia
    pip install tornado
    pip install websocket-client
    pip install xattr
    pip install zerorpc
    pip install leveldb
    pip install paho-mqtt
}

function install_nodejs()
{
    curl -sL https://deb.nodesource.com/setup_6.x | bash -
    apt-get install -y nodejs
    if [ ! -f /usr/bin/node ]; then
        ln -s /usr/bin/nodejs /usr/bin/node
    fi

    CURRENT=`pwd`
    FILENAME=`readlink -f $0`
    INST=`/usr/bin/dirname $FILENAME`
    cd $INST
    npm install ws
    npm install websocket
    npm install crypto
    npm install mqtt
    npm install electron-webrtc
    npm install simple-peer
    npm install node-uuid
    npm install zerorpc
    npm install xvfb
    cd $CURRENT
}

if [ $# = 0 ]; then
    install_edgenode;
    install_nodejs;
elif [ $# = 1 -a $1 = "-a" ]; then
    install_edgenode;
    install_supernode;
    install_nodejs;
elif [ $# = 1 -a $1 = "-n" ]; then
    install_nodejs;
elif [ $# = 1 -a $1 = "-e" ]; then
    install_edgenode;
elif [ $# = 1 -a $1 = "-s" ]; then
    install_supernode;
else
    echo "usage: $0 [-s]"
    echo "-s: server installation"
    exit
fi
