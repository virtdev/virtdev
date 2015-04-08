#      virtdev.py
#      
#      Copyright (C) 2014 Yi-Wei Ci <ciyiwei@hotmail.com>
#      
#      This program is free software; you can redistribute it and/or modify
#      it under the terms of the GNU General Public License as published by
#      the Free Software Foundation; either version 2 of the License, or
#      (at your option) any later version.
#      
#      This program is distributed in the hope that it will be useful,
#      but WITHOUT ANY WARRANTY; without even the implied warranty of
#      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#      GNU General Public License for more details.
#      
#      You should have received a copy of the GNU General Public License
#      along with this program; if not, write to the Free Software
#      Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#      MA 02110-1301, USA.

LO = False
DEBUG = True
SANDBOX = True
LOG_ERR = True
VISIBLE = False
BLUETOOTH = False
STATISTIC = False
FILE_SHADOW = True
AUTH_BROKER = False
AUTH_WORKER = False
FILE_SERVICE = True
CACHE_SERVICE = False
EVENT_SERVICE = False
SUPERNODE_SERVICE = False

LO_PORT = 15101
AUTH_PORT = 16101
DAEMON_PORT = 17101
FILTER_PORT = 18101
BROKER_PORT = 19101
HANDLER_PORT = 20101
NOTIFIER_PORT = 21001
CONDUCTOR_PORT = 22101
SUPERNODE_PORT = 23101
DATA_HTTP_PORT = 50070
DISPATCHER_PORT = 24101
META_SERVER_PORT = 27017
DATA_SERVER_PORT = 34310
EVENT_RECEIVER_PORT = 25101
EVENT_COLLECTOR_PORT = 26101
CACHE_PORTS = {}

LO_ADDR = '127.0.0.1'
SANDBOX_ADDR = '127.0.0.1'
NOTIFIER_ADDR = '127.0.0.1'
SUPERNODES = ['192.168.10.50']
META_SERVERS = ['192.168.10.50']
AUTH_SERVERS = ['192.168.10.50']
AUTH_BROKERS = ['192.168.10.50']
DATA_SERVERS = ['192.168.10.50', '192.168.10.50']
EVENT_SERVERS = ['192.168.10.50', '192.168.10.50']
CACHE_SERVERS = ['192.168.10.50', '192.168.10.50']

IFNAME = 'eth0'
IFBACK = 'eth1'
FS_PATH = '/vdev'
LIB_PATH = '/var/lib/vdev'
RUN_PATH = '/var/run/vdev'
MOUNTPOINT = '/mnt/vdev'
