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
 
VDEV_LO = False
VDEV_DEBUG = True
VDEV_SANDBOX = True
VDEV_LOG_ERR = True
VDEV_SPECIAL = False
VDEV_BLUETOOTH = False
VDEV_STATISTIC = False
VDEV_FILE_SHADOW = True
VDEV_AUTH_BROKER = False
VDEV_AUTH_WORKER = False

VDEV_FILE_SERVICE = True
VDEV_AUTH_SERVICE = False
VDEV_CACHE_SERVICE = False
VDEV_EVENT_SERVICE = False
VDEV_SUPERNODE_SERVICE = False

VDEV_LO_PORT = 12101
VDEV_DB_PORT = 27017
VDEV_FS_PORT = 14101
VDEV_AUTH_PORT = 16101
VDEV_MAPPER_PORT = 17101
VDEV_BROKER_PORT = 18101
VDEV_HANDLER_PORT = 19101
VDEV_SUPERNODE_PORT = 20101
VDEV_DISPATCHER_PORT = 21101
VDEV_EVENT_RECEIVER_PORT = 22101
VDEV_EVENT_COLLECTOR_PORT = 23101
VDEV_NOTIFIER_PORT = 26001
VDEV_DAEMON_PORT = 25101
VDEV_DFS_HTTP_PORT = 50070
VDEV_DFS_PORT = 34310
VDEV_CACHE_PORTS = {}

VDEV_SANDBOX_ADDR = '127.0.0.1'
VDEV_SUPERNODES = ['192.168.10.50']
VDEV_DB_SERVERS = ['192.168.10.50']
VDEV_AUTH_SERVERS = ['192.168.10.50']
VDEV_DEFAULT_SERVERS = ['192.168.10.50']
VDEV_DFS_SERVERS = ['192.168.10.50', '192.168.10.50']
VDEV_EVENT_SERVERS = ['192.168.10.50', '192.168.10.50']
VDEV_CACHE_SERVERS = ['192.168.10.50', '192.168.10.50']

VDEV_IFNAME = 'eth0'
VDEV_IFBACK = 'eth1'
VDEV_FS_PATH = '/vdev'
VDEV_FS_MOUNTPOINT = '/mnt/vdev'
VDEV_LIB_PATH = '/var/lib/vdev'
VDEV_RUN_PATH = '/var/run/vdev'
