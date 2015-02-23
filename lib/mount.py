#      mount.py
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

import os
from lib.util import service_start, service_join, close_port
from conf.virtdev import VDEV_LO, VDEV_AUTH_SERVICE, VDEV_AUTH_WORKER, VDEV_AUTH_BROKER, VDEV_CACHE_SERVICE, VDEV_EVENT_SERVICE, VDEV_SUPERNODE_SERVICE, VDEV_FILE_SERVICE, VDEV_FILE_SHADOW

_active = False

def _clean():
    os.system('killall edge 2>/dev/null')
    os.system('rm -f /var/run/vdev-tunnel-*')
    
    ports = []
    if VDEV_CACHE_SERVICE:
        from conf.virtdev import VDEV_CACHE_PORTS
        for i in VDEV_CACHE_PORTS:
            ports.append(VDEV_CACHE_PORTS[i])
    
    if VDEV_LO:
        from conf.virtdev import VDEV_LO_PORT
        ports.append(VDEV_LO_PORT)
    
    if VDEV_SUPERNODE_SERVICE:
        from conf.virtdev import VDEV_SUPERNODE_PORT
        ports.append(VDEV_SUPERNODE_PORT)
    
    if VDEV_AUTH_BROKER:
        from conf.virtdev import VDEV_AUTH_PORT, VDEV_BROKER_PORT
        ports.append(VDEV_AUTH_PORT)
        ports.append(VDEV_BROKER_PORT)
    
    if VDEV_EVENT_SERVICE:
        from conf.virtdev import VDEV_EVENT_RECEIVER_PORT, VDEV_EVENT_COLLECTOR_PORT
        ports.append(VDEV_EVENT_RECEIVER_PORT)
        ports.append(VDEV_EVENT_COLLECTOR_PORT)
    
    if VDEV_FILE_SERVICE:
        from conf.virtdev import VDEV_FS_PORT, VDEV_DAEMON_PORT, VDEV_MAPPER_PORT, VDEV_HANDLER_PORT, VDEV_DISPATCHER_PORT
        ports.append(VDEV_FS_PORT)
        ports.append(VDEV_DAEMON_PORT)
        ports.append(VDEV_MAPPER_PORT)
        ports.append(VDEV_HANDLER_PORT)
        ports.append(VDEV_DISPATCHER_PORT)
    
    for i in ports:
        close_port(i)

def mount():
    global _active
    if _active:
        print 'cannot mount again'
        return
    else:
        _active = True
    
    _clean()
    query = None
    services = []
    
    if VDEV_AUTH_WORKER or (VDEV_FILE_SERVICE and not VDEV_FILE_SHADOW):
        from db.query import VDevDBQuery
        query = VDevDBQuery()
    
    if VDEV_AUTH_SERVICE:
        from auth.authd import VDevAuthD
        services.append(VDevAuthD(query))
    
    if VDEV_CACHE_SERVICE:
        from db.cached import VDevDBCacheD
        services.append(VDevDBCacheD())
    
    if VDEV_SUPERNODE_SERVICE:
        from supernode import VDevSupernode
        services.append(VDevSupernode())
    
    if services:
        service_start(*services)
    
    if VDEV_FILE_SERVICE:
        from fuse import FUSE
        from fs.vdfs import VDevFS
        from conf.virtdev import VDEV_FS_MOUNTPOINT
        
        os.system('fusermount -u %s 2>/dev/null' % VDEV_FS_MOUNTPOINT)
        if not os.path.exists(VDEV_FS_MOUNTPOINT):
            os.makedirs(VDEV_FS_MOUNTPOINT, 0o755)
        FUSE(VDevFS(query), VDEV_FS_MOUNTPOINT, foreground=True)
    elif services:
        service_join(*services)
    