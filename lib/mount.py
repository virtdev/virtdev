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
import time
import tunnel
from lib.log import log
from subprocess import call
from lib.util import DEVNULL, service_start, service_join, close_port
from conf.virtdev import LO, AUTH_WORKER, AUTH_BROKER, CACHE_SERVICE, EVENT_SERVICE, SUPERNODE_SERVICE, FILE_SERVICE, FILE_SHADOW, MOUNTPOINT, RUN_PATH, LIB_PATH

LOG = True

def _log(text):
    if LOG:
        log(text)

def _clean():
    ports = []
    tunnel.clean()
    if CACHE_SERVICE:
        from conf.virtdev import CACHE_PORTS
        for i in CACHE_PORTS:
            ports.append(CACHE_PORTS[i])
    
    if LO:
        from conf.virtdev import LO_PORT
        ports.append(LO_PORT)
    
    if SUPERNODE_SERVICE:
        from conf.virtdev import SUPERNODE_PORT
        ports.append(SUPERNODE_PORT)
    
    if AUTH_BROKER:
        from conf.virtdev import AUTH_PORT, BROKER_PORT
        ports.append(AUTH_PORT)
        ports.append(BROKER_PORT)
    
    if EVENT_SERVICE:
        from conf.virtdev import EVENT_RECEIVER_PORT, EVENT_COLLECTOR_PORT
        ports.append(EVENT_RECEIVER_PORT)
        ports.append(EVENT_COLLECTOR_PORT)
    
    if FILE_SERVICE:
        from conf.virtdev import CONDUCTOR_PORT, DAEMON_PORT, FILTER_PORT, HANDLER_PORT, DISPATCHER_PORT
        ports.append(DAEMON_PORT)
        ports.append(FILTER_PORT)
        ports.append(HANDLER_PORT)
        ports.append(CONDUCTOR_PORT)
        ports.append(DISPATCHER_PORT)
    
    for i in ports:
        close_port(i)

def _mount(query):
    from fuse import FUSE
    from fs.vdfs import VDevFS
    
    call(['umount', '-lf', MOUNTPOINT], stderr=DEVNULL, stdout=DEVNULL)
    time.sleep(1)
    
    if not os.path.exists(MOUNTPOINT):
        os.makedirs(MOUNTPOINT, 0o755)
    
    if not os.path.exists(RUN_PATH):
        os.makedirs(RUN_PATH, 0o755)
    
    if not os.path.exists(LIB_PATH):
        os.makedirs(LIB_PATH, 0o755)
    
    _log('mount vdfs ...')
    FUSE(VDevFS(query), MOUNTPOINT, foreground=True)

def mount():
    _clean()
    query = None
    services = []
    if AUTH_WORKER or (FILE_SERVICE and not FILE_SHADOW):
        from db.query import VDevQuery
        query = VDevQuery()
    
    if AUTH_BROKER or AUTH_WORKER:
        from auth.authd import VDevAuthD
        services.append(VDevAuthD(query))
    
    if CACHE_SERVICE:
        from db.cached import VDevCacheD
        services.append(VDevCacheD())
    
    if SUPERNODE_SERVICE:
        from supernode import VDevSupernode
        services.append(VDevSupernode())
    
    if services:
        service_start(*services)
    
    if FILE_SERVICE:
        _mount(query)
    elif services:
        service_join(*services)
