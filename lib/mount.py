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
from lib.util import DEVNULL, srv_start, srv_join, close_port
from conf.virtdev import LO, FS, WORKER, BROKER, CACHE_SERVER, EVENT_SERVER, SUPERNODE, SHADOW, MOUNTPOINT, RUN_PATH, LIB_PATH

LOG = True

def _log(text):
    if LOG:
        log(text)

def _clean():
    ports = []
    tunnel.clean()
    if CACHE_SERVER:
        from conf.virtdev import CACHE_PORTS
        for i in CACHE_PORTS:
            ports.append(CACHE_PORTS[i])
    
    if LO:
        from conf.virtdev import LO_PORT
        ports.append(LO_PORT)
    
    if SUPERNODE:
        from conf.virtdev import SUPERNODE_PORT
        ports.append(SUPERNODE_PORT)
    
    if BROKER:
        from conf.virtdev import VDEV_PORT, BROKER_PORT
        ports.append(VDEV_PORT)
        ports.append(BROKER_PORT)
    
    if EVENT_SERVER:
        from conf.virtdev import EVENT_RECEIVER_PORT, EVENT_COLLECTOR_PORT
        ports.append(EVENT_RECEIVER_PORT)
        ports.append(EVENT_COLLECTOR_PORT)
    
    if FS:
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
    from fs.vdfs import VDFS
    
    call(['umount', '-lf', MOUNTPOINT], stderr=DEVNULL, stdout=DEVNULL)
    time.sleep(1)
    
    if not os.path.exists(MOUNTPOINT):
        os.makedirs(MOUNTPOINT, 0o755)
    
    if not os.path.exists(RUN_PATH):
        os.makedirs(RUN_PATH, 0o755)
    
    if not os.path.exists(LIB_PATH):
        os.makedirs(LIB_PATH, 0o755)
    
    _log('mounting vdfs ..')
    FUSE(VDFS(query), MOUNTPOINT, foreground=True)

def mount():
    _clean()
    query = None
    srv = []
    if WORKER or (FS and not SHADOW):
        from db.query import Query
        query = Query()
    
    if BROKER or WORKER:
        from srv.server import Server
        srv.append(Server(query))
    
    if CACHE_SERVER:
        from db.cached import CacheD
        srv.append(CacheD())
    
    if SUPERNODE:
        from supernode import Supernode
        srv.append(Supernode())
    
    if srv:
        srv_start(srv)
    
    if FS:
        _mount(query)
    elif srv:
        srv_join(srv)
