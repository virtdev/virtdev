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
import channel
import resource
from lib.log import log
from subprocess import call
from db.router import Router
from conf.path import PATH_MOUNTPOINT, PATH_RUN, PATH_LIB
from lib.util import DEVNULL, srv_start, srv_join, close_port, ifaddr
from conf.virtdev import LO, FS, DISTRIBUTOR, DATA_SERVER, SHADOW, EXTEND, META_SERVERS
from conf.virtdev import DATA_SERVERS, CACHE_SERVERS, ROOT_SERVERS, BRIDGE_SERVERS, BROKER_SERVERS
from conf.virtdev import REQUEST_SERVERS, MASTER, USER_FINDER, USER_MAPPER, DEVICE_FINDER, DEVICE_MAPPER, IFBACK, PROXY_PORT

PRINT = False

def _clean():
    ports = []
    channel.clean()
    addr = ifaddr()
    baddr = ifaddr(ifname=IFBACK)
    
    ports.append(PROXY_PORT)
    if not SHADOW and addr in CACHE_SERVERS:
        from conf.virtdev import CACHE_PORTS
        for i in CACHE_PORTS:
            ports.append(CACHE_PORTS[i])
    
    if LO:
        from conf.virtdev import LO_PORT
        ports.append(LO_PORT)
    
    if not SHADOW and addr in BRIDGE_SERVERS:
        from conf.virtdev import BRIDGE_PORT
        ports.append(BRIDGE_PORT)
    
    if baddr in BROKER_SERVERS:
        from conf.virtdev import BROKER_PORT
        ports.append(BROKER_PORT)
    
    if addr in ROOT_SERVERS:
        from conf.virtdev import ROOT_PORT
        ports.append(ROOT_PORT)
    
    if MASTER:
        from conf.virtdev import MASTER_PORT
        ports.append(MASTER_PORT)
        
    if USER_FINDER:
        from conf.virtdev import USER_FINDER_PORT
        ports.append(USER_FINDER_PORT)
    
    if DEVICE_FINDER:
        from conf.virtdev import DEVICE_FINDER_PORT
        ports.append(DEVICE_FINDER_PORT)
        
    if USER_MAPPER:
        from conf.virtdev import USER_MAPPER_PORT
        ports.append(USER_MAPPER_PORT)
    
    if DEVICE_MAPPER:
        from conf.virtdev import DEVICE_MAPPER_PORT
        ports.append(DEVICE_MAPPER_PORT)
    
    if DATA_SERVER or addr in DATA_SERVERS:
        from conf.virtdev import EVENT_COLLECTOR_PORT    
        ports.append(EVENT_COLLECTOR_PORT)
    
    if baddr in REQUEST_SERVERS:
        from conf.virtdev import REQUESTER_PORT
        ports.append(REQUESTER_PORT)
    
    if baddr in REQUEST_SERVERS or (FS and not SHADOW):
        from conf.virtdev import EVENT_MONITOR_PORT
        ports.append(EVENT_MONITOR_PORT)
    
    if FS:
        from conf.virtdev import CONDUCTOR_PORT, DAEMON_PORT, FILTER_PORT, HANDLER_PORT, DISPATCHER_PORT
        ports.append(DAEMON_PORT)
        ports.append(FILTER_PORT)
        ports.append(HANDLER_PORT)
        ports.append(CONDUCTOR_PORT)
        ports.append(DISPATCHER_PORT)
    
    for i in ports:
        close_port(i)

def _mount(query, router):
    from fuse import FUSE
    from fs.vdfs import VDFS
    
    call(['umount', '-lf', PATH_MOUNTPOINT], stderr=DEVNULL, stdout=DEVNULL)
    time.sleep(1)
    
    if not os.path.exists(PATH_MOUNTPOINT):
        os.makedirs(PATH_MOUNTPOINT, 0o755)
    
    if not os.path.exists(PATH_RUN):
        os.makedirs(PATH_RUN, 0o755)
    
    if not os.path.exists(PATH_LIB):
        os.makedirs(PATH_LIB, 0o755)
    
    if PRINT:
        log('Mounting VDFS ...')
    FUSE(VDFS(query, router), PATH_MOUNTPOINT, foreground=True)

def mount():
    srv = []
    query = None
    addr = ifaddr()
    data_router = None
    baddr = ifaddr(ifname=IFBACK)
    
    _clean()
    resource.setrlimit(resource.RLIMIT_NOFILE, (999999, 999999)) 
    
    if MASTER:
        from db.master import MasterServer
        srv_start([MasterServer()])
    
    if baddr in REQUEST_SERVERS or (FS and not SHADOW):
        from db.query import Query
        meta_router = Router(META_SERVERS)
        if not EXTEND:
            data_router = Router(DATA_SERVERS)
        else:
            data_router = meta_router
        query = Query(meta_router, data_router)
    
    if DISTRIBUTOR:
        from srv.distributor import Distributor
        srv.append(Distributor(query))
    
    if baddr in BROKER_SERVERS:
        from srv.broker import Broker
        srv.append(Broker(addr, baddr))
    
    if not SHADOW and addr in CACHE_SERVERS:
        from db.cache import CacheServer
        srv.append(CacheServer())
    
    if not SHADOW and addr in BRIDGE_SERVERS:
        from bridge import bridge
        if bridge:
            srv.append(bridge)
    
    if DATA_SERVER or addr in DATA_SERVERS:
        from event.collector import EventCollector
        srv.append(EventCollector(baddr))
    
    if USER_FINDER:
        from db.finder import UserFinder
        srv.append(UserFinder())
    
    if DEVICE_FINDER:
        from db.finder import DeviceFinder
        srv.append(DeviceFinder())
    
    if USER_MAPPER:
        from db.mapper import UserMapper
        srv.append(UserMapper())
    
    if DEVICE_MAPPER:
        from db.mapper import DeviceMapper
        srv.append(DeviceMapper())
    
    if baddr in REQUEST_SERVERS:
        from srv.requester import Requester
        srv.append(Requester(baddr, query))
    
    if srv:
        srv_start(srv)
    
    if FS:
        _mount(query, data_router)
    elif srv:
        srv_join(srv)
