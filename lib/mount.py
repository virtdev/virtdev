#      mount.py
#      
#      Copyright (C) 2014-2016 Yi-Wei Ci <ciyiwei@hotmail.com>
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
from conf.log import LOG_MOUNT
from conf.virtdev import PATH_MNT, PATH_RUN, PATH_LIB
from lib.util import DEVNULL, srv_start, srv_join, close_port, ifaddr
from conf.virtdev import LO, FS, SHADOW, EXTEND, IFBACK, ADAPTER_PORT
from conf.virtdev import MASTER, DISTRIBUTOR, DATA_SERVER, USR_FINDER, USR_MAPPER, DEV_FINDER, DEV_MAPPER
from conf.virtdev import META_SERVERS, DATA_SERVERS, CACHE_SERVERS, ROOT_SERVERS, BRIDGE_SERVERS, BROKER_SERVERS, WORKER_SERVERS

def _clean():
    ports = []
    channel.clean()
    addr = ifaddr()
    baddr = ifaddr(ifname=IFBACK)
    
    ports.append(ADAPTER_PORT)
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
        
    if USR_FINDER:
        from conf.virtdev import USR_FINDER_PORT
        ports.append(USR_FINDER_PORT)
    
    if DEV_FINDER:
        from conf.virtdev import DEV_FINDER_PORT
        ports.append(DEV_FINDER_PORT)
        
    if USR_MAPPER:
        from conf.virtdev import USR_MAPPER_PORT
        ports.append(USR_MAPPER_PORT)
    
    if DEV_MAPPER:
        from conf.virtdev import DEV_MAPPER_PORT
        ports.append(DEV_MAPPER_PORT)
    
    if DATA_SERVER or addr in DATA_SERVERS:
        from conf.virtdev import EVENT_COLLECTOR_PORT    
        ports.append(EVENT_COLLECTOR_PORT)
    
    if baddr in WORKER_SERVERS:
        from conf.virtdev import REQUESTER_PORT
        ports.append(REQUESTER_PORT)
    
    if baddr in WORKER_SERVERS or (FS and not SHADOW):
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

def _check_settings():
    from lib.protocols import PROTOCOL_WRTC
    from conf.virtdev import PROTOCOL, EXPOSE
    if not SHADOW and EXPOSE and PROTOCOL != PROTOCOL_WRTC:
        raise Exception('Error: invalid settings')
    
def _mount(query, router):
    from fuse import FUSE
    from fs.vdfs import VDFS
    
    call(['umount', '-lf', PATH_MNT], stderr=DEVNULL, stdout=DEVNULL)
    time.sleep(1)
    
    if not os.path.exists(PATH_MNT):
        os.makedirs(PATH_MNT, 0o755)
    
    if not os.path.exists(PATH_RUN):
        os.makedirs(PATH_RUN, 0o755)
    
    if not os.path.exists(PATH_LIB):
        os.makedirs(PATH_LIB, 0o755)
    
    if LOG_MOUNT:
        log('Mounting VDFS ...')
    
    FUSE(VDFS(query, router), PATH_MNT, foreground=True)

def mount():
    srv = []
    data = None
    query = None
    addr = ifaddr()
    baddr = ifaddr(ifname=IFBACK)
    
    _clean()
    _check_settings()
    resource.setrlimit(resource.RLIMIT_NOFILE, (999999, 999999)) 
    
    if MASTER:
        from db.master import MasterServer
        srv_start([MasterServer()])
    
    if baddr in WORKER_SERVERS or (FS and not SHADOW):
        from db.query import Query
        meta = Router(META_SERVERS)
        if not EXTEND:
            data = Router(DATA_SERVERS)
        else:
            data = meta
        query = Query(meta, data)
    
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
    
    if USR_FINDER:
        from db.finder import UserFinder
        srv.append(UserFinder())
    
    if DEV_FINDER:
        from db.finder import DeviceFinder
        srv.append(DeviceFinder())
    
    if USR_MAPPER:
        from db.mapper import UserMapper
        srv.append(UserMapper())
    
    if DEV_MAPPER:
        from db.mapper import DeviceMapper
        srv.append(DeviceMapper())
    
    if baddr in WORKER_SERVERS:
        from srv.worker import Worker
        srv.append(Worker(baddr, query))
    
    if srv:
        srv_start(srv)
    
    if FS:
        _mount(query, data)
    elif srv:
        srv_join(srv)
