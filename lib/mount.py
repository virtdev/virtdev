#      mount.py
#      
#      Copyright (C) 2016 Yi-Wei Ci <ciyiwei@hotmail.com>
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
from db.router import Router
from conf.log import LOG_MOUNT
from conf.prot import PROT_NETWORK
from conf.env import PATH_MNT, PATH_LIB
from lib.protocols import PROTOCOL_WRTC
from lib.domains import DOMAIN_DEV, DOMAIN_USR
from lib.util import start_servers, wait_servers, stop_servers, ifaddr
from conf.virtdev import LO, FS, SHADOW, IFBACK, EXPOSE, GATEWAY_SERVERS, BRIDGE_SERVERS
from conf.meta import DISTRIBUTOR, META_SERVERS, CACHE_SERVERS, BROKER_SERVERS, WORKER_SERVERS
from conf.route import ROUTE, MASTER_ADDR, DATA_SERVER, USR_FINDER, USR_MAPPER, DEV_FINDER, DEV_MAPPER, DATA_SERVERS

def _clean():
    ports = []
    channel.clean()
    addr = ifaddr(ifname=IFBACK)
    
    if ifaddr() in GATEWAY_SERVERS:
        from conf.virtdev import GATEWAY_PORT
        ports.append(GATEWAY_PORT)
    
    if not SHADOW and ifaddr() in BRIDGE_SERVERS:
        from conf.virtdev import BRIDGE_PORT
        ports.append(BRIDGE_PORT)
    
    if not SHADOW and addr in CACHE_SERVERS:
        from conf.meta import CACHE_PORTS
        for i in CACHE_PORTS:
            ports.append(CACHE_PORTS[i])
    
    if LO:
        from conf.defaults import LO_PORT
        ports.append(LO_PORT)
    
    if addr in BROKER_SERVERS:
        from conf.meta import BROKER_PORT
        ports.append(BROKER_PORT)
    
    if addr == MASTER_ADDR:
        from conf.route import MASTER_PORT
        ports.append(MASTER_PORT)
    
    if USR_FINDER:
        from conf.route import USR_FINDER_PORT
        ports.append(USR_FINDER_PORT)
    
    if DEV_FINDER:
        from conf.route import DEV_FINDER_PORT
        ports.append(DEV_FINDER_PORT)
    
    if USR_MAPPER:
        from conf.route import USR_MAPPER_PORT
        ports.append(USR_MAPPER_PORT)
    
    if DEV_MAPPER:
        from conf.route import DEV_MAPPER_PORT
        ports.append(DEV_MAPPER_PORT)
    
    if DATA_SERVER or addr in DATA_SERVERS or (not ROUTE and addr in META_SERVERS):
        from conf.meta import EVENT_COLLECTOR_PORT    
        ports.append(EVENT_COLLECTOR_PORT)
    
    if addr in WORKER_SERVERS:
        from conf.meta import WORKER_PORT
        ports.append(WORKER_PORT)
    
    if addr in WORKER_SERVERS or (FS and not SHADOW):
        from conf.meta import EVENT_MONITOR_PORT
        ports.append(EVENT_MONITOR_PORT)
    
    if FS:
        from conf.defaults import CONDUCTOR_PORT, DAEMON_PORT, FILTER_PORT, HANDLER_PORT, DISPATCHER_PORT
        ports.append(DAEMON_PORT)
        ports.append(FILTER_PORT)
        ports.append(HANDLER_PORT)
        ports.append(CONDUCTOR_PORT)
        ports.append(DISPATCHER_PORT)
    
    stop_servers(ports)

def _check_settings():
    if not SHADOW and EXPOSE and PROT_NETWORK != PROTOCOL_WRTC:
        raise Exception('Error: invalid settings')

def _init():
    _clean()
    _check_settings()
    resource.setrlimit(resource.RLIMIT_NOFILE, (999999, 999999))
    if not SHADOW:
        channel.initialize()

def _unmount(path):
    os.system('umount -lf %s 2>/dev/null' % path)
    time.sleep(1)

def _mount(query, router):
    from fuse import FUSE
    from fs.vdfs import VDFS
    
    _unmount(PATH_MNT)
    if not os.path.exists(PATH_MNT):
        os.makedirs(PATH_MNT, 0o755)
    
    if not os.path.exists(PATH_LIB):
        os.makedirs(PATH_LIB, 0o755)
    
    if LOG_MOUNT:
        log('starting vdfs ...')
    
    FUSE(VDFS(query, router), PATH_MNT, foreground=True)

def mount():
    data = None
    query = None
    servers = []
    addr = ifaddr(ifname=IFBACK)
    
    _init()
    if not SHADOW and addr in BROKER_SERVERS:
        from srv.broker import Broker
        servers.append(Broker(ifaddr(), addr))
    
    if not SHADOW and ifaddr() in BRIDGE_SERVERS:
        import bridge
        servers.append(bridge.bridge)
    
    if addr == MASTER_ADDR:
        from db.master import Master
        servers.append(Master())
    
    if addr in WORKER_SERVERS or (FS and not SHADOW):
        from db.query import Query
        meta = Router(META_SERVERS)
        if ROUTE:
            data = Router(DATA_SERVERS)
        else:
            data = meta
        query = Query(meta, data)
    
    if DISTRIBUTOR:
        from srv.distributor import Distributor
        servers.append(Distributor(query))
    
    if not SHADOW and addr in CACHE_SERVERS:
        from db.cache import CacheServer
        servers.append(CacheServer())
    
    if DATA_SERVER or addr in DATA_SERVERS or (not ROUTE and addr in META_SERVERS):
        from event.collector import EventCollector
        servers.append(EventCollector(addr))
    
    if USR_FINDER or DEV_FINDER:
        from db.finder import Finder
        if USR_FINDER:
            servers.append(Finder(domain=DOMAIN_USR))
        if DEV_FINDER:
            servers.append(Finder(domain=DOMAIN_DEV))
    
    if USR_MAPPER or DEV_MAPPER:
        from db.mapper import Mapper
        if USR_MAPPER:
            servers.append(Mapper(domain=DOMAIN_USR))
        if DEV_MAPPER:
            servers.append(Mapper(domain=DOMAIN_DEV))
    
    if addr in WORKER_SERVERS:
        from srv.worker import Worker
        servers.append(Worker(addr, query))
    
    if servers:
        start_servers(servers)
    
    if FS:
        _mount(query, data)
    elif servers:
        wait_servers(servers)
