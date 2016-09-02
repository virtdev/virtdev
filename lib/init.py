# init.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import time
import channel
import resource
from lib.log import log
from db.router import Router
from conf.log import LOG_MOUNT
from conf.prot import PROT_NETWORK
from lib.protocols import PROTOCOL_WRTC
from lib.domains import DOMAIN_DEV, DOMAIN_USR
from conf.virtdev import LO, FS, SHADOW, IFBACK, EXPOSE, GATEWAY_SERVERS, BRIDGE_SERVERS
from conf.meta import DISTRIBUTOR, META_SERVERS, CACHE_SERVERS, BROKER_SERVERS, WORKER_SERVERS
from lib.util import start_servers, wait_servers, stop_servers, ifaddr, get_conf_path, get_mnt_path, mkdir
from conf.route import ROUTE, MASTER_ADDR, DATA_SERVER, USR_FINDER, USR_MAPPER, DEV_FINDER, DEV_MAPPER, DATA_SERVERS

def _clean_fs():
    ports = []
    from conf.defaults import CONDUCTOR_PORT, DAEMON_PORT, FILTER_PORT, HANDLER_PORT, DISPATCHER_PORT
    ports.append(DAEMON_PORT)
    ports.append(FILTER_PORT)
    ports.append(HANDLER_PORT)
    ports.append(CONDUCTOR_PORT)
    ports.append(DISPATCHER_PORT)
    stop_servers(ports)

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
    
    stop_servers(ports)
    if FS:
        _clean_fs()

def _clean_edgenode():
    _clean_fs()

def _clean_supernode():
    ports = []
    from conf.virtdev import GATEWAY_PORT
    ports.append(GATEWAY_PORT)
    
    from conf.virtdev import BRIDGE_PORT
    ports.append(BRIDGE_PORT)
    
    from conf.meta import BROKER_PORT
    ports.append(BROKER_PORT)
    
    from conf.meta import EVENT_COLLECTOR_PORT    
    ports.append(EVENT_COLLECTOR_PORT)
    
    from conf.meta import WORKER_PORT
    ports.append(WORKER_PORT)
    
    from conf.meta import EVENT_MONITOR_PORT
    ports.append(EVENT_MONITOR_PORT)
    
    stop_servers(ports)
    _clean_fs()

def _check_settings():
    if not SHADOW and EXPOSE and PROT_NETWORK != PROTOCOL_WRTC:
        raise Exception('Error: invalid settings')

def _unmount(path):
    os.system('umount -lf %s 2>/dev/null' % path)
    time.sleep(1)

def _initialize(query, router, edgenode, supernode):
    from fuse import FUSE
    from fs.vdfs import VDFS
    
    if supernode:
        from lib.util import set_supernode
        set_supernode()
    elif edgenode:
        from lib.util import set_edgenode
        set_edgenode()
    
    mnt = get_mnt_path()
    _unmount(mnt)
    mkdir(mnt)
    
    conf = get_conf_path()
    mkdir(conf)
    
    if LOG_MOUNT:
        log('starting vdfs ...')
    
    FUSE(VDFS(query, router), mnt, foreground=True)

def _check_user(addr):
    from db.interface.module.mongo import Mongo
    db = Mongo('user')
    conn = db.connect(addr)
    if not db.get(conn, 'user'):
        from conf.user import UID, USER, PASSWORD
        db.put(conn, {'user':USER}, {'user':USER, 'password':PASSWORD, 'uid':UID}, create=True)

def _init_supernode():
    servers = []
    addr = '127.0.0.1'
    _check_user(addr)
    
    from srv.broker import Broker
    servers.append(Broker(addr, addr))
    
    from bridge import Bridge
    servers.append(Bridge())
    
    from db.query import Query
    meta = Router([addr])
    meta.local = True
    query = Query(meta, meta)
    
    from srv.distributor import Distributor
    servers.append(Distributor(query))
    
    from event.collector import EventCollector
    servers.append(EventCollector(addr))
    
    from srv.worker import Worker
    servers.append(Worker(addr, query))
    
    start_servers(servers)
    return (query, meta, servers)

def _init_servers():
    data = None
    query = None
    servers = []
    addr = ifaddr(ifname=IFBACK)
    
    if not SHADOW and addr in BROKER_SERVERS:
        from srv.broker import Broker
        servers.append(Broker(ifaddr(), addr))
    
    if not SHADOW and ifaddr() in BRIDGE_SERVERS:
        from bridge import Bridge
        servers.append(Bridge())
    
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
    
    return (query, data, servers)

def initialize(edgenode=False, supernode=False):
    if edgenode:
        _clean_edgenode()
    elif supernode:
        _clean_supernode()
    else:
        _clean()
    
    _check_settings()
    
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (999999, 999999))
    except:
        pass
    
    if not SHADOW:
        channel.initialize()
    
    if supernode:
        query, router, servers = _init_supernode()
    elif not edgenode:
        query, router, servers = _init_servers()
    else:
        query, router, servers = (None, None, None)
    
    if FS:
        _initialize(query, router, edgenode, supernode)
    elif servers:
        wait_servers(servers)
