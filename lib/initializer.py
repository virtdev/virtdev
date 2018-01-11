# init.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import time
import resource
from lib import channel
from lib.types import *
from lib.log import log
from conf.route import *
from conf.virtdev import *
from conf.defaults import *
from db.router import Router
from protocols import PROTOCOL_WRTC
from lib.util import start_servers, wait_servers, stop_servers, ifaddr, get_conf_path, get_mnt_path, mkdir

def _clear_local():
    ports = []
    ports.append(DAEMON_PORT)
    ports.append(FILTER_PORT)
    ports.append(HANDLER_PORT)
    ports.append(DISPATCHER_PORT)
    if LO:
        ports.append(LO_PORT)
    stop_servers(ports)

def _clear_srv():
    ports = []
    addr = ifaddr()
    if addr in GATEWAY_SERVERS:
        ports.append(GATEWAY_PORT)

    if addr in BRIDGE_SERVERS:
        ports.append(BRIDGE_PORT)

    if addr in SIGNALING_SERVERS:
        ports.append(SIGNALING_PORT)

    addr = ifaddr(ifname=IFBACK)
    if addr in CACHE_SERVERS:
        for i in CACHE_PORTS:
            ports.append(CACHE_PORTS[i])

    if addr in BROKER_SERVERS:
        ports.append(BROKER_PORT)

    if addr == MASTER_ADDR:
        ports.append(MASTER_PORT)

    if USR_FINDER:
        ports.append(USR_FINDER_PORT)

    if DEV_FINDER:
        ports.append(DEV_FINDER_PORT)

    if USR_MAPPER:
        ports.append(USR_MAPPER_PORT)

    if DEV_MAPPER:
        ports.append(DEV_MAPPER_PORT)

    if DATA_SERVER or addr in DATA_SERVERS or (not ROUTE and addr in META_SERVERS):
        ports.append(EVENT_COLLECTOR_PORT)

    if addr in WORKER_SERVERS:
        ports.append(WORKER_PORT)

    if addr in WORKER_SERVERS or FS:
        ports.append(EVENT_MONITOR_PORT)

    if ports:
        stop_servers(ports)

def _clear_default():
    if not SHADOW:
        _clear_srv()
    else:
        _clear_local()

def _clear_edgenode():
    _clear_local()

def _clear_supernode():
    ports = []
    ports.append(BRIDGE_PORT)
    ports.append(BROKER_PORT)
    ports.append(WORKER_PORT)
    ports.append(GATEWAY_PORT)
    ports.append(SIGNALING_PORT)
    ports.append(EVENT_MONITOR_PORT)
    ports.append(EVENT_COLLECTOR_PORT)
    stop_servers(ports)

def _clear_log():
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)

def _clear(edgenode, supernode):
    _clear_log()
    if supernode:
        _clear_supernode()
    elif edgenode:
        _clear_edgenode()
    else:
        _clear_default()

def _check_settings():
    if not SHADOW and EXPOSE:
        raise Exception('Error: invalid settings')
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (999999, 999999))
    except:
        pass

def _unmount(path):
    os.system('umount -lf %s 2>/dev/null' % path)
    time.sleep(1)

def _create_fs(query, router, edgenode):
    from fuse import FUSE
    from fs.vdfs import VDFS

    if edgenode:
        from lib.util import set_edgenode
        set_edgenode()

    mnt = get_mnt_path()
    _unmount(mnt)
    mkdir(mnt)

    conf = get_conf_path()
    mkdir(conf)

    FUSE(VDFS(query, router), mnt, foreground=True)

def _init_supernode():
    servers = []
    addr = META_SERVERS[0]

    from srv.broker import Broker
    servers.append(Broker(GATEWAY_SERVERS[0], addr))

    from lib.bridge import Bridge
    servers.append(Bridge())

    from lib.signaling import Signaling
    servers.append(Signaling())

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

def _init_default():
    data = None
    query = None
    servers = []

    if not SHADOW:
        if ifaddr() in BRIDGE_SERVERS:
            from lib.bridge import Bridge
            servers.append(Bridge())

        if ifaddr() in SIGNALING_SERVERS:
            from lib.signaling import Signaling
            servers.append(Signaling())

        addr = ifaddr(ifname=IFBACK)
        if addr in BROKER_SERVERS:
            from srv.broker import Broker
            servers.append(Broker(ifaddr(), addr))

        if addr == MASTER_ADDR:
            from db.master import Master
            servers.append(Master())

        if addr in WORKER_SERVERS or FS:
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

        if addr in CACHE_SERVERS:
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

def _init_edgenode():
    return (None, None, [])

def initialize(edgenode=False, supernode=False):
    _clear(edgenode, supernode)
    _check_settings()

    if supernode:
        query, router, servers = _init_supernode()
    elif edgenode:
        query, router, servers = _init_edgenode()
    else:
        query, router, servers = _init_default()

    if FS:
        _create_fs(query, router, edgenode)

    wait_servers(servers)
