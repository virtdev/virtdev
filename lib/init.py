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
from conf.meta import *
from conf.route import *
from conf.virtdev import *
from conf.defaults import *
from db.router import Router
from conf.log import LOG_MOUNT
from conf.prot import PROT_NETWORK
from lib.protocols import PROTOCOL_WRTC
from lib.domains import DOMAIN_DEV, DOMAIN_USR
from lib.util import start_servers, wait_servers, stop_servers, ifaddr, get_conf_path, get_mnt_path, mkdir

def _clean_local():
	ports = []
	ports.append(DAEMON_PORT)
	ports.append(FILTER_PORT)
	ports.append(HANDLER_PORT)
	ports.append(CONDUCTOR_PORT)
	ports.append(DISPATCHER_PORT)

	if LO:
		ports.append(LO_PORT)

	stop_servers(ports)

def _clean_servers():
	ports = []
	addr = ifaddr()
	if addr in GATEWAY_SERVERS:
		ports.append(GATEWAY_PORT)

	if addr in BRIDGE_SERVERS:
		ports.append(BRIDGE_PORT)

	if addr in SIGNAL_SERVERS:
		ports.append(SIGNAL_PORT)

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

def _clean():
	channel.clean()
	if not SHADOW:
		_clean_servers()
	else:
		_clean_local()

def _clean_edgenode():
	_clean_local()

def _clean_supernode():
	ports = []
	ports.append(BRIDGE_PORT)
	ports.append(SIGNAL_PORT)
	ports.append(BROKER_PORT)
	ports.append(WORKER_PORT)
	ports.append(GATEWAY_PORT)
	ports.append(EVENT_MONITOR_PORT)
	ports.append(EVENT_COLLECTOR_PORT)
	stop_servers(ports)

def _check_settings():
	if not SHADOW and EXPOSE and PROT_NETWORK != PROTOCOL_WRTC:
		raise Exception('Error: invalid settings')

def _unmount(path):
	os.system('umount -lf %s 2>/dev/null' % path)
	time.sleep(1)

def _init_vdfs(query, router, edgenode, supernode):
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
		log('starting vdfs, mnt=%s' % str(mnt))

	FUSE(VDFS(query, router), mnt, foreground=True)

def _init_supernode():
	servers = []
	addr = '127.0.0.1'

	from srv.broker import Broker
	servers.append(Broker(addr, addr))

	from lib.bridge import Bridge
	servers.append(Bridge())

	from lib.sig import Signal
	servers.append(Signal())

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

def _init():
	data = None
	query = None
	servers = []

	if not SHADOW:
		if ifaddr() in BRIDGE_SERVERS:
			from lib.bridge import Bridge
			servers.append(Bridge())

		if ifaddr() in SIGNAL_SERVERS:
			from lib.sig import Signal
			servers.append(Signal())

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
	if supernode:
		_clean_supernode()
	elif edgenode:
		_clean_edgenode()
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
	elif edgenode:
		query, router, servers = _init_edgenode()
	else:
		query, router, servers = _init()

	if FS:
		_init_vdfs(query, router, edgenode, supernode)
	elif servers:
		wait_servers(servers)
