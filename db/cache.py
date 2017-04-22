# cache.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import memcache
from lib.log import log_err
from threading import Thread
from hash_ring import HashRing
from conf.meta import CACHE_SERVERS, CACHE_PORTS
from lib.util import call, start_servers, wait_servers

class Cache(object):
	def __init__(self, port):
		self._port = port

	def _get_addr(self, key):
		servers = map(lambda addr: '%s:%d' % (addr, self._port), CACHE_SERVERS)
		return HashRing(servers).get_node(key)

	def get(self, key):
		value = None
		addr = self._get_addr(key)
		try:
			cli = memcache.Client([addr], debug=0)
			value = cli.get(key)
		except:
			log_err(self, 'failed to get from %s, key=%s' % (addr, key))
		finally:
			return value

	def put(self, key, value):
		addr = self._get_addr(key)
		try:
			cli = memcache.Client([addr], debug=0)
			cli.set(key, value)
		except:
			log_err(self, 'failed to put, key=%s' % key)

	def delete(self, key):
		addr = self._get_addr(key)
		try:
			cli = memcache.Client([addr], debug=0)
			cli.delete(key)
		except:
			log_err(self, 'failed to delete, key=%s' % key)

class CacheServer(Thread):
	def _create(self, port):
		call('memcached', '-u', 'root', '-m', '10', '-p', str(port))

	def run(self):
		servers = []
		for i in CACHE_PORTS:
			servers.append(Thread(target=self._create, args=(CACHE_PORTS[i],)))

		if servers:
			start_servers(servers)
			wait_servers(servers)
