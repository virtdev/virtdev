#      cache.py
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

import memcache
from lib.log import log_err
from subprocess import call
from threading import Thread
from hash_ring import HashRing
from lib.util import srv_start, srv_join
from conf.virtdev import CACHE_SERVERS, CACHE_PORTS

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
        call(['memcached', '-u', 'root', '-m', '10', '-p', str(port)])
    
    def run(self):
        srv = []
        for i in CACHE_PORTS:
            srv.append(Thread(target=self._create, args=(CACHE_PORTS[i],)))
        
        if srv:
            srv_start(srv)
            srv_join(srv)
