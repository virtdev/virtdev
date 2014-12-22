#      cache.py
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

import memcache
from lib.log import log
from hash_ring import HashRing
from conf.virtdev import VDEV_CACHE_SERVERS

class VDevDBCache(object):
    def __init__(self, port):
        self._port = port
    
    def _locate(self, key):
        servers = map(lambda addr: '%s:%d' % (addr, self._port), VDEV_CACHE_SERVERS)
        return HashRing(servers).get_node(key)
    
    def put(self, key, value):
        addr = self._locate(key)
        try:
            cli = memcache.Client([addr], debug=0)
            cli.set(key, value)
        except:
            log(self, 'failed to put, key=%s' % key)
    
    def get(self, key):
        value = None
        addr = self._locate(key)
        try:
            cli = memcache.Client([addr], debug=0)
            value = cli.get(key)
        except:
            log(self, 'failed to get from %s, key=%s' % (addr, key))
        finally:
            return value
    
    def remove(self, key):
        addr = self._locate(key)
        try:
            cli = memcache.Client([addr], debug=0)
            cli.delete(key)        
        except:
            log(self, 'failed to remove, key=%s' % key)
    