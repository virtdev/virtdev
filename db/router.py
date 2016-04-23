#      router.py
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

import time
import zerorpc
from lib.domains import *
from member import Member
from random import randint
from lib.util import zmqaddr
from threading import Thread
from lib.log import log_get, log_err
from conf.virtdev import EXTEND, MASTER_ADDR, MASTER_PORT, USR_MAPPER_PORT, DEV_MAPPER_PORT

ID_SIZE = 32
GROUP_MAX = 32
WAIT_TIME = 100 # seconds
CACHE_MAX = 10000

class Router(object):
    def __init__(self, servers=None, sync=True):
        self._cache = {}
        self._servers = servers
        self._user_mapper = Member()
        self._device_mapper = Member()
        if EXTEND and sync:
            mappers = self._load_mappers(DOMAIN_USR)
            if not mappers:
                log_err('failed to initialize')
                raise Exception(log_get(self, 'failed to initialize'))
            self._user_mapper.set_members(mappers)
            mappers = self._load_mappers(DOMAIN_DEV)
            if not mappers:
                log_err('failed to initialize')
                raise Exception(log_get(self, 'failed to initialize'))
            self._device_mapper.set_members(mappers)
            self._thread = Thread(target=self._update_mappers)
            self._thread.start()
    
    def _load_mappers(self, domain):
        c = zerorpc.Client()
        c.connect(zmqaddr(MASTER_ADDR, MASTER_PORT))
        try:
            return c.get_mappers(domain)
        finally:
            c.close()
    
    def _check_mappers(self, addr, pos, domain):
        if domain == DOMAIN_USR:
            port = USR_MAPPER_PORT
        else:
            port = DEV_MAPPER_PORT
        c = zerorpc.Client()
        c.connect(zmqaddr(addr, port))
        try:
            return c.get_members(pos)
        finally:
            c.close()
    
    def _get(self, addr, key, domain):
        if domain == DOMAIN_USR:
            port = USR_MAPPER_PORT
        else:
            port = DEV_MAPPER_PORT
        c = zerorpc.Client()
        c.connect(zmqaddr(addr, port))
        try:
            return c.get(key)
        finally:
            c.close()
    
    def get(self, key, domain=None):
        if not EXTEND or not domain:
            if not self._servers:
                log_err(self, 'failed to get')
                raise Exception(log_get(self, 'failed to get'))
            n = abs(hash(key)) % len(self._servers)
            return self._servers[n]
        else:
            if len(key) > ID_SIZE:
                key = key[:ID_SIZE]
            
            addr = self._cache.get(key)
            if addr:
                return addr
            
            if domain == DOMAIN_USR:
                mapper = self._user_mapper
            elif domain == DOMAIN_DEV:
                mapper = self._device_mapper
            else:
                log_err(self, 'failed to get, invalid domain')
                raise Exception(log_get(self, 'failed to get, invalid domain'))
            
            length = mapper.length()
            if length < GROUP_MAX:
                groups = length
            else:
                groups = GROUP_MAX
            g = abs(hash(key)) % groups
            sz = length / groups
            if sz <= 0:
                log_err(self, 'failed to get, invalid group size')
                raise Exception(log_get(self, 'failed to get, invalid group size'))
            n = g * sz + randint(0, sz - 1)
            addr = mapper.get(n)
            val = self._get(addr, key, domain)
            if val:
                if len(self._cache) >= CACHE_MAX:
                    self._cache.popitem()
                self._cache.update({key:val})
                return val
    
    def _do_update_mappers(self, domain):
        if domain == DOMAIN_USR:
            mapper = self._user_mapper
        else:
            mapper = self._device_mapper
        pos = mapper.length()
        addr = mapper.get(randint(0, pos - 1))
        members = self._check_mappers(addr, pos, domain)
        if members:
            mapper.add_members(members, pos)
    
    def _update_mappers(self):
        cnt = 0
        while True:
            try:
                time.sleep(WAIT_TIME)
                if not cnt:
                    self._do_update_mappers(DOMAIN_USR)
                    cnt += 1
                else:
                    self._do_update_mappers(DOMAIN_DEV)
                    cnt = 0
            except:
                pass
