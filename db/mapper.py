#      mapper.py
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
from threading import Thread
from conf.log import LOG_MAPPER
from lib.util import zmqaddr, ifaddr
from lib.log import log_debug, log_err, log_get
from master import get_servers, get_mappers, get_finders
from conf.virtdev import USR_MAPPER_PORT, DEV_MAPPER_PORT, USR_FINDER_PORT, DEV_FINDER_PORT

GROUP_MAX = 32
WAIT_TIME = 100 # seconds
CACHE_MAX = 100000

class MapperCache(object):
    def __init__(self, domain, server):
        self._cache = {}
        self._domain = domain
        self._server = server
        self._finder = Member()
        finders = get_finders(domain)
        if not finders:
            log_err(self, 'failed to initialize')
            raise Exception(log_get(self, 'failed to initialize'))
        self._finder.set_members(finders)
        self._thread = Thread(target=self._update_finders)
        self._thread.start()
    
    def _log(self, text):
        if LOG_MAPPER:
            log_debug(self, text)
    
    def _get_finder(self, addr):
        finder = zerorpc.Client()
        if self._domain == DOMAIN_USR:
            finder.connect(zmqaddr(addr, USR_FINDER_PORT))
        else:
            finder.connect(zmqaddr(addr, DEV_FINDER_PORT))
        return finder
    
    def _do_update_finders(self):
        pos = self._finder.length()
        n = randint(0, pos - 1)
        addr = self._finder.get(n)
        finder = self._get_finder(addr)
        if finder:
            try:
                members = finder.get_members(pos)
                if members:
                    self._log('new finders %s (pos=%d)' % (str(members), pos))
                    self._finder.add_members(members, pos)
            finally:
                finder.close()
        
    def _update_finders(self):
        while True:
            try:
                time.sleep(WAIT_TIME)
                self._do_update_finders()
            except:
                log_err(self, 'failed to update finders')
    
    def _get(self, key):
        length = self._finder.length()
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
        addr = self._finder.get(n)
        finder = self._get_finder(addr)
        if finder:
            try:
                val = finder.get(key)
                if type(val) == list and len(val) == 2 and type(val[0]) == float and type(val[1]) == int:
                    return val
            finally:
                finder.close()
    
    def _map(self, key, t, area):
        length = self._server.length()
        if not length:
            return
        
        start = 0
        end = length - 1
        left = self._server.get(start)
        right = self._server.get(end)
        if not left or not right:
            return
        
        if 1 == length:
            return left[6]
        
        while True:
            if length < 2:
                if right[0] < t:
                    pos = end
                else:
                    pos = start
                break
            length /= 2
            middle = start + length
            curr = self._server.get(middle)
            if not curr:
                return
            if curr[0] < t:
                start = middle
                left = curr
            else:
                end = middle
                right = curr
        
        curr = self._server.get(pos)
        if not curr:
            return
        
        if curr[3] == 1:
            return curr[6]
        elif curr[2] < curr[3]:
            length = curr[3]
            start = pos - curr[2]
            end = pos + curr[3] - curr[2] - 1
            left = self._server.get(start)
            right = self._server.get(end)
            if left and right:
                while True:
                    if length < 2:
                        if left[1] != right[1]:
                            if abs(area - left[1]) <= abs(area - right[1]):
                                return left[6]
                            else:
                                return right[6]
                        else:
                            if left != right:
                                if left[5] <= 1 or left[4] >= left[5]:
                                    return
                                n = abs(hash(key)) % left[5]
                                start += n - left[4]
                                left = self._server.get(start)
                                if not left:
                                    return
                            return left[6]
                    length /= 2
                    middle = start + length
                    curr = self._server.get(middle)
                    if not curr:
                        return
                    if curr[1] < area:
                        start = middle
                        left = curr
                    else:
                        right = curr
    
    def get(self, key):
        res = self._cache.get(key)
        if res:
            return res
        val = self._get(key)
        if val:
            res = self._map(key, val[0], val[1])
            if not res:
                log_err(self, 'failed to get')
                raise Exception(log_get(self, 'failed to get'))
            if len(self._cache) >= CACHE_MAX:
                self._cache.popitem()
            self._cache.update({key:res})
            self._log('get, key=%s, val=%s' % (str(key), str(res)))
            return res

class Mapper(Member):
    def __init__(self, domain):
        Member.__init__(self)
        mappers = get_mappers(domain)
        if not mappers:
            log_err(self, 'failed to initialize')
            raise Exception(log_get(self, 'failed to initialize'))
        self.set_members(mappers)
        self._server = Member()
        servers = get_servers(domain)
        if not servers:
            log_err(self, 'failed to initialize')
            raise Exception(log_get(self, 'failed to initialize'))
        self._server.set_members(servers)
        self._cache = MapperCache(domain, self._server)
    
    def set_servers(self, servers):
        return self._server.set_members(servers, sort=True)
    
    def add_servers(self, servers, pos):
        return self._server.add_members(servers, pos, sort=True)
    
    def check_servers(self, servers, pos):
        return self._server.check_members(servers, pos)
    
    def get_servers(self, pos):
        return self._server.get_members(pos)
    
    def get(self, key):
        return self._cache.get(key)

class MapperServer(Thread):
    def __init__(self, domain):
        Thread.__init__(self)
        self._domain = domain
        self._mapper = Mapper(domain)
    
    def run(self):
        srv = zerorpc.Server(self._mapper)
        if self._domain == DOMAIN_USR:
            srv.bind(zmqaddr(ifaddr(), USR_MAPPER_PORT))
        elif self._domain == DOMAIN_DEV:
            srv.bind(zmqaddr(ifaddr(), DEV_MAPPER_PORT))
        else:
            log_err(self, 'invalid domain')
            raise Exception(log_get(self, 'invalid domain'))
        srv.run()

class UserMapper(MapperServer):
    def __init__(self):
        MapperServer.__init__(self, DOMAIN_USR)

class DeviceMapper(MapperServer):
    def __init__(self):
        MapperServer.__init__(self, DOMAIN_DEV)
