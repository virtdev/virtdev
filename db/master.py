#      master.py
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

import copy
import zerorpc
from lib.domains import *
from conf.log import LOG_MASTER
from threading import Thread, Lock
from interface.localdb import LocalDB
from lib.log import log_debug, log_err, log_get
from lib.util import zmqaddr, lock, server_info
from conf.route import MASTER_ADDR, FINDER_SERVERS, MAPPER_SERVERS, DATA_SERVERS
from conf.route import MASTER_PORT, USR_FINDER_PORT, DEV_FINDER_PORT, USR_MAPPER_PORT, DEV_MAPPER_PORT

class MasterCoordinator(object):
    def __init__(self, port):
        self._port = port
        self._lock = Lock()
    
    def _log(self, text):
        if LOG_MASTER:
            if self._port == USR_FINDER_PORT:
                role = 'USR_FINDER@Master'
            elif self._port == DEV_FINDER_PORT:
                role = 'DEV_FINDER@Master'
            elif self._port == USR_MAPPER_PORT:
                role = 'USR_MAPPER@Master'
            elif self._port == DEV_MAPPER_PORT:
                role = 'DEV_MAPPER@Master'
            else:
                role = 'DATA_SERVER@Master'
            log_debug(role, text)
    
    def _check(self, addr, buf, pos, member):
        c = zerorpc.Client()
        c.connect(zmqaddr(addr, self._port))
        try:
            if member:
                self._log('check members, addr=%s, pos=%d' % (addr, pos))
                return c.check_members(buf, pos)
            else:
                self._log('check servers, addr=%s, pos=%d' % (addr, pos))
                return c.check_servers(buf, pos)
        finally:
            c.close()
    
    def _set(self, addr, buf, member):
        c = zerorpc.Client()
        c.connect(zmqaddr(addr, self._port))
        try:
            if member:
                self._log('set members, addr=%s' % addr)
                c.set_members(buf)
            else:
                self._log('set servers, addr=%s' % addr)
                c.set_servers(buf)
        finally:
            c.close()
    
    def _add(self, addr, buf, pos, member):
        c = zerorpc.Client()
        c.connect(zmqaddr(addr, self._port))
        try:
            if member:
                self._log('add members, addr=%s, pos=%d' % (addr, pos))
                c.add_members(buf, pos)
            else:
                self._log('add servers, addr=%s, pos=%d' % (addr, pos))
                c.add_servers(buf, pos)
        finally:
            c.close()
    
    @lock
    def coordinate(self, dest, orig, delta, member=True):
        try:
            pos = len(orig)
            for i in dest:
                if not self._check(i, delta, pos, member):
                    self._set(i, orig, member)
            for i in dest:
                self._add(i, delta, pos, member)
            return True
        except:
            log_err(self, 'failed to coordinate')

class MasterCache(object):
    def __init__(self, name, cache, port=0):
        self._lock = Lock()
        self._db = LocalDB(name)
        self._cache = copy.copy(cache)
        self._coordinator = MasterCoordinator(port)
        res = self._load()
        if res:
            self._cache += res
    
    def _load(self):
        res = []
        for i in list(self._db.get_keys()):
            val = self._db.get(i)
            if not val:
                log_err(self, 'failed to load')
                raise Exception(log_get(self, 'failed to load'))
            res.append(str(val))
        return res
    
    def coordinate(self, delta, orig=None):
        if not delta:
            return
        if orig != None:
            member = False
            dest = self.get()
        else:
            member = True
            orig = self.get()
            dest = orig + delta
        for i in delta:
            if i in orig:
                return
        return self._coordinator.coordinate(dest, orig, delta, member)
    
    @lock
    def get(self):
        return copy.copy(self._cache)
    
    @lock
    def put(self, buf):
        if not buf or type(buf) != list:
            log_err(self, 'failed to put')
            raise Exception(log_get(self, 'failed to put'))
        
        for i in buf:
            if i in self._cache:
                log_err(self, 'failed to put')
                raise Exception(log_get(self, 'failed to put'))
        
        pos = len(self._cache)
        bench = self._db.bench()
        for i in buf:
            self._db.put(str(pos), str(i))
            pos += 1
        self._db.commit(bench)
        self._cache += buf
        return True

class MasterServer(object):
    def __init__(self):
        self._usr_server = MasterCache('us', server_info(DATA_SERVERS))
        self._dev_server = MasterCache('ds', server_info(DATA_SERVERS))
        self._usr_finder = MasterCache('uf', FINDER_SERVERS, USR_FINDER_PORT)
        self._dev_finder = MasterCache('df', FINDER_SERVERS, DEV_FINDER_PORT)
        self._usr_mapper = MasterCache('um', MAPPER_SERVERS, USR_MAPPER_PORT)
        self._dev_mapper = MasterCache('dm', MAPPER_SERVERS, DEV_MAPPER_PORT)
    
    def get_servers(self, domain):
        if domain == DOMAIN_USR:
            return self._usr_server.get()
        elif domain == DOMAIN_DEV:
            return self._dev_server.get()
    
    def get_finders(self, domain):
        if domain == DOMAIN_USR:
            return self._usr_finder.get()
        elif domain == DOMAIN_DEV:
            return self._dev_finder.get()
    
    def get_mappers(self, domain):
        if domain == DOMAIN_USR:
            return self._usr_mapper.get()
        elif domain == DOMAIN_DEV:
            return self._dev_mapper.get()
    
    def add_servers(self, servers, domain):
        if not servers:
            return
        if domain == DOMAIN_USR:
            orig = self._usr_server.get()
            if self._usr_mapper.coordinate(servers, orig):
                return self._usr_server.put(servers)
        elif domain == DOMAIN_DEV:
            orig = self._dev_server.get()
            if self._dev_mapper.coordinate(servers, orig):
                return self._dev_server.put(servers)
    
    def add_finders(self, finders, domain):
        if not finders:
            return
        if domain == DOMAIN_USR:
            if self._usr_finder.coordinate(finders):
                return self._usr_finder.put(finders)
        elif domain == DOMAIN_DEV:
            if self._dev_finder.coordinate(finders):
                return self._dev_finder.put(finders)
    
    def add_mappers(self, mappers, domain):
        if not mappers:
            return
        if domain == DOMAIN_USR:
            if self._usr_mapper.coordinate(mappers):
                return self._usr_mapper.put(mappers)
        elif domain == DOMAIN_DEV:
            if self._dev_mapper.coordinate(mappers):
                return self._dev_mapper.put(mappers)

class Master(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._master = MasterServer()
    
    def run(self):
        srv = zerorpc.Server(self._master)
        srv.bind(zmqaddr(MASTER_ADDR, MASTER_PORT))
        srv.run()
