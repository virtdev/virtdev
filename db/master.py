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
from lib.log import log_debug, log_err, log_get
from lib.util import zmqaddr, lock, server_list
from conf.virtdev import MASTER, MASTER_ADDR, MASTER_PORT, FINDER_SERVERS, MAPPER_SERVERS, DATA_SERVERS, USR_FINDER_PORT, DEV_FINDER_PORT, USR_MAPPER_PORT, DEV_MAPPER_PORT

if MASTER:
    import sophia

def _log(text):
    if LOG_MASTER:
        log_debug('Master', text)

def get_mappers(domain):
    c = zerorpc.Client()
    c.connect(zmqaddr(MASTER_ADDR, MASTER_PORT))
    try:
        mappers = c.get_mappers(domain)
        _log('domain=%s, mappers=%s' % (domain, str(mappers)))
        return mappers
    finally:
        c.close()

def get_finders(domain):
    c = zerorpc.Client()
    c.connect(zmqaddr(MASTER_ADDR, MASTER_PORT))
    try:
        finders = c.get_finders(domain)
        _log('domain=%s, finders=%s' % (domain, str(finders)))
        return finders
    finally:
        c.close()

def get_servers(domain):
    c = zerorpc.Client()
    c.connect(zmqaddr(MASTER_ADDR, MASTER_PORT))
    try:
        servers = c.get_servers(domain)
        _log('domain=%s, servers=%s' % (domain, str(servers)))
        return servers
    finally:
        c.close()

class MasterCoordinator(object):
    def __init__(self, port):
        self._port = port
        self._lock = Lock()
    
    def _log(self, text):
        if LOG_MASTER:
            if self._port == USR_FINDER_PORT:
                role = 'userFinder'
            elif self._port == DEV_FINDER_PORT:
                role = 'deviceFinder'
            elif self._port == USR_MAPPER_PORT:
                role = 'userMapper'
            elif self._port == DEV_MAPPER_PORT:
                role = 'deviceMapper'
            else:
                role = 'dataServer'
            log_debug(self, '%s->%s' % (role, text))
    
    def _check(self, addr, buf, pos, member):
        c = zerorpc.Client()
        c.connect(zmqaddr(addr, self._port))
        try:
            if member:
                self._log('check_members, addr=%s, pos=%d' % (addr, pos))
                return c.check_members(buf, pos)
            else:
                self._log('check_servers, addr=%s, pos=%d' % (addr, pos))
                return c.check_servers(buf, pos)
        finally:
            c.close()
    
    def _set(self, addr, buf, member):
        c = zerorpc.Client()
        c.connect(zmqaddr(addr, self._port))
        try:
            if member:
                self._log('set_members, addr=%s' % addr)
                c.set_members(buf)
            else:
                self._log('set_servers, addr=%s' % addr)
                c.set_servers(buf)
        finally:
            c.close()
    
    def _add(self, addr, buf, pos, member):
        c = zerorpc.Client()
        c.connect(zmqaddr(addr, self._port))
        try:
            if member:
                self._log('add_members, addr=%s, pos=%d' % (addr, pos))
                c.add_members(buf, pos)
            else:
                self._log('add_servers, addr=%s, pos=%d' % (addr, pos))
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
        self._name = name
        self._lock = Lock()
        self._cache = copy.copy(cache)
        self._coordinator = MasterCoordinator(port)
        res = self._load()
        if res:
            self._cache += res
    
    def _load(self):
        res = []
        db = sophia.Database()
        db.open(self._name)
        try:
            for i in list(db.iterkeys()):
                val = db.get(i)
                if not val:
                    log_err(self, 'failed to load')
                    raise Exception(log_get(self, 'failed to load'))
                res.append(str(val))
            return res
        finally:
            db.close()
    
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
        
        db = sophia.Database()
        db.open(self._name)
        try:
            pos = len(self._cache)
            db.begin()
            for i in buf:
                db.set(str(pos), str(i))
                pos += 1
            db.commit()
        finally:
            db.close()
        self._cache += buf
        return True

class Master(object):
    def __init__(self):
        self._user_server = MasterCache('us', server_list(DATA_SERVERS))
        self._user_finder = MasterCache('uf', FINDER_SERVERS, USR_FINDER_PORT)
        self._user_mapper = MasterCache('um', MAPPER_SERVERS, USR_MAPPER_PORT)
        self._device_server = MasterCache('ds', server_list(DATA_SERVERS))
        self._device_finder = MasterCache('df', FINDER_SERVERS, DEV_FINDER_PORT)
        self._device_mapper = MasterCache('dm', MAPPER_SERVERS, DEV_MAPPER_PORT)
    
    def get_servers(self, domain):
        if domain == DOMAIN_USR:
            return self._user_server.get()
        elif domain == DOMAIN_DEV:
            return self._device_server.get()
    
    def get_finders(self, domain):
        if domain == DOMAIN_USR:
            return self._user_finder.get()
        elif domain == DOMAIN_DEV:
            return self._device_finder.get()
    
    def get_mappers(self, domain):
        if domain == DOMAIN_USR:
            return self._user_mapper.get()
        elif domain == DOMAIN_DEV:
            return self._device_mapper.get()
    
    def add_servers(self, servers, domain):
        if not servers:
            return
        if domain == DOMAIN_USR:
            orig = self._user_server.get()
            if self._user_mapper.coordinate(servers, orig):
                return self._user_server.put(servers)
        elif domain == DOMAIN_DEV:
            orig = self._device_server.get()
            if self._device_mapper.coordinate(servers, orig):
                return self._device_server.put(servers)
    
    def add_finders(self, finders, domain):
        if not finders:
            return
        if domain == DOMAIN_USR:
            if self._user_finder.coordinate(finders):
                return self._user_finder.put(finders)
        elif domain == DOMAIN_DEV:
            if self._device_finder.coordinate(finders):
                return self._device_finder.put(finders)
    
    def add_mappers(self, mappers, domain):
        if not mappers:
            return
        if domain == DOMAIN_USR:
            if self._user_mapper.coordinate(mappers):
                return self._user_mapper.put(mappers)
        elif domain == DOMAIN_DEV:
            if self._device_mapper.coordinate(mappers):
                return self._device_mapper.put(mappers)

class MasterServer(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._master = Master()
    
    def run(self):
        srv = zerorpc.Server(self._master)
        srv.bind(zmqaddr(MASTER_ADDR, MASTER_PORT))
        srv.run()
