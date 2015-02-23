#      manager.py
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

import os
import re
import time
import shelve
from lib import tunnel
from lib import notifier
from lib.lock import VDevLock
from server import VDevServer
from lib.daemon import VDevDaemon
from threading import Lock, Thread
from proc.sandbox import VDevSandbox
from lib.log import log_err, log_get
from lib.request import VDevAuthRequest
from proc.synchronizer import VDevSynchronizer
from vdev import VDEV_MODE_SYNC, VDEV_OPEN, VDEV_CLOSE
from conf.virtdev import VDEV_LO, VDEV_BLUETOOTH, VDEV_SANDBOX, VDEV_MAPPER_PORT, VDEV_HANDLER_PORT, VDEV_DISPATCHER_PORT
from conf.virtdev import VDEV_LIB_PATH, VDEV_RUN_PATH, VDEV_FILE_SERVICE, VDEV_FILE_SHADOW, VDEV_SPECIAL, VDEV_FS_MOUNTPOINT
from lib.util import USERNAME_SIZE, PASSWORD_SIZE, VDEV_FLAG_SPECIAL, netaddresses, get_node, vdev_name, cat, lock, named_lock

class DeviceManager(object):
    def __init__(self, server): 
        self._lock = VDevLock()
        self._server = server
    
    @named_lock
    def open(self, name):
        for device in self._server.devices:
            dev = device.find(name)
            if dev:
                dev.proc(name, VDEV_OPEN)
                return
    
    @named_lock
    def close(self, name):
        for device in self._server.devices:
            dev = device.find(name)
            if dev:
                dev.proc(name, VDEV_CLOSE)
                return
    
    @named_lock
    def add(self, name, mode=None, freq=None, profile=None):
        return self._server.request.device.add(node=get_node(), addr=self._server.addr, name=name, mode=mode, freq=freq, profile=profile)
    
    @named_lock
    def sync(self, name, buf):
        if self._server.synchronizer.get_mode(name) & VDEV_MODE_SYNC:
            return self._server.request.device.sync(name=name, buf=buf)
    
    @named_lock
    def get(self, name):
        return self._server.request.device.get(name=name)
    
    @named_lock
    def diff(self, name, label, item, buf):
        return self._server.request.device.diff(name=name, label=label, item=item, buf=buf)
    
    @named_lock
    def remove(self, name):
        return self._server.request.device.remove(node=get_node(), name=name)

class GuestManager(object):
    def __init__(self, server):
        self._lock = Lock()
        self._server = server
    
    @lock
    def join(self, dest, src):
        return self._server.request.guest.join(dest=dest, src=src)
    
    @lock
    def accept(self, dest, src):
        return self._server.request.guest.accept(dest=dest, src=src)
    
    @lock
    def drop(self, dest, src):
        return self._server.request.guest.drop(dest=dest, src=src)

class NodeManager(object):
    def __init__(self, server):
        self._lock = Lock()
        self._server = server
    
    @lock
    def search(self, user, random, limit):
        return self._server.request.node.search(user=user, random=random, limit=limit)
    
    @lock
    def find(self, user, node):
        return self._server.request.node.search(user=user, node=node)

class TunnelManager(object):
    def __init__(self, server):
        self._lock = VDevLock()
        self._server = server
    
    @named_lock
    def open(self, name):
        uid, addr = self._server.get_device(name)
        if not uid:
            log_err(self, 'failed to create, no uid')
            raise Exception(log_get(self, 'failed to create'))
        if not tunnel.exist(addr):
            token = self._server.get_token(uid)
            if not token:
                log_err(self, 'failed to create, no token')
                raise Exception(log_get(self, 'failed to create'))
            tunnel.connect(addr, token)
    
    @named_lock
    def close(self, name):
        addr = self._server.get_device(name)[1]
        if tunnel.exist(addr):
            tunnel.disconnect(addr)
    
    @named_lock
    def put(self, name, **args):
        dev = self._server.get_device(name)
        addr = tunnel.addr2ip(dev[1])
        tunnel.put(addr, 'put', args, self._server.uid, self._server.token)

class MemberManager(object):
    def __init__(self, server):
        self._lock = Lock()
        self._server = server
        self._path = os.path.join(VDEV_LIB_PATH, vdev_name(server.uid))
    
    @lock
    def list(self):
        d = shelve.open(self._path)
        try:
            keys = d.keys()
            if len(keys) > 0:
                i = keys[0]
                res = cat(i, d[i]['user'], d[i]['node'], d[i]['state'])
                for j in range(1, len(keys)):
                    i = keys[j]
                    res += ';%s' % cat(i, d[i]['user'], d[i]['node'], d[i]['state'])
                return res
        finally:
            d.close()
    
    @lock
    def update(self, item):
        if type(item) != dict or len(item) != 1:
            log_err(self, 'failed to update, invalid type')
            return
        d = shelve.open(self._path)
        try:
            d.update(item)
        finally:
            d.close()
    
    @lock
    def remove(self, name):
        name = str(name)
        d = shelve.open(self._path)
        try:
            if d.has_key(name):
                del d[name]
        finally:
            d.close()

class VDevManager(object):
    def _init_sandbox(self):
        self._mapper = VDevSandbox(VDEV_MAPPER_PORT)
        self._handler = VDevSandbox(VDEV_HANDLER_PORT)
        self._dispatcher = VDevSandbox(VDEV_DISPATCHER_PORT)
        self._mapper.start()
        self._handler.start()
        self._dispatcher.start()
    
    def _init_server(self):
        server = VDevServer(self)
        self.node = NodeManager(server)
        self.guest = GuestManager(server)
        self.device = DeviceManager(server)
        self.tunnel = TunnelManager(server)
        self.member = MemberManager(server)
        self._server = server
        server.start()
    
    def _init_daemon(self):
        self._daemon = VDevDaemon(self)
        self._daemon.start()
    
    def _save_addr(self):
        d = shelve.open(VDEV_RUN_PATH)
        try:
            d['addr'] = self.addr
            d['node'] = get_node()
            d['vdev'] = vdev_name(self.uid)
        finally:
            d.close()
    
    def _get_password(self):
        path = os.path.join(VDEV_LIB_PATH, 'user')
        d = shelve.open(path)
        try:
            user = d['user']
            password = d['password']
        finally:
            d.close()
        return (user, password)
    
    def _check_user(self, user, password):
        length = len(user)
        if length > 0 and length < USERNAME_SIZE and re.match('^[0-9a-zA-Z]+$', user):
            name = user + (USERNAME_SIZE - length) * '*'
        else:
            log_err(self, 'failed to login, invalid user name')
            return
        
        if len(password) != PASSWORD_SIZE:
            log_err(self, 'failed to login, invalid password')
            return
        
        flags = 0
        if VDEV_SPECIAL:
            flags |= VDEV_FLAG_SPECIAL
        
        req = VDevAuthRequest(name, password)
        res = req.auth.login(node=get_node(), networks=netaddresses(mask=True), flags=flags)
        if not res:
            log_err(self, 'failed to login')
            return
        return (res['uid'], res['addr'], res['token'])
    
    def _auth(self, user, password):
        uid, addr, token = self._check_user(user, password)
        self.token = token
        self.user = user
        self.addr = addr
        self.uid = uid
        return uid
    
    def _prepare(self):
        self.lo = None
        self._bt = None
        self.devices = []
        self.guest = None
        self.device = None
        self.tunnel = None
        self._mapper = None
        self._handler = None
        self._dispatcher = None
        
        if not VDEV_FILE_SERVICE or not VDEV_FILE_SHADOW:
            return
        
        user, password = self._get_password()
        if not user or not password:
            log_err(self, 'failed to initialize devices, invalid password')
            raise Exception(log_get(self, 'failed to initialize devices'))
        
        uid = self._auth(user, password)
        if not uid:
            log_err(self, 'failed to initialize devices, invalid uid')
            raise Exception(log_get(self, 'failed to initialize devices'))
        
        if VDEV_BLUETOOTH:
            from dev.bt import VDevBT
            self._bt = VDevBT(self)
            self.devices.append(self._bt)
        
        if VDEV_LO:
            from dev.lo import VDevLo
            self.lo = VDevLo(self)
            self.devices.append(self.lo)
        
        if VDEV_SANDBOX:
            self._init_sandbox()
        
        self.synchronizer = VDevSynchronizer(self)
        self._init_server()
        
        if VDEV_SPECIAL:
            self._init_daemon()
    
    def __init__(self):
        self.uid = None
        self.addr = None
        self.token = None
        self._server = None
        self._daemon = None
        self._active = False
        self._prepare()
        
    def _start_devices(self):
        path = os.path.join(VDEV_FS_MOUNTPOINT, self.uid)
        while not os.path.exists(path):
            time.sleep(0.1)
        for device in self.devices:
            device.start()
    
    def start(self):
        if not self._active:
            Thread(target=self._start_devices).start()
            self._save_addr()
            self._active = True
    
    def notify(self, op, buf):
        notifier.push(op, buf)
    
    def chkaddr(self, name):
        if name and self._server:
            token = self._server.get_token(name)
            if token:
                _, addr = self._server.get_device(name)
                return (addr, token)
    