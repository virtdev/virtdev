#      manager.py
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

import os
import re
import time
import shelve
from json import dumps
from lib import channel
from lib import notifier
from proc.core import Core
from proc.proc import Proc
from lib.daemon import Daemon
from lib.lock import NamedLock
from lib.request import Request
from lib.modes import MODE_VISI
from threading import Lock, Thread
from lib.log import log_err, log_get
from conf.path import PATH_LIB, PATH_MNT
from conductor import Conductor, conductor
from lib.operations import OP_OPEN, OP_CLOSE
from lib.util import USERNAME_SIZE, PASSWORD_SIZE, get_node, get_name, lock, named_lock
from conf.virtdev import LO, BT, USB, FS, SHADOW, EXPOSE, COMPUTE, PROC_ADDR, FILTER_PORT, HANDLER_PORT, DISPATCHER_PORT

LOGIN_RETRY = 1
CONNECT_RETRY = 1
LOGIN_INTERVAL = 5 # seconds

class DeviceManager(object):
    def __init__(self): 
        self._lock = NamedLock()
    
    @named_lock
    def open(self, name):
        for parent in conductor.devices:
            device = parent.find(name)
            if device:
                device.proc(name, OP_OPEN)
                return
    
    @named_lock
    def close(self, name):
        for parent in conductor.devices:
            device = parent.find(name)
            if device:
                device.proc(name, OP_CLOSE)
                return
    
    def add(self, name, mode=None, freq=None, prof=None): 
        return conductor.request.device.add(node=get_node(), addr=conductor.addr, name=name, mode=mode, freq=freq, prof=prof)
    
    def put(self, name, buf):
        return conductor.request.device.put(name=name, buf=buf)
    
    def find(self, name):
        return conductor.request.device.find(name=name)
    
    def get(self, name, field, item, buf):
        return conductor.request.device.get(name=name, field=field, item=item, buf=buf)
    
    def delete(self, name):
        return conductor.request.device.delete(node=get_node(), name=name)

class GuestManager(object):
    def __init__(self):
        self._lock = Lock()
    
    @lock
    def join(self, dest, src):
        return conductor.request.guest.join(user=conductor.user, dest=dest, src=src)
    
    @lock
    def accept(self, dest, src):
        return conductor.request.guest.accept(user=conductor.user, dest=dest, src=src)
    
    @lock
    def drop(self, dest, src):
        return conductor.request.guest.drop(dest=dest, src=src)

class NodeManager(object):
    def __init__(self):
        self._lock = Lock()
    
    @lock
    def find(self, user, node):
        return conductor.request.node.find(user=user, node=node)

class ChannelManager(object):
    def __init__(self):
        self._lock = NamedLock()
    
    def _try_connect(self, name):
        uid, node, addr = conductor.get_device(name)
        if not uid:
            log_err(self, 'failed to connect')
            raise Exception(log_get(self, 'failed to connect'))
        if not channel.exist(addr):
            key = conductor.get_key(uid, node)
            if not key:
                log_err(self, 'failed to connect')
                raise Exception(log_get(self, 'failed to connect'))
            try:
                channel.connect(uid, addr, key)
            except:
                conductor.remove_device(name)
                conductor.remove_key(uid, node)
        return True
    
    @named_lock
    def connect(self, name):
        for _ in range(CONNECT_RETRY + 1):
            if self._try_connect(name):
                return    
        log_err(self, 'failed to connect')
        raise Exception(log_get(self, 'failed to connect'))
    
    @named_lock
    def disconnect(self, name):
        _, _, addr = conductor.get_device(name)
        if channel.exist(addr):
            channel.disconnect(addr)
    
    @named_lock
    def put(self, name, **args):
        uid = conductor.uid
        token = conductor.token 
        _, _, addr = conductor.get_device(name)
        channel.put(uid, addr, 'put', args, token)
    
    @named_lock
    def push(self, name, **args):
        uid = conductor.uid
        token = conductor.token 
        _, _, addr = conductor.get_device(name)
        channel.push(uid, addr, 'put', args, token)

class MemberManager(object):
    def __init__(self):
        self._lock = Lock()
        self._path = os.path.join(PATH_LIB, get_name(conductor.uid, get_node()))
    
    @lock
    def list(self):
        d = shelve.open(self._path)
        try:
            keys = d.keys()
            if len(keys) > 0:
                i = keys[0]
                res = dumps({'name':i, 'user':d[i]['user'], 'node':d[i]['node'], 'state':d[i]['state']})
                for j in range(1, len(keys)):
                    i = keys[j]
                    res += ';' + dumps({'name':i, 'user':d[i]['user'], 'node':d[i]['node'], 'state':d[i]['state']})
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
    def delete(self, name):
        name = str(name)
        d = shelve.open(self._path)
        try:
            if d.has_key(name):
                del d[name]
        finally:
            d.close()

class Manager(object):
    def __init__(self):
        self._lo = None
        self._bt = None
        self.uid = None
        self.key = None
        self._usb = None
        self.addr = None
        self.guest = None
        self.token = None
        self.devices = []
        self.device = None
        self.channel = None
        self._daemon = None
        self._filter = None
        self._ready = False
        self._handler = None
        self._active = False
        self._listener = None
        self._dispatcher = None
        self._init()
    
    def _init(self):
        if not FS or not SHADOW:
            return
        self._init_user()
        self._init_proc()
        self._init_manager()
        self._init_core()
        self._init_devices()
        if EXPOSE:
            self._init_daemon()
    
    def _init_proc(self):
        self._filter = Proc(self, PROC_ADDR, FILTER_PORT)
        self._handler = Proc(self, PROC_ADDR, HANDLER_PORT)
        self._dispatcher = Proc(self, PROC_ADDR, DISPATCHER_PORT)
        self._filter.start()
        self._handler.start()
        self._dispatcher.start()
    
    def _init_manager(self):
        Conductor().create(self)
        self.node = NodeManager()
        self.guest = GuestManager()
        self.device = DeviceManager()
        self.member = MemberManager()
        self.channel = ChannelManager()        
    
    def _init_daemon(self):
        self._daemon = Daemon(self)
        self._daemon.start()
    
    def _init_devices(self):
        if BT:
            from interface.bt import Bluetooth
            self._bt = Bluetooth(self.uid, self.core)
            self.devices.append(self._bt)
        
        if LO:
            from interface.lo import Lo
            self._lo = Lo(self.uid, self.core)
            self.devices.append(self._lo)
        
        if USB:
            from interface.usb import USBSerial
            self._usb = USBSerial(self.uid, self.core)
            self.devices.append(self._usb)
    
    def _init_core(self):
        self.core = Core(self)
    
    def _init_user(self):
        user, password = self._get_password()
        if not user or not password:
            log_err(self, 'failed to initialize, no password')
            raise Exception(log_get(self, 'failed to initialize'))
        
        try:
            uid, addr, token, key = self._login(user, password)
        except:
            log_err(self, 'failed to initialize')
            raise Exception(log_get(self, 'failed to initialize'))
        
        if not uid:
            log_err(self, 'failed to initialize, no uid')
            raise Exception(log_get(self, 'failed to initialize'))
        
        self.uid = uid
        self.key = key
        self.addr = addr
        self.user = user
        self.token = token
    
    def _get_password(self):
        path = os.path.join(PATH_LIB, 'user')
        d = shelve.open(path)
        try:
            user = d['user']
            password = d['password']
        finally:
            d.close()
        return (user, password)
    
    def _login(self, user, password):
        length = len(user)
        if length > 0 and length < USERNAME_SIZE and re.match('^[0-9a-zA-Z]+$', user):
            name = user + (USERNAME_SIZE - length) * '*'
        else:
            log_err(self, 'failed to login, invalid user name')
            return
        
        if len(password) != PASSWORD_SIZE:
            log_err(self, 'failed to login, invalid password')
            return
        
        if EXPOSE:
            mode = MODE_VISI
        else:
            mode = 0
        
        for _ in range(LOGIN_RETRY + 1):
            req = Request(name, password)
            res = req.user.login(node=get_node(), mode=mode)
            if res and not channel.has_network(res['addr']):
                return (res['uid'], res['addr'], res['token'], res['key'])
            time.sleep(LOGIN_INTERVAL)
        log_err(self, 'failed to login')
    
    def _start(self):
        while not self.uid:
            time.sleep(0.1)
        path = os.path.join(PATH_MNT, self.uid)
        while not os.path.exists(path):
            time.sleep(0.1)
        for device in self.devices:
            device.start()
        self._ready = True
    
    def start(self):
        if not self._active:
            self._active = True
            Thread(target=self._start).start()
    
    def chkaddr(self, name):
        if name:
            uid, node, addr = conductor.get_device(name)
            key = conductor.get_key(uid, node)
            if key:
                return (addr, key)
    
    def chknode(self):
        if self._ready:
            return (get_node(), self.addr)
        else:
            return ('', '')
    
    def notify(self, op, buf):
        notifier.notify(op, buf)
    
    def has_lo(self):
        return self._lo != None
    
    def create(self, device, init):
        if self._lo:
            return self._lo.create(device, init)
    
    @property
    def compute_unit(self):
        if COMPUTE:
            if self._lo:
                return self._lo.compute_unit
