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
from daemon import Daemon
from proc.core import Core
from proc.proc import Proc
from lib.lock import NamedLock
from conductor import Conductor
from lib.request import Request
from threading import Lock, Thread
from lib.op import OP_OPEN, OP_CLOSE
from lib.log import log, log_err, log_get
from lib.mode import MODE_SYNC, MODE_VISI 
from lib.util import USERNAME_SIZE, PASSWORD_SIZE, netaddresses, get_node, dev_name, cat, lock, named_lock
from conf.virtdev import LO, BT, USB, FS, PROC_ADDR, FILTER_PORT, HANDLER_PORT, DISPATCHER_PORT, LIB_PATH, RUN_PATH, SHADOW, VISIBLE, MOUNTPOINT

LOG=True

class DeviceManager(object):
    def __init__(self, cond): 
        self._cond = cond
        self._lock = NamedLock()
    
    @named_lock
    def open(self, name):
        for device in self._cond.devices:
            dev = device.find(name)
            if dev:
                dev.proc(name, OP_OPEN)
                return
    
    @named_lock
    def close(self, name):
        for device in self._cond.devices:
            dev = device.find(name)
            if dev:
                dev.proc(name, OP_CLOSE)
                return
    
    @named_lock
    def add(self, name, mode=None, freq=None, prof=None):
        return self._cond.request.device.add(node=get_node(), addr=self._cond.addr, name=name, mode=mode, freq=freq, prof=prof)
    
    @named_lock
    def update(self, name, buf):
        if self._cond.core.get_mode(name) & MODE_SYNC:
            return self._cond.request.device.update(name=name, buf=buf)
    
    @named_lock
    def get(self, name):
        return self._cond.request.device.get(name=name)
    
    @named_lock
    def diff(self, name, label, item, buf):
        return self._cond.request.device.diff(name=name, label=label, item=item, buf=buf)
    
    @named_lock
    def remove(self, name):
        return self._cond.request.device.remove(node=get_node(), name=name)

class GuestManager(object):
    def __init__(self, cond):
        self._cond = cond
        self._lock = Lock()
    
    @lock
    def join(self, dest, src):
        return self._cond.request.guest.join(dest=dest, src=src)
    
    @lock
    def accept(self, dest, src):
        return self._cond.request.guest.accept(dest=dest, src=src)
    
    @lock
    def drop(self, dest, src):
        return self._cond.request.guest.drop(dest=dest, src=src)

class NodeManager(object):
    def __init__(self, cond):
        self._cond = cond
        self._lock = Lock()
    
    @lock
    def search(self, user, random, limit):
        return self._cond.request.node.search(user=user, random=random, limit=limit)
    
    @lock
    def find(self, user, node):
        return self._cond.request.node.search(user=user, node=node)

class TunnelManager(object):
    def __init__(self, cond):
        self._cond = cond
        self._lock = NamedLock()
    
    def _try_open(self, name):
        uid, node, addr = self._cond.get_device(name)
        if not uid:
            log_err(self, 'failed to open, no uid')
            raise Exception(log_get(self, 'failed to open'))
        if not tunnel.exist(addr):
            key = self._cond.get_key(uid, node)
            if not key:
                log_err(self, 'failed to open, no token')
                raise Exception(log_get(self, 'failed to open'))
            try:
                tunnel.connect(addr, key)
            except:
                self._cond.remove_device(name)
                self._cond.remove_key(uid, node)
        return True
    
    @named_lock
    def open(self, name):
        if not self._try_open(name):
            if not self._try_open(name):
                log_err(self, 'failed to open')
                raise Exception(log_get(self, 'failed to open'))
    
    @named_lock
    def close(self, name):
        _, _, addr = self._cond.get_device(name)
        if tunnel.exist(addr):
            tunnel.disconnect(addr)
    
    @named_lock
    def put(self, name, **args):
        _, _, addr = self._cond.get_device(name)
        ip_addr = tunnel.addr2ip(addr)
        tunnel.put(ip_addr, 'put', args, self._cond.uid, self._cond.token)
    
    @named_lock
    def push(self, name, **args):
        _, _, addr = self._cond.get_device(name)
        ip_addr = tunnel.addr2ip(addr)
        tunnel.push(ip_addr, 'put', args, self._cond.uid, self._cond.token)

class MemberManager(object):
    def __init__(self, cond):
        self._cond = cond
        self._lock = Lock()
        self._path = os.path.join(LIB_PATH, dev_name(cond.uid))
    
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

class Manager(object):
    def _log(self, text):
        if LOG:
            log(text)
    
    def _init_proc(self):
        self._filter = Proc(self, (PROC_ADDR, FILTER_PORT))
        self._handler = Proc(self, (PROC_ADDR, HANDLER_PORT))
        self._dispatcher = Proc(self, (PROC_ADDR, DISPATCHER_PORT))
        self._filter.start()
        self._handler.start()
        self._dispatcher.start()
    
    def _init_cond(self):
        cond = Conductor(self)
        self.node = NodeManager(cond)
        self.guest = GuestManager(cond)
        self.device = DeviceManager(cond)
        self.tunnel = TunnelManager(cond)
        self.member = MemberManager(cond)
        self._cond = cond
        cond.start()
    
    def _init_daemon(self):
        self._daemon = Daemon(self)
        self._daemon.start()
    
    def _init_dev(self):
        if BT:
            from interfaces.bt import Bluetooth
            self._bt = Bluetooth(self.uid, self.core)
            self.devices.append(self._bt)
        
        if LO:
            from interfaces.lo import Lo
            self._lo = Lo(self.uid, self.core)
            self.devices.append(self._lo)
        
        if USB:
            from interfaces.usb import USBSerial
            self._usb = USBSerial(self.uid, self.core)
            self.devices.append(self._usb)
        
        name = dev_name(self.uid)
        self.device.add(name)
        self._log('dev: name=%s, node=%s' % (name, get_node()))
    
    def _init_core(self):
        self.core = Core(self)
    
    def _get_password(self):
        path = os.path.join(LIB_PATH, 'user')
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
        
        mode = 0
        if VISIBLE:
            mode |= MODE_VISI
        
        req = Request(name, password)
        res = req.user.login(node=get_node(), networks=netaddresses(mask=True), mode=mode)
        if not res:
            log_err(self, 'failed to login')
            return
        return (res['uid'], res['addr'], res['token'], res['key'])
    
    def _init_user(self):
        user, password = self._get_password()
        if not user or not password:
            log_err(self, 'failed to get password')
            raise Exception(log_get(self, 'failed to get password'))
        
        try:
            uid, addr, token, key = self._login(user, password)
        except:
            log_err(self, 'failed to login')
            raise Exception(log_get(self, 'failed to login'))
        
        if not uid:
            log_err(self, 'invalid uid')
            raise Exception(log_get(self, 'invalid uid'))
        
        self.uid = uid
        self.key = key
        self.addr = addr
        self.user = user
        self.token = token
    
    def _initialize(self):
        if not FS or not SHADOW:
            return
        
        self._init_user()
        self._init_proc()    
        self._init_core()
        self._init_cond()
        self._init_dev()
        
        if VISIBLE:
            self._init_daemon()
    
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
        self.tunnel = None
        self._daemon = None
        self._filter = None
        self._handler = None
        self._active = False
        self._listener = None
        self._dispatcher = None
        self._initialize()
    
    def _save_addr(self):
        path = os.path.join(RUN_PATH, 'addr')
        d = shelve.open(path)
        try:
            d['addr'] = self.addr
            d['node'] = get_node()
            d['name'] = dev_name(self.uid)
        finally:
            d.close()
    
    def _start(self):
        path = os.path.join(MOUNTPOINT, self.uid)
        while not os.path.exists(path):
            time.sleep(0.1)
        for device in self.devices:
            device.start()
    
    def start(self):
        if not self._active:
            Thread(target=self._start).start()
            self._save_addr()
            self._active = True
    
    def chkaddr(self, name):
        if name and self._cond:
            uid, node, addr = self._cond.get_device(name)
            key = self._cond.get_key(uid, node)
            if key:
                return (addr, key)
    
    def notify(self, op, buf):
        notifier.push(op, buf)
    
    def has_lo(self):
        return self._lo != None
    
    def create(self, device, init):
        if self._lo:
            return self._lo.create(device, init)
    
    def get_passive(self):
        if self._lo:
            return self._lo.get_passive()
