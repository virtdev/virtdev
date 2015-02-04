#      link.py
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

from lib import tunnel
from random import randint
from dev.vdev import update_device
from lib.log import log_err, log_get
from threading import Thread, Lock, Event
from lib.util import DEFAULT_NAME, str2tuple
from oper import OP_ADD, OP_DIFF, OP_SYNC, OP_INVALIDATE, OP_MOUNT, OP_TOUCH, OP_ENABLE, OP_DISABLE, OP_JOIN, OP_ACCEPT

VDEV_FS_LINK_QUEUE_MAX = 64
VDEV_FS_LINK_QUEUE_LEN = 512
VDEV_FS_LINK_BUF_SIZE = 1 << 20

def excl(func):
    def _excl(*args, **kwargs):
        self = args[0]
        self._lock.acquire()
        try:
            return func(*args, **kwargs)
        finally:
            self._lock.release()
    return _excl

class VDevFSLinkQueue(Thread):
    def __init__(self, proc):
        Thread.__init__(self)
        self._event = Event()
        self._lock = Lock()
        self._queue = []
        self.proc = proc
    
    def _notify(self):
        if not self._event.is_set():
            self._event.set()
    
    @excl
    def _clear(self):
        if self._event.is_set():
            self._event.clear()
    
    def wait(self):
        self._clear()
        self._event.wait()
    
    @excl
    def push(self, buf):     
        if len(self._queue) >= VDEV_FS_LINK_QUEUE_LEN:
            return False
        self._queue.append(buf)
        self._notify()
        return True
    
    @excl
    def pop(self):
        length = len(self._queue)
        if length > 0:
            self._notify()
            return self._queue.pop()
    
    def run(self):
        while True:
            buf = self.pop()
            if buf:
                self.proc(**buf)
            else:
                self.wait()

class VDevFSLink(object):
    def __init__(self, async=True):
        self._queues = []
        self._operations = []
        if async:
            for i in range(VDEV_FS_LINK_QUEUE_MAX):
                self._queues.append(VDevFSLinkQueue(self.proc))
                self._queues[i].start()
        self._async = async
    
    def _chkargs(self, args):
        name = args.get('name')
        if not name:
            log_err(self, 'invalid arguments')
            raise Exception(log_get(self, 'invalid arguments'))
        op = args.get('op')
        if not op or op not in self._operations:
            log_err(self, 'invalid arguments')
            raise Exception(log_get(self, 'invalid arguments'))
        buf = args.get('buf')
        if buf:
            if type(buf) != str and type(buf) != unicode:
                log_err(self, 'invalid arguments')
                raise Exception(log_get(self, 'invalid arguments'))
            if len(buf) > VDEV_FS_LINK_BUF_SIZE:
                log_err(self, 'invalid arguments')
                raise Exception(log_get(self, 'invalid arguments'))
    
    def put(self, **args):
        self._chkargs(args)
        if self._async:
            return self._queues[randint(0, VDEV_FS_LINK_QUEUE_MAX - 1)].push(args)
        else:
            return self.proc(**args)

class VDevFSUplink(VDevFSLink):
    def __init__(self, manager):
        VDevFSLink.__init__(self, async=False)
        self._operations = [OP_ADD, OP_DIFF, OP_SYNC]
        self._manager = manager
    
    def proc(self, name, op, **args):
        if op == OP_DIFF:
            return self._manager.device.diff(name, **args)
        elif op == OP_SYNC:
            return self._manager.device.sync(name, **args)
        elif op == OP_ADD:
            return self._manager.device.add(name, **args)

class VDevFSDownlink(VDevFSLink):
    def __init__(self, query):
        VDevFSLink.__init__(self)
        self._operations = [OP_MOUNT, OP_INVALIDATE, OP_TOUCH, OP_ENABLE, OP_DISABLE, OP_JOIN, OP_ACCEPT]
        self.query = query
        self._devices = {}
        self._tokens = {}
    
    def _add_device(self, uid, name, addr):
        res = (uid, addr)
        self._devices.update({name:res})
        return res
    
    def _get_token(self, uid, cache=False):
        if not cache:
            return self.query.token_get(uid)
        token = self._tokens.get(uid)
        if not token:
            token = self.query.token_get(uid)
            if token:
                self._tokens.update({uid:token})
        if not token:
            log_err(self, 'failed to get token')
            raise Exception(log_get(self, 'failed to get token'))
        return token
            
    def _get_device(self, name, cache=False):
        if not cache:
            res = self.query.device_get(name)
            if res:
                res = (res['uid'], res['addr'])
            return res
        res = self._devices.get(name)
        if not res:
            res = self.query.device_get(name)
            if res:
                return self._add_device(res['uid'], name, res['addr'])
        else:
            return res
    
    def _connect(self, name, uid=None, addr=None):
        try:
            if name:
                uid, addr = self._get_device(name)
        except:
            log_err(self, 'failed to connect, cannot get uid')
            return
        token = self._get_token(uid)
        if not token:
            log_err(self, 'failed to connect, cannot get token')
            raise Exception(log_get(self, 'failed to connect'))
        try:
            tunnel.connect(addr, token, static=True)
            return addr
        except:
            log_err(self, 'failed to connect')
    
    def _disconnect(self, addr):
        tunnel.disconnect(addr, force=True)
    
    def _request(self, addr, op, args):
        try:
            tunnel.put(tunnel.addr2ip(addr), op, args)
            return True
        except:
            pass
    
    def proc(self, name, op, **args):
        if name == DEFAULT_NAME:
            addr = self._connect(None, uid=args['uid'], addr=args['addr'])
            del args['addr']
            del args['uid']
        else:
            addr = self._connect(name)
        if not addr:
            log_err(self, 'failed to process, cannot get address, name=%s, op=%s' % (name, op))
            return
        if not self._request(addr, op, args):
            log_err(self, 'failed to process, cannot send request, addr=%s, name=%s, op=%s' % (addr, name, op))
        self._disconnect(addr)
    
    def _touch(self, addr, token):
        try:
            tunnel.connect(addr, token, static=True)
            return True
        except:
            pass
    
    def mount(self, uid, name, mode, vertex, typ, parent):
        addr = None
        match = False
        if parent:
            puid, addr = self._get_device(parent)
            if puid != uid:
                log_err(self, 'failed to mount, invalid uid')
                raise Exception(log_get(self, 'failed to mount'))
            members = self.query.member_get(uid)
            if not members:
                log_err(self, 'failed to mount, cannot get members')
                raise Exception(log_get(self, 'failed to mount'))
            for i in members:
                p, node = str2tuple(i)
                if p == parent:
                    token = self.query.token_get(uid)
                    if self._touch(addr, token):
                        match = True
                    break
        else:
            nodes = self.query.node_get(uid)
            if not nodes:
                log_err(self, 'failed to mount, cannot find any node')
                raise Exception(log_get(self, 'failed to mount'))    
            token = self.query.token_get(uid)
            for i in nodes:
                node, addr, _ = str2tuple(i)
                if self._touch(addr, token):
                    match = True
                    break
        
        if not match:
            log_err(self, 'failed to mount, cannot get node')
            raise Exception(log_get(self, 'failed to mount'))
        
        attr = {}
        attr.update({'type':typ})
        attr.update({'name':name})
        attr.update({'mode':mode})
        attr.update({'vertex':vertex})
        try:
            self._request(addr, OP_MOUNT, {'attr':str(attr)})
            update_device(self.query, uid, node, addr, name)
            self._add_device(uid, name, addr)
        finally:
            self._disconnect(addr)
        return True
    