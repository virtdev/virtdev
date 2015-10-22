#      conductor.py
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

import socket
from lib.pool import Pool
from oper import Operation
from lib.queue import Queue
from lib import tunnel, package
from lib.request import Request
from threading import Thread, Event
from lib.log import log_err, log_get
from lib.util import UID_SIZE, get_name
from conf.virtdev import CONDUCTOR_PORT

QUEUE_LEN = 2
POOL_SIZE = 32

class ConductorQueue(Queue):
    def __init__(self, srv):
        Queue.__init__(self, QUEUE_LEN)
        self._srv = srv
    
    def proc(self, buf):
        self._srv.proc(buf)

class Conductor(Thread):
    def _init_sock(self, addr):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((addr, CONDUCTOR_PORT))
        self._sock.listen(5)
    
    def __init__(self, manager):
        Thread.__init__(self)
        key = manager.key
        uid = manager.uid
        addr = manager.addr
        token = manager.token
        tunnel.create(uid, addr, key)
        
        self._keys = {}
        self._devices = {}
        self._pool = Pool()
        self._event = Event()
        self._tokens = {uid:token}
        self._op = Operation(manager)
        self._init_sock(manager.addr)
        for _ in range(POOL_SIZE):
            self._pool.add(ConductorQueue(self))
        
        self.uid = uid
        self.addr = addr
        self.token = token
        self.user = manager.user
        self.devices = manager.devices
        self.request = Request(uid, token)
    
    def _get_token(self, uid, update=False):
        token = None
        if not update:
            token = self._tokens.get(uid)
        if not token:
            if uid == self.uid:
                log_err(self, 'failed to get token, invalid uid')
                raise Exception(log_get(self, 'failed to get token'))
            token = self.request.token.get(name=uid)
            if token:
                self._tokens.update({uid:token})
        if not token:
            log_err(self, 'failed to get token')
            raise Exception(log_get(self, 'failed to get token'))
        return token
    
    def get_device(self, name):
        res = self._devices.get(name)
        if not res:
            res = self.request.device.get(name=name)
            if res:
                res = (res['uid'], res['node'], res['addr'])
                self._devices.update({name:res})
        if not res:
            log_err(self, 'failed to get device')
            raise Exception(log_get(self, 'failed to get device'))
        return res
    
    def remove_device(self, name):
        if self._devices.has_key(name):
            del self._devices[name]
    
    def get_key(self, uid, node):
        name = get_name(uid, node)
        key = self._keys.get(name)
        if not key:
            key = self.request.key.get(name=name)
            if key:
                self._keys.update({name:key})
        if not key:
            log_err(self, 'failed to get key')
            raise Exception(log_get(self, 'failed to get key'))
        return key
    
    def remove_key(self, uid, node):
        name = uid + node
        if self._keys.get(name):
            del self._keys[name]
    
    def proc(self, sock):
        if not sock:
            return
        op = None
        try:
            buf = tunnel.recv(sock)
            if len(buf) <= UID_SIZE:
                log_err(self, 'failed to process, invalid request')
                return
            uid = buf[0:UID_SIZE]
            token = self._get_token(uid)
            if not token:
                log_err(self, 'failed to process, no token')
                return
            try:
                req = package.unpack(uid, buf, token)
            except:
                token = self._get_token(uid, update=True)
                if not token:
                    log_err(self, 'failed to process, no token')
                    return
                req = package.unpack(uid, buf, token)
            if req:
                op = req.get('op')
                args = req.get('args')
                if not op or op[0] == '_' or type(args) != dict:
                    log_err(self, 'failed to process, invalid request, op=%s' % str(op))
                    return
                func = getattr(self._op, op)
                if func:
                    func(**args)
                else:
                    log_err(self, 'failed to process, invalid operation %s' % str(op))
        except:
            if op:
                log_err(self, 'failed to process, op=%s' % str(op))
        finally:
            sock.close()
    
    def run(self):
        while True:
            try:
                sock, _ = self._sock.accept()
                self._pool.push(sock)
            except:
                log_err(self, 'failed to receive request')
