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

import time
import tunnel
from pool import Pool
from queue import Queue
from log import log_err, log_get
from mode import MODE_LINK, MODE_CLONE
from util import str2tuple, update_device, get_name
from op import OP_ADD, OP_DIFF, OP_INVALIDATE, OP_MOUNT, OP_TOUCH, OP_ENABLE, OP_DISABLE, OP_JOIN, OP_ACCEPT

QUEUE_LEN = 4
POOL_SIZE = 0
DOWNLINK_RETRY = True
DOWNLINK_RETRY_MAX = 3
DOWNLINK_RETRY_INTERVAL = 10 # seconds

def chkargs(func):
    def _chkargs(self, name, op, **args):
        if not name or op not in self.operations:
            raise Exception('invalid link, name=%s, op=%s' % (str(name), str(op)))
        return func(self, name, op, **args)
    return _chkargs

class Uplink(object):
    def __init__(self, manager):
        self.operations = [OP_ADD, OP_DIFF]
        self._manager = manager
    
    @chkargs
    def put(self, name, op, **args):
        if op == OP_DIFF:
            return self._manager.device.diff(name, **args)
        elif op == OP_ADD:
            return self._manager.device.add(name, **args)

class DownlinkQueue(Queue):
    def __init__(self, link):
        Queue.__init__(self, QUEUE_LEN)
        self._link = link
    
    def _proc(self, name, op, **args):
        res = self._link.get_device(name)
        uid = res['uid']
        addr = res['addr']
        node = res['node']
        if self._link.connect(uid, node, addr):
            self._link.request(uid, addr, op, args)
            self._link.disconnect(addr)
    
    def proc(self, buf):
        self._proc(**buf)

class Downlink(object):
    def __init__(self, query):
        self.operations = [OP_MOUNT, OP_INVALIDATE, OP_TOUCH, OP_ENABLE, OP_DISABLE, OP_JOIN, OP_ACCEPT]
        self._query = query
        if POOL_SIZE:
            self._pool = Pool()
            for _ in range(POOL_SIZE):
                self._pool.add(DownlinkQueue(self))
        else:
            self._pool = None
    
    def get_device(self, name):
        return self._query.device.get(name)
    
    def connect(self, uid, node, addr, touch=False):
        key = self._query.key.get(get_name(uid, node))
        if not key:
            log_err(self, 'failed to connect, no key')
            return    
        try:
            tunnel.connect(uid, addr, key, static=True, touch=touch)
            return key
        except:
            pass
    
    def request(self, uid, addr, op, args, token=None):
        try:
            if not token:
                token = self._query.token.get(uid)
            tunnel.put(uid, addr, op, args, token)
            return True
        except:
            log_err(self, 'failed to request, addr=%s, op=%s' % (addr, op))
    
    def disconnect(self, addr):
        try:
            tunnel.disconnect(addr, force=True)
        except:
            pass
    
    def _retry(self, uid, addr, key, op, args, token):
        if not DOWNLINK_RETRY:
            return
        for _ in range(DOWNLINK_RETRY_MAX):
            try:
                tunnel.release(addr)
                time.sleep(DOWNLINK_RETRY_INTERVAL)
                tunnel.connect(uid, addr, key, static=True, touch=True)
                try:
                    tunnel.put(uid, addr, op, args, token)
                finally:
                    tunnel.disconnect(addr, force=True)
                return True
            except:
                pass
        log_err(self, 'failed to retry, addr=%s, op=%s' % (addr, op))
    
    def mount(self, uid, name, mode, vertex, typ, parent, timeout):
        key = None
        node = None
        addr = None
        
        if vertex and not parent:
            parent = vertex[0]
        
        if parent:
            res = self._query.device.get(parent)
            if res['uid'] != uid:
                log_err(self, 'failed to mount, invalid uid')
                raise Exception(log_get(self, 'failed to mount'))
            addr = res['addr']
            members = self._query.member.get(uid)
            if not members:
                log_err(self, 'failed to mount, cannot get members')
                raise Exception(log_get(self, 'failed to mount'))
            for i in members:
                p, node = str2tuple(i)
                if p == parent:
                    key = self.connect(uid, node, addr)
                    if not key:
                        log_err(self, 'failed to mount, no connection')
                        raise Exception(log_get(self, 'failed to mount'))
                    break
        else:
            nodes = self._query.node.get(uid)
            if not nodes:
                log_err(self, 'failed to mount, cannot get nodes')
                raise Exception(log_get(self, 'failed to mount'))
            for i in nodes:
                node, addr, _ = str2tuple(i)
                key = self.connect(uid, node, addr, touch=True)
                if key:
                    break
        
        if not key:
            log_err(self, 'failed to mount, no connection')
            raise Exception(log_get(self, 'failed to mount'))
        
        attr = {}
        attr.update({'type':typ})
        attr.update({'name':name})
        attr.update({'vertex':vertex})
        attr.update({'mode':mode | MODE_LINK})
        if mode & MODE_CLONE:
            attr.update({'parent':parent})
        if vertex:
            attr.update({'timeout':timeout})
        
        args = {'attr':str(attr)}
        try:
            token = self._query.token.get(uid)
            ret = self.request(uid, addr, OP_MOUNT, args, token=token)
            update_device(self._query, uid, node, addr, name)
        finally:
            self.disconnect(addr)
        
        if not ret:
            return self._retry(uid, addr, key, OP_MOUNT, args, token=token)
        
        return ret
    
    def _put(self, name, op, **args):
        res = self.get_device(name)
        uid = res['uid']
        addr = res['addr']
        node = res['node']
        key = self.connect(uid, node, addr)
        if key:
            token = self._query.token.get(uid)
            ret = self.request(uid, addr, op, args, token=token)
            self.disconnect(addr)
            if not ret:
                return self._retry(uid, addr, key, op, args, token=token)
            return ret
    
    @chkargs
    def put(self, name, op, **args):
        if self._pool:
            buf = {'name':name, 'op':op}
            if args:
                buf.update(args)
            self._pool.push(buf)
            return True
        else:
            return self._put(name, op, **args)
