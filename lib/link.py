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

import tunnel
from pool import Pool
from queue import Queue
from log import log_err, log_get
from mode import MODE_LINK, MODE_CLONE
from util import str2tuple, update_device
from op import OP_ADD, OP_DIFF, OP_SYNC, OP_INVALIDATE, OP_MOUNT, OP_TOUCH, OP_ENABLE, OP_DISABLE, OP_JOIN, OP_ACCEPT

QUEUE_LEN = 4
POOL_SIZE = 32
RETRY_MAX = 5
WAIT_TIME = 0.1
BUF_SIZE = 1 << 22

def chkargs(func):
    def _chkargs(self, name, op, **args):
        if not name:
            raise Exception('link: chkargs failed, invalid name')
        if op not in self.operations:
            raise Exception('link: chkargs failed, invalid operation')
        buf = args.get('buf')
        if buf:
            if type(buf) != str and type(buf) != unicode:
                raise Exception('link: chkargs failed, invalid buf')
            if len(buf) > BUF_SIZE:
                raise Exception('link: chkargs failed, invalid length of buf')
        return func(self, name, op, **args)
    return _chkargs

class Uplink(object):
    def __init__(self, manager):
        self._manager = manager
        self.operations = [OP_ADD, OP_DIFF, OP_SYNC]
    
    @chkargs
    def put(self, name, op, **args):
        if op == OP_DIFF:
            return self._manager.device.diff(name, **args)
        elif op == OP_SYNC:
            return self._manager.device.update(name, **args)
        elif op == OP_ADD:
            return self._manager.device.add(name, **args)

class DownlinkQueue(Queue):
    def __init__(self, downlink):
        Queue.__init__(self, QUEUE_LEN)
        self.operations = [OP_MOUNT, OP_INVALIDATE, OP_TOUCH, OP_ENABLE, OP_DISABLE, OP_JOIN, OP_ACCEPT]
        self._downlink = downlink
    
    @chkargs
    def _request(self, name, op, **args):
        if not name:
            uid = args['uid']
            addr = args['addr']
            node = args['node']
            del args['uid']
            del args['addr']
            del args['node']
        else:
            res = self._downlink.get_device(name)
            uid = res['uid']
            addr = res['addr']
            node = res['node']
        
        if self._downlink.connect(uid, node, addr):
            self._downlink.request(uid, addr, op, args)
            self._downlink.disconnect(addr)
    
    def proc(self, buf):
        self._request(**buf)

class Downlink(object):
    def __init__(self, query):
        self._pool = Pool()
        self._query = query
        for _ in range(POOL_SIZE):
            self._pool.add(DownlinkQueue(self))
    
    def get_device(self, name):
        return self._query.device.get(name)
    
    def connect(self, uid, node, addr):
        key = self._query.key.get(uid + node)
        if not key:
            log_err(self, 'failed to connect, cannot get key')
            raise Exception(log_get(self, 'failed to connect'))
        try:
            tunnel.connect(addr, key, static=True)
            return True
        except:
            log_err(self, 'failed to connect')
    
    def request(self, uid, addr, op, args):
        try:
            token = self._query.token.get(uid)
            tunnel.put(tunnel.addr2ip(addr), op, args, uid, token)
            return True
        except:
            log_err(self, 'failed to request, addr=%s, op=%s' % (addr, op))
    
    def disconnect(self, addr):
        try:
            tunnel.disconnect(addr, force=True)
        except:
            pass
    
    def _touch(self, addr, key):
        try:
            tunnel.connect(addr, key, static=True, touch=True)
            return True
        except:
            pass
    
    def mount(self, uid, name, mode, vertex, typ, parent, timeout):
        addr = None
        exist = None
        
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
                    key = self._query.key.get(uid + node)
                    exist = self._touch(addr, key)
                    break
        else:
            nodes = self._query.node.get(uid)
            if not nodes:
                log_err(self, 'failed to mount, cannot find any node')
                raise Exception(log_get(self, 'failed to mount'))    
            for i in nodes:
                node, addr, _ = str2tuple(i)
                key = self._query.key.get(uid + node)
                exist = self._touch(addr, key)
                if exist:
                    break
        
        if not exist:
            log_err(self, 'failed to mount, cannot get node')
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
        
        try:
            self.request(uid, addr, OP_MOUNT, {'attr':str(attr)})
            update_device(self._query, uid, node, addr, name)
        finally:
            self.disconnect(addr)
        
        return True
    
    def put(self, **args):
        self._pool.push(args)
        return True
