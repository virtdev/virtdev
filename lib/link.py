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
from pool import VDevPool
from mode import MODE_LINK
from queue import VDevQueue
from log import log_err, log_get
from util import DEFAULT_NAME, str2tuple, update_device
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

class VDevUplink(object):
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

class VDevDownlinkQueue(VDevQueue):
    def __init__(self, downlink):
        VDevQueue.__init__(self, QUEUE_LEN)
        self.operations = [OP_MOUNT, OP_INVALIDATE, OP_TOUCH, OP_ENABLE, OP_DISABLE, OP_JOIN, OP_ACCEPT]
        self._downlink = downlink
    
    @chkargs
    def _request(self, name, op, **args):
        if name == DEFAULT_NAME:
            addr = self._downlink.connect(None, uid=args['uid'], addr=args['addr'])
            del args['addr']
            del args['uid']
        else:
            addr = self._downlink.connect(name)
        
        if addr:
            self._downlink.request(addr, op, args)
            self._downlink.disconnect(addr)
    
    def _proc(self, buf):
        self._request(**buf)

class VDevDownlink(object):
    def __init__(self, query):
        self._tokens = {}
        self._devices = {}
        self._query = query
        self._pool = VDevPool()
        for _ in range(POOL_SIZE):
            self._pool.add(VDevDownlinkQueue(self))
    
    def _add_device(self, uid, name, addr):
        res = (uid, addr)
        self._devices.update({name:res})
        return res
    
    def _try_get_token(self, uid, cache):
        if not cache:
            return self._query.token.get(uid)
        token = self._tokens.get(uid)
        if not token:
            token = self._query.token.get(uid)
            if token:
                self._tokens.update({uid:token})
        if not token:
            log_err(self, 'failed to get token')
            raise Exception(log_get(self, 'failed to get token'))
        return token
    
    def _get_token(self, uid, cache=False):
        for _ in range(RETRY_MAX):
            try:
                ret = self._try_get_token(uid, cache)
                if ret:
                    return ret
            except:
                pass
            time.sleep(WAIT_TIME)
    
    def _try_get_device(self, name, cache):
        if not cache:
            res = self._query.device.get(name)
            if res:
                res = (res['uid'], res['addr'])
            return res
        res = self._devices.get(name)
        if not res:
            res = self._query.device.get(name)
            if res:
                return self._add_device(res['uid'], name, res['addr'])
        else:
            return res
    
    def _get_device(self, name, cache=False):
        for _ in range(RETRY_MAX):
            try:
                ret = self._try_get_device(name, cache)
                if ret:
                    return ret
            except:
                pass
            time.sleep(WAIT_TIME)
    
    def connect(self, name, uid=None, addr=None):
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
    
    def request(self, addr, op, args):
        try:
            tunnel.put(tunnel.addr2ip(addr), op, args)
            return True
        except:
            log_err(self, 'failed to request, addr=%s, op=%s' % (addr, op))
    
    def disconnect(self, addr):
        try:
            tunnel.disconnect(addr, force=True)
        except:
            pass
    
    def _touch(self, addr, token):
        try:
            tunnel.connect(addr, token, static=True, touch=True)
            return True
        except:
            pass
    
    def mount(self, uid, name, mode, vertex, typ, parent):
        addr = None
        exist = None
        if parent:
            puid, addr = self._get_device(parent)
            if puid != uid:
                log_err(self, 'failed to mount, invalid uid')
                raise Exception(log_get(self, 'failed to mount'))
            members = self._query.member.get(uid)
            if not members:
                log_err(self, 'failed to mount, cannot get members')
                raise Exception(log_get(self, 'failed to mount'))
            for i in members:
                p, node = str2tuple(i)
                if p == parent:
                    token = self._query.token.get(uid)
                    exist = self._touch(addr, token)
                    break
        else:
            nodes = self._query.node.get(uid)
            if not nodes:
                log_err(self, 'failed to mount, cannot find any node')
                raise Exception(log_get(self, 'failed to mount'))    
            token = self._query.token.get(uid)
            for i in nodes:
                node, addr, _ = str2tuple(i)
                exist = self._touch(addr, token)
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
        
        try:
            self.request(addr, OP_MOUNT, {'attr':str(attr)})
            update_device(self._query, uid, node, addr, name)
            self._add_device(uid, name, addr)
        finally:
            self.disconnect(addr)
        return True
    
    def put(self, **args):
        self._pool.push(args)
        return True
    
    