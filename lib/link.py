#      link.py
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

import time
import channel
from pool import Pool
from queue import Queue
from conf.virtdev import DEBUG
from multiprocessing import cpu_count
from modes import MODE_LINK, MODE_CLONE
from log import log_debug, log_err, log_get
from util import str2tuple, update_device, get_name
from operations import OP_ADD, OP_GET, OP_INVALIDATE, OP_MOUNT, OP_TOUCH, OP_ENABLE, OP_DISABLE, OP_JOIN, OP_ACCEPT

LINK_RETRY = 2
LINK_INTERVAL = 12 # seconds

CACHE = False
QUEUE_LEN = 4
POOL_SIZE = cpu_count() * 4

def chkop(func):
    def _chkop(self, name, op, **args):
        if op not in self.operations:
            raise Exception('Error: invalid operation, op=%s' % str(op))
        return func(self, name, op, **args)
    return _chkop

class Uplink(object):
    def __init__(self, manager):
        self.operations = [OP_ADD, OP_GET]
        self._manager = manager
    
    @chkop
    def put(self, name, op, **args):
        if op == OP_GET:
            return self._manager.device.get(name, **args)
        elif op == OP_ADD:
            return self._manager.device.add(name, **args)

class DownlinkQueue(Queue):
    def __init__(self, link):
        Queue.__init__(self, QUEUE_LEN)
        self._link = link
    
    def _do_proc(self, name, op, **args):
        self._link.request(name, op, **args)
    
    def _proc(self, buf):
        self._do_proc(**buf)
    
    def _proc_safe(self, buf):
        try:
            self._proc(buf)
        except:
            log_err(self, 'failed to process')
    
    def proc(self, buf):
        if DEBUG:
            self._proc(buf)
        else:
            self._proc_safe(buf)

class Downlink(object):
    def __init__(self, query):
        self.operations = [OP_MOUNT, OP_INVALIDATE, OP_TOUCH, OP_ENABLE, OP_DISABLE, OP_JOIN, OP_ACCEPT]
        self._query = query
        if CACHE:
            self._pool = Pool()
            for _ in range(POOL_SIZE):
                self._pool.add(DownlinkQueue(self))
        else:
            self._pool = None
    
    def _get_device(self, name):
        return self._query.device.get(name)
    
    def _connect(self, uid, node, addr, verify=False):
        key = self._query.key.get(get_name(uid, node))
        if not key:
            log_err(self, 'failed to connect, no key')
            return    
        try:
            channel.connect(uid, addr, key, static=True, verify=verify)
            return key
        except:
            log_debug(self, 'failed to connect, addr=%s' % str(addr))
    
    def _request(self, uid, addr, op, args, token):
        try:
            channel.put(uid, addr, op, args, token)
            return True
        except:
            log_err(self, 'failed to request, addr=%s, op=%s' % (addr, op))
    
    def _disconnect(self, addr):
        try:
            channel.disconnect(addr)
        except:
            pass
    
    def _retry(self, uid, addr, key, op, args, token):
        for _ in range(LINK_RETRY):
            try:
                time.sleep(LINK_INTERVAL)
                channel.connect(uid, addr, key, static=True, verify=True)
                try:
                    channel.put(uid, addr, op, args, token)
                finally:
                    channel.disconnect(addr)
                return True
            except:
                pass
        log_err(self, 'failed to retry, addr=%s, op=%s' % (addr, op))
    
    def mount(self, uid, name, mode, vrtx, typ, parent, timeout):
        key = None
        node = None
        addr = None
        
        if vrtx and not parent:
            parent = vrtx[0]
        
        if parent:
            res = self._query.device.get(parent)
            if not res or res.get('uid') != uid:
                log_err(self, 'failed to mount, invalid uid')
                raise Exception(log_get(self, 'failed to mount'))
            
            addr = res.get('addr')
            if not addr:
                log_err(self, 'failed to mount, invalid addr')
                raise Exception(log_get(self, 'failed to mount'))
            
            members = self._query.member.get(uid)
            if not members:
                log_err(self, 'failed to mount, cannot get members')
                raise Exception(log_get(self, 'failed to mount'))
            
            for i in members:
                p, node = str2tuple(i)
                if p == parent:
                    key = self._connect(uid, node, addr, verify=True)
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
                key = self._connect(uid, node, addr, verify=True)
                if key:
                    break
        
        if not key:
            log_err(self, 'failed to mount, no connection')
            raise Exception(log_get(self, 'failed to mount'))
        
        attr = {}
        attr.update({'type':typ})
        attr.update({'name':name})
        attr.update({'vertex':vrtx})
        attr.update({'mode':mode | MODE_LINK})
        if mode & MODE_CLONE:
            attr.update({'parent':parent})
        if vrtx:
            attr.update({'timeout':timeout})
        
        args = {'attr':str(attr)}
        try:
            token = self._query.token.get(uid)
            ret = self._request(uid, addr, OP_MOUNT, args, token)
            update_device(self._query, uid, node, addr, name)
        finally:
            self._disconnect(addr)
        
        if not ret:
            return self._retry(uid, addr, key, OP_MOUNT, args, token)
        
        return ret
    
    def request(self, name, op, **args):
        if name:
            res = self._get_device(name)
            uid = res['uid']
            addr = res['addr']
            node = res['node']
        else:
            uid = args.pop('uid')
            addr = args.pop('addr')
            node = args.pop('node')
        key = self._connect(uid, node, addr)
        if key:
            token = self._query.token.get(uid)
            ret = self._request(uid, addr, op, args, token)
            self._disconnect(addr)
            if not ret:
                return self._retry(uid, addr, key, op, args, token)
            return ret
    
    @chkop
    def put(self, name, op, **args):
        if CACHE:
            buf = {'name':name, 'op':op}
            if args:
                buf.update(args)
            self._pool.push(buf)
            return True
        else:
            return self.request(name, op, **args)
