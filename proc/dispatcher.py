#      dispatcher.py
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

import proc
from lib.util import lock
from lib.pool import Pool
from threading import Lock
from random import randint
from lib.queue import Queue
from fs.path import is_local
from lib.loader import Loader
from fs.attr import ATTR_DISPATCHER
from lib.log import log, log_get, log_err
from conf.virtdev import PROC_ADDR, DISPATCHER_PORT

LOG = True
QUEUE_LEN = 2
POOL_SIZE = 64

class DispatcherQueue(Queue):
    def __init__(self, dispatcher, core):
        Queue.__init__(self, QUEUE_LEN)
        self._dispatcher = dispatcher
        self._core = core
    
    def preinsert(self, buf):
        self._dispatcher.add_source(buf[0])
    
    def prepush(self, buf):
        self._dispatcher.add_source(buf[0])
    
    def proc(self, buf):
        self._dispatcher.remove_source(buf[0])
        self._core.put(*buf)

class Dispatcher(object):
    def __init__(self, uid, tunnel, core, addr=PROC_ADDR):
        self._uid = uid
        self._queue = []
        self._paths = {}
        self._shown = {}
        self._hidden = {}
        self._source = {}
        self._core = core
        self._lock = Lock()
        self._pool = Pool()
        self._tunnel = tunnel
        self._dispatchers = {}
        self._loader = Loader(self._uid)
        self._addr = (addr, DISPATCHER_PORT)
        for _ in range(POOL_SIZE):
            self._pool.add(DispatcherQueue(self, self._core))
    
    def _log(self, s):
        if LOG:
            log(log_get(self, s))
    
    def _get_code(self, name):
        buf = self._dispatchers.get(name)
        if not buf:
            buf = self._loader.get_attr(name, ATTR_DISPATCHER, str)
            self._dispatchers.update({name:buf})
        return buf
    
    @lock
    def _check_source(self, name):
        if self._source.has_key(name):
            return True
    
    @lock
    def add_source(self, name):
        if not self._source.has_key(name):
            self._source.update({name:1})
        else:
            self._source[name] += 1
    
    @lock
    def remove_source(self, name):
        if self._source.has_key(name):
            self._source[name] -= 1
            if self._source[name] <= 0:
                del self._source[name]
    
    def _send(self, dest, src, buf, flags):
        self._log('send, dest=%s, src=%s' % (dest, src))
        if self._check_source(src):
            queue = self._pool.select(src)
            queue.insert((dest, src, buf, flags))
        else:
            self._pool.push((dest, src, buf, flags))
    
    def add(self, edge, hidden=False):
        src = edge[0]
        dest = edge[1]
        if hidden:
            paths = self._hidden
        else:
            paths = self._shown
        if paths.has_key(src) and paths[src].has_key(dest):
            return
        
        if not paths.has_key(src):
            paths[src] = {}
        
        if not self._paths.has_key(src):
            self._paths[src] = {}
        
        local = is_local(self._uid, dest)
        paths[src].update({dest:local})
        if not self._paths[src].has_key(dest):
            self._paths[src].update({dest:1})
            if not local:
                self._tunnel.open(dest)
        else:
            self._paths[src][dest] += 1
        self._log('add, edge=%s, local=%s' % (str(edge), str(local)))
    
    def remove(self, edge, hidden=False):
        src = edge[0]
        dest = edge[1]
        if hidden:
            paths = self._hidden
        else:
            paths = self._shown
        if not paths.has_key(src) or not paths[src].has_key(dest):
            return
        local = paths[src][dest]
        del paths[src][dest]
        self._paths[src][dest] -= 1
        if 0 == self._paths[src][dest]:
            del self._paths[src][dest]
            if not local:
                self._tunnel.close(dest)
        self._log('remove, edge=%s, local=%s' % (str(edge), str(local)))
    
    def remove_all(self, name):
        paths = self._shown.get(name)
        for i in paths:
            self.remove((name, i))
        paths = self._hidden.get(name)
        for i in paths:
            self.remove((name, i), hidden=True)
        if self._dispatchers.has_key(name):
            del self._dispatchers[name]
    
    def sendto(self, dest, src, buf, hidden=False, flags=0):
        if not buf:
            return
        self.add((src, dest), hidden=hidden)
        if self._hidden:
            local = self._hidden[src][dest]
        else:
            local = self._shown[src][dest]
        if not local:
            self._log('sendto->push, dest=%s, src=%s' % (dest, src))
            self._tunnel.push(dest, dest=dest, src=src, buf=buf, flags=flags)
        else:
            self._send(dest, src, buf, flags)
    
    def send(self, name, buf, flags=0):
        if not buf:
            return
        dest = self._shown.get(name)
        if not dest:
            return
        for i in dest:
            if not dest[i]:
                self._log('send->push, dest=%s, src=%s' % (i, name))
                self._tunnel.push(i, dest=i, src=name, buf=buf, flags=flags)
            else:
                self._send(i, name, buf, flags)
    
    def send_blocks(self, name, blocks):
        if not blocks:
            return
        dest = self._shown.get(name)
        if not dest:
            return
        cnt = 0
        keys = dest.keys()
        keys_len = len(keys)
        blks_len = len(blocks)
        window = (blks_len + keys_len - 1) / keys_len
        start = randint(0, keys_len - 1)
        for _ in range(keys_len):
            i = keys[start]
            for _ in range(window):
                if blocks[cnt]:
                    if not dest[i]:
                        self._log('send_blocks->push, dest=%s, src=%s' % (i, name))
                        self._tunnel.push(i, dest=i, src=name, buf=blocks[cnt], flags=0)
                    else:
                        self._send(i, name, blocks[cnt], 0)
                cnt += 1
                if cnt == blks_len:
                    return
            start += 1
            if start == keys_len:
                start = 0
    
    def has_path(self, name):
        return self._shown.has_key(name)
    
    def update_paths(self, name, edges):
        if not edges:
            return
        for dest in edges:
            if dest.startswith('.'):
                dest = dest[1:]
            if dest != name:
                self.add((name, dest))
    
    def check(self, name):
        if self._dispatchers.has_key(name):
            if self._dispatchers[name]:
                return True
        else:
            buf = self._loader.get_attr(name, ATTR_DISPATCHER, str)
            self._dispatchers.update({name:buf})
            if buf:
                return True
    
    def put(self, name, buf):
        try:
            code = self._get_code(name)
            if code:
                return proc.put(self._addr, code=code, args=buf)
        except:
            log_err(self, 'failed to put')
