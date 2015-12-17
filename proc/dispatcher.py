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
from lib.pool import Pool
from threading import Lock
from random import randint
from lib.queue import Queue
from lib.loader import Loader
from lib.util import lock, is_local
from lib.log import log, log_get, log_err
from lib.attributes import ATTR_DISPATCHER
from conf.virtdev import PROC_ADDR, DISPATCHER_PORT

PRINT = False
QUEUE_LEN = 2
POOL_SIZE = 0

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
    def __init__(self, uid, channel, core, addr=PROC_ADDR):
        self._uid = uid
        self._queue = []
        self._paths = {}
        self._shown = {}
        self._hidden = {}
        self._source = {}
        self._core = core
        self._lock = Lock()
        self._dispatchers = {}
        self._channel = channel
        self._loader = Loader(self._uid)
        self._addr = (addr, DISPATCHER_PORT)
        if POOL_SIZE:
            self._pool = Pool()
            for _ in range(POOL_SIZE):
                self._pool.add(DispatcherQueue(self, self._core))
        else:
            self._pool = None
    
    def _print(self, text):
        if PRINT:
            log(log_get(self, text))
    
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
        self._print('send, dest=%s, src=%s' % (dest, src))
        if POOL_SIZE:
            if self._check_source(src):
                queue = self._pool.select(src)
                queue.insert((dest, src, buf, flags))
            else:
                self._pool.push((dest, src, buf, flags))
        else:
            self._core.put(dest, src, buf, flags)
    
    def add_edge(self, edge, hidden=False):
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
                self._channel.connect(dest)
        else:
            self._paths[src][dest] += 1
        self._print('add_edge, edge=%s, local=%s' % (str(edge), str(local)))
    
    def remove_edge(self, edge, hidden=False):
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
                self._channel.disconnect(dest)
        self._print('remove_edge, edge=%s, local=%s' % (str(edge), str(local)))
    
    def remove_edges(self, name):
        paths = self._shown.get(name)
        for i in paths:
            self.remove_edge((name, i))
        paths = self._hidden.get(name)
        for i in paths:
            self.remove_edge((name, i), hidden=True)
        if self._dispatchers.has_key(name):
            del self._dispatchers[name]
    
    def remove(self, name):
        if self._dispatchers.has_key(name):
            del self._dispatchers[name]
    
    def sendto(self, dest, src, buf, hidden=False, flags=0):
        if not buf:
            return
        self.add_edge((src, dest), hidden=hidden)
        if self._hidden:
            local = self._hidden[src][dest]
        else:
            local = self._shown[src][dest]
        if not local:
            self._print('sendto->push, dest=%s, src=%s' % (dest, src))
            self._channel.push(dest, dest=dest, src=src, buf=buf, flags=flags)
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
                self._print('send->push, dest=%s, src=%s' % (i, name))
                self._channel.push(i, dest=i, src=name, buf=buf, flags=flags)
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
        len_keys = len(keys)
        len_blks = len(blocks)
        window = (len_blks + len_keys - 1) / len_keys
        start = randint(0, len_keys - 1)
        for _ in range(len_keys):
            i = keys[start]
            for _ in range(window):
                if blocks[cnt]:
                    if not dest[i]:
                        self._print('send_blocks->push, dest=%s, src=%s' % (i, name))
                        self._channel.push(i, dest=i, src=name, buf=blocks[cnt], flags=0)
                    else:
                        self._send(i, name, blocks[cnt], 0)
                cnt += 1
                if cnt == len_blks:
                    return
            start += 1
            if start == len_keys:
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
                self.add_edge((name, dest))
    
    def check(self, name):
        if self._dispatchers.get(name):
            return True
        else:
            buf = self._loader.get_attr(name, ATTR_DISPATCHER, str)
            if buf:
                self._dispatchers.update({name:buf})
                return True
    
    def put(self, name, buf):
        try:
            code = self._get_code(name)
            if code == None:
                code = self._get_code(name)
                if not code:
                    return
            return proc.put(self._addr, code=code, args=buf)
        except:
            log_err(self, 'failed to put')
