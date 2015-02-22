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

import time
import sandbox
from math import ceil
from random import randint
from fs.path import is_local
from loader import VDevLoader
from base64 import encodestring
from sandbox import VDEV_SANDBOX_PUT
from lib.util import hash_name, lock
from lib.log import log, log_get, log_err
from threading import Thread, Lock, Event
from conf.virtdev import VDEV_DISPATCHER_PORT
from dev.vdev import VDEV_MODE_FI, VDEV_MODE_FO, VDEV_MODE_PI, VDEV_MODE_PO, VDEV_MODE_REFLECT

VDEV_DISPATCHER_LOG = True
VDEV_DISPATCHER_WAIT_TIME = 0.01
VDEV_DISPATCHER_TIMEOUT = 30000
VDEV_DISPATCHER_QUEUE_MAX = 64
VDEV_DISPATCHER_QUEUE_LEN = 2

class VDevDispatcherQueue(Thread):
    def __init__(self, dispatcher, manager):
        Thread.__init__(self)
        self._dispatcher = dispatcher
        self.manager = manager
        self._event = Event()
        self._lock = Lock()
        self._queue = []
        self.start()
    
    @lock
    def insert(self, buf):
        self._dispatcher.add_source(buf[0])
        self._queue.insert(0, buf)
        self._event.set()
    
    @lock
    def push(self, buf):
        if len(self._queue) < VDEV_DISPATCHER_QUEUE_LEN:
            self._dispatcher.add_source(buf[0])
            self._queue.append(buf)
            self._event.set()
            return True
    
    def get_length(self):
        return len(self._queue)
    
    @lock
    def pop(self):
        buf = None
        if len(self._queue) > 0:
            buf = self._queue.pop(0)
            if len(self._queue) == 0:
                self._event.clear()
        return buf
    
    def run(self):
        while True:
            self._event.wait()
            buf = self.pop()
            if buf:
                try:
                    self._dispatcher.remove_source(buf[0])
                    self.manager.synchronizer.put(*buf)
                except:
                    log_err(self, 'failed')

class VDevDispatcher(object):
    def __init__(self, manager):
        self._queue = []
        self._input = {}
        self._paths = {}
        self._output = {}
        self._hidden = {}
        self._queues = []
        self._source = {}
        self._lock = Lock()
        self._dispatchers = {}
        self.manager = manager
        self._uid = manager.uid
        self._loader = VDevLoader(self._uid)
        self._queues = [VDevDispatcherQueue(self, manager) for _ in range(VDEV_DISPATCHER_QUEUE_MAX)]
    
    def _log(self, s):
        if VDEV_DISPATCHER_LOG:
            log(log_get(self, s))
    
    def _get_code(self, name):
        buf = self._dispatchers.get(name)
        if not buf:
            buf = self._loader.get_dispatcher(name)
            self._dispatchers.update({name:buf})
        return buf
    
    def _get_queue(self):
        n = 0
        length = VDEV_DISPATCHER_QUEUE_LEN
        i = randint(0, VDEV_DISPATCHER_QUEUE_MAX - 1)
        for _ in range(VDEV_DISPATCHER_QUEUE_MAX):
            l = self._queues[i].get_length()
            if l == 0:
                length = 0
                n = i
                break
            elif l < length:
                length = l
                n = i
            i += 1
            if i == VDEV_DISPATCHER_QUEUE_MAX:
                i = 0
        if length < VDEV_DISPATCHER_QUEUE_LEN:
            return self._queues[n]
    
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
    
    def _hash(self, name):
        n = hash_name(name) % VDEV_DISPATCHER_QUEUE_MAX
        return self._queues[n]
    
    def _send(self, dest, src, buf, flags):
        self._log('send, dest=%s, src=%s' % (dest, src))
        if self._check_source(src):
            q = self._hash(src)
            q.insert((dest, src, buf, flags))
        else:
            while True:
                q = self._get_queue()
                if not q:
                    time.sleep(VDEV_DISPATCHER_WAIT_TIME)
                else:
                    if q.push((dest, src, buf, flags)):
                        return
    
    def add(self, edge, output=True, hidden=False):
        src = edge[0]
        dest = edge[1]
        if hidden:
            paths = self._hidden
            if paths.has_key(src) and paths[src].has_key(dest):
                return
        else:
            if output:
                paths = self._output
            else:
                paths = self._input
            
        if not paths.has_key(src):
            paths[src] = {}
        
        if not self._paths.has_key(src):
            self._paths[src] = {}
        
        if not paths[src].has_key(dest):
            local = is_local(self._uid, dest)
            paths[src].update({dest:local})
            if not self._paths[src].has_key(dest):
                self._paths[src].update({dest:1})
                if not local:
                    self.manager.tunnel.open(dest)
            else:
                self._paths[src][dest] += 1
            self._log('add, edge=%s, local=%s' % (str(edge), str(local)))
    
    def remove(self, edge, output=True, hidden=False):
        src = edge[0]
        dest = edge[1]
        if hidden:
            paths = self._hidden
        else:
            if output:
                paths = self._output
            else:
                paths = self._input
        if not paths.has_key(src):
            return
        local = paths[src][dest]
        del paths[src][dest]
        self._paths[src][dest] -= 1
        if 0 == self._paths[src][dest]:
            del self._paths[src][dest]
            if not local:
                self.manager.tunnel.close(dest)
    
    def remove_all(self, name):
        paths = self._output.get(name)
        for i in paths:
            self.remove((name, i))
        paths = self._input.get(name)
        for i in paths:
            self.remove((name, i), output=False)
        paths = self._hidden.get(name)
        for i in paths:
            self.remove((name, i), hidden=True)
        if self._dispatchers.has_key(name):
            del self._dispatchers[name]
    
    def sendto(self, dest, src, buf, output=True, hidden=False):
        if self._hidden:
            flags = 0
            local = self._hidden[src][dest]
        else:
            if output:
                flags = 0
                paths = self._output.get(src)
            else:
                flags = VDEV_MODE_REFLECT
                paths = self._input.get(src)
            if not paths:
                return
            local = paths[dest]
        if not local:
            self._log('sendto, dest=%s, src=%s' % (dest, src))
            self.manager.tunnel.put(dest, dest=dest, src=src, buf=buf, flags=flags)
        else:
            self._send(dest, src, buf, flags)
    
    def _gen_buf(self, buf, pos, total):
        length = len(buf)
        i = int(ceil(length / total))
        start = pos * i
        if start < total:
            end = (pos + 1) * i
            if end > total:
                end = total
            if type(buf) == list:
                return buf[start:end]
            elif type(buf) == dict:
                res = {}
                keys = buf.keys[start:end]
                for i in keys:
                    res.update({i:buf[i]})
                return res
    
    def _gen_path(self, paths):
        i = randint(0, len(paths) - 1)
        n = paths.keys()[i]
        return {n:paths[n]}
    
    def send(self, name, buf, mode, output=True):
        if output:
            flags = 0
            paths = self._output.get(name)
        else:
            flags = VDEV_MODE_REFLECT
            paths = self._input.get(name)
        if not paths:
            return
        part = False
        if mode & VDEV_MODE_FI or mode & VDEV_MODE_FO:
            dest = self._gen_path(paths)
        elif mode & VDEV_MODE_PI or mode & VDEV_MODE_PO:
            if type(buf) == list or type(buf) == dict:
                total = len(paths)
                part = True
                dest = paths
            else:
                dest = self._gen_path(paths)
        else:
            dest = paths
    
        for i in dest:
            if not part:
                temp = buf
            else:
                temp = self._gen_buf(buf, i, total)
            if not temp:
                continue
            if not dest[i]:
                self._log('send->tunnel->put, dest=%s, src=%s' % (i, name))
                self.manager.tunnel.put(i, dest=i, src=name, buf=temp, flags=flags)
            else:
                self._send(i, name, temp, flags)
    
    def has_input(self, name):
        return self._input.has_key(name)
    
    def has_output(self, name):
        return self._output.has_key(name)
    
    def update_input(self, name, buf):
        if not buf:
            self._input[name] = {}
            return
        for i in buf:
            self.add((name, i), output=False)
    
    def update_output(self, name, items):
        if not items:
            self._output[name] = {}
            return
        for dest in items:
            if not dest.startswith('.'):
                self.add((name, dest))
            else:
                self.add((name, dest[1:]), hidden=True)
    
    def check(self, name):
        if self._dispatchers.has_key(name):
            if self._dispatchers[name]:
                return True
        else:
            buf = self._loader.get_dispatcher(name)
            self._dispatchers.update({name:buf})
            if buf:
                return True
    
    def put(self, name, buf):
        try:
            code = self._get_code(name)
            if code:
                return sandbox.request(VDEV_DISPATCHER_PORT, VDEV_SANDBOX_PUT, code=encodestring(code), args=buf)
        except:
            log_err(self, 'failed to put')
    