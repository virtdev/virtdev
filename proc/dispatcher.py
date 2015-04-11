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

import sandbox
from lib.util import lock
from threading import Lock
from fs.path import is_local
from loader import VDevLoader
from lib.pool import VDevPool
from lib.queue import VDevQueue
from base64 import encodestring
from sandbox import SANDBOX_PUT
from lib.mode import MODE_REFLECT
from conf.virtdev import DISPATCHER_PORT
from lib.log import log, log_get, log_err

LOG = True
QUEUE_LEN = 2
POOL_SIZE = 64

class VDevDispatcherQueue(VDevQueue):
    def __init__(self, dispatcher, core):
        VDevQueue.__init__(self, QUEUE_LEN)
        self._dispatcher = dispatcher
        self._core = core
    
    def _insert(self, buf):
        self._dispatcher.add_source(buf[0])
    
    def _push(self, buf):
        self._dispatcher.add_source(buf[0])
    
    def _proc(self, buf):
        self._dispatcher.remove_source(buf[0])
        self._core.put(*buf)

class VDevDispatcher(object):
    def __init__(self, uid, tunnel, core):
        self._uid = uid
        self._queue = []
        self._input = {}
        self._paths = {}
        self._output = {}
        self._hidden = {}
        self._source = {}
        self._core = core
        self._lock = Lock()
        self._tunnel = tunnel
        self._dispatchers = {}
        self._pool = VDevPool()
        self._loader = VDevLoader(self._uid)
        for _ in range(POOL_SIZE):
            self._pool.add(VDevDispatcherQueue(self, self._core))
    
    def _log(self, s):
        if LOG:
            log(log_get(self, s))
    
    def _get_code(self, name):
        buf = self._dispatchers.get(name)
        if not buf:
            buf = self._loader.get_dispatcher(name)
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
                    self._tunnel.open(dest)
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
                self._tunnel.close(dest)
    
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
                flags = MODE_REFLECT
                paths = self._input.get(src)
            if not paths:
                return
            local = paths[dest]
        if not local:
            self._log('sendto->push, dest=%s, src=%s' % (dest, src))
            self._tunnel.push(dest, dest=dest, src=src, buf=buf, flags=flags)
        else:
            self._send(dest, src, buf, flags)
    
    def send(self, name, buf, mode, output=True):
        if not buf:
            return
        
        if output:
            flags = 0
            dest = self._output.get(name)
        else:
            flags = MODE_REFLECT
            dest = self._input.get(name)
        
        if not dest:
            return
    
        for i in dest:
            if not dest[i]:
                self._log('send->push, dest=%s, src=%s' % (i, name))
                self._tunnel.push(i, dest=i, src=name, buf=buf, flags=flags)
            else:
                self._send(i, name, buf, flags)
    
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
                return sandbox.request(DISPATCHER_PORT, SANDBOX_PUT, code=encodestring(code), args=buf)
        except:
            log_err(self, 'failed to put')
    