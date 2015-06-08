#      core.py
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
from freq import Freq
from mode import Mode
from fs.path import load
from filter import Filter
from parent import Parent
from timeout import Timeout
from threading import Event
from handler import Handler
from threading import Thread
from datetime import datetime
from lib.lock import NamedLock
from lib.util import named_lock
from dispatcher import Dispatcher
from lib.log import log_get, log_err, log
from lib.op import OP_GET, OP_PUT, OP_OPEN, OP_CLOSE
from lib.mode import MODE_VIRT, MODE_SWITCH, MODE_IN, MODE_OUT, MODE_REFLECT, MODE_CLONE

LOG = True
QUEUE_LEN = 4
INSP_INTV = 5 # seconds
WAIT_TIME = 0.1 # seconds

class Core(object):
    def __init__(self, manager):
        self._events = {}
        self._members = {}
        self._manager = manager
        self._uid = manager.uid
        self._lock = NamedLock()
        self._mode = Mode(self._uid)
        self._freq = Freq(self._uid)
        self._filter = Filter(self._uid)
        self._parent = Parent(self._uid)
        self._timeout = Timeout(self._uid)
        self._handler = Handler(self._uid)
        self._inspector = Thread(target=self._start_inspector)
        self._dispatcher = Dispatcher(self._uid, manager.tunnel, self)
        self._inspector.start()
    
    def _log(self, s):
        if LOG:
            log(log_get(self, s))
    
    def _can_put(self, dest, src, flags):
        if flags & MODE_REFLECT:
            return True
        else:
            return not self._members[dest] or self._members[dest].has_key(src)
    
    def _check_paths(self, name):
        if not self._dispatcher.has_path(name):
            edges = load(self._uid, name, 'edge')
            self._dispatcher.update_paths(name, edges)
    
    def _check_members(self, name):
        if not self._members.has_key(name):
            self._members[name] = {}
            vertices = load(self._uid, name, 'vertex')
            for i in vertices:
                self._members[name][i] = []
    
    def _count(self, name):
        cnt = 0
        for i in self._members[name].keys():
            if len(self._members[name][i]) > 0:
                cnt += 1
        return cnt
    
    def _check_timeout(self, name):
        if not self._members[name]:
            return
        timeout = None
        try:
            timeout = self._timeout.get(name)
        except:
            log_err(self, 'failed to check timeout')
        if not timeout:
            return
        t_min = None
        for i in self._members[name]:
            if len(self._members[name][i]) > 0:
                _, t = self._members[name][i][0]
                if not t_min:
                    t_min = t
                elif t < t_min:
                    t_min = t
        if t_min:
            t = datetime.utcnow()
            if (t - t_min).total_seconds() >= timeout:
                return True
    
    @named_lock
    def _inspect(self, name):
        try:
            if self._check_timeout(name):
                args = self._pop_args(name)
                if self._filter.check(name):
                    args = self._filter.put(name, args)
                if args and type(args) == dict:
                    res = self._proc(name, args)
                    if res:
                        self._dispatch(name, res)
        except:
            log_err(self, 'failed to inspect, name=%s' % str(name))
    
    def _start_inspector(self):
        while True:
            time.sleep(INSP_INTV)
            members = self._members.keys()
            for name in members:
                self._inspect(name)
    
    def _is_ready(self, dest, src, flags):
        if flags & MODE_REFLECT or not self._members[dest]:
            return True
        elif self._members[dest].has_key(src) and not len(self._members[dest][src]):
            if self._count(dest) + 1 == len(self._members[dest]):
                return True
    
    def _get_event(self, dest, src):
        if not self._events.has_key(dest):
            self._events[dest] = {}
        if not self._events[dest].has_key(src):
            ev = Event()
            self._events[dest][src] = [ev, 1]
        else:
            ev = self._events[dest][src][0]
            self._events[dest][src][1] += 1
        ev.clear()
        return ev
    
    def _set_event(self, dest, src):
        if self._events.has_key(dest):
            if self._events[dest].has_key(src):
                ev = self._events[dest][src][0]
                ev.set()
    
    def _put_event(self, dest, src):
        if self._events.has_key(dest):
            if self._events[dest].has_key(src):
                cnt = self._events[dest][src][1]
                if cnt == 1:
                    del self._events[dest][src]
                elif cnt > 1:
                    self._events[dest][src][1] -= 1
                else:
                    log_err(self, 'failed to put event, dest=%s, src=%s' % (dest, src))
                    raise Exception(log_get(self, 'failed to put event'))
            if not self._events[dest]:
                del self._events[dest]
    
    @named_lock
    def _remove_dispatcher(self, name, edge):
        self._dispatcher.remove(edge)
    
    def remove_dispatcher(self, edge):
        name = edge[0]
        self._remove_dispatcher(name, edge)
    
    @named_lock
    def remove_handler(self, name):
        self._handler.remove(name)
    
    @named_lock
    def remove_filter(self, name):
        self._filter.remove(name)
    
    @named_lock
    def remove_mode(self, name):
        self._mode.remove(name)
    
    @named_lock
    def remove_freq(self, name):
        self._freq.remove(name)
    
    @named_lock
    def remove_timeout(self, name):
        self._timeout.remove(name)
    
    @named_lock
    def _add_dispatcher(self, name, edge):
        self._dispatcher.add(edge)
    
    def add_dispatcher(self, edge):
        name = edge[0]
        self._add_dispatcher(name, edge)
    
    def get_mode(self, name):
        return self._mode.get(name)
    
    def get_freq(self, name):
        return self._freq.get(name)
    
    def _remove(self, name):
        if not self._members.has_key(name):
            return
        if self._events.has_key(name):
            for i in self._events[name]:
                self._events[name][i].set()
        del self._members[name]
    
    @named_lock
    def remove(self, name):
        self._dispatcher.remove_all(name)
        self.remove_handler(name)
        self.remove_filter(name)
        self.remove_mode(name)
        self._remove(name)
    
    def _dispatch(self, name, buf):
        self._check_paths(name)
        if not self._dispatcher.check(name):
            self._log('dispatch->send, name=%s' % name)
            self._dispatcher.send(name, buf)
        else:
            blocks = self._dispatcher.put(name, buf)
            if blocks and type(blocks) == list:
                self._log('dispatch->send_blocks, name=%s' % name)
                self._dispatcher.send_blocks(name, blocks)
    
    @named_lock
    def dispatch(self, name, buf):
        self._dispatch(name, buf)
    
    def get_oper(self, buf, mode):
        if type(buf) != dict:
            return
        if mode & MODE_SWITCH:
            ret = None
            for i in buf:
                if type(buf[i]) == dict and buf[i].has_key('Enable'):
                    tmp = buf[i]['Enable'] in ('True', True)
                    if ret != None:
                        if tmp != ret:
                            return
                    else:
                        ret = tmp
            if ret:
                return OP_OPEN
            else:
                return OP_CLOSE
        elif mode & MODE_IN:
            if 1 == len(buf):
                return OP_PUT
        elif mode & MODE_OUT:
            return OP_GET
    
    def _handle(self, name, buf):
        mode = self._mode.get(name)
        if not mode & MODE_VIRT:
            oper = self.get_oper(buf, mode)
            if not oper:
                return
            for device in self._manager.devices:
                dev = device.find(name)
                if dev:
                    self._log('handle, name=%s, oper=%s, dev=%s' % (name, oper, dev.d_name))
                    return dev.proc(name, oper, buf)
        else:
            return buf
    
    def has_handler(self, name):
        return self._handler.check(name)
    
    def handle(self, name, buf):
        return self._handler.put(name, buf)
    
    def _proc(self, name, buf):
        if not self.has_handler(name):
            return self._handle(name, buf)
        else:
            return self.handle(name, buf)
    
    def _get_args(self, dest, src, buf, flags):
        args = {}
        args[src] = buf
        if not flags & MODE_REFLECT:
            for i in self._members[dest]:
                if i != src and len(self._members[dest][i]) > 0:
                    args[i] = self._members[dest][i][0][0]
                    self._members[dest][i].pop(0)
                    self._set_event(dest, i)
        return args
    
    def _pop_args(self, name):
        args = {}
        for i in self._members[name]:
            if len(self._members[name][i]) > 0:
                args[i] = self._members[name][i][0][0]
                self._members[name][i].pop(0)
                self._set_event(name, i)
        return args
    
    def _check_queue(self, dest, src):
        while len(self._members[dest][src]) >= QUEUE_LEN:
            ev = self._get_event(dest, src)
            self._lock.release(dest)
            try:
                ev.wait()
            finally:
                self._lock.acquire(dest)
            self._put_event(dest, src)
    
    def _try_put(self, dest, src, buf, flags):
        args = None
        if not self._is_ready(dest, src, flags):
            self._check_queue(dest, src)
            self._members[dest][src].append((buf, datetime.utcnow()))
            if self._check_timeout(dest):
                args = self._pop_args(dest)
        else:
            args = self._get_args(dest, src, buf, flags)
        if args:
            if self._filter.check(dest):
                args = self._filter.put(dest, args)
            if args and type(args) == dict:
                return self._proc(dest, args)
    
    @named_lock
    def _put(self, dest, src, buf, flags):
        if not buf:
            self._log('put, no content, dest=%s, src=%s' % (dest, src))
            return
        res = None
        mode = self._mode.get(dest)
        if not mode & MODE_CLONE:
            if not flags & MODE_REFLECT:
                self._check_members(dest)
            if self._can_put(dest, src, flags):
                res = self._try_put(dest, src, buf, flags)
        else:
            if not flags & MODE_REFLECT:
                flags |= MODE_REFLECT
                src = self._parent.get(dest)
            else:
                flags &= ~MODE_REFLECT
            res = buf
        if res:
            if not flags & MODE_REFLECT:
                self._dispatch(dest, res)
            else:
                self._log('put->sendto, dest=%s, src=%s' % (src, dest))
                self._dispatcher.sendto(src, dest, res, hidden=True, flags=flags)
    
    def put(self, dest, src, buf, flags):
        self._put(dest, src, buf, flags)
        return True
