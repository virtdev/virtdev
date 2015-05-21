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

from freq import Freq
from mode import Mode
from fs.path import load
from filter import Filter
from threading import Event
from handler import Handler
from lib.lock import NamedLock
from lib.util import named_lock
from lib.log import log_get, log
from dispatcher import Dispatcher
from lib.op import OP_GET, OP_PUT, OP_OPEN, OP_CLOSE
from lib.mode import MODE_VIRT, MODE_SWITCH, MODE_IN, MODE_OUT, MODE_REFLECT

LOG = True
QUEUE_LEN = 4
WAIT_TIME = 0.1 #seconds

class Core(object):
    def __init__(self, manager):
        self._queue = {}
        self._members = {}
        self._manager = manager
        self._uid = manager.uid
        self._lock = NamedLock()
        self._mode = Mode(self._uid)
        self._freq = Freq(self._uid)
        self._filter = Filter(self._uid)
        self._handler = Handler(self._uid)
        self._dispatcher = Dispatcher(self._uid, manager.tunnel, self)
    
    def _log(self, s):
        if LOG:
            log(log_get(self, s))
    
    def _can_put(self, dest, src, flags):
        if flags & MODE_REFLECT:
            return True
        else:
            return not self._members[dest] or self._members[dest].has_key(src)
    
    def _check_input(self, name):
        if not self._dispatcher.has_input(name):
            self._dispatcher.update_input(name, self._members[name].keys())
    
    def _check_output(self, name):
        if not self._dispatcher.has_output(name):
            edges = load(self._uid, name, 'edge')
            self._dispatcher.update_output(name, edges)
    
    def _check_members(self, name):
        if not self._members.has_key(name):
            self._members[name] = {}
            vertices = load(self._uid, name, 'vertex')
            for i in vertices:
                self._members[name][i] = []
    
    def _check(self, dest, src, flags):
        if flags & MODE_REFLECT:
            self._dispatcher.add((dest, src), hidden=True)
        else:
            self._check_members(dest)
            if self._members[dest].has_key(src):
                self._check_output(dest)
            else:
                self._check_input(dest)
    
    def _count(self, name):
        cnt = 0
        for i in self._members[name].keys():
            if len(self._members[name][i]) > 0:
                cnt += 1
        return cnt
    
    def _is_ready(self, dest, src, flags):
        if flags & MODE_REFLECT or not self._members[dest]:
            return True
        elif self._members[dest].has_key(src) and not len(self._members[dest][src]):
            if self._count(dest) + 1 == len(self._members[dest]):
                return True
    
    def _get_event(self, dest, src):
        if not self._queue.has_key(dest):
            self._queue[dest] = {}
        if not self._queue[dest].has_key(src):
            ev = Event()
            self._queue[dest][src] = ev
        else:
            ev = self._queue[dest][src]
        ev.clear()
        return ev
    
    def _del_event(self, dest, src):
        if self._queue.has_key(dest):
            if self._queue[dest].has_key[src]:
                del self._queue[dest][src]
                if not self._queue[dest]:
                    del self._queue[dest]
    
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
        if self._queue.has_key(name):
            for i in self._members[name]:
                if self._queue[name].has_key(i):
                    self._queue[name][i].set()
                    del self._queue[name][i]
            del self._queue[name]
        del self._members[name]
    
    @named_lock
    def remove(self, name):
        self._dispatcher.remove_all(name)
        self.remove_handler(name)
        self.remove_filter(name)
        self.remove_mode(name)
        self._remove(name)
    
    def _dispatch(self, name, buf):
        self._check_output(name)
        if not self._dispatcher.check(name):
            self._log('dispatch->send, name=%s' % name)
            mode = self._mode.get(name)
            self._dispatcher.send(name, buf, mode)
        else:
            res = self._dispatcher.put(name, buf)
            if res:
                for i in res:
                    self._log('dispatch->sendto, name=%s, dest=%s' % (name, i[0]))
                    self._dispatcher.sendto(i[0], name, i[1])
    
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
    
    def _default_callback(self, name, buf):
        mode = self._mode.get(name)
        if not mode & MODE_VIRT:
            oper = self.get_oper(buf, mode)
            if not oper:
                return
            for device in self._manager.devices:
                dev = device.find(name)
                if dev:
                    self._log('default_callback, name=%s, oper=%s, dev=%s' % (name, oper, dev.d_name))
                    return dev.proc(name, oper, buf)
        else:
            if type(buf) == dict and 1 == len(buf):
                self._log('default_callback, name=%s' % name)
                return buf[buf.keys()[0]]
            oper = self.get_oper(buf, mode)
            self._log('default_callback, name=%s, oper=%s' % (name, oper))
            if not oper:
                return
            if oper == OP_OPEN:
                return {'Enable':'True'}
            elif oper == OP_CLOSE:
                return {'Enable':'False'}
    
    def has_callback(self, name):
        return self._handler.check(name)
    
    def callback(self, name, buf):
        return self._handler.put(name, buf)
    
    def _proc(self, name, buf):
        if not self.has_callback(name):
            return self._default_callback(name, buf)
        else:
            return self.callback(name, buf)
    
    def _get_args(self, dest, src, buf, flags):
        args = {}
        args[src] = buf
        if not flags & MODE_REFLECT:
            for i in self._members[dest]:
                if i != src:
                    args[i] = self._members[dest][i][0]
                    self._members[dest][i].pop(0)
        return args
    
    def _forward(self, name, buf):
        mode = self._mode.get(name)
        if not mode & MODE_VIRT:
            return
        if not self._filter.check(name):
            self._log('forward->send, name=%s' % name)
            self._dispatcher.send(name, buf, mode, output=False)
        else:
            res = self._filter.put(name, buf)
            if res:
                for i in res:
                    self._log('forward->sendto, name=%s, dest=%s' % (name, i[0]))
                    self._dispatcher.sendto(i[0], name, i[1], output=False)
    
    def _try_put(self, dest, src, buf, flags):
        if not self._is_ready(dest, src, flags):
            self._log('try_put, not ready, dest=%s, src=%s' % (dest, src))
            if len(self._members[dest][src]) >= QUEUE_LEN:
                return self._get_event(dest, src)
            else:
                self._members[dest][src].append(buf)
        else:
            args = self._get_args(dest, src, buf, flags)
            ret = self._proc(dest, args)
            if ret:
                if not flags & MODE_REFLECT:
                    self._dispatch(dest, ret)
                else:
                    self._log('try_put->sendto, dest=%s, src=%s' % (dest, src))
                    self._dispatcher.sendto(src, dest, ret, hidden=True)
    
    @named_lock
    def _put(self, dest, src, buf, flags):
        self._check(dest, src, flags)
        if self._can_put(dest, src, flags):
            return self._try_put(dest, src, buf, flags)
        else:
            self._forward(dest, buf)
    
    def put(self, dest, src, buf, flags):
        ev = self._put(dest, src, buf, flags)
        if ev:
            self._log('put->wait, dest=%s, src=%s' % (dest, src))
            ev.wait(WAIT_TIME)
            return
        return True
    