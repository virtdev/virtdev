#      synchronizer.py
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

from fs.path import load
from mode import VDevMode
from freq import VDevFreq
from mapper import VDevMapper
from handler import VDevHandler
from lib.log import log_get, log
from threading import Lock, Event
from dispatcher import VDevDispatcher
from dev.vdev import VDEV_MODE_VIRT, VDEV_MODE_SWITCH, VDEV_MODE_IN, VDEV_MODE_OUT, VDEV_MODE_REFLECT, VDEV_GET, VDEV_PUT, VDEV_OPEN, VDEV_CLOSE

VDEV_SYNCHRONIZER_DEPTH = 4
VDEV_SYNCHRONIZER_WAIT_TIME = 0.1 #second
VDEV_SYNCHRONIZER_LOG = True

class VDevSynchronizer(object):
    def __init__(self, manager):
        self.manager = manager
        self._uid = manager.uid
        self._mode = VDevMode(self._uid)
        self._freq = VDevFreq(self._uid)
        self._mapper = VDevMapper(self._uid)
        self._handler = VDevHandler(self._uid)
        self._dispatcher = VDevDispatcher(manager)
        self._prelock = Lock()
        self._members = {}
        self._queue = {}
        self._lock = {}
    
    def _log(self, s):
        if VDEV_SYNCHRONIZER_LOG:
            log(log_get(self, s))
    
    def _can_put(self, dest, src, flags):
        if flags & VDEV_MODE_REFLECT:
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
        if flags & VDEV_MODE_REFLECT:
            self._dispatcher.add((dest, src), hidden=True)
        else:
            self._check_members(dest)
            if self._members[dest].has_key(src):
                self._check_output(dest)
            else:
                self._check_input(dest)
    
    def _lock_acquire(self, name):
        self._prelock.acquire()
        try:
            if self._lock.has_key(name):
                self._lock[name][1] += 1
                lock = self._lock[name][0]
            else:
                lock = Lock()
                self._lock[name] = [lock, 1]
        finally:
            self._prelock.release()
        lock.acquire()
    
    def _lock_release(self, name):
        self._prelock.acquire()
        try:
            if self._lock.has_key(name):
                lock = self._lock[name][0]
                lock.release()
                self._lock[name][1] -= 1
                if self._lock[name][1] == 0:
                    del self._lock[name]
        finally:
            self._prelock.release()
    
    def _count(self, name):
        cnt = 0
        for i in self._members[name].keys():
            if len(self._members[name][i]) > 0:
                cnt += 1
        return cnt
    
    def _is_ready(self, dest, src, flags):
        if flags & VDEV_MODE_REFLECT or not self._members[dest]:
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
    
    def remove_dispatcher(self, edge):
        name = edge[0]
        self._lock_acquire(name)
        try:
            self._dispatcher.remove(edge)
        finally:
            self._lock_release(name)
    
    def remove_handler(self, name):
        self._lock_acquire(name)
        try:
            self._handler.remove(name)
        finally:
            self._lock_release(name)
    
    def remove_mapper(self, name):
        self._lock_acquire(name)
        try:
            self._mapper.remove(name)
        finally:
            self._lock_release(name)
    
    def remove_mode(self, name):
        self._lock_acquire(name)
        try:
            self._mode.remove(name)
        finally:
            self._lock_release(name)
    
    def remove_freq(self, name):
        self._lock_acquire(name)
        try:
            self._freq.remove(name)
        finally:
            self._lock_release(name)
    
    def add_dispatcher(self, edge):
        name = edge[0]
        self._lock_acquire(name)
        try:
            self._dispatcher.add(edge)
        finally:
            self._lock_release(name)
    
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
    
    def remove(self, name):
        self._lock_acquire(name)
        try:
            self._dispatcher.remove_all(name)
            self.remove_handler(name)
            self.remove_mapper(name)
            self.remove_mode(name)
            self._remove(name)
        finally:
            self._lock_release(name)
    
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
    
    def dispatch(self, name, buf):
        self._lock_acquire(name)
        try:
            self._dispatch(name, buf)
        finally:
            self._lock_release(name)
    
    def get_oper(self, buf, mode):
        if type(buf) != dict:
            return
        if mode & VDEV_MODE_SWITCH:
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
                return VDEV_OPEN
            else:
                return VDEV_CLOSE
        elif mode & VDEV_MODE_IN:
            if 1 == len(buf):
                return VDEV_PUT
        elif mode & VDEV_MODE_OUT:
            return VDEV_GET
    
    def _default_callback(self, name, buf):
        mode = self._mode.get(name)
        if not mode & VDEV_MODE_VIRT:
            oper = self.get_oper(buf, mode)
            if not oper:
                return
            for device in self.manager.devices:
                dev = device.find(name)
                if dev:
                    self._log('default callback, name=%s, oper=%s, dev=%s' % (name, oper, dev.d_name))
                    return dev.proc(name, oper, buf)
        else:
            if type(buf) == dict and 1 == len(buf):
                self._log('default callback, name=%s' % name)
                return buf[buf.keys()[0]]
            oper = self.get_oper(buf, mode)
            self._log('default callback, name=%s, oper=%s' % (name, oper))
            if not oper:
                return
            if oper == VDEV_OPEN:
                return {'Enable':'True'}
            elif oper == VDEV_CLOSE:
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
        if not flags & VDEV_MODE_REFLECT:
            for i in self._members[dest]:
                if i != src:
                    args[i] = self._members[dest][i][0]
                    self._members[dest][i].pop(0)
        return args
    
    def _put(self, dest, src, buf, flags):
        if not self._is_ready(dest, src, flags):
            self._log('put->collect, dest=%s, src=%s' % (dest, src))
            if len(self._members[dest][src]) >= VDEV_SYNCHRONIZER_DEPTH:
                return self._get_event(dest, src)
            else:
                self._members[dest][src].append(buf)
        else:
            args = self._get_args(dest, src, buf, flags)
            ret = self._proc(dest, args)
            if ret:
                if not flags & VDEV_MODE_REFLECT:
                    self._dispatch(dest, ret)
                else:
                    self._log('put->dispatch, %s=>%s' % (dest, src))
                    self._dispatcher.sendto(src, dest, ret, hidden=True)
    
    def _forward(self, name, buf):
        mode = self._mode.get(name)
        if not mode & VDEV_MODE_VIRT:
            return
        if not self._mapper.check(name):
            self._log('forward->send, name=%s' % name)
            self._dispatcher.send(name, buf, mode, output=False)
        else:
            res = self._mapper.put(name, buf)
            if res:
                for i in res:
                    self._log('forward->sendto, name=%s, dest=%s' % (name, i[0]))
                    self._dispatcher.sendto(i[0], name, i[1], output=False)
    
    def put(self, dest, src, buf, flags):
        retry = None
        self._lock_acquire(dest)
        try:
            self._check(dest, src, flags)
            if self._can_put(dest, src, flags):
                retry = self._put(dest, src, buf, flags)
            else:
                self._forward(dest, buf)
            return True
        finally:
            self._lock_release(dest)
            if retry:
                self._log('put->wait, dest=%s, src=%s' % (dest, src))
                retry.wait(VDEV_SYNCHRONIZER_WAIT_TIME)
    