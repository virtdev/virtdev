# dispatcher.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from proc import proc
from lib.pool import Pool
from random import randint
from lib.queue import Queue
from lib.loader import Loader
from lib.lock import NamedLock
from conf.defaults import DEBUG
from threading import Lock, Thread
from conf.log import LOG_DISPATCHER
from lib.attributes import ATTR_DISPATCHER
from lib.log import log_debug, log_err, log_get
from conf.defaults import PROC_ADDR, DISPATCHER_PORT
from lib.util import lock, named_lock, edge_lock, is_local

SAFE = True
TIMEOUT = 3600
POOL_SIZE = 16
QUEUE_LEN = 1

class DispatcherQueue(Queue):
    def __init__(self, parent):
        Queue.__init__(self, parent, QUEUE_LEN, TIMEOUT)
        self._parent = parent

    def proc(self, buf):
        self._parent.proc(*buf)

class DispatcherPool(Pool):
    def __init__(self, core):
        Pool.__init__(self)
        self._core = core
        self._busy = set()
        self._ready = set()
        self._lock = Lock()
        for _ in range(POOL_SIZE):
            self.add(DispatcherQueue(self))

    def _log(self, text):
        if LOG_DISPATCHER:
            log_debug(self, text)

    def _is_busy(self, name):
        return name in self._busy

    def _set_busy(self, name):
        self._busy.add(name)

    def _clear_busy(self, name):
        if name in self._busy:
            self._busy.remove(name)

    def _is_ready(self, name):
        return name in self._ready

    def _set_ready(self, name):
        self._ready.add(name)

    def _clear_ready(self, name):
        if name in self._ready:
            self._ready.remove(name)

    @lock
    def _acquire(self, name):
        self._log('>>acquire<<, name=%s' % name)
        self._set_busy(name)

    @lock
    def _release(self, name):
        self._clear_busy(name)
        self._clear_ready(name)
        self._log('>>release<<, name=%s' % name)

    @lock
    def put(self, dest, src, buf, flags):
        if not self._is_ready(dest):
            if self.push((dest, src, buf, flags), async=True):
                self._log('put, dest=%s, src=%s' % (dest, src))
                self._set_ready(dest)
                return True
        if self._is_busy(src):
            self._log('put, no wait, dest=%s, src=%s' % (dest, src))
            Thread(target=self._core.put, args=(dest, src, buf, flags)).start()
            return True

    def _do_proc(self, dest, src, buf, flags):
        self._acquire(dest)
        try:
            self._log('proc, dest=%s, src=%s' % (dest, src))
            self._core.put(dest, src, buf, flags)
        finally:
            self._release(dest)

    def _proc_safe(self, dest, src, buf, flags):
        try:
            self._do_proc(dest, src, buf, flags)
        except:
            log_err(self, 'failed to process, dest=%s, src=%s' % (str(dest), str(src)))

    def _proc_unsafe(self, dest, src, buf, flags):
        self._do_proc(dest, src, buf, flags)

    def proc(self, dest, src, buf, flags):
        if DEBUG and not SAFE:
            self._proc_unsafe(dest, src, buf, flags)
        else:
            self._proc_safe(dest, src, buf, flags)

class Dispatcher(object):
    def __init__(self, uid, channel, core, addr=PROC_ADDR):
        self._uid = uid
        self._queue = []
        self._paths = {}
        self._hidden = {}
        self._core = core
        self._addr = addr
        self._pool = None
        self._visible = {}
        self._dispatchers = {}
        self._channel = channel
        self._lock = NamedLock()
        self._loader = Loader(self._uid)
        self._pool = DispatcherPool(core)

    def _log(self, text):
        if LOG_DISPATCHER:
            log_debug(self, text)

    def _get_code(self, name):
        buf = self._dispatchers.get(name)
        if not buf:
            buf = self._loader.get_attr(name, ATTR_DISPATCHER, str)
            self._dispatchers.update({name:buf})
        return buf

    def _send(self, dest, src, buf, flags):
        self._log('send, dest=%s, src=%s' % (dest, src))
        self._channel.put(dest, src, buf=buf, flags=flags)

    def _put(self, dest, src, buf, flags):
        self._log('put, dest=%s, src=%s' % (dest, src))
        while not self._pool.put(dest, src, buf, flags):
            self._pool.wait()

    def _do_deliver(self, dest, src, buf, flags, local):
        if local:
            self._put(dest, src, buf, flags)
        else:
            self._send(dest, src, buf, flags)

    def _deliver_safe(self, dest, src, buf, flags, local):
        try:
            self._do_deliver(dest, src, buf, flags, local)
        except:
            log_err(self, 'failed to deliver, dest=%s, src=%s' % (str(dest), str(src)))

    def _deliver_unsafe(self, dest, src, buf, flags, local):
        self._do_deliver(dest, src, buf, flags, local)

    def _deliver(self, dest, src, buf, flags, local):
        if DEBUG and not SAFE:
            self._deliver_unsafe(dest, src, buf, flags, local)
        else:
            self._deliver_safe(dest, src, buf, flags, local)

    @edge_lock
    def add_edge(self, edge, hidden=False):
        src = edge[0]
        dest = edge[1]

        if hidden:
            paths = self._hidden
        else:
            paths = self._visible

        if paths.has_key(src) and paths[src].has_key(dest):
            return

        if not paths.has_key(src):
            paths[src] = {}

        if not self._paths.has_key(src):
            self._paths[src] = {}

        local = is_local(self._uid, dest)
        paths[src].update({dest:local})
        if not self._paths[src].has_key(dest):
            if not local:
                self._channel.allocate(dest)
            self._paths[src].update({dest:1})
        else:
            self._paths[src][dest] += 1
        self._log('add edge, dest=%s, src=%s, local=%s' % (dest, src, str(local)))

    @edge_lock
    def remove_edge(self, edge, hidden=False):
        src = edge[0]
        dest = edge[1]
        if hidden:
            paths = self._hidden
        else:
            paths = self._visible
        if not paths.has_key(src) or not paths[src].has_key(dest):
            return
        local = paths[src][dest]
        del paths[src][dest]
        self._paths[src][dest] -= 1
        if 0 == self._paths[src][dest]:
            del self._paths[src][dest]
            if not local:
                self._channel.free(dest)
        self._log('remove edge, dest=%s, src=%s, local=%s' % (dest, src, str(local)))

    @named_lock
    def has_edge(self, name):
        return self._visible.has_key(name)

    def update_edges(self, name, edges):
        if not edges:
            return
        for dest in edges:
            if dest.startswith('.'):
                dest = dest[1:]
            if dest != name:
                self.add_edge((name, dest))

    def remove_edges(self, name):
        paths = self._visible.get(name)
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
        self._log('sendto, dest=%s, src=%s' % (dest, src))
        self.add_edge((src, dest), hidden=hidden)
        if self._hidden:
            local = self._hidden[src][dest]
        else:
            local = self._visible[src][dest]
        self._deliver(dest, src, buf, flags, local=local)

    def send(self, name, buf, flags=0):
        if not buf:
            return
        dest = self._visible.get(name)
        if not dest:
            return
        for i in dest:
            self._log('send, dest=%s, src=%s' % (i, name))
            self._deliver(i, name, buf, flags, local=dest[i])

    def send_blocks(self, name, blocks):
        if not blocks:
            return
        dest = self._visible.get(name)
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
                    self._log('send a block, dest=%s, src=%s' % (i, name))
                    self._deliver(i, name, blocks[cnt], 0, local=dest[i])
                cnt += 1
                if cnt == len_blks:
                    return
            start += 1
            if start == len_keys:
                start = 0

    def put(self, name, buf):
        try:
            code = self._get_code(name)
            if code == None:
                code = self._get_code(name)
                if not code:
                    return
            return proc.put(self._addr, DISPATCHER_PORT, code, buf)
        except:
            log_err(self, 'failed to put, name=%s' % (str(name)))

    def check(self, name):
        if self._dispatchers.get(name):
            return True
        else:
            buf = self._loader.get_attr(name, ATTR_DISPATCHER, str)
            if buf:
                self._dispatchers.update({name:buf})
                return True
