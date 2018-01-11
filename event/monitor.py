# monitor.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import zerorpc
from lib.types import *
from conf.defaults import *
from lib.log import log_err
from lib.lock import NamedLock
from threading import Thread, Event
from lib.util import zmqaddr, named_lock

WAIT_TIME = 3600 # seconds

class Monitor(object):
    def __init__(self, addr, router):
        self._queue = {}
        self._addr = addr
        self._events = {}
        self._results = {}
        self._router = router
        self._lock = NamedLock()

    @named_lock
    def _get_event(self, uid):
        ev = self._events.get(uid)
        if not ev:
            event = Event()
            self._events[uid] = [event, 1]
        else:
            ev[1] += 1
            event = ev[0]
        if event.is_set():
            event.clear()
        return event

    @named_lock
    def _put_event(self, uid):
        if self._events.has_key(uid):
            self._events[uid][1] -= 1
            if self._events[uid][1] <= 0:
                del self._events[uid]

    @named_lock
    def _get(self, uid):
        ret = self._results.get(uid)
        if ret:
            del self._results[uid]
        return ret

    def get(self, uid):
        ret = ''
        cli = zerorpc.Client()
        addr = self._router.get(uid, DOMAIN_USR)
        cli.connect(zmqaddr(addr, EVENT_COLLECTOR_PORT))
        try:
            while not ret:
                ret = cli.get(uid, self._addr)
                if not ret:
                    event = self._get_event(uid)
                    try:
                        event.wait(WAIT_TIME)
                        ret = self._get(uid)
                    finally:
                        self._put_event(uid)
            return str(ret)
        except:
            log_err(self, 'failed to get, uid=%s' % uid)
        finally:
            cli.close()

    @named_lock
    def put(self, uid, buf):
        if not buf:
            return
        if not self._results.get(uid):
            self._results[uid] = []
        for i in buf:
            if i not in self._results[uid]:
                self._results[uid].append(i)
        ev = self._events.get(uid)
        if ev:
            ev[0].set()

class EventMonitor(Thread):
    def __init__(self, addr, router):
        Thread.__init__(self)
        self._router = router
        self._addr = addr

    def run(self):
        srv = zerorpc.Server(Monitor(self._addr, self._router))
        srv.bind(zmqaddr(self._addr, EVENT_MONITOR_PORT))
        srv.run()
