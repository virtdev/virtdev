# channel.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import time
import zerorpc
from errno import *
from lib import resolv
from random import randint
from conf.defaults import *
from dev.udo import OUTPUT_MAX
from lib.lock import NamedLock
from conf.log import LOG_CHANNEL
from lib.bridge import get_bridge
from threading import Event, Thread
from conf.virtdev import BRIDGE_PORT
from lib.log import log_debug, log_err, log_get
from lib.util import popen, get_dir, gen_key, pkill, close_port, named_lock, zmqaddr, chkpid

RELIABLE = True
KEEP_CONNECTION = True

RETRY_MAX = 5
CHANNEL_MAX = 256

TIMEOUT_PUT = 3600          # seconds
TIMEOUT_SEND = 120          # seconds
TIMEOUT_EXIST = 15          # seconds
TIMEOUT_CONNECT = 30        # seconds

EV_PUT = 'put'
EV_SEND = 'send'
EV_EXIST = 'exist'
EV_CONNECT = 'connect'
EV_TYPES = [EV_PUT, EV_SEND, EV_EXIST, EV_CONNECT]
EV_TIMEOUT = {EV_PUT:TIMEOUT_PUT, EV_SEND:TIMEOUT_SEND, EV_EXIST:TIMEOUT_EXIST, EV_CONNECT:TIMEOUT_CONNECT}

ADAPTER_NAME = 'wrtc.adpt'

def check_type(func):
    def _check_type(*args, **kwargs):
        self = args[0]
        ev_type = args[1]
        if ev_type not in EV_TYPES:
            log_err(self, 'invalid event type')
            raise Exception(log_get(self, 'invalid event type'))
        self._lock.acquire(ev_type)
        try:
            return func(*args, **kwargs)
        finally:
            self._lock.release(ev_type)
    return _check_type

class ChannelEvent(object):
    def __init__(self):
        self._lock = NamedLock()
        self._error = {i:{} for i in EV_TYPES}
        self._event = {i:{} for i in EV_TYPES}
        self._count = {i:{} for i in EV_TYPES}

    @check_type
    def get(self, ev_type, ev_name):
        ev = self._event[ev_type].get(ev_name)
        if not ev:
            ev = Event()
            self._event[ev_type][ev_name] = ev
            self._count[ev_type][ev_name] = 1
        else:
            self._count[ev_type][ev_name] += 1
        return ev

    @check_type
    def put(self, ev_type, ev_name):
        if self._event[ev_type].has_key(ev_name):
            self._count[ev_type][ev_name] -= 1
            if 0 == self._count[ev_type][ev_name]:
                del self._event[ev_type][ev_name]
                del self._count[ev_type][ev_name]

        if self._error[ev_type].has_key(ev_name):
            ret = self._error[ev_type][ev_name]
            del self._error[ev_type][ev_name]
            return ret

    @check_type
    def set(self, ev_type, ev_args):
        name = ev_args.get('name')
        if name:
            ev = self._event[ev_type].get(name)
            if ev:
                if ev_args.has_key('error'):
                    self._error[ev_type][name] = ev_args['error']
                ev.set()
            else:
                log_debug(self, 'cannot set, no event, ev_type=%s, ev_args=%s' % (str(ev_type), str(ev_args)))
        else:
            log_debug(self, 'cannot set, no name, ev_type=%s, ev_args=%s' % (str(ev_type), str(ev_args)))

    def wait(self, ev, ev_timeout):
        return ev.wait(ev_timeout)

class ChannelEventHandler(object):
    def __init__(self, channel_event):
        self._channel_event = channel_event

    def emit(self, event, args):
        if event and args:
            self._channel_event.set(event, args)
        else:
            log_err(self, 'invalid event')

class ChannelEventMonitor(Thread):
    def __init__(self, channel_event):
        Thread.__init__(self)
        self._channel_event = channel_event

    def _clear(self):
        close_port(CHANNEL_EVENT_PORT)

    def run(self):
        self._clear()
        srv = zerorpc.Server(ChannelEventHandler(self._channel_event))
        srv.bind(zmqaddr(CHANNEL_EVENT_ADDR, CHANNEL_EVENT_PORT))
        srv.run()

class Channel(object):
    def __init__(self, event=True):
        self._alloc = {}
        self._sources = {}
        self._channels = {}
        self._adapter = None
        self._lock = NamedLock()
        self._event_monitor = None
        self._channel_event = ChannelEvent()
        self._path = self._get_adapter_path()
        if event:
            self._start_event_monitor()

    def _log(self, text):
        if LOG_CHANNEL:
            log_debug(self, text)

    def _start_event_monitor(self):
        if not self._event_monitor:
            self._event_monitor = ChannelEventMonitor(self._channel_event)
            self._event_monitor.start()

    def _get_adapter_path(self):
        return os.path.join(get_dir(), 'protocols', 'wrtc', ADAPTER_NAME)

    def _get_timeout(self, t):
        return randint(t, 2 * t)

    def _request(self, op, args, event=None, timeout=None):
        if not args:
            log_debug(self, 'failed to request, no arguments')
            return
        ev = None
        if event:
            ev = self._channel_event.get(event['ev_type'], event['ev_name'])
            if not ev:
                log_err(self, 'failed to get event')
                return
        ret = None
        try:
            cli = zerorpc.Client()
            cli.connect(zmqaddr(ADAPTER_ADDR, ADAPTER_PORT))
            try:
                cli.request(op, args);
            finally:
                cli.close()
            if event:
                if not timeout:
                    timeout = EV_TIMEOUT[event['ev_type']]
                ret = self._channel_event.wait(ev, timeout)
        finally:
            if event:
                err = self._channel_event.put(event['ev_type'], event['ev_name'])
                if err != None:
                    ret = err
        return ret

    def _exist(self, addr):
        event = {'ev_name':addr, 'ev_type':EV_EXIST}
        ret = self._request('exist', {'addr':addr}, event)
        if ret == True:
            return True
        elif ret == -ENOENT:
            return False

    def _connect(self, addr, key, bridge, source=None, timeout=None, release=True):
        args = {'addr':addr, 'key':key, 'bridge':bridge}
        if source:
            args.update({'source':source})
        event = {'ev_name':addr, 'ev_type':EV_CONNECT}
        ret = self._request('connect', args, event, timeout)
        if ret != True:
            if release:
                self._release(addr)
            raise Exception(log_get(self, 'failed to connect'))

    def _disconnect(self, addr):
        try:
            self._request('disconnect', {'addr':addr})
        except:
            log_debug(self, 'failed to disconnect, addr=' % addr)

    def _attach(self, addr):
        key = gen_key()
        src = resolv.get_addr()
        bridge = get_bridge(src)
        self._request('attach', {'addr':src, 'key':key, 'bridge':bridge})
        self._sources[addr] = src
        return {'addr':src, 'key':key, 'bridge':bridge}

    def _detach(self, addr):
        src = self._sources.get(addr)
        if src:
            self._request('detach', {'addr':src})
            del self._sources[addr]

    def _put(self, addr, buf):
        event = {'ev_name':addr, 'ev_type': EV_PUT}
        return self._request('put', {'addr':addr, 'buf':buf}, event)

    def _send(self, addr, buf):
        event = {'ev_name':addr, 'ev_type':EV_SEND}
        return self._request('send', {'addr':addr, 'buf':buf}, event)

    def _try_connect(self, addr, timeout=None):
        if not self._alloc.has_key(addr):
            log_err(self, 'failed to connect, no channel, addr=%s' % str(addr))
            raise Exception(log_get(self, 'failed to connect'))
        key = self._alloc[addr]['key']
        bridge = self._alloc[addr]['bridge']
        self._connect(addr, key, bridge, timeout=timeout, release=False)

    def _reconnect(self, addr, reset=False):
        try:
            if reset:
                self._disconnect(addr)
            self._try_connect(addr, timeout=self._get_timeout(TIMEOUT_CONNECT))
            return True
        except:
            return -ENOENT

    def _try_put(self, addr, buf):
        ret = self._put(addr, buf)
        if ret == True:
            return
        for i in range(RETRY_MAX):
            self._log('failed to put, reconnect, retry=%d, addr=%s' % (i + 1, addr))
            if ret == -ENOENT:
                reset = False
            else:
                reset = True
            ret = self._reconnect(addr, reset)
            if ret == True:
                ret = self._put(addr, buf)
                if ret == True:
                    return
        log_err(self, 'failed to put, addr=%s, ret=%s' % (str(addr), str(ret)))
        raise Exception(log_get(self, 'failed to put'))

    def _try_send(self, addr, buf):
        ret = self._send(addr, buf)
        if ret == True:
            return
        for i in range(RETRY_MAX):
            self._log('failed to send, reconnect, retry=%d, addr=%s' % (i + 1, addr))
            if ret == -ENOENT:
                reset = False
            else:
                reset = True
            ret = self._reconnect(addr, reset)
            if ret == True:
                ret = self._send(addr, buf)
                if ret == True:
                    return
        log_err(self, 'failed to send, addr=%s, ret=%s' % (str(addr), str(ret)))
        raise Exception(log_get(self, 'failed to send'))

    def _release(self, addr, detach=True):
        try:
            self._disconnect(addr)
            if self._channels.has_key(addr):
                del self._channels[addr]
                if detach == True:
                    self._detach(addr)
        except:
            log_debug(self, 'failed to release')

    @named_lock
    def _try_release(self, addr):
        total = self._channels.get(addr)
        if total != None and total <= 0:
            self._release(addr)

    @named_lock
    def disconnect(self, addr, release):
        if self._channels.has_key(addr):
            if self._channels[addr] > 0:
                self._channels[addr] -= 1
            if self._channels[addr] == 0 and release:
                self._release(addr)
                self._log('remove channel, addr=%s' % addr)

    def _wait(self):
        while True:
            channels = self._channels.keys()
            for addr in channels:
                self._try_release(addr)
                if len(self._channels) < CHANNEL_MAX:
                    return
            time.sleep(1)

    @named_lock
    def _check_channel(self, addr, key, bridge, gateway):
        if self._channels.has_key(addr):
            self._channels[addr] += 1
        else:
            if gateway:
                source = self._attach(addr)
                self._connect(addr, key, bridge, source)
            else:
                self._connect(addr, key, bridge)
            self._channels[addr] = 1
            self._log('create channel, addr=%s' % addr)

    def connect(self, addr, key, bridge, gateway):
        if len(self._channels) >= CHANNEL_MAX and addr not in self._channels:
            self._wait()
        self._check_channel(addr, key, bridge, gateway)

    @named_lock
    def send(self, addr, buf):
        self._send(addr, buf)

    @named_lock
    def allocate(self, addr, key, bridge):
        try:
            if self._alloc.has_key(addr):
                self._free(addr)
            self._alloc[addr] = {'key':key, 'bridge':bridge}
        except:
            log_err(self, 'failed to allocate')
            raise Exception(log_get(self, 'failed to allocate'))

    def _free(self, addr):
        self._release(addr)
        if self._alloc.has_key(addr):
            del self._alloc[addr]

    @named_lock
    def free(self, addr):
        if self._alloc.has_key(addr):
            self._free(addr)

    def _do_put(self, addr, buf, reliable):
        if reliable:
            self._try_put(addr, buf)
        else:
            self._try_send(addr, buf)

    @named_lock
    def put(self, addr, buf, reliable=RELIABLE, keep_connection=KEEP_CONNECTION):
        if not self._alloc.has_key(addr):
            log_err(self, 'failed to put, no channel')
            raise Exception(log_get(self, 'failed to put'))

        if not keep_connection:
            self._try_connect(addr)
            self._do_put(addr, buf, reliable)
            self._disconnect(addr)
        else:
            self._do_put(addr, buf, reliable)

    @named_lock
    def exist(self, addr):
        return self._exist(addr)

    def has_network(self, addr):
        return False

    def _clear(self):
        close_port(ADAPTER_PORT)

    def initialize(self):
        self._clear()
        args = [self._path,
              '-s', str(OUTPUT_MAX),
              '-p', str(BRIDGE_PORT),
              '-l', str(ADAPTER_PORT),
              '-e', str(CHANNEL_EVENT_PORT)]
        self._log('initialize, cmd=%s' % ' '.join(args))
        pid = popen(*args)
        if not chkpid(pid):
            raise Exception(log_get(self, 'failed to initialize'))
        return pid

    def create(self, addr, key, bridge):
        self._clear()
        args = [self._path,
              '-a', addr,
              '-k', key,
              '-b', bridge,
              '-s', str(OUTPUT_MAX),
              '-p', str(BRIDGE_PORT),
              '-w', str(TIMEOUT_PUT),
              '-t', str(TIMEOUT_SEND),
              '-l', str(ADAPTER_PORT),
              '-c', str(CONDUCTOR_PORT),
              '-e', str(CHANNEL_EVENT_PORT)]
        self._log('create, cmd=%s' % ' '.join(args))
        pid = popen(*args)
        if not chkpid(pid):
            raise Exception(log_get(self, 'failed to create'))
        return pid
