#      channel.py (wrtc)
#      
#      Copyright (C) 2016 Yi-Wei Ci <ciyiwei@hotmail.com>
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

import os
import json
import time
from lib import resolv
from conf.defaults import *
from lib.lock import NamedLock
from conf.log import LOG_CHANNEL
from threading import Lock, Event
from lib.bridge import get_bridge
from conf.virtdev import BRIDGE_PORT
from lib.log import log_debug, log_err, log_get
from lib.ws import WSHandler, ws_addr, ws_start, ws_connect
from lib.util import popen, get_dir, gen_key, pkill, close_port

RETRY_MAX = 1
CHANNEL_MAX = 256
ADAPTER_NAME = 'wrtc'
WAIT_INTERVAL = 1 # sec
KEEP_CONNECTION = True

TIMEOUT_CONN = 30 # sec
TIMEOUT_SEND = 15 # sec

EV_PUT = 'put'
EV_SEND = 'send'
EV_CONNECT = 'connect'
EV_TYPES = [EV_PUT, EV_SEND, EV_CONNECT]

def check_adapter(func):
    def _check_adapter(*args, **kwargs):
        self = args[0]
        addr = args[1]
        if not self._adapter:
            self._create_adapter()
        if not self._adapter:
            log_err(self, 'no adapter')
            raise Exception(log_get(self, 'no adapter'))
        self._lock.acquire(addr)
        try:
            return func(*args, **kwargs)
        finally:
            self._lock.release(addr)
    return _check_adapter

def check_type(func):
    def _check_type(*args, **kwargs):
        self = args[0]
        ev_type = args[1]
        if ev_type not in EV_TYPES:
            log_err(self, 'invalid event type')
            raise Exception(log_get(self, 'invalid event type'))
        self._lock.acquire()
        try:
            return func(*args, **kwargs)
        finally:
            self._lock.release()
    return _check_type

class ChannelEvent(object):
    def __init__(self):
        self._lock = Lock()
        self._error = {i:{} for i in EV_TYPES}
        self._event = {i:{} for i in EV_TYPES}
        self._count = {i:{} for i in EV_TYPES}
    
    @check_type
    def _add_event(self, ev_type, ev_name):
        ev = self._event[ev_type].get(ev_name)
        if not ev:
            ev = Event()
            self._event[ev_type][ev_name] = ev
            self._count[ev_type][ev_name] = 1
        else:
            self._count[ev_type][ev_name] += 1
        return ev
    
    @check_type
    def _remove_event(self, ev_type, ev_name):
        if self._event[ev_type].has_key(ev_name):
            self._count[ev_type][ev_name] -= 1
            if 0 == self._count[ev_type][ev_name]:
                del self._event[ev_type][ev_name]
                del self._count[ev_type][ev_name]
    
    @check_type
    def set(self, ev_type, ev_args):
        ev_name = ev_args.get('name')
        if ev_name:
            ev = self._event[ev_type].get(ev_name)
            if ev:
                if ev_args.has_key('error'): 
                    self._error[ev_type][ev_name] = ev_args['error']
                ev.set()
    
    def wait(self, ev_type, ev_name, timeout):
        ev = self._add_event(ev_type, ev_name)
        ret = ev.wait(timeout)
        if self._error[ev_type].has_key(ev_name):
            ret = self._error[ev_type][ev_name]
            del self._error[ev_type][ev_name]
        self._remove_event(ev_type, ev_name)
        return ret

_channel_event = ChannelEvent()

class ChannelEventHandler(WSHandler):
    def on_message(self, buf):
        if buf:
            args = json.loads(buf)
            if args:
                ev_type = args.get('ev_type')
                ev_args = args.get('ev_args')
                if ev_type and ev_args:
                    _channel_event.set(ev_type, ev_args)

class Channel(object):
    def __init__(self):
        self._alloc = {}
        self._sources = {}
        self._channels = {}
        self._adapter = None
        self._lock = NamedLock()
        self._init_lock = Lock()
        self._path = self._get_adapter_path()
        self._init_monitor()
    
    def _init_monitor(self):
        close_port(CHANNEL_EVENT_PORT)
        ws_start(ChannelEventHandler, CHANNEL_EVENT_PORT, CHANNEL_EVENT_ADDR)
    
    def _log(self, text):
        if LOG_CHANNEL:
            log_debug(self, text)
    
    def _get_adapter_path(self):
        return os.path.join(get_dir(), 'lib', 'protocol', 'wrtc', ADAPTER_NAME)
    
    def _request(self, **args):
        if args:
            self._adapter.send(json.dumps(args))
        else:
            log_debug(self, 'failed to request')
    
    def _create_adapter(self):
        self._init_lock.acquire()
        try:
            if not self._adapter:
                addr = ws_addr(ADAPTER_ADDR, ADAPTER_PORT)
                self._adapter = ws_connect(addr)
        except:
            log_err(self, 'failed to create adapter')
        finally:
            self._init_lock.release()
    
    def _is_put(self, addr):
        return _channel_event.wait(EV_PUT, addr, TIMEOUT_SEND)
    
    def _is_sent(self, addr):
        return _channel_event.wait(EV_SEND, addr, TIMEOUT_SEND)
    
    def _is_connected(self, addr):
        return _channel_event.wait(EV_CONNECT, addr, TIMEOUT_CONN)
    
    def _open(self, addr, key, bridge, source=None):
        if source:
            self._request(cmd='open', addr=addr, key=key, bridge=bridge, source=source)
        else:
            self._request(cmd='open', addr=addr, key=key, bridge=bridge)
    
    def _close(self, addr):
        self._request(cmd='close', addr=addr)
    
    def _attach(self, addr):
        key = gen_key()
        src = resolv.get_addr()
        bridge = get_bridge(src)
        self._request(cmd='attach', addr=src, key=key, bridge=bridge)
        self._sources[addr] = src
        return {'addr':src, 'key':key, 'bridge':bridge}
    
    def _detach(self, addr):
        src = self._sources.get(addr)
        if src:
            self._request(cmd='detach', addr=src)
            del self._sources[addr]
    
    def _put(self, addr, buf):
        self._request(cmd='put', addr=addr, buf=buf)
        ret = self._is_put(addr)
        if ret == True:
            return
        for _ in range(RETRY_MAX):
            if ret == -1:
                self._log('put, retry connecting, addr=%s' % str(addr))
                try:
                    self._try_connect(addr)
                except:
                    continue
            self._log('put, retry sending, addr=%s' % str(addr))
            self._request(cmd='put', addr=addr, buf=buf)
            ret = self._is_put(addr)
            if ret == True:
                return
        log_err(self, 'failed to put, addr=%s' % str(addr))
        raise Exception(log_get(self, 'failed to put'))
    
    def _send(self, addr, buf):
        self._request(cmd='send', addr=addr, buf=buf)
        if self._is_sent(addr):
            return
        for _ in range(RETRY_MAX):
            if not self._exist(addr):
                self._log('send, retry connecting, addr=%s' % str(addr))
                try:
                    self._try_connect(addr)
                except:
                    continue
            self._log('send, retry sending, addr=%s' % str(addr))
            self._request(cmd='send', addr=addr, buf=buf)
            if self._is_sent(addr):
                return
        log_err(self, 'failed to send, addr=%s' % str(addr))
        raise Exception(log_get(self, 'failed to send'))
    
    def _release_connection(self, addr):
        self._close(addr)
        self._detach(addr)
    
    def _do_disconnect(self, addr):
        self._release_connection(addr)
        if self._channels.has_key(addr):
            del self._channels[addr]
    
    @check_adapter
    def _disconnect(self, addr):
        total = self._channels.get(addr)
        if total != None and total <= 0:
            self._do_disconnect(addr)
    
    @check_adapter
    def disconnect(self, addr, release):
        if self._channels.has_key(addr):
            if self._channels[addr] > 0:
                self._channels[addr] -= 1
                if self._channels[addr] == 0:
                    self._do_disconnect(addr)
                    self._log('disconnect, addr=%s' % addr)
    
    def _wait(self):
        while True:
            channels = self._channels.keys()
            for addr in channels:
                self._disconnect(addr)
                if len(self._channels) < CHANNEL_MAX:
                    return
            time.sleep(WAIT_INTERVAL)
    
    def _do_connect(self, addr, key, bridge, gateway=False):
        if gateway:
            source = self._attach(addr)
            self._open(addr, key, bridge, source)
        else:
            self._open(addr, key, bridge)
        
        if not self._is_connected(addr):
            self._release_connection(addr)
            log_err(self, 'failed to connect, addr=%s' % str(addr))
            raise Exception(log_get(self, 'failed to connect'))
    
    @check_adapter
    def _connect(self, addr, key, bridge, gateway):
        if self._channels.has_key(addr):
            self._channels[addr] += 1
        else:
            self._do_connect(addr, key, bridge, gateway)
            self._channels[addr] = 1
            self._log('connect, addr=%s' % addr)
    
    def connect(self, addr, key, bridge, gateway):
        if len(self._channels) >= CHANNEL_MAX and addr not in self._channels:
            self._wait()
        self._connect(addr, key, bridge, gateway)
    
    @check_adapter
    def send(self, addr, buf):
        self._send(addr, buf)
        self._log('send, addr=%s' % addr)
    
    @check_adapter
    def allocate(self, addr, key, bridge):
        if self._alloc.has_key(addr):
            self._free(addr)
        self._alloc[addr] = {'key':key, 'bridge':bridge}
    
    def _free(self, addr):
        self._do_disconnect(addr)
        del self._alloc[addr]
    
    @check_adapter
    def free(self, addr):
        if self._alloc.has_key(addr):
            self._free(addr)
    
    def _try_connect(self, addr):
        if not self._alloc.has_key(addr):
            log_err(self, 'failed to connect, no allocation, addr=%s' % str(addr))
            raise Exception(log_get(self, 'failed to connect'))
        key = self._alloc[addr]['key']
        bridge = self._alloc[addr]['bridge']
        self._do_connect(addr, key, bridge)
    
    @check_adapter
    def put(self, addr, buf):
        if not self._alloc.has_key(addr):
            log_err(self, 'failed to put, no channel')
            raise Exception(log_get(self, 'failed to put'))
        
        self._log('put, addr=%s, len=%s' % (addr, len(buf)))
        if not KEEP_CONNECTION:
            self._try_connect(addr)
            self._put(addr, buf)
            self._close(addr)
        else:
            if not self._exist(addr):
                self._try_connect(addr)
            self._put(addr, buf)
    
    def _exist(self, addr):
        self._request(cmd='exist', addr=addr)
        ret = self._adapter.recv()
        if ret == 'exist':
            return True
    
    @check_adapter
    def exist(self, addr):
        return self._exist(addr)
    
    def has_network(self, addr):
        return False
    
    def initialize(self):
        popen(self._path, 
              '-p', str(BRIDGE_PORT),
              '-l', str(ADAPTER_PORT),
              '-e', str(CHANNEL_EVENT_PORT))
    
    def create(self, addr, key, bridge):
        popen(self._path,
              '-a', addr,
              '-k', key,
              '-b', bridge,
              '-p', str(BRIDGE_PORT),
              '-t', str(TIMEOUT_SEND),
              '-l', str(ADAPTER_PORT),
              '-c', str(CONDUCTOR_PORT),
              '-e', str(CHANNEL_EVENT_PORT))
    
    def clean(self):
        pkill(ADAPTER_NAME)
