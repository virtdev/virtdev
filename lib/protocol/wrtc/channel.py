# channel.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import json
import time
from errno import *
from lib import resolv
from conf.defaults import *
from threading import Thread
from lib.lock import NamedLock
from conf.log import LOG_CHANNEL
from threading import Lock, Event
from lib.bridge import get_bridge
from conf.virtdev import BRIDGE_PORT
from lib.log import log_debug, log_err, log_get
from lib.ws import WSHandler, ws_addr, ws_start, ws_connect
from lib.util import popen, get_dir, gen_key, pkill, close_port

RELIABLE = True
RETRY_MAX = 1
CHANNEL_MAX = 256
ADAPTER_NAME = 'wrtc'
KEEP_CONNECTION = True

WAIT_INTERVAL = 1 # seconds
TIMEOUT_SEND = 15 # seconds
TIMEOUT_EXIST = 3 # seconds
TIMEOUT_CONNECT = 15 # seconds
TIMEOUT_PUT = TIMEOUT_SEND

EV_PUT = 'put'
EV_SEND = 'send'
EV_EXIST = 'exist'
EV_CONNECT = 'connect'

EV_TYPES = [EV_PUT, EV_SEND, EV_EXIST, EV_CONNECT]
EV_TIMEOUT = {EV_PUT:TIMEOUT_PUT, EV_SEND:TIMEOUT_SEND, EV_EXIST:TIMEOUT_EXIST, EV_CONNECT:TIMEOUT_CONNECT}

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
    
    def wait(self, ev, ev_timeout, retry):
        if not retry:
            return ev.wait(ev_timeout)
        else:
            for _ in range(RETRY_MAX + 1):
                if ev.wait(ev_timeout):
                    return True

_channel_event = ChannelEvent()

def _get_event(ev_type, ev_name):
    return _channel_event.get(ev_type, ev_name)

def _put_event(ev_type, ev_name):
    return _channel_event.put(ev_type, ev_name)

def _set_event(ev_type, ev_args):
    _channel_event.set(ev_type, ev_args)
    
def _wait_event(ev, ev_timeout, retry=False):
    return _channel_event.wait(ev, ev_timeout, retry)

class ChannelEventHandler(WSHandler):
    def _handle(self, buf):
        if buf:
            args = json.loads(buf)
            if args:
                ev_type = args.get('event')
                ev_args = args.get('args')
                if ev_type and ev_args:
                    _set_event(ev_type, ev_args)
                else:
                    log_err(self, 'invalid event')
        else:
            log_err(self, 'no event')
    
    def on_message(self, buf):
        Thread(target=self._handle, args=(buf,)).start()

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
    
    def _request(self, args, event=None, retry=False):
        if not args:
            log_debug(self, 'failed to request, no arguments')
            return
        
        ev = None
        if event:
            ev = _get_event(event['ev_type'], event['ev_name'])
            if not ev:
                log_err(self, 'failed to get event')
                return
        
        ret = None
        try:
            self._adapter.send(json.dumps(args))
            if event:
                ret = _wait_event(ev, EV_TIMEOUT[event['ev_type']], retry)
        finally:
            if event:
                err = _put_event(event['ev_type'], event['ev_name'])
                if err != None:
                    ret = err
        return ret
    
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
    
    def _exist(self, addr):
        args = {'cmd':'exist', 'addr':addr}
        event = {'ev_name':addr, 'ev_type':EV_EXIST}
        ret = self._request(args, event, retry=True)
        if ret == True:
            return True
        elif ret == -ENOENT:
            return False
    
    def _close(self, addr):
        args = {'cmd':'close', 'addr':addr}
        self._request(args)
    
    def _attach(self, addr):
        key = gen_key()
        src = resolv.get_addr()
        bridge = get_bridge(src)
        args = {'cmd':'attach', 'addr':src, 'key':key, 'bridge':bridge}
        self._request(args)
        self._sources[addr] = src
        return {'addr':src, 'key':key, 'bridge':bridge}
    
    def _detach(self, addr):
        src = self._sources.get(addr)
        if src:
            args = {'cmd':'detach', 'addr':src}
            self._request(args)
            del self._sources[addr]
    
    def _do_put(self, addr, buf):
        args = {'cmd':'put', 'addr':addr, 'buf':buf}
        event = {'ev_name':addr, 'ev_type': EV_PUT}
        return self._request(args, event, retry=True)
    
    def _put(self, addr, buf):
        ret = self._do_put(addr, buf)
        if ret == True:
            return
        for _ in range(RETRY_MAX):
            if ret == -ENOENT:
                self._log('put, retry, addr=%s' % str(addr))
                try:
                    self._try_connect(addr)
                except:
                    continue
                ret = self._do_put(addr, buf)
                if ret == True:
                    return
            else:
                break
        log_err(self, 'failed to put, addr=%s' % str(addr))
        raise Exception(log_get(self, 'failed to put'))
    
    def _do_send(self, addr, buf):
        args = {'cmd':'send', 'addr':addr, 'buf':buf}
        event = {'ev_name':addr, 'ev_type':EV_SEND}
        return self._request(args, event, retry=True)
    
    def _send(self, addr, buf):
        ret = self._do_send(addr, buf)
        if ret == True:
            return
        for _ in range(RETRY_MAX):
            if ret == -ENOENT:
                self._log('send, retry, addr=%s' % str(addr))
                try:
                    self._try_connect(addr)
                except:
                    continue
                ret = self._do_send(addr, buf)
                if ret == True:
                    return
            else:
                break
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
        args = {'cmd':'open', 'addr':addr, 'key':key, 'bridge':bridge}
        if gateway:
            args.update({'source':self._attach(addr)})
        event = {'ev_name':addr, 'ev_type':EV_CONNECT}
        if self._request(args, event) != True:
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
        
        if not KEEP_CONNECTION:
            self._try_connect(addr)
        
        if RELIABLE:
            self._put(addr, buf)
        else:
            self._send(addr, buf)
        
        if not KEEP_CONNECTION:
            self._close(addr)
        
        self._log('put, addr=%s, len=%s' % (addr, len(buf)))
    
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
