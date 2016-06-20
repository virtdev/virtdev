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
from threading import Lock
from conf.log import LOG_WRTC
from lib.lock import NamedLock
from lib.bridge import get_bridge
from websocket import create_connection
from lib.log import log_debug, log_err, log_get
from lib.util import call, popen, get_dir, gen_key
from conf.virtdev import BRIDGE_PORT, CONDUCTOR_PORT, ADAPTER_ADDR, ADAPTER_PORT

CHANNEL_MAX = 256
VERIFY_RETRY = 24
WAIT_INTERVAL = 1 # seconds
VERIFY_INTERVAL = 1 # seconds
ADAPTER_NAME = 'wrtc'

def adapter(func):
    def _adapter(*args, **kwargs):
        self = args[0]
        addr = args[1]
        if not self._check_adapter():
            log_err(self, 'no adapter')
            return
        self._lock.acquire(addr)
        try:
            return func(*args, **kwargs)
        finally:
            self._lock.release(addr)
    return _adapter

class Channel(object):
    def __init__(self):
        self._adapter = None
        self._sources = {}
        self._channels = {}
        self._lock = NamedLock()
        self._init_lock = Lock()
        self._path = os.path.join(get_dir(), 'bin', ADAPTER_NAME)
    
    def _log(self, text):
        if LOG_WRTC:
            log_debug(self, text)
    
    def _request(self, **args):
        if args:
            self._adapter.send(json.dumps(args))
    
    def _create_adapter(self):
        self._init_lock.acquire()
        try:
            if not self._adapter:
                addr = "ws://%s:%d" % (ADAPTER_ADDR, ADAPTER_PORT)
                self._adapter = create_connection(addr)
        except:
            log_err(self, 'failed to create adapter')
        finally:
            self._init_lock.release()
    
    def _check_adapter(self):
        if not self._adapter:
            self._create_adapter()
        if self._adapter:
            return True
    
    def _disconnect(self, addr):
        self._request(cmd='close', addr=addr)
        if self._sources.has_key(addr):
            self._request(cmd='detach', addr=self._sources[addr])
            del self._sources[addr]
    
    def _do_release(self, addr):
        self._disconnect(addr)
        del self._channels[addr]
    
    def _release(self, addr):
        total = self._channels.get(addr)
        if total != None and total <= 0:
            self._do_release(addr)
    
    def _wait(self):
        while True:
            channels = self._channels.keys()
            for addr in channels:
                self._release(addr)
                if len(self._channels) < CHANNEL_MAX:
                    return
            time.sleep(WAIT_INTERVAL)
    
    def _verify(self, addr):
        for _ in range(VERIFY_RETRY + 1):
            self._request(cmd='exist', addr=addr)
            ret = self._adapter.recv()
            if ret == 'exist':
                return True
            time.sleep(VERIFY_INTERVAL)
    
    def _can_connect(self, addr):
        if len(self._channels) >= CHANNEL_MAX and addr not in self._channels:
            self._wait()
        return True
    
    def _check_source(self, name):
        key = gen_key()
        addr = resolv.get_addr()
        bridge = get_bridge(addr)
        self._request(cmd='attach', addr=addr, key=key, bridge=bridge)
        self._sources[name] = addr
        return {'addr':addr, 'key':key, 'bridge':bridge}
    
    @adapter
    def connect(self, addr, key, static, verify, bridge):
        if self._can_connect(addr):
            if self._channels.has_key(addr):
                self._channels[addr] += 1
            else:
                if static:
                    source = self._check_source(addr)
                    self._request(cmd='open', addr=addr, key=key, bridge=bridge, source=source)
                    self._log('connect, addr=%s, source=%s' % (addr, source))
                else:
                    self._request(cmd='open', addr=addr, key=key, bridge=bridge)
                if verify:
                    if not self._verify(addr):
                        log_err(self, 'failed to connect')
                        raise Exception(log_get(self, 'failed to connect'))
                self._channels[addr] = 1
    
    @adapter
    def disconnect(self, addr, release):
        if self._channels.has_key(addr):
            if self._channels[addr] > 0:
                self._channels[addr] -= 1
                if self._channels[addr] == 0:
                    self._do_release(addr)
    
    @adapter
    def put(self, addr, buf):
        self._request(cmd='write', addr=addr, buf=buf)
    
    @adapter
    def exist(self, addr):
        return self._verify(addr)
    
    def has_network(self, addr):
        return False
    
    def initialize(self):
        self._log('initialize')
        popen(self._path, '-p', str(BRIDGE_PORT), '-l', str(ADAPTER_PORT))
    
    def create(self, addr, key, bridge):
        self._log('create, addr=%s, bridge=%s' % (addr, bridge))
        popen(self._path, '-b', bridge, '-p', str(BRIDGE_PORT), '-a', addr, '-k', key, '-c', str(CONDUCTOR_PORT), '-l', str(ADAPTER_PORT))
    
    def clean(self):
        call('killall', '-9', ADAPTER_NAME)
