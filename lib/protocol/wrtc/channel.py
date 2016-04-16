#      channel.py (wrtc)
#      
#      Copyright (C) 2015 Yi-Wei Ci <ciyiwei@hotmail.com>
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
from subprocess import Popen
from conf.log import LOG_WRTC
from lib.lock import NamedLock
from threading import Thread, Lock
from lib.util import DEVNULL, get_dir
from websocket import create_connection
from lib.log import log_debug, log_err, log_get
from conf.virtdev import BRIDGE_PORT, CONDUCTOR_PORT, ADAPTER_ADDR, ADAPTER_PORT, PATH_RUN

CONNECT_MAX = 5
CHANNEL_MAX = 4096
CHECK_INTERVAL = 0.1 #seconds
CONNECT_INTERVAL = 1 # seconds

def chkadapter(func):
    def _chkadapter(*args, **kwargs):
        self = args[0]
        addr = args[1]
        if not self._check_adapter():
            log_err(self, 'failed to check adapter')
            return
        self._lock.acquire(addr)
        try:
            return func(*args, **kwargs)
        finally:
            self._lock.release(addr)
    return _chkadapter

class Channel(object):
    def __init__(self):
        self._adapter = None
        self._channels = {}
        self._lock = NamedLock()
        self._adapter_lock = Lock()
        self._adapter_addr = "ws://%s:%d" % (ADAPTER_ADDR, ADAPTER_PORT)
    
    def _log(self, text):
        if LOG_WRTC:
            log_debug(self, text)
    
    def _create_adapter(self, addr, key, bridge):
        path = os.path.join(get_dir(), 'lib', 'protocol', 'wrtc', 'adapter.js')
        cmd = ['node', path, '-b', bridge, '-p', str(BRIDGE_PORT), '-a', addr, '-k', key, '-c', str(CONDUCTOR_PORT), '-l', str(ADAPTER_PORT)]
        self._log('create adapter, cmd=%s' % ''.join([i + ' ' for i in cmd]))
        pid = Popen(cmd, stdout=DEVNULL, stderr=DEVNULL).pid
        path = os.path.join(PATH_RUN, 'adapter.pid')
        with open(path, 'w') as f:
            f.write(str(pid))
    
    def _connect_adapter(self):
        self._adapter_lock.acquire()
        try:
            if not self._adapter:
                self._adapter = create_connection(self._adapter_addr)
        except:
            log_err(self, 'failed to connect to adapter')
        finally:
            self._adapter_lock.release()
    
    def _check_adapter(self):
        if not self._adapter:
            self._connect_adapter()
        if self._adapter:
            return True
    
    def create(self, addr, key, bridge):
        Thread(target=self._create_adapter, args=(addr, key, bridge)).start()
    
    def clean(self):
        pass
    
    def _do_release(self, addr):
        self._disconnect(addr)
        del self._channels[addr]
    
    def _release(self, addr):
        total = self._channels.get(addr)
        if total != None and total <= 0:
            self._do_release(addr)
    
    def _recycle(self):
        while True:
            channels = self._channels.keys()
            for addr in channels:
                self._release(addr)
                if len(self._channels) < CHANNEL_MAX:
                    return
            time.sleep(CHECK_INTERVAL)
    
    def _exist(self, addr):
        req = json.dumps({'cmd':'exist', 'addr':addr})
        for _ in range(CONNECT_MAX):
            self._adapter.send(req)
            ret = self._adapter.recv()
            if ret == 'exist':
                return True
            time.sleep(CONNECT_INTERVAL)
    
    @chkadapter
    def connect(self, addr, key, static, verify, bridge):
        if len(self._channels) >= CHANNEL_MAX and addr not in self._channels:
            self._recycle()
        if self._channels.has_key(addr):
            self._channels[addr] += 1
        else:
            req = json.dumps({'cmd':'open', 'addr':addr, 'key':key, 'bridge':bridge})
            self._adapter.send(req)
            if verify:
                if not self._exist(addr):
                    log_err(self, 'failed to connect')
                    raise Exception(log_get(self, 'failed to connect'))
            self._channels[addr] = 1
    
    def _disconnect(self, addr):
        req = json.dumps({'cmd':'close', 'addr':addr})
        self._adapter.send(req)
    
    @chkadapter
    def disconnect(self, addr, release):
        if self._channels.has_key(addr):
            if self._channels[addr] > 0:
                self._channels[addr] -= 1
                if self._channels[addr] == 0:
                    self._do_release(addr)
    
    @chkadapter
    def put(self, addr, buf):
        req = json.dumps({'cmd':'write', 'addr':addr, 'buf':buf})
        self._adapter.send(req)
    
    @chkadapter
    def exist(self, addr):
        return self._exist(addr)
