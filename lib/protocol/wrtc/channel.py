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
from lib.lock import NamedLock
from conf.path import PATH_RUN
from threading import Thread, Lock
from lib.util import DEVNULL, get_dir
from websocket import create_connection
from lib.log import log_err, log_get, log
from conf.virtdev import BRIDGE_PORT, CONDUCTOR_PORT, PROXY_ADDR, PROXY_PORT

PRINT = True
CONNECT_MAX = 5
CHANNEL_MAX = 4096
CHECK_INTERVAL = 0.1 #seconds
CONNECT_INTERVAL = 1 # seconds

def chkproxy(func):
    def _chkproxy(*args, **kwargs):
        self = args[0]
        addr = args[1]
        if not self._check_proxy():
            log_err(self, 'failed to check proxy')
            return
        self._lock.acquire(addr)
        try:
            return func(*args, **kwargs)
        finally:
            self._lock.release(addr)
    return _chkproxy

class Channel(object):
    def __init__(self):
        self._proxy = None
        self._channels = {}
        self._lock = NamedLock()
        self._proxy_lock = Lock()
        self._proxy_addr = "ws://%s:%d" % (PROXY_ADDR, PROXY_PORT)
    
    def _create_proxy(self, addr, key, bridge):
        path = os.path.join(get_dir(), 'lib', 'protocol', 'wrtc', 'proxy.js')
        cmd = ['node', path, '-b', bridge, '-p', str(BRIDGE_PORT), '-a', addr, '-k', key, '-c', str(CONDUCTOR_PORT), '-l', str(PROXY_PORT)]
        if PRINT:
            log(log_get(self, 'create_proxy, cmd=%s' % ''.join([i + ' ' for i in cmd])))
        pid = Popen(cmd, stdout=DEVNULL, stderr=DEVNULL).pid
        path = os.path.join(PATH_RUN, 'proxy.pid')
        with open(path, 'w') as f:
            f.write(str(pid))
    
    def _connect_proxy(self):
        self._proxy_lock.acquire()
        try:
            if not self._proxy:
                self._proxy = create_connection(self._proxy_addr)
        except:
            log_err(self, 'failed to connect to proxy')
        finally:
            self._proxy_lock.release()
    
    def _check_proxy(self):
        if not self._proxy:
            self._connect_proxy()
        if self._proxy:
            return True
    
    def create(self, addr, key, bridge):
        Thread(target=self._create_proxy, args=(addr, key, bridge)).start()
    
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
        req = json.dumps({'cmd':'exist', 'add':addr})
        for _ in range(CONNECT_MAX):
            self._proxy.send(req)
            ret = self._proxy.recv()
            if ret == 'exist':
                return True
            time.sleep(CONNECT_INTERVAL)
    
    @chkproxy
    def connect(self, addr, key, static, verify, bridge):
        if len(self._channels) >= CHANNEL_MAX and addr not in self._channels:
            self._recycle()
        if self._channels.has_key(addr):
            self._channels[addr] += 1
        else:
            req = json.dumps({'cmd':'open', 'add':addr, 'key':key, 'bridge':bridge})
            self._proxy.send(req)
            if verify:
                if not self._exist(addr):
                    log_err(self, 'failed to connect')
                    raise Exception(log_get(self, 'failed to connect'))
            self._channels[addr] = 1
    
    def _disconnect(self, addr):
        req = json.dumps({'cmd':'close', 'add':addr})
        self._proxy.send(req)
    
    @chkproxy
    def disconnect(self, addr, release):
        if self._channels.has_key(addr):
            if self._channels[addr] > 0:
                self._channels[addr] -= 1
                if self._channels[addr] == 0:
                    self._do_release(addr)
    
    @chkproxy
    def put(self, addr, buf):
        req = json.dumps({'cmd':'write', 'add':addr, 'buf':buf})
        self._proxy.send(req)
    
    @chkproxy
    def exist(self, addr):
        return self._exist(addr)
