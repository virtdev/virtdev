#      udi.py
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

import os
import ast
import req
import time
import copy
from lib import stream
from udo import VDevUDO
from threading import Lock, Thread
from lib.log import log_get, log_err
from multiprocessing.pool import ThreadPool

PAIR_INTERVAL = 7 # sec
SCAN_INTERVAL = 7 # sec
MOUNT_TIMEOUT = 15 # sec
MOUNT_RESET_INTERVAL = 0.1 # sec
DRIVER_PATH = os.path.join(os.getcwd(), 'drivers')
            
class VDevUDI(object):
    def __init__(self, uid, core):
        self._thread = None
        self._local = False
        self._lock = Lock()
        self._devices = {}
        self._core = core
        self._uid =uid
    
    def scan(self):
        pass
    
    def connect(self, device):
        pass
    
    def get_name(self, parent, child=None):
        pass
    
    def _get_info(self, buf):
        try:
            info = ast.literal_eval(buf)
            if type(info) != dict:
                log_err(self, 'invalid info')
                return
            return info
        except:
            log_err(self, 'invalid info')
    
    def _create_device(self, info, index=None):
        if not info.has_key('type'):
            log_err(self, 'cannot get type')
            raise Exception(log_get(self, 'cannot get type'))
        
        dev = VDevUDO()
        if index != None:
            dev.set_index(int(index))
        dev.set_type(str(info['type']))
        
        if info.has_key('freq'):
            dev.set_freq(float(info['freq']))
        
        if info.has_key('mode'):
            mode = int(info['mode'])
            dev.set_mode(mode)
        
        if info.has_key('range'):
            dev.set_range(dict(info['range']))
        
        if self._local:
            dev.set_local()
        
        return dev
    
    def _get_children(self, parent, info):
        devices = {}
        try:
            for i in info:
                dev = self._create_device(info[i], i)
                child = self.get_name(parent, i)
                devices.update({child:dev})
        except:
            log_err(self, 'invalid info')
            return
        for i in devices:
            devices[i].mount(self._uid, i, self._core)
        return devices
    
    def _mount(self, sock, device, init):
        info = None
        if not self._local:
            stream.put(sock, req.req_reset(), local=self._local)
            time.sleep(MOUNT_RESET_INTERVAL)
        stream.put(sock, req.req_mount(), local=self._local)
        buf = stream.get(sock, local=self._local)
        if buf:
            info = self._get_info(buf)
        if not info:
            log_err(self, 'no info')
            return
        name = self.get_name(device)
        if not self._local:
            children = self._get_children(name, info)
            if not children:
                return
            parent = VDevUDO(children)
        else:
            info = info['None']
            if not info:
                return
            parent = self._create_device(info)
        parent.mount(self._uid, name, self._core, sock=sock, init=init)
        self._devices.update({name:parent})
        return name
    
    def _proc(self, target, args, timeout):
        pool = ThreadPool(processes=1)
        try:
            result = pool.apply_async(target, args=args)
            result.wait(timeout)
            return result.get()
        except:
            pool.terminate()
    
    def _register(self, device, init=True):
        sock = self._proc(self.connect, (device,), PAIR_INTERVAL)
        if sock:
            name = self._proc(self._mount, (sock, device, init), MOUNT_TIMEOUT)
            if not name:
                log_err(self, 'failed to mount')
                sock.close()
            else:
                return name
    
    def register(self, device, init=True):
        return self._register(device, init)
    
    def find(self, name):
        devices = copy.copy(self._devices)
        for i in devices:
            d = devices[i].find(name)
            if d:
                return d
    
    def _check(self, devices):
        for d in devices:
            self._register(d)
    
    def start(self):
        self._thread = Thread(target=self._run)
        self._thread.start()
    
    def _run(self):
        while True:
            devices = self.scan()
            if devices:
                self._check(devices)
            time.sleep(SCAN_INTERVAL)
    