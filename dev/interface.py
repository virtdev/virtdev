#      interface.py
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
import imp
import time
import copy
from lib import stream
from drivers.SU import SU
from lib.log import log_err
from lib.util import get_name
from threading import Lock, Thread
from multiprocessing.pool import ThreadPool

VDEV_PAIR_INTERVAL = 7 # sec
VDEV_SCAN_INTERVAL = 7 # sec
VDEV_MOUNT_TIMEOUT = 15 # sec
VDEV_MOUNT_RESET_INTERVAL = 1 # sec
VDEV_DRIVER_PATH = os.path.join(os.getcwd(), 'drivers')

import sys
sys.path.append(os.getcwd())

def load_device(typ):
    try:
        device = typ.upper()
        module = imp.load_source(device, os.path.join(VDEV_DRIVER_PATH, '%s.py' % device))
        if module and hasattr(module, device):
            dev = getattr(module, device)
            if dev:
                d = dev()
                d.set_type(typ)
                return d
    except:
        pass
            
class VDevInterface(Thread):
    def __init__(self, manager):
        self.manager = manager
        self._uid = self.manager.uid
        Thread.__init__(self)
        self._lock = Lock()
        self._anon = False
        self._devices = {}
    
    def scan(self):
        pass
    
    def connect(self, device):
        pass
    
    def _get_name(self, device):
        pass
    
    def _get_device_info(self, buf):
        try:
            info = ast.literal_eval(buf)
            if type(info) != dict:
                log_err(self, 'invalid device info')
                return
            return info
        except:
            log_err(self, 'invalid device info')
    
    def _add_device(self, name, info):
        index = {}
        devices = {}
        try:
            for i in info:
                device = load_device(info[i])
                if device:
                    child = get_name(self._uid, name, i)
                    devices.update({child:device})
                    index.update({child:i})
                else:
                    log_err(self, 'failed to add devices')
                    return
        except:
            log_err(self, 'failed to add devices')
            return
        for i in devices:
            devices[i].mount(self.manager, i, index=int(index[i]))
        return devices
    
    def _mount_device(self, sock, device):
        stream.put(sock, req.req_reset())
        time.sleep(VDEV_MOUNT_RESET_INTERVAL)
        stream.put(sock, req.req_pair())
        res = stream.get(sock)
        if res:
            info = self._get_device_info(res)
            if not info:
                log_err(self, 'no device info')
                return
        name = get_name(self._uid, device)
        devices = self._add_device(name, info)
        su = SU(devices)
        su.mount(self.manager, name, sock=sock)
        self._devices.update({name:su})
        return name
    
    def _mount_anon(self, sock, device, init):
        typ, name = self._get_name(device)
        dev = load_device(typ)
        dev.mount(self.manager, name, sock=sock, init=init)
        self._devices.update({name:dev})
        return name
    
    def _mount(self, sock, device, init):
        if not self._anon:
            return self._mount_device(sock, device)
        else:
            return self._mount_anon(sock, device, init)
    
    def _proc(self, target, args, timeout):
        pool = ThreadPool(processes=1)
        try:
            result = pool.apply_async(target, args=args)
            result.wait(timeout)
            return result.get()
        except:
            pool.terminate()
    
    def _register(self, device, init=True):
        sock = self._proc(self.connect, (device,), VDEV_PAIR_INTERVAL)
        if sock:
            name = self._proc(self._mount, (sock, device, init), VDEV_MOUNT_TIMEOUT)
            if not name:
                log_err(self, 'failed to register')
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
    
    def run(self):
        while True:
            devices = self.scan()
            if devices:
                self._check(devices)
            time.sleep(VDEV_SCAN_INTERVAL)
    