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
import req
import imp
import time
import copy
from lib import stream
from drivers.SU import SU
from lib.util import get_name
from threading import Lock, Thread
from lib.log import log_get, log_err
from multiprocessing.pool import ThreadPool

VDEV_PAIR_INTERVAL = 7 # sec
VDEV_SCAN_INTERVAL = 7 # sec
VDEV_MOUNT_TIMEOUT = 15 # sec
VDEV_MOUNT_RESET_INTERVAL = 1 # sec
VDEV_DRIVER_PATH = os.path.join(os.getcwd(), 'drivers')

import sys
sys.path.append(os.getcwd())

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
    
    def connect(self, name):
        pass
    
    def _load_device(self, name):
        try:
            module = imp.load_source(name, os.path.join(VDEV_DRIVER_PATH, '%s.py' % name))
            if module and hasattr(module, name):
                device = getattr(module, name)
                if device:
                    return device()
        except:
            log_err(self, 'failed to load device %s' % name)
    
    def _device_types(self, buf):
        try:
            names = eval(buf)
            if type(names) != dict:
                log_err(self, 'invalid device names')
                return
            return names
        except:
            log_err(self, 'invalid device names')
    
    def _add_devices(self, parent, types):
        index = {}
        devices = {}
        try:
            for i in types:
                device = self._load_device(types[i])
                if device:
                    name = get_name(self._uid, parent, i)
                    devices.update({name:device})
                    index.update({name:i})
                else:
                    log_err(self, 'failed to add devices')
                    return
        except:
            log_err(self, 'failed to add devices')
            return
        for name in devices:
            devices[name].mount(self.manager, name, int(index[name]))
        return devices
    
    def _mount_device(self, sock, name, types):
        parent = get_name(self._uid, name)
        devices = self._add_devices(parent, types)
        su = SU(devices)
        su.mount(self.manager, parent, sock=sock)
        self._devices.update({parent:su})
    
    def _mount_anon(self, sock, name, types):
        if len(types) != 1:
            log_err(self, 'failed to mount')
            raise Exception(log_get(self, 'failed to mount'))
        anon = get_name(self._uid, name)
        device = self._load_device(types[types.keys()[0]])
        device.mount(self.manager, anon, sock=sock)
        self._devices.update({anon:device})
    
    def _mount(self, sock, name):
        stream.put(sock, req.req_reset(), anon=self._anon)
        time.sleep(VDEV_MOUNT_RESET_INTERVAL)
        stream.put(sock, req.req_pair(), anon=self._anon)
        res = stream.get(sock, anon=self._anon)
        if res:
            types = self._device_types(res)
            if types:
                if not self._anon:
                    self._mount_device(sock, name, types)
                else:
                    self._mount_anon(sock, name, types)
                return True
    
    def _proc(self, target, args, timeout):
        pool = ThreadPool(processes=1)
        try:
            result = pool.apply_async(target, args=args)
            result.wait(timeout)
            return result.get()
        except:
            pool.terminate()
    
    def _register(self, name):
        sock = self._proc(self.connect, (name,), VDEV_PAIR_INTERVAL)
        if sock:
            if not self._proc(self._mount, (sock, name), VDEV_MOUNT_TIMEOUT):
                log_err(self, 'failed to register')
                sock.close()
            else:
                return True
    
    def find(self, name):
        devices = copy.copy(self._devices)
        for i in devices:
            if devices[i].exists(name):
                return devices[i]
    
    def _check(self, names):
        for name in names:
            self._register(name)
    
    def run(self):
        while True:
            names = self.scan()
            if names:
                self._check(names)
            time.sleep(VDEV_SCAN_INTERVAL)
    