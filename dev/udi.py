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
import req
import time
import copy
from udo import UDO
from lib import stream
from threading import Lock, Thread
from lib.log import log_get, log_err
from lib.util import get_name, check_info
from multiprocessing.pool import ThreadPool

PAIR_INTERVAL = 7 # seconds
SCAN_INTERVAL = 7 # seconds
MOUNT_TIMEOUT = 15 # seconds
DRIVER_PATH = os.path.join(os.getcwd(), 'drivers')
            
class UDI(object):
    def __init__(self, uid, core):
        self._thread = None
        self._lock = Lock()
        self._devices = {}
        self._core = core
        self._uid = uid
        self.setup()
    
    def setup(self):
        pass
    
    def scan(self):
        pass
    
    def connect(self, device):
        pass
    
    def get_uid(self):
        return self._uid
    
    def get_name(self, parent, child=None):
        return get_name(self._uid, parent, child)
    
    def _create_device(self, info, local, index=None):
        if not info.has_key('type'):
            log_err(self, 'cannot get type')
            raise Exception(log_get(self, 'cannot get type'))
        
        dev = UDO(local=local)
        if index != None:
            dev.set_index(int(index))
        dev.set_type(str(info['type']))
        
        if info.get('freq'):
            dev.set_freq(float(info['freq']))
        
        if info.get('mode'):
            mode = int(info['mode'])
            dev.set_mode(mode)
        
        if info.get('range'):
            dev.set_range(dict(info['range']))
        
        return dev
    
    def _get_children(self, parent, info, local):
        devices = {}
        try:
            for i in info:
                dev = self._create_device(info[i], local, i)
                child = self.get_name(parent, i)
                devices.update({child:dev})
        except:
            log_err(self, 'invalid info')
            return
        for i in devices:
            devices[i].mount(self._uid, i, self._core)
        return devices
    
    def _get_info(self, sock, local):
        stream.put(sock, req.req_mount(), local=local)
        buf = stream.get(sock, local=local)
        if buf:
            return check_info(buf)
    
    def _mount(self, sock, local, device, init):
        info = self._get_info(sock, local)
        if not info:
            log_err(self, 'no info')
            return
        name = self.get_name(device)
        if info.has_key('None'):
            info = info['None']
            if not info:
                log_err(self, 'invalid info')
                return
            if local:
                parent = self._create_device(info, local)
            else:
                log_err(self, 'invalid device')
                return
        else:
            children = self._get_children(name, info, local)
            if not children:
                log_err(self, 'no device')
                return
            parent = UDO(children, local)
            init = False
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
    
    def _create(self, device, init=True):
        sock, local = self._proc(self.connect, (device,), PAIR_INTERVAL)
        if sock:
            name = self._proc(self._mount, (sock, local, device, init), MOUNT_TIMEOUT)
            if not name:
                log_err(self, 'cannot mount')
                sock.close()
            else:
                return name
    
    def create(self, device, init=True):
        return self._create(device, init)
    
    def find(self, name):
        devices = copy.copy(self._devices)
        for i in devices:
            d = devices[i].find(name)
            if d:
                return d
    
    def _start(self):
        while True:
            devices = self.scan()
            if devices:
                for d in devices:
                    self._create(d)
            time.sleep(SCAN_INTERVAL)
    
    def start(self):
        self._thread = Thread(target=self._start)
        self._thread.start()
