# lo.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib import io
from dev.udi import UDI
from dev.stub import Stub
from random import randint
from lib.log import log_err
from threading import Thread
from lib.loader import Loader
from dev.driver import load_driver
from conf.defaults import LO_ADDR, LO_PORT
from SocketServer import BaseRequestHandler
from lib.modes import MODE_CTRL, MODE_CLONE
from lib.util import get_devices, create_server

_devices = {}

def device_name(typ, name, mode=0):
    return '%s_%s_%d' % (typ, name, mode)

def connect(device):
    try:
        sock = io.connect(LO_ADDR, LO_PORT)
        io.put(sock, device, local=True)
        if device != io.get(sock, local=True):
            io.close(sock)
        else:
            return sock
    except:
        pass

def _get_type(device):
    res = device.split('_')
    if len(res) == 3:
        return res[0]

def _get_name(device):
    res = device.split('_')
    if len(res) == 3:
        return res[1]

def _get_mode(device):
    res = device.split('_')
    if len(res) == 3:
        return int(res[2])

class LoServer(BaseRequestHandler):
    def handle(self):
        device = None
        try:
            device = io.get(self.request, local=True)
            if not device:
                return
            typ = _get_type(device)
            name = _get_name(device)
            mode = _get_mode(device)
            if typ and name:
                driver = load_driver(typ, name)
                if not mode & MODE_CLONE:
                    driver.setup()
                if driver:
                    stub = Stub(self.request, driver)
                    _devices.update({device:stub})
                    io.put(self.request, device, local=True)
                    stub.start()
                else:
                    log_err(self, 'failed to handle, cannot load driver, type=%s, name=%s' % (typ, name))
        except:
            if _devices.has_key(device):
                _devices.pop(device)

class Lo(UDI):
    def get_name(self, device, child=None):
        return _get_name(device)
    
    def get_mode(self, device):
        return _get_mode(device)
    
    def setup(self):
        self._active = False
        self._loader = Loader(self.get_uid())
        Thread(target=create_server, args=(LO_ADDR, LO_PORT, LoServer)).start()
    
    def _get_device(self, name):
        mode = self._core.get_mode(name)
        prof = self._loader.get_profile(name)
        if prof:
            return device_name(prof['type'], name, mode)
    
    def scan(self):
        device_list = []
        if not self._active:
            self._active = True
            names = get_devices(self._uid, sort=True)
            if names:
                for name in names:
                    device = self._get_device(name)
                    if device and device not in _devices:
                        device_list.append(device)
        return device_list
    
    def connect(self, device):
        return (connect(device), True)
    
    @property
    def compute_unit(self):
        if not _devices:
            return
        keys = _devices.keys()
        length = len(keys)
        i = randint(0, length - 1)
        for _ in range(length):
            device = _devices[keys[i]]
            if device.get_mode() & MODE_CTRL:
                return device
            i += 1
            if i == length:
                i = 0
