#      lo.py
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

import socket
from lib import stream
from dev.udi import UDI
from fs.path import load
from random import randint
from lib.log import log_err
from threading import Thread
from lib.loader import Loader
from lib.util import load_driver
from conf.virtdev import LO_ADDR, LO_PORT
from lib.mode import MODE_LO, MODE_PASSIVE

def device_name(typ, name):
    return '%s_%s' % (typ, name)

def connect(device):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((LO_ADDR, LO_PORT))
        stream.put(sock, device, local=True)
        if device != stream.get(sock, local=True):
            sock.close()
        else:
            return sock
    except:
        pass

class Lo(UDI):
    def _get_type(self, device):
        res = device.split('_')
        if len(res) == 2:
            return res[0]
    
    def get_name(self, device, child=None):
        res = device.split('_')
        if len(res) == 2:
            return res[1]
    
    def _listen(self):
        while True:
            sock = self._sock.accept()[0]
            try:
                device = stream.get(sock, local=True)
                if not device:
                    sock.close()
                    continue
                name = self.get_name(device)
                typ = self._get_type(device)
                if name and typ:
                    driver = load_driver(typ, name)
                    self._lo.update({device:driver})
                    if driver:
                        stream.put(sock, device, local=True)
                        driver.start(sock)
                    else:
                        log_err(self, 'failed to load %s' % typ)
                        sock.close()
            except:
                log_err(self, 'failed to listen')
                sock.close()
    
    def _init_srv(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((LO_ADDR, LO_PORT))
        self._sock.listen(5)
        self._listener = Thread(target=self._listen)
        self._listener.start()
    
    def setup(self):
        self._lo = {}
        self._init_srv()
        self._active = False
        self._loader = Loader(self.get_uid())
    
    def _get_device(self, name):
        mode = self._core.get_mode(name)
        if not (mode & MODE_LO):
            return
        prof = self._loader.get_profile(name)
        if not prof:
            return
        return device_name(prof['type'], name)
    
    def scan(self):
        device_list = []
        if self._active:
            return device_list
        self._active = True
        names = load(self._uid, sort=True)
        if not names:
            return device_list
        for name in names:
            device = self._get_device(name)
            if not device:
                continue
            if device not in self._lo:
                device_list.append(device)
        return device_list
    
    def connect(self, device):
        return (connect(device), True)
    
    def get_passive(self):
        if not self._lo:
            return
        keys = self._lo.keys()
        length = len(keys)
        i = randint(0, length - 1)
        for _ in range(length):
            device = self._lo[keys[i]]
            if device.get_mode() & MODE_PASSIVE:
                return device
            i += 1
            if i == length:
                i = 0
