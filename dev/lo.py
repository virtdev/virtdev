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
from udi import VDevUDI
from fs.path import load
from lib.log import log_err
from threading import Thread
from lib.mode import MODE_LO
from lib.util import load_driver
from proc.loader import VDevLoader
from conf.virtdev import LO_ADDR, LO_PORT

def get_device(typ, name):
    return '%s_%s' % (typ, name)

class VDevLo(VDevUDI):
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
                buf = stream.get(sock, local=True)
                name = self.get_name(buf)
                typ = self._get_type(buf)
                if name and typ:
                    driver = load_driver(typ, name, sock)
                    if driver:
                        self._lo.update({str(driver):driver})
                        driver.start()
                    else:
                        log_err(self, 'failed to load device, type=%s' % typ)
            except:
                log_err(self, 'failed to listen')
                sock.close()
    
    def _init_sock(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((LO_ADDR, LO_PORT))
        self._sock.listen(5)
    
    def _init_listener(self):
        self._listener = Thread(target=self._listen)
        self._listener.start()
    
    def __init__(self, uid, core):
        VDevUDI.__init__(self, uid, core)
        self._lo = {}
        self._init_sock()
        self._local = True
        self._active = False
        self._init_listener()
        self._loader = VDevLoader(uid)
    
    def _get_device(self, name):
        mode = self._core.get_mode(name)
        if not (mode & MODE_LO):
            return
        prof = self._loader.get_profile(name)
        if not prof:
            return
        return get_device(prof['type'], name)
    
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
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((LO_ADDR, LO_PORT))
        stream.put(sock, device, local=True)
        return sock
    