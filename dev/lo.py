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

import os
import imp
import socket
from lib import stream
from fs.path import load
from lib.log import log_err
from threading import Thread
from fs.attr import get_attr
from vdev import VDEV_MODE_ANON
from interface import VDevInterface
from conf.virtdev import VDEV_LO_PORT

VDEV_LO_ADDR = '127.0.0.1'
VDEV_ANON_PATH = os.path.join(os.getcwd(), 'anon')

def get_device(typ, name):
    return '%s_%s' % (typ, name)

def load_anon(typ, name=None, sock=None):
    try:
        module = imp.load_source(typ, os.path.join(VDEV_ANON_PATH, '%s.py' % typ.lower()))
        if module and hasattr(module, typ):
            anon = getattr(module, typ)
            if anon:
                return anon(name, sock)
    except:
        pass

class VDevLo(VDevInterface):
    def _get_name(self, device):
        res = device.split('_')
        if len(res) == 2:
            return (res[0], res[1])
        else:
            return (None, None)
    
    def _listen(self):
        while True:
            sock = self._sock.accept()[0]
            try:
                buf = stream.get(sock, anon=True)
                typ, name = self._get_name(buf)
                if typ and name:
                    anon = load_anon(typ, name, sock)
                    if anon:
                        self._lo.update({str(anon):anon})
                    else:
                        log_err(self, 'failed to load anon device, type=%s' % typ)
            except:
                log_err(self, 'failed to listen')
                sock.close()
    
    def _init_sock(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((VDEV_LO_ADDR, VDEV_LO_PORT))
        self._sock.listen(5)
    
    def _init_listener(self):
        self._listener = Thread(target=self._listen)
        self._listener.start()
    
    def __init__(self, manager):
        VDevInterface.__init__(self, manager)
        self._lo = {}
        self._anon = True
        self._active = False
        self._init_sock()
        self._init_listener()
    
    def _get_device(self, name):
        mode = self.manager.synchronizer.get_mode(name)
        if not (mode & VDEV_MODE_ANON):
            return
        profile = get_attr(self.manager.uid, name, 'profile')
        if not profile:
            return
        lines = profile.split('\n')
        for l in lines:
            if l.startswith('type='):
                typ = l.strip()[len('type='):]
                return get_device(typ, name)
    
    def scan(self):
        device_list = []
        if self._active:
            return device_list
        self._active = True
        uid = self.manager.uid
        names = load(uid, sort=True)
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
        sock.connect((VDEV_LO_ADDR, VDEV_LO_PORT))
        stream.put(sock, device, anon=True)
        return sock
    