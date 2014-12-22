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
from lib.log import log_err
from threading import Thread
from anon.camera import Camera
from interface import VDevInterface
from drivers.CAM import VDEV_CAM_LIST
from drivers.REC import VDEV_REC_LIST
from conf.virtdev import VDEV_LO_PORT
from anon.recognizer import Recognizer
from anon.anon import VDevAnon, anon_identity

VDEV_LO_ADDR = '127.0.0.1'

class VDevLo(VDevInterface):
    def _listen(self):
        while True:
            sock = None
            try:
                sock = self._sock.accept()[0]
                if not sock:
                    continue
                device = None
                item = stream.get(sock, anon=True)
                dev, identity = anon_identity(item)
                try:
                    if dev == 'CAM':
                        device = VDevAnon(Camera(identity), sock)
                    if dev == 'REC':
                        device = VDevAnon(Recognizer(identity), sock)
                except:
                    log_err(self, 'failed to listen, invalid device')
                    continue
                if device:
                    self._orig.update({str(device):device})
            except:
                if sock:
                    log_err(self, 'failed to listen')
                    sock.close()
    
    def _init_sock(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((VDEV_LO_ADDR, VDEV_LO_PORT))
        self._sock.listen(5)
    
    def __init__(self, manager):
        VDevInterface.__init__(self, manager)
        self._init_sock()
        self._listener = Thread(target=self._listen)
        self._listener.start()
        self._anon = True
    
    def _list_devices(self):
        return VDEV_CAM_LIST + VDEV_REC_LIST
    
    def scan(self):
        devices = self._list_devices()
        if not devices:
            return
        dev_list = []
        for item in devices:
            if not self._orig.has_key(item):
                dev_list.append(item)
        return dev_list
    
    def connect(self, name):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((VDEV_LO_ADDR, VDEV_LO_PORT))
        stream.put(sock, name, anon=True)
        return sock
    