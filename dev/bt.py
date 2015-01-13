#      bt.py
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
import bluetooth
from interface import VDevInterface
from conf.virtdev import VDEV_LIB_PATH

VDEV_BT_PORT = 1
VDEV_BT_PIN = '1234'
VDEV_BT_DEVICE_MAX = 32

class VDevBT(VDevInterface):
    def _connect(self, name):
        sock = bluetooth.BluetoothSocket(bluetooth.RFCOMM)
        sock.connect((name, VDEV_BT_PORT))
        return sock
    
    def _prepare(self, name):
        os.system('bluez-test-device remove %s' % name)
        os.system('echo %s | bluez-simple-agent hci0 %s' % (VDEV_BT_PIN, name))
    
    def connect(self, name):
        self._prepare(name)
        return self._connect(name)
    
    def _check_device_list(self):
        cnt = 0
        device_list = []
        path = os.path.join(VDEV_LIB_PATH, 'devices')
        if os.path.exists(path):
            with open(path, 'r') as f:
                while True:
                    buf = f.readline().strip()
                    if buf:
                        device_list.append(buf)
                        cnt += 1
                        if cnt == VDEV_BT_DEVICE_MAX:
                            break
                    else:
                        break
        return device_list
    
    def scan(self):
        device_list = self._check_device_list()
        if not device_list:
            return
        res = []
        devices = bluetooth.discover_devices()
        if devices:
            for i in devices:
                if i in device_list:
                    res.append(i)
        return res
    