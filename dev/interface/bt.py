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
import lo
import bluetooth
from dev.udi import UDI
from lib.bt import BluetoothSocket
from conf.virtdev import LO, PATH_LIB

PIN = '1234'
DEVICE_MAX = 32

class Bluetooth(UDI):    
    def _get_devices(self):
        cnt = 0
        device_list = []
        path = os.path.join(PATH_LIB, 'devices')
        if os.path.exists(path):
            with open(path, 'r') as f:
                while True:
                    buf = f.readline().strip()
                    if buf:
                        device_list.append(buf)
                        cnt += 1
                        if cnt == DEVICE_MAX:
                            break
                    else:
                        break
        return device_list
    
    def scan(self):
        device_list = []
        devices = self._get_devices()
        if not devices:
            return device_list
        bt_devices = bluetooth.discover_devices()
        if bt_devices:
            for i in bt_devices:
                if i in devices:
                    device_list.append(i)
        return device_list
    
    def _init(self, device):
        os.system('bluez-test-device remove %s' % device)
        os.system('echo %s | bluez-simple-agent hci0 %s' % (PIN, device))
    
    def connect(self, device):
        self._init(device)
        if LO: 
            sock = lo.connect(lo.device_name('Controller', device))
            if sock:
                return (sock, True)
        return (BluetoothSocket(device), False)
