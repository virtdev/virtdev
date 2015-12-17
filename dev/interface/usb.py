#      usb.py
#      
#      Copyright (C) 2015 Yi-Wei Ci <ciyiwei@hotmail.com>
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
from dev.udi import UDI
from conf.virtdev import LO
from lib.usb import USBSocket

DEV = '/dev'

class USBSerial(UDI):
    def setup(self):
        self._usb = {}
    
    def scan(self):
        devices = filter(lambda x:x.startswith('ttyACM'), os.listdir(DEV))
        if devices:
            names = map(lambda x: os.path.join(DEV, x), devices)
            return filter(lambda x: x not in self._usb, names)
    
    def connect(self, device):
        ret = None
        if LO: 
            sock = lo.connect(lo.device_name('Controller', device))
            if sock:
                ret = (sock, True)
        if not ret:
            ret = (USBSocket(device), False)
        self._usb.update({device:None})
        return ret
