#      usb.py
#      
#      Copyright (C) 2016 Yi-Wei Ci <ciyiwei@hotmail.com>
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

import time
import serial

RATE = 9600
WAIT_TIME = 2 # seconds
TIMEOUT = 0.5 # seconds

class USBSocket(object):
    def __init__(self, name, rate=RATE, timeout=TIMEOUT):
        self._serial = serial.Serial(name, rate, timeout=timeout)
        time.sleep(WAIT_TIME)
    
    def sendall(self, buf):
        if self._serial:
            self._serial.write(buf)
    
    def send(self, buf):
        if self._serial:
            self._serial.write(buf)
    
    def recv(self, length):
        if self._serial:
            return self._serial.read(length)
    
    def close(self):
        try:
            if self._serial:
                self._serial.close()
                self._serial = None
        except:
            pass
    
    def __del__(self):
        self.close()
