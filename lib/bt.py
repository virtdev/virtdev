#      bt.py
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
import bluetooth
from conf.defaults import BT_PORT

TIMEOUT = 0.5
WAIT_TIME = 0.5 # seconds

class BluetoothSocket(object):
    def __init__(self, name):
        self._socket = bluetooth.BluetoothSocket(bluetooth.RFCOMM)
        self._socket.connect((name, BT_PORT))
        self._socket.settimeout(TIMEOUT)
    
    def sendall(self, buf):
        if self._socket:
            self._socket.sendall(buf)
    
    def send(self, buf):
        if self._socket:
            self._socket.send(buf)
    
    def recv(self, length):
        if self._socket:
            return self._socket.recv(length)
    
    def close(self):
        try:
            if self._socket:
                self._socket.close()
                self._socket = None
                time.sleep(WAIT_TIME)
        except:
            pass
    
    def __del__(self):
        self.close()
