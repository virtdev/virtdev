#      fileloader.py
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
from lib import mode
from dev.driver import Driver
from base64 import encodestring

PATH_FL = '/opt/fileloader'

class FileLoader(Driver):
    def __init__(self, name=None):
        Driver.__init__(self, name=name, mode=mode.OVP | mode.MODE_SWITCH, freq=1)
    
    def setup(self):
        if self.get_name():
            path = self._get_path()
            if not os.path.exists(path):
                os.makedirs(path, 0o755)
        self._files = None
        self._active = False
    
    def _get_path(self):
        return os.path.join(PATH_FL, self.get_name())
    
    def _load(self):
        path = self._get_path()
        for name in os.listdir(path):
            file_path = os.path.join(path, name)
            with open(file_path) as f:
                buf = f.read()
            if buf:
                yield {'Name':name, 'File':encodestring(buf)}
    
    def get(self):
        if not self._active:
            return
        try:
            return self._files.next()
        except StopIteration:
            self._active = False
    
    def open(self):
        self._files = self._load()
        self._active = True
