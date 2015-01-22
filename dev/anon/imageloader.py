#      imageloader.py
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
from aop import VDevAnonOper
from base64 import encodestring

PATH_IL = '/opt/images'

class Imageloader(VDevAnonOper):
    def __init__(self, index):
        VDevAnonOper.__init__(self, index)
        self._start = False
    
    def get(self):
        if not self._start:
            return
        for name in os.listdir(PATH_IL):
            path = os.path.join(PATH_IL, name)
            with open(path) as f:
                buf = f.read()
            if buf:
                yield {'Image':encodestring(buf)}
        self._start = False
    
    def open(self):
        self._start = True
    