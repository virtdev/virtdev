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

class ImageLoader(VDevAnonOper):
    def __init__(self, index=0):
        VDevAnonOper.__init__(self, index)
        self._images = None
        self._active = False
    
    def _load(self):
        for name in os.listdir(PATH_IL):
            path = os.path.join(PATH_IL, name)
            with open(path) as f:
                buf = f.read()
            if buf:
                yield {'Name':name, 'Image':encodestring(buf)}
        
    def get(self):
        if not self._active:
            return
        try:
            return self._images.next()
        except StopIteration:
            self._active = False
    
    def open(self):
        self._images = self._load()
        self._active = True

if __name__ == '__main__':
    loader = ImageLoader()
    loader.open()
    while True:
        res = loader.get()
        if not res:
            break
        print('ImageLoader: name=%s' % res['Name'])
