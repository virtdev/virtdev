#      loader.py
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
from fs.path import DOMAIN
from util import check_profile
from fs.attr import ATTR_PROFILE
from conf.virtdev import MOUNTPOINT

class Loader(object):
    def __init__(self, uid):
        self._uid = uid
    
    def _get_path(self, name, attr):
        return os.path.join(MOUNTPOINT, self._uid, DOMAIN['attr'], name, attr)
    
    def _read(self, name, attr):
        path = self._get_path(name, attr)
        if not os.path.exists(path):
            return ''
        with open(path, 'r') as f:
            buf = f.read()
        return buf
    
    def _readlines(self, name, attr):
        path = self._get_path(name, attr)
        if not os.path.exists(path):
            return
        with open(path, 'r') as f:
            lines = f.readlines()
        return lines
    
    def get_attr(self, name, attr, typ):
        buf = self._read(name, attr)
        if buf:
            return typ(buf)
    
    def get_profile(self, name):
        buf = self._readlines(name, ATTR_PROFILE)
        if buf:
            return check_profile(buf)
