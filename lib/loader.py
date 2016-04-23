#      loader.py
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

import os
import json
from log import log_warnning
from util import unicode2str
from fields import FIELD_ATTR
from conf.virtdev import PATH_MNT
from attributes import ATTR_PROFILE

BUF_LEN = 1024

class Loader(object):
    def __init__(self, uid):
        self._uid = uid
    
    def _get_path(self, name, attr):
        return os.path.join(PATH_MNT, self._uid, FIELD_ATTR, name, attr)
    
    def _read(self, name, attr):
        path = self._get_path(name, attr)
        try:
            fd = os.open(path, os.O_RDONLY)
            try:
                return os.read(fd, BUF_LEN)
            finally:
                os.close(fd)
        except:
            log_warnning(self, 'failed to read, name=%s' % str(name))
    
    def get_attr(self, name, attr, typ):
        buf = self._read(name, attr)
        if buf:
            return typ(buf)
    
    def get_profile(self, name):
        buf = self._read(name, ATTR_PROFILE)
        if buf:
            attr = json.loads(buf)
            return unicode2str(attr)
