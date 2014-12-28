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
from fs.path import VDEV_FS_LABELS
from conf.virtdev import VDEV_FS_MOUNTPOINT
from fs.attr import VDEV_ATTR_MODE, VDEV_ATTR_HANDLER, VDEV_ATTR_MAPPER, VDEV_ATTR_DISPATCHER, VDEV_ATTR_FREQ

class VDevLoader(object):
    def __init__(self, uid):
        self._uid = uid
    
    def _get_path(self, name, attr):
        return os.path.join(VDEV_FS_MOUNTPOINT, self._uid, VDEV_FS_LABELS['attr'], name, attr)
    
    def _get(self, name, attr):
        path = self._get_path(name, attr)
        if not os.path.exists(path):
            return ''
        with open(path, 'r') as f:
            buf = f.read()
        return buf
    
    def get_mapper(self, name):
        return self._get(name, VDEV_ATTR_MAPPER)
    
    def get_handler(self, name):
        return self._get(name, VDEV_ATTR_HANDLER)
    
    def get_freq(self, name):
        return float(self._get(name, VDEV_ATTR_FREQ))
    
    def get_mode(self, name):
        return int(self._get(name, VDEV_ATTR_MODE))
    
    def get_dispatcher(self, name):
        return self._get(name, VDEV_ATTR_DISPATCHER)
    