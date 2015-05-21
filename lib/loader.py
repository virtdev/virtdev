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
import ast
from fs.path import DOMAIN
from conf.virtdev import MOUNTPOINT
from lib.log import log_get, log_err
from fs.attr import ATTR_MODE, ATTR_HANDLER, ATTR_FILTER, ATTR_DISPATCHER, ATTR_FREQ, ATTR_PROFILE

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
    
    def get_filter(self, name):
        return self._read(name, ATTR_FILTER)
    
    def get_handler(self, name):
        return self._read(name, ATTR_HANDLER)
    
    def get_dispatcher(self, name):
        return self._read(name, ATTR_DISPATCHER)
    
    def get_freq(self, name):
        return float(self._read(name, ATTR_FREQ))
    
    def get_mode(self, name):
        return int(self._read(name, ATTR_MODE))
    
    def get_profile(self, name):
        prof = {}
        lines = self._readlines(name, ATTR_PROFILE)
        if not lines:
            return prof
        for l in lines:
            pair = l.strip().split('=')
            if len(pair) != 2:
                log_err(self, 'invalid profile, profile=%s' % ''.join(lines))
                raise Exception(log_get(self, 'invalid profile'))
            if pair[0] == 'type':
                prof.update({'type':str(pair[1])})
            elif pair[0] == 'range':
                r = ast.literal_eval(pair[1])
                if type(r) != dict:
                    log_err(self, 'invalid range')
                    raise Exception(log_get(self, 'invalid range'))
                prof.update({'range':r})
            elif pair[0] == 'index':
                if pair[1] == 'None':
                    prof.update({'index':None})
                else:
                    prof.update({'index':int(pair[1])})
        if not prof.has_key('type'):
            log_err(self, 'invalid profile, profile=%s' % ''.join(lines))
            raise Exception(log_get(self, 'invalid profile'))
        return prof
