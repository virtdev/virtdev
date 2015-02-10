#      timer.py
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
import shelve
from datetime import datetime
from dev.anon import VDevAnon

DEBUG_TIMER = False
PATH_TIMER = '/opt/timer'

def get_path(name):
    return os.path.join(PATH_TIMER, name) 

class Timer(VDevAnon):
    def __init__(self, name=None, sock=None):
        VDevAnon.__init__(self, name, sock)
        if name:
            path = get_path(name)
            if not os.path.exists(path):
                os.makedirs(path, 0o755)
    
    def _create(self, name):
        path = os.path.join(get_path(self._name), name)
        d = shelve.open(path)
        try:
            d['start'] =  str(datetime.utcnow())
            if DEBUG_TIMER:
                print('Timer: name=%s, time=%s' % (name, d['start']))
            return True
        finally:
            d.close()
    
    def put(self, buf):
        args = self.get_args(buf)
        if args and type(args) == dict:
            name = args.get('Name')
            if name:
                if self._create(name):
                    args.update({'Timer':self._name})
                    return args
    
