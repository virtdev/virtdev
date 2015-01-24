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
from aop import VDevAnonOper
from datetime import datetime

DEBUG_TIMER = False
PATH_TIMER = '/tmp'

class Timer(VDevAnonOper):
    def __init__(self, index=0):
        VDevAnonOper.__init__(self, index)
        path = self._get_path()
        if not os.path.exists(path):
            os.makedirs(path, 0o755)
    
    def _get_path(self):
        return os.path.join(PATH_TIMER, str(self))
    
    def _create(self, name):
        start = False
        path = os.path.join(self._get_path(), name)
        if not os.path.exists(path):
            start = True
        d = shelve.open(path)
        try:
            if start:
                d['start'] =  str(datetime.utcnow())
                return True
            else:
                t_end = datetime.utcnow()
                t_start = datetime.strptime(d['start'], "%Y-%m-%d %H:%M:%S.%f")
                t = (t_end - t_start).total_seconds()
                d['time'] = t 
                if DEBUG_TIMER:
                    print('Timer: name=%s, time=%f' % (name, t))
        finally:
            d.close()
    
    def put(self, buf):
        args = self._get_args(buf)
        if args and type(args) == dict:
            name = args.get('Name')
            if name:
                if self._create(name):
                    return args
    
if __name__ == '__main__':
    import md5
    import time
    timer = Timer()
    name = md5.new('test').hexdigest()
    args = str({'Name':name})
    timer.put(args)
    time.sleep(1)
    timer.put(args)
    