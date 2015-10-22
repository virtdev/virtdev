#      timerecorder.py
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
from dev.driver import Driver

PRINT = False
INTERVAL = 1
PATH_TIMER = '/opt/timer'
PATH_RECORDER = '/opt/timerecorder'

class TimeRecorder(Driver):
    def setup(self):
        if self.get_name():
            path = os.path.join(PATH_RECORDER, self.get_name())
            if not os.path.exists(path):
                os.makedirs(path, 0o755)
        self._cnt = {}
    
    def _get_timer_path(self, timer, name):
        return os.path.join(PATH_TIMER, timer, name)
    
    def _get_path(self, timer, name):
        return os.path.join(PATH_RECORDER, self.get_name(), name)
    
    def _save(self, timer, name):
        t_end = datetime.utcnow()
        path = self._get_timer_path(timer, name)
        if not os.path.exists(path):
            return
        d = shelve.open(path)
        try:
            t_start = datetime.strptime(d['t'], "%Y-%m-%d %H:%M:%S.%f")
        finally:
            d.close()
        t = (t_end - t_start).total_seconds()
        path = self._get_path(timer, name)
        d = shelve.open(path)
        try:
            d['t'] = t
        finally:
            d.close()
        if PRINT:
            print('TimeRecorder: name=%s, time=%f' % (name, t))
        return t
    
    def put(self, buf):
        args = self.get_args(buf)
        if args and type(args) == dict:
            name = args.get('name')
            timer = args.get('timer')
            if name and timer:
                if not self._cnt.has_key(name):
                    cnt = 0
                else:
                    cnt = self._cnt.get(name)
            
                if cnt < INTERVAL:
                    cnt += 1
                    self._cnt.update({name:cnt})
                    if cnt == INTERVAL: 
                        t = self._save(timer, name)
                        if t:
                            return {'name':name, 'time':t}
