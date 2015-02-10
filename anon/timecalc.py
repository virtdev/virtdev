#      timecalc.py
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
from threading import Lock
from dev.anon import VDevAnon

DEBUG_TIMECALC = False
TIMECALC_TOTAL = 1000
PATH_TIMECALC = '/opt/timecalc'

class TimeCalc(VDevAnon):
    def __init__(self, name=None, sock=None):
        VDevAnon.__init__(self, name, sock)
        if not os.path.exists(PATH_TIMECALC):
            os.makedirs(PATH_TIMECALC, 0o755)
        self._lock = Lock()
        self._time = 0
        self._cnt = 0
    
    def _get_path(self):
        return os.path.join(PATH_TIMECALC, self._name)
    
    def _save_time(self, time):
        path = self._get_path()
        d = shelve.open(path)
        try:
            d['time'] = time
        finally:
            d.close()
            
    def _calc(self, time):
        self._lock.acquire()
        try:
            if self._cnt < TIMECALC_TOTAL:
                self._time += time
                self._cnt += 1
                if self._cnt == TIMECALC_TOTAL:
                    t = self._time / TIMECALC_TOTAL
                    self._time = 0
                    self._cnt = 0
                    self._save_time(t)
                    if DEBUG_TIMECALC:
                        print('TimeCalc: time=%f' % t)
                    return t
        finally:
            self._lock.release()
    
    def put(self, buf):
        args = self.get_args(buf)
        if args and type(args) == dict:
            time = args.get('Time')
            if time:
                t = self._calc(time)
                if t:
                    return {'Time':t}
