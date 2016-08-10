# timerecorder.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import shelve
from datetime import datetime
from dev.driver import Driver, check_input

PRINT = False
INTERVAL = 1
HOME_TIMER = '/opt/timer'
HOME_RECORDER = '/opt/timerecorder'

class TimeRecorder(Driver):
    def setup(self):
        if self.get_name():
            path = os.path.join(HOME_RECORDER, self.get_name())
            if not os.path.exists(path):
                os.makedirs(path, 0o755)
        self._cnt = {}
    
    def _get_timer_path(self, timer, name):
        return os.path.join(HOME_TIMER, timer, name)
    
    def _get_path(self, timer, name):
        return os.path.join(HOME_RECORDER, self.get_name(), name)
    
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
    
    @check_input
    def put(self, args):
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
