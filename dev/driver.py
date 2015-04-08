#      driver.py
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

import ast
from lib import stream
from lib.log import log_err
from lib.mode import MODE_LO
from threading import Thread
from dev.req import REQ_OPEN, REQ_CLOSE, REQ_GET, REQ_PUT, REQ_MOUNT, parse

class VDevDriver(object):
    def __init__(self, name=None, sock=None):
        if not name:
            name = str(self)
        self._thread = None
        self._name = name
        self._sock = sock
    
    def __str__(self):
        return self.__class__.__name__
    
    def _get_info(self):
        ret = {}
        try:
            mode = self.mode
            if mode:
                ret.update({'mode':mode})
            freq = self.freq
            if freq:
                ret.update({'freq':freq})
            prof = self.profile
            if prof:
                ret.update(prof)
            return ret
        except:
            log_err(self, 'invalid device, type=%s' % str(self))
    
    @property
    def profile(self):
        info = self.info()
        if not info:
            return
        prof = {'type':str(self)}
        if info.has_key('range'):
            prof.update({'range':dict(info['range'])})
        return prof
    
    @property
    def freq(self):
        info = self.info()
        if not info or not info.has_key('freq'):
            return
        return float(info['freq'])
    
    @property
    def mode(self):
        info = self.info()
        if not info or not info.has_key('mode'):
            return
        return int(info['mode']) | MODE_LO
    
    def info(self):
        pass
    
    def open(self):
        pass
    
    def close(self):
        pass
    
    def put(self, buf):
        pass
    
    def get(self):
        pass
    
    def get_args(self, buf):
        try:
            args = ast.literal_eval(buf)
            if type(args) != dict:
                return
            return args
        except:
            pass
    
    def start(self):
        self._thread = Thread(target=self._run)
        self._thread.start()
    
    def _reply(self, buf):
        stream.put(self._sock, str({'None':buf}), local=True)
    
    def _run(self):
        if not self._sock:
            return
        while True:
            try:
                ret = ''
                req = stream.get(self._sock, local=True)
                _, flags, buf = parse(req)
                if flags & REQ_OPEN:
                    ret = self.open()
                    if ret:
                        self._reply(ret)
                elif flags & REQ_CLOSE:
                    ret = self.close()
                    if ret:
                        self._reply(ret)
                elif flags & REQ_GET:
                    try:
                        res = self.get()
                        if res:
                            ret = res
                    finally:
                        self._reply(ret)
                elif flags & REQ_PUT:
                    try:
                        res = self.put(buf)
                        if res:
                            ret = res
                    finally:
                        self._reply(ret)
                elif flags & REQ_MOUNT:
                    self._reply(self._get_info())
            except:
                log_err(self, 'failed to process')
                return
    