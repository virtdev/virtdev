#      anon.py
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
from threading import Thread
from dev.req import VDEV_REQ_OPEN, VDEV_REQ_CLOSE, VDEV_REQ_GET, VDEV_REQ_PUT, parse

class VDevAnon(Thread):
    def __init__(self, name=None, sock=None):
        if not name:
            name = str(self)
        self._name = name
        self._sock = sock
        Thread.__init__(self)
        self.start()
    
    def __str__(self):
        return self.__class__.__name__
    
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
    
    def run(self):
        if not self._sock:
            return
        while True:
            try:
                ret = ''
                req = stream.get(self._sock, anon=True)
                _, flags, buf = parse(req)
                if flags & VDEV_REQ_OPEN:
                    ret = self.open()
                    if ret:
                        stream.put(self._sock, str(ret), anon=True)
                elif flags & VDEV_REQ_CLOSE:
                    ret = self.close()
                    if ret:
                        stream.put(self._sock, str(ret), anon=True)
                elif flags & VDEV_REQ_GET:
                    try:
                        res = self.get()
                        if res:
                            ret = res
                    finally:
                        stream.put(self._sock, str(ret), anon=True)
                elif flags & VDEV_REQ_PUT:
                    try:
                        res = self.put(buf)
                        if res:
                            ret = res
                    finally:
                        stream.put(self._sock, str(ret), anon=True)
            except:
                log_err(self, 'failed to process')
