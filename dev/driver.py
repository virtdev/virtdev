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
from lib import mode
from lib import stream
from lib.util import info
from threading import Thread
from dev.req import REQ_OPEN, REQ_CLOSE, REQ_GET, REQ_PUT, REQ_MOUNT, parse

class Driver(object):
    def __init__(self, name=None, mode=mode.IV, rng=None, freq=None):
        self.__name = name
        self.__mode = mode
        self.__freq = freq
        self.__range = rng
        self.__sock = None
        self.__index = None
        self.__thread = None
    
    def __str__(self):
        return self.__class__.__name__
    
    def setup(self):
        pass
    
    def open(self):
        pass
    
    def close(self):
        pass
    
    def put(self, buf):
        pass
    
    def get(self):
        pass
    
    def evaluate(self):
        pass
    
    def get_type(self):
        return str(self)
    
    def get_profile(self):
        typ = self.get_type()
        rng = self.get_range()
        prof = {'type':typ}
        if rng:
            prof.update({'range':rng})
        return prof
    
    def get_freq(self):
        return self.__freq
    
    def get_range(self):
        return self.__range
    
    def get_mode(self):
        return self.__mode | mode.MODE_LO
    
    def get_info(self):
        return {'None':info(self.get_type(), self.get_mode(), self.get_freq(), self.get_range())}
    
    def get_args(self, buf):
        try:
            args = ast.literal_eval(buf)
            if type(args) not in [dict, list]:
                return
            return args
        except:
            pass
    
    def get_index(self):
        return self.__index
    
    def get_name(self):
        return self.__name
    
    def start(self, sock):
        self.__sock = sock
        self.__thread = Thread(target=self.__proc)
        self.__thread.start()
    
    def __reply(self, buf):
        stream.put(self.__sock, str({self.__index:buf}), local=True)
    
    def __proc(self):
        if not self.__sock:
            return
        try:
            while True:
                ret = ''
                req = stream.get(self.__sock, local=True)
                self.__index, flags, buf = parse(req)
                if flags & REQ_OPEN:
                    ret = self.open()
                    if ret:
                        self.__reply(ret)
                elif flags & REQ_CLOSE:
                    ret = self.close()
                    if ret:
                        self.__reply(ret)
                elif flags & REQ_GET:
                    try:
                        res = self.get()
                        if res:
                            ret = res
                    finally:
                        self.__reply(ret)
                elif flags & REQ_PUT:
                    try:
                        res = self.put(buf)
                        if res:
                            ret = res
                    finally:
                        self.__reply(ret)
                elif flags & REQ_MOUNT:
                    stream.put(self.__sock, str(self.get_info()), local=True)
        finally:
            self.__sock.close()
