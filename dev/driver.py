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
from threading import Thread
from dev.req import REQ_OPEN, REQ_CLOSE, REQ_GET, REQ_PUT, REQ_MOUNT, parse

class Driver(object):
    def __init__(self, name=None, mode=mode.IV, freq=None, spec=None):
        self.__name = name
        self.__mode = mode
        self.__freq = freq
        self.__spec = spec
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
        profile = {'type':self.get_type()}
        spec = self.get_spec()
        if spec:
            profile.update({'spec':spec})
        return profile
    
    def get_freq(self):
        return self.__freq
    
    def get_spec(self):
        return self.__spec
    
    def get_mode(self):
        return self.__mode | mode.MODE_LO
    
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
    
    def get_info(self):
        info = {'type':self.get_type(), 'mode':self.get_mode()}
        freq = self.get_freq()
        if freq:
            info.update({'freq':freq})
        spec = self.get_spec()
        if spec:
            info.update({'spec':spec})
        return str({'None':info})
    
    def start(self, sock):
        if sock:
            self.__sock = sock
            self.__thread = Thread(target=self.__proc)
            self.__thread.start()
    
    def __send(self, buf, pack=True):
        if pack:
            stream.put(self.__sock, {self.__index:buf}, local=True)
        else:
            stream.put(self.__sock, buf, local=True)
    
    def __recv(self):
        req = stream.get(self.__sock, local=True)
        self.__index, cmd, buf = parse(req)
        return (cmd, buf)
    
    def __release(self):
        if self.__sock:
            self.__sock.close()
    
    def __proc(self):
        try:
            while True:
                ret = ''
                cmd, buf = self.__recv()
                if cmd & REQ_OPEN:
                    ret = self.open()
                    if ret:
                        self.__send(ret)
                elif cmd & REQ_CLOSE:
                    ret = self.close()
                    if ret:
                        self.__send(ret)
                elif cmd & REQ_GET:
                    try:
                        res = self.get()
                        if res:
                            ret = res
                    finally:
                        self.__send(ret)
                elif cmd & REQ_PUT:
                    try:
                        res = self.put(buf)
                        if res:
                            ret = res
                    finally:
                        self.__send(ret)
                elif cmd & REQ_MOUNT:
                    self.__send(self.get_info(), pack=False)
        finally:
            self.__release()
