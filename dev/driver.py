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
from threading import Thread
from lib.modes import MODE_IV, MODE_LO, MODE_POLL, MODE_TRIG, MODE_PASSIVE
from dev.req import REQ_OPEN, REQ_CLOSE, REQ_GET, REQ_PUT, REQ_MOUNT, parse

FREQ_MIN = 1 # HZ
FREQ_MAX = 100 # HZ

def _get_arguments(buf):
    try:
        args = ast.literal_eval(buf)
        if type(args) != dict:
            return
        return args
    except:
        pass

def has_freq(mode):
    return mode & MODE_POLL or (mode & MODE_TRIG and mode & MODE_PASSIVE)

def check_input(func):
    def _check_input(*args, **kwargs):
        self = args[0]
        buf = args[1]
        arguments = _get_arguments(buf)
        if arguments:
            return func(self, arguments)
    return _check_input

def check_output(func):
    def _check_output(*args, **kwargs):
        self = args[0]
        buf = args[1]
        arguments = _get_arguments(buf)
        if arguments:
            ret = func(self, arguments)
            if ret and type(ret) == dict:
                name = arguments.get('name')
                if name:
                    ret.update({'name':name})
                timer = arguments.get('timer')
                if timer:
                    ret.update({'timer':timer})
                return ret
    return _check_output

class Driver(object):
    def __init__(self, name=None, mode=MODE_IV, freq=None, spec=None):
        self.__name = name
        self.__mode = mode
        self.__spec = spec
        self.__sock = None
        self.__index = None
        self.__thread = None
        self._init_freq(freq)
    
    def _init_freq(self, freq):
        self.__freq = freq
        if not freq:
            if has_freq(self.__mode):
                self.__freq = FREQ_MIN
        else:
            if freq > FREQ_MAX:
                self.__freq = FREQ_MAX
            if freq < FREQ_MIN:
                self.__freq = FREQ_MIN
    
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
        return self.__mode | MODE_LO
    
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
