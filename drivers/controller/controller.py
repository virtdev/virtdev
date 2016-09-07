# controller.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import ast
from threading import Lock
from lib.usb import USBSocket
from dev.driver import Driver
from lib.pyboard import Pyboard
from lib.bt import BluetoothSocket
from lib.util import lock, device_info
from lib.modes import MODE_TRIG, MODE_POLL, MODE_CTRL

class Controller(Driver):
    def __init__(self, name=None):
        Driver.__init__(self, name=name, mode=MODE_TRIG | MODE_POLL | MODE_CTRL)
    
    def _parse(self, buf):
        info = device_info(buf)
        if not info:
            return
        for i in info:
            if not info[i].has_key('mode'):
                return 
            info[i]['mode'] |= MODE_CTRL
        return str(info)
    
    def setup(self):
        self._lock = Lock()
        self._info = None
        name = self.get_name()
        if name:
            sock = None
            try:
                if name.startswith('/dev/tty'):
                    sock = USBSocket(name)
                else:
                    sock = BluetoothSocket(name)
                self._pyb = Pyboard(sock)
                buf = self._pyb.enter('mount()')
                self._info = self._parse(buf)
            except:
                pass
            
            if not self._info:
                if sock:
                    sock.close()
                raise Exception('Error: failed to setup controller')
    
    def _exec(self, cmd):
        output = self._pyb.enter(cmd)
        if output:
            ret = ast.literal_eval(str(output).strip())
            if type(ret) == dict:
                return ret
    
    def _mangle(self, code):
        return str(code).replace('def ', 'def __')
    
    def _set_func(self, code):
        self._pyb.enter(self._mangle(code))
    
    def _set_args(self, args):
        self._pyb.enter('__args = ' + str(args))
    
    def _set_kwargs(self, kwargs):
        self._pyb.enter('__kwargs = ' + str(kwargs))
    
    @lock
    def _proc(self, op, args, kwargs):
        self._set_args(args)
        self._set_kwargs(kwargs)
        return self._exec("process(%d, '%s')" % (self.get_index(), op))
    
    def get(self):
        return self._proc('get')
    
    def put(self, *args, **kwargs):
        return self._proc('put', args, kwargs)
    
    def open(self):
        self._proc('open')
    
    def close(self):
        self._proc('close')
    
    @lock
    def execute(self, code, **args):
        self._set_func(code)
        self._set_kwargs(args)
        return self._exec("execute()")
    
    def get_info(self):
        return self._info
