#      controller.py
#      
#      Copyright (C) 2015 Yi-Wei Ci <ciyiwei@hotmail.com>
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
from threading import Lock
from lib.log import log_get
from lib.usb import USBSocket
from dev.driver import Driver
from lib.pyboard import Pyboard
from lib.bt import BluetoothSocket
from lib.util import lock, check_info

class Controller(Driver):
    def _check_info(self, buf):
        info = check_info(buf)
        if not info:
            return
        for i in info:
            if not info[i].has_key('mode'):
                return 
            info[i]['mode'] |= mode.MODE_PASSIVE
        return str(info)
    
    def setup(self):
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
                self._pyb.enter('setup()')
                buf = self._pyb.enter('mount()')
                self._info = self._check_info(buf)
            except:
                pass
            
            if not self._info:
                if sock:
                    sock.close()
                raise Exception(log_get(self, 'no info'))
        self.set(mode=mode.MODE_TRIG | mode.MODE_POLL | mode.MODE_PASSIVE)
        self._lock = Lock()
    
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
    
    @lock
    def _proc(self, op, args=None):
        self._set_args(args)
        return self._exec("process(%d, '%s')" % (self.get_index(), op))
    
    def get(self):
        return self._proc('get')
    
    def put(self, args):
        return self._proc('put', args)
    
    def open(self):
        self._proc('open')
    
    def close(self):
        self._proc('close')
    
    @lock
    def execute(self, code, args):
        self._set_func(code)
        self._set_args(args)
        return self._exec("execute()")
    
    def get_info(self):
        return self._info
