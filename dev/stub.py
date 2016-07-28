#      stub.py
#      
#      Copyright (C) 2016 Yi-Wei Ci <ciyiwei@hotmail.com>
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

from lib import io
from conf.log import LOG_STUB
from conf.defaults import DEBUG
from lib.log import log_debug, log_err
from lib.cmd import CMD_OPEN, CMD_CLOSE, CMD_GET, CMD_PUT, CMD_MOUNT, parse

class Stub(object):
    def __init__(self, sock, driver):
        self._driver = driver
        self._active = False
        self._socket = sock
        self._index = None
        if LOG_STUB:
            self._log_cnt = 0
    
    def _log(self, cmd):
        if LOG_STUB:
            if cmd == CMD_OPEN:
                text = 'open'
            elif cmd == CMD_CLOSE:
                text = 'close'
            elif cmd == CMD_GET:
                text = 'get'
            elif cmd == CMD_PUT:
                text = 'put'
            else:
                text = 'unknown'
            text += ', driver=%s, name=%s [%d]' % (str(self._driver), self._driver.get_name(), self._log_cnt)
            log_debug(self, text)
            self._log_cnt += 1
    
    def _get(self):
        req = io.get(self._socket, local=True)
        self._index, cmd, buf = parse(req)
        self._driver.set_index(self._index)
        return (cmd, buf)
    
    def _put(self, buf, index=True):
        if index:
            io.put(self._socket, {self._index:buf}, local=True)
        else:
            io.put(self._socket, buf, local=True)
    
    def _create(self, cmd, buf):
        try:
            if cmd == CMD_MOUNT:
                result = self._driver.get_info()
                self._put(result, index=False)
                self._active = True
        except:
            log_err(self, 'failed to create, driver=%s, cmd=%s' % (str(self._driver), str(cmd)))
    
    def _proc(self, cmd, buf):
        result = ''
        force = False
        if cmd == CMD_OPEN:
            result = self._driver.open()
        elif cmd == CMD_CLOSE:
            result = self._driver.close()
        elif cmd == CMD_GET:
            force = True
            ret = self._driver.get()
            if ret:
                result = ret
        elif cmd == CMD_PUT:
            force = True
            ret = self._driver.put(buf)
            if ret:
                result = ret
        else:
            log_err(self, 'failed to process, invalid command, driver=%s, cmd=%s' % (str(self._driver), str(cmd)))
        
        if result or force:
            if result:
                self._log(cmd=cmd)
            self._put(result)
    
    def _proc_safe(self, cmd, buf):
        try:
            self._proc(cmd, buf)
        except:
            log_err(self, 'failed to process, driver=%s, cmd=%s' % (str(self._driver), str(cmd)))
    
    def start(self):
        try:
            while True:            
                cmd, buf = self._get()
                if self._active:
                    if DEBUG:
                        self._proc(cmd, buf)
                    else:
                        self._proc_safe(cmd, buf)
                else:
                    self._create(cmd, buf)
        finally:
            io.close(self._socket)
