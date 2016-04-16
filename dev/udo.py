#      udo.py
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
import ast
import req
import time
import driver
from lib import stream
from conf.log import LOG_UDO
from lib.loader import Loader
from datetime import datetime
from conf.virtdev import PATH_MNT
from lib.log import log_debug, log_err
from lib.util import lock, mount_device
from threading import Thread, Event, Lock
from lib.attributes import ATTR_MODE, ATTR_FREQ
from lib.operations import OP_GET, OP_PUT, OP_OPEN, OP_CLOSE
from lib.modes import MODE_VIRT, MODE_SWITCH, MODE_SYNC, MODE_TRIG, MODE_ACTIVE

RETRY_MAX = 2
SLEEP_TIME = 15 # seconds
OUTPUT_MAX = 1 << 26
POLL_INTERVAL = 0.01  # seconds

class UDO(object):
    def __init__(self, children={}, local=False):
        self._buf = {}
        self._event = {}
        self._lock = Lock()
        self._local = local
        self._children = children
        self._freq = driver.FREQ_MAX
        self._type = self._get_type()
        self._thread_listen = None
        self._thread_poll = None
        self._requester = None
        self._active = False
        self._atime = None
        self._index = None
        self._core = None
        self._sock = None
        self._name = None
        self._uid = None
        self._spec = {}
        self._mode = 0
    
    def _get_type(self):
        return self.__class__.__name__
    
    def _log(self, text):
        if LOG_UDO:
            log_debug(self, text)
    
    def _read(self):
        empty = (None, None)
        try:
            buf = stream.get(self._sock, self._local)
            if len(buf) > OUTPUT_MAX:
                return empty
            
            if not buf and self._local:
                return (self, '')
            
            output = ast.literal_eval(buf)
            if type(output) != dict:
                log_err(self, 'failed to read, cannot parse, name=%s' % self.d_name)
                return empty
            
            if len(output) != 1:
                log_err(self, 'failed to read, invalid length, name=%s' % self.d_name)
                return empty
            
            device = None
            index = output.keys()[0]
            output = output[index]
            if self._children:
                for i in self._children:
                    if self._children[i].d_index == int(index):
                        device = self._children[i]
                        break
            elif 0 == index:
                device = self
            if not device:
                log_err(self, 'failed to read, cannot get device, name=%s' % self.d_name)
                return empty
            buf = device.check_output(output)
            return (device, buf)
        except:
            return empty
    
    def _write(self, buf):
        if not buf:
            log_err(self, 'failed to write')
            return
        try:
            stream.put(self._sock, buf, self._local)
            return True
        except:
            pass
    
    def _initialize(self):
        if self._children:
            self._mode |= MODE_VIRT
        
        if self._mode & MODE_VIRT or self._index == None:
            mode = None
            freq = None
            prof = None
        else:
            mode = self._mode
            freq = self._freq
            prof = self.d_profile
            path = os.path.join(PATH_MNT, self._uid, self._name)
            if os.path.exists(path):
                loader = Loader(self._uid)
                curr_prof = loader.get_profile(self._name)
                if curr_prof['type'] == prof['type']:
                    curr_mode = loader.get_attr(self._name, ATTR_MODE, int)
                    if ((curr_mode | MODE_SYNC) == (mode | MODE_SYNC)) and curr_prof.get('spec') == prof.get('spec'):
                        mode = None
                        freq = None
                        prof = None
                    else:
                        if not (curr_mode & MODE_SYNC):
                            mode &= ~MODE_SYNC
                        else:
                            mode |= MODE_SYNC
                        freq = loader.get_attr(self._name, ATTR_FREQ, float)
        
        if not self._children:
            mount_device(self._uid, self.d_name, mode, freq, prof)
            self._log('mount->type=%s [%s*]' % (self.d_type, self.d_name[:8]))
    
    @property
    def d_name(self):
        return self._name
    
    @property
    def d_mode(self):
        if not self._core or self._children:
            return self._mode
        else:
            cnt =  RETRY_MAX
            while True:
                mode = self._core.get_mode(self.d_name)
                if mode != None:
                    self._mode = mode
                    return mode
                cnt -= 1
                if cnt > 0:
                    time.sleep(SLEEP_TIME)
                else:
                    break
            log_err(self, 'failed to get mode, name=%s' % self.d_name)
            return self._mode
    
    @property
    def d_freq(self):
        if not self._core or self._children:
            return self._freq
        else:
            cnt = RETRY_MAX
            while True:
                freq = self._core.get_freq(self.d_name)
                if freq != None:
                    self._freq = freq
                    return freq
                cnt -= 1
                if cnt > 0:
                    time.sleep(SLEEP_TIME)
                else:
                    break
            log_err(self, 'failed to get freq, name=%s' % self.d_name)
            return self._freq
    
    @property
    def d_index(self):
        return self._index
    
    @property
    def d_spec(self):
        return self._spec
    
    @property
    def d_type(self):
        return self._type
    
    @property
    def d_intv(self):
        freq = self.d_freq
        if freq > 0:
            return 1.0 / freq
        else:
            return float('inf')
    
    @property
    def d_profile(self):
        prof = {}
        prof.update({'type':self._type})
        prof.update({'spec':self._spec})
        prof.update({'index':self._index})
        return prof
    
    def set_type(self, val):
        self._type = val
    
    def set_freq(self, val):
        if val > driver.FREQ_MAX:
            self._freq = driver.FREQ_MAX
        elif val < driver.FREQ_MIN:
            self._freq = driver.FREQ_MIN
        else:
            self._freq = val
    
    def set_spec(self, val):
        self._spec = dict(val)
    
    def set_index(self, val):
        self._index = int(val)
    
    def set_mode(self, val):
        self._mode = int(val)
    
    def set_active(self):
        self._active = True
    
    def set_inactive(self):
        self._active = False
    
    def find(self, name):
        if self.d_name == name:
            return self
        else:
            return self._children.get(name)
    
    def check_atime(self):
        now = datetime.now()
        if self._atime:
            intv = (now - self._atime).total_seconds()
            if intv >= self.d_intv:
                self._atime = now
                return True
        else:
            self._atime = now
    
    def check_output(self, output):
        for i in output:
            item = self._spec.get(i)
            if not item:
                continue
            try:
                val = output[i]
                typ = item['type']
                if (type(val) == int or type(val) == float) and type(typ) == list and (val < typ[0] or val > typ[1]):
                    log_err(self, 'failed to check output')
                    return
            except:
                log_err(self, 'failed to check output')
                return
        return output
    
    def _add_event(self, name):
        self._buf[name] = None
        self._event[name] = Event()
    
    def _del_event(self, name):
        del self._event[name]
    
    def _set(self, device, buf):
        if not device:
            return
        name = device.d_name
        if self._event.has_key(name):
            event = self._event[name]
            if not event.is_set():
                self._buf[name] = buf
                event.set()
                return True
    
    def _wait(self, name):
        self._event[name].wait()
        buf = self._buf[name]
        del self._buf[name]
        del self._event[name]
        return buf
    
    def check_index(self):
        index = self.d_index
        if index != None:
            return index
        else:
            return 0
    
    @lock
    def _check_device(self, device):
        index = device.check_index()
        if device.can_get():
            if device.check_atime():
                self._write(req.req_get(index))
        
        if device.can_open():
            self._write(req.req_open(index))
            device.set_active()
        
        if device.can_close():
            self._write(req.req_close(index))
            device.set_inactive()
    
    def _poll(self):
        try:
            while True:
                time.sleep(POLL_INTERVAL)
                if self._children:
                    for i in self._children:
                        child = self._children[i]
                        if child.can_poll():
                            self._check_device(child)
                elif self.can_poll():
                    self._check_device(self)
        except:
            log_err(self, 'failed to poll')
    
    def _listen(self):
        try:
            while True:
                device, buf = self._read()
                if device and not self._set(device, buf) and buf:
                    mode = device.d_mode
                    if not (mode & MODE_TRIG) or device.check_atime():
                        res = None
                        name = device.d_name
                        if mode & MODE_SYNC:
                            self._core.sync(name, buf)
                        if self._core.has_handler(name):
                            res = self._core.handle(name, {name:buf})
                        else:
                            res = buf
                        if res:
                            self._core.dispatch(name, res)
        except:
            log_err(self, 'failed to listen, device=%s' % self.d_name)
            self._sock.close()
    
    def can_get(self):
        return driver.has_freq(self.d_mode)
    
    def can_open(self):
        return not self._active and self.d_mode & MODE_ACTIVE
    
    def can_close(self):
        return self._active and not(self.d_mode & MODE_ACTIVE)
    
    def can_poll(self):
        return self.can_get() or self.can_open() or self.can_close()
    
    def _start(self):
        if self._sock:
            self._thread_poll = Thread(target=self._poll)
            self._thread_poll.start()
            self._thread_listen = Thread(target=self._listen)
            self._thread_listen.start()
    
    def mount(self, uid, name, core, sock=None, init=True):
        self._uid = uid
        self._name = name
        self._core = core
        self._sock = sock
        if init:
            self._initialize()
        self._start()
    
    @lock
    def proc(self, name, op, buf=None):
        if not self._sock:
            return
        
        dev = self.find(name)
        if not dev:
            log_err(self, 'failed to process, cannot find device')
            return
        
        index = dev.d_index
        if op == OP_OPEN:
            if dev.d_mode & MODE_SWITCH:
                self._write(req.req_open(index))
        elif op == OP_CLOSE:
            if dev.d_mode & MODE_SWITCH:
                self._write(req.req_close(index))
        elif op == OP_GET:
            self._add_event(name)
            if self._write(req.req_get(index)):
                return self._wait(name)
            else:
                self._del_event(name)
        elif op == OP_PUT:
            self._add_event(name)
            if self._write(req.req_put(index, str(buf[buf.keys()[0]]))):
                return self._wait(name)
            else:
                self._del_event(name) 
        else:
            log_err(self, 'failed to process, invalid operation')
