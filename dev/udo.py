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
from lib import stream
from lib.loader import Loader
from datetime import datetime
from lib.log import log, log_err
from conf.path import PATH_MOUNTPOINT
from lib.util import lock, mount_device
from fs.attr import ATTR_MODE, ATTR_FREQ
from threading import Thread, Event, Lock
from lib.op import OP_GET, OP_PUT, OP_OPEN, OP_CLOSE
from lib.mode import MODE_POLL, MODE_VIRT, MODE_SWITCH, MODE_SYNC, MODE_TRIG, MODE_PASSIVE

PRINT = False
WAITTIME = 2 # seconds
RETRY_MAX = 5
FREQ_MAX = 100
FREQ_MIN = 0.01
TIMEOUT_MAX = 600 # seconds
OUTPUT_MAX = 1 << 26

class UDO(object):
    def __init__(self, children={}, local=False):
        self._buf = {}
        self._event = {}
        self._lock = Lock()
        self._local = local
        self._children = children
        self._thread_listen = None
        self._thread_poll = None
        self._requester = None
        self._atime = None
        self._index = None
        self._core = None
        self._sock = None
        self._name = None
        self._type = None
        self._uid = None
        self._spec = {}
        self._mode = 0
        self._freq = 0
    
    def _print(self, text):
        if PRINT:
            log(text)
    
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
                log_err(self, 'failed to read, invalid type, name=%s' % self.d_name)
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
                log_err(self, 'failed to read, invalid index, name=%s' % self.d_name)
                return empty
            buf = device.check_output(output)
            return (device, buf)
        except:
            return empty
    
    def _write(self, buf):
        if not buf:
            log_err(self, 'failed to write, empty buf')
            return
        try:
            stream.put(self._sock, buf, self._local)
            return True
        except:
            pass
    
    def _init(self):
        if self._children:
            self._mode |= MODE_VIRT
        
        if not self.d_freq:
            if self._children:
                poll = False
                for i in self._children:
                    if self._can_poll(self._children[i].d_mode):
                        poll = True
                        break
                if poll:
                    self._freq = FREQ_MAX
                    self._mode |= MODE_POLL
            elif self._mode & MODE_POLL:
                self._freq = FREQ_MAX
            else:
                self._freq = FREQ_MIN
        
        if self.d_mode & MODE_VIRT or self.d_index == None:
            mode = None
            freq = None
            prof = None
        else:
            mode = self._mode
            freq = self._freq
            prof = self.d_profile
            path = os.path.join(PATH_MOUNTPOINT, self._uid, self._name)
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
            self._print('mount: type=%s [%s*]' % (self.d_type, self.d_name[:8]))
    
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
                    return mode
                cnt -= 1
                if cnt > 0:
                    time.sleep(WAITTIME)
                else:
                    break
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
                    return freq
                cnt -= 1
                if cnt > 0:
                    time.sleep(WAITTIME)
                else:
                    break
            return self._freq
    
    @property
    def d_index(self):
        return self._index
    
    @property
    def d_spec(self):
        return self._spec
    
    @property
    def d_type(self):
        if not self._type:
            return self.__class__.__name__
        else:
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
        prof.update({'type':self.d_type})
        prof.update({'spec':self.d_spec})
        prof.update({'index':self.d_index})
        return prof
    
    def set_type(self, val):
        self._type = val
    
    def set_freq(self, val):
        if val > FREQ_MAX:
            self._freq = FREQ_MAX
        elif val < FREQ_MIN:
            self._freq = FREQ_MIN
        else:
            self._freq = val
    
    def set_spec(self, val):
        self._spec = dict(val)
    
    def set_index(self, val):
        self._index = int(val)
    
    def set_mode(self, val):
        self._mode = int(val)
    
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
                    log_err(self, 'failed to check output, out of range')
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
    
    @lock
    def _check_device(self, device):
        if device.check_atime():
            index = device.d_index
            if index == None:
                index = 0
            self._write(req.req_get(index))
    
    def _poll(self):
        try:
            while True:
                time.sleep(self.d_intv)
                if self._children:
                    for i in self._children:
                        child = self._children[i]
                        if self._can_poll(child.d_mode):
                            self._check_device(child)
                elif self._can_poll(self.d_mode):
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
    
    def _can_poll(self, mode):
        return mode & MODE_POLL or (mode & MODE_TRIG and mode & MODE_PASSIVE)
    
    def _start(self, freq, mode):
        if self._sock and freq > 0 and self._can_poll(mode):
            self._thread_poll = Thread(target=self._poll)
            self._thread_poll.start()
        if self._sock:
            self._thread_listen = Thread(target=self._listen)
            self._thread_listen.start()
    
    def mount(self, uid, name, core, sock=None, init=True):
        self._uid = uid
        self._name = name
        self._core = core
        self._sock = sock
        if init:
            self._init()
            freq = self.d_freq
            mode = self.d_mode
        else:
            freq = self._freq
            mode = self._mode
        self._start(freq, mode)
    
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
