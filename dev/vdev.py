#      vdev.py
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
import xattr
from lib import stream
from lib.jpg import JPG
from fs.oper import OP_MOUNT
from datetime import datetime
from lib.lock import VDevLock
from lib.log import log, log_err
from threading import Thread, Event, Lock
from multiprocessing.pool import ThreadPool
from conf.virtdev import VDEV_FS_MOUNTPOINT

VDEV_MODE_FI = 0x000000001  # Full Input
VDEV_MODE_FO = 0x000000002  # Full Output
VDEV_MODE_PI = 0x000000004  # Partial Input
VDEV_MODE_PO = 0x000000008  # Partial Output
VDEV_MODE_POLL = 0x00000010
VDEV_MODE_TRIG = 0x00000020
VDEV_MODE_SYNC = 0x00000040
VDEV_MODE_VISI = 0x00000080
VDEV_MODE_VIRT = 0x00000100
VDEV_MODE_SWITCH = 0x00000200
VDEV_MODE_IN   = 0x00000400
VDEV_MODE_OUT  = 0x00000800
VDEV_MODE_REFLECT = 0x00001000
VDEV_MODE_ANON = 0x00002000
VDEV_MODE_LINK = 0x00004000

VDEV_ENABLE_POLLING = True
VDEV_FREQ = 10 # HZ
VDEV_OUTPUT_MAX = 1 << 20

VDEV_GET = 'get'
VDEV_PUT = 'put'
VDEV_OPEN = 'open'
VDEV_CLOSE = 'close'

def mount_device(uid, name, mode, freq, profile):
    attr = {}
    attr.update({'name':name})
    attr.update({'mode':mode})
    attr.update({'freq':freq})
    attr.update({'profile':profile})
    path = os.path.join(VDEV_FS_MOUNTPOINT, uid)
    xattr.setxattr(path, OP_MOUNT, str(attr))

def update_device(query, uid, node, addr, name):
    query.device.put(name, {'uid':uid, 'addr':addr, 'node':node})
    query.member.remove(uid, (name,))
    query.member.put(uid, (name, node))

def excl(func):
    def _excl(*args, **kwargs):
        self = args[0]
        name = args[1]
        lock = self._lock.acquire(name)
        try:
            return func(*args, **kwargs)
        finally:
            lock.release()
    return _excl

class VDev(object):
    def __init__(self, mode=0, **fields):
        self._buf = {}
        self._event = {}
        self._children = {}
        self._requester = None
        self.manager = None
        self._atime = None
        self._range = None
        self._sock = None
        self._name = None
        self._type = None
        self._uid = None
        self._index = 0
        self._mode = mode
        self._wrlock = Lock()
        self._fields = fields
        self._freq = VDEV_FREQ
        self._lock = VDevLock()
        self._anon = mode & VDEV_MODE_ANON
        self._thread = Thread(target=self._run)
    
    def _get_path(self):
        return os.path.join(VDEV_FS_MOUNTPOINT, self._uid, self._name)
    
    def _read(self):
        empty = (None, None)
        buf = stream.get(self._sock, self._anon)
        if len(buf) > VDEV_OUTPUT_MAX:
            return empty
        
        if not buf and self._anon:
            return (self, '')
        
        output = ast.literal_eval(buf)
        if type(output) != dict:
            log_err(self, 'failed to read, invalid type')
            return empty
        
        if output.has_key('_i'):
            device = None
            index = int(output['_i'])
            for i in self._children:
                if self._children[i].d_index == index:
                    device = self._children[i]
                    break
            if not device:
                log_err(self, 'failed to read, invalid index')
                return empty
        else:
            device = self
        buf = device.check_output(output)
        return (device, buf)
    
    def _write(self, buf):
        if not buf:
            log_err(self, 'failed to write, invalid request')
            return
        self._wrlock.acquire()
        try:
            stream.put(self._sock, buf, self._anon)
            return True
        except:
            log_err(self, 'failed to write')
        finally:
            self._wrlock.release()
    
    def _mount(self):
        path = self._get_path()
        if self.d_mode & VDEV_MODE_VIRT or os.path.exists(path):
            mode = None
            freq = None
            profile = None
        else:
            mode = self.d_mode
            freq = self.d_freq
            profile = self.d_profile
        mount_device(self._uid, self.d_name, mode, freq, profile)
    
    def _check_device(self, device):
        if device.check_atime():
            index = device.d_index
            if None == index:
                index = 0
            self._write(req.req_get(index))
    
    def _poll(self):
        if not self.d_mode & VDEV_MODE_POLL:
            return
        while True:
            time.sleep(self.d_intv)
            if self._children:
                for i in self._children:
                    child = self._children[i]
                    if child.d_mode & VDEV_MODE_POLL:
                        self._check_device(child)
            if self.d_mode & VDEV_MODE_POLL:
                self._check_device(self)
    
    @property
    def d_name(self):
        return self._name
    
    @property
    def d_mode(self):
        if not self.manager:
            return self._mode
        else:
            try:
                return self.manager.synchronizer.get_mode(self.d_name)
            except:
                return self._mode
    
    @property
    def d_freq(self):
        if not self.manager:
            return self._freq
        else:
            try:
                return self.manager.synchronizer.get_freq(self.d_name)
            except:
                return self._freq
    
    @property
    def d_index(self):
        return self._index
    
    @property
    def d_range(self):
        return self._range
    
    @property
    def d_type(self):
        if not self._type:
            return self.__class__.__name__
        else:
            return self._type
    
    @property
    def d_intv(self):
        return 1.0 / self.d_freq
    
    @property
    def d_fields(self):
        return self._fields
    
    @property
    def d_profile(self):
        profile = {}
        profile.update({'type':str(self.d_type)})
        profile.update({'range':str(self.d_range)})
        profile.update({'index':str(self.d_index)})
        profile.update({'fields':str(self.d_fields)})
        return profile
    
    def set_type(self, typ):
        self._type = typ
    
    def set_freq(self, f):
        self._freq = f
    
    def set_range(self, r):
        self._range = r
    
    def set_children(self, children):
        self._children = children
    
    def find(self, name):
        if self.d_name == name:
            return self
        else:
            return self._children.get(name)
    
    def add_child(self, device):
        self._children.update({device.d_name:device})
    
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
        if output.has_key('_i'):
            del output['_i']
        for i in output:
            if not self._fields.has_key(i):
                log_err(self, 'invalid output')
                return
            try:
                val = output[i]
                field = self._fields[i]
                if field == 'int': 
                    int(val)
                elif field == 'bool':
                    if val != 'True' and val != 'False':
                        log_err(self, 'invalid output')
                        return
                elif field == 'jpg':
                    JPG(val)
            except:
                log_err(self, 'invalid output')
                return
        return output
    
    def sync(self, buf):
        if type(buf) != str and type(buf) != unicode:
            buf = str(buf)
        path = self._get_path()
        if os.path.exists(path):
            with open(path, 'wb') as f:
                f.write(buf)
            return True
    
    def has_field(self, name, field):
        if self._children:
            child = self._children.get(name)
            if child:
                if child._fields:
                    return child._fields.has_key(field)
    
    def _register(self, name):
        self._buf[name] = None
        self._event[name] = Event()
    
    def _set(self, device, buf):
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
    
    @excl
    def proc(self, name, op, buf=None):
        dev = self.find(name)
        if not dev:
            log_err(self, 'failed to process, cannot find device')
            return
        index = dev.d_index
        if op == VDEV_OPEN:
            if dev.d_mode & VDEV_MODE_SWITCH:
                self._write(req.req_open(index))
        elif op == VDEV_CLOSE:
            if dev.d_mode & VDEV_MODE_SWITCH:
                self._write(req.req_close(index))
        elif op == VDEV_GET:
            self._register(name)
            self._write(req.req_get(index))
            return self._wait(name)
        elif op == VDEV_PUT:
            self._register(name)
            self._write(req.req_put(index, str(buf[buf.keys()[0]])))
            return self._wait(name) 
        else:
            log_err(self, 'failed to process, invalid operation')
    
    def mount(self, manager, name, index=None, sock=None, init=True):
        self.manager = manager
        self._uid = manager.uid
        self._index = index
        self._name = name
        self._sock = sock
        if init:
            self._mount()
        self._thread.start()
        log('mount: type=%s, index=%s [%s*]' % (self.d_type, self.d_index, self.d_name[:8]))
    
    def _run(self):
        if not self._sock:
            return
        if VDEV_ENABLE_POLLING:
            poll = ThreadPool(processes=1)
            poll.apply_async(self._poll)
        while True:
            try:
                device, buf = self._read()
                if not device:
                    continue
                
                name = device.d_name
                if self.manager.synchronizer.has_callback(name):
                    output = self.manager.synchronizer.callback(name, {name:buf})
                    if not output:
                        continue
                    buf = ast.literal_eval(output)
                    if type(buf) != dict:
                        log_err(self, 'invalid result of callback function')
                        continue
                    if buf:
                        mode = device.d_mode
                        if mode & VDEV_MODE_REFLECT:
                            op = self.manager.synchronizer.get_oper({name:buf}, mode)
                            if op != None:
                                buf = self.proc(name, op)
                
                if not self._set(device, buf) and buf:
                    mode = device.d_mode
                    if not (mode & VDEV_MODE_TRIG) or device.check_atime():
                        if mode & VDEV_MODE_SYNC:
                            device.sync(buf)
                        self.manager.synchronizer.dispatch(name, buf)
            except:
                log_err(self, 'device=%s, restarting' % self.d_name)
                poll.terminate()
                self._sock.close()
                break
    