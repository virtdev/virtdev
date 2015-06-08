#      vdfs.py
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
import uuid
import time
import stat
from edge import Edge
from data import Data
from errno import EINVAL
from vertex import Vertex
from watcher import Watcher
from lib.lock import NamedLock
from dev.manager import Manager
from lib.log import log_err, log
from conf.virtdev import DATA_SERVERS
from fuse import FuseOSError, Operations
from dev.interfaces.lo import device_name
from dev.udo import FREQ_MAX, TIMEOUT_MAX
from lib.util import DIR_MODE, named_lock, load_driver
from lib.mode import MODE_VIRT, MODE_VISI, MODE_LO, MODE_LINK, MODE_CLONE
from attr import Attr, ATTR_MODE, ATTR_PROFILE, ATTR_HANDLER, ATTR_FILTER, ATTR_DISPATCHER, ATTR_FREQ, ATTR_PARENT, ATTR_TIMEOUT
from lib.op import OP_POLL, OP_MOUNT, OP_CLONE, OP_CREATE, OP_COMBINE, OP_INVALIDATE, OP_TOUCH, OP_ENABLE, OP_DISABLE, OP_DIFF, OP_SYNC, OP_ADD, OP_JOIN, OP_ACCEPT

_stat_dir = dict(st_mode=(stat.S_IFDIR | DIR_MODE), st_nlink=1)
_stat_dir['st_ctime'] = _stat_dir['st_mtime'] = _stat_dir['st_atime'] = time.time()

PATH_MAX = 1024
TYPE_VDEV = 'VDev'

def show_path(func):
    def _show_path(*args, **kwargs):
        path = args[1]
        log('%s: path=%s' % (func.func_name, path))
        return func(*args, **kwargs)
    return _show_path

class VDFS(Operations):
    def __init__(self, query=None):
        self._events = {}
        self._results = {}
        self._query = query
        watcher = Watcher()
        if not query:
            self._shadow = True
            manager = Manager()
            self._edge = Edge(core=manager.core)
            self._attr = Attr(core=manager.core)
            self._vertex = Vertex(core=manager.core)
            self._data = Data(self._vertex, self._edge, self._attr, watcher=watcher, core=manager.core)
            
            from lib.link import Uplink
            self._link = Uplink(manager)
        else:
            manager = None
            self._shadow = False
            router = query.router
            for i in DATA_SERVERS:
                router.add_server('fs', i)
            self._edge = Edge(router=router)
            self._vertex = Vertex(router=router)
            self._attr = Attr(watcher=watcher, router=router)
            self._data = Data(self._vertex, self._edge, self._attr, watcher=watcher, router=router)
            
            from lib.link import Downlink
            link = Downlink(query)
            self._query.link = link
            self._link = link
        
        self._manager = manager
        self._lock = NamedLock()
        if manager:
            manager.start()
    
    def _check_name(self, name):
        try:
            return uuid.UUID(name).hex
        except:
            return
    
    def _check_uid(self, uid):
        try:
            return uuid.UUID(uid).hex
        except:
            return
    
    def _parse(self, path):
        if len(path) > PATH_MAX:
            log_err(self, 'failed to parse')
            raise FuseOSError(EINVAL)
        
        if path == '/' or path[:2] == '/.':
            return (None, None, None)
        
        field = path[1:].split('/')
        uid = self._check_uid(field[0])
        if not uid:
            log_err(self, 'failed to parse, invalid uid, path=%s' % path)
            raise FuseOSError(EINVAL)
        
        name = ''
        total = len(field)
        if total == 1:
            obj = self._data
        else:
            if field[1] == self._vertex.label:
                obj = self._vertex
            elif field[1] == self._edge.label:
                obj = self._edge
            elif field[1] == self._attr.label:
                obj = self._attr
            else:
                if total != 2:
                    log_err(self, 'failed to parse, invalid path, path=%s' % path)
                    raise FuseOSError(EINVAL)
                name = self._check_name(field[1])
                if not name:
                    log_err(self, 'failed to parse, invalid name, path=%s' % path)
                    raise FuseOSError(EINVAL)
                obj = self._data
    
            if total > 2:
                if obj == self._attr:
                    if total > 4:
                        log_err(self, 'failed to parse, too much fields, path=%s' % path)
                        raise FuseOSError(EINVAL)
                    name = self._check_name(field[2])
                    if not name:
                        log_err(self, 'failed to parse, invalid name, path=%s' % path)
                        raise FuseOSError(EINVAL)
                    if total == 4:
                        name = os.path.join(name, field[3])
                else:      
                    name = self._check_name(field[-1])
                    if not name:
                        log_err(self, 'failed to parse, invalid name, path=%s' % path)
                        raise FuseOSError(EINVAL)
                    if total >= 4:
                        parent = self._check_name(field[-2])
                        name = os.path.join(parent, name)
        
        return (obj, uid, name)
    
    def _sync_create(self, obj, uid, name):
        if not self._shadow:
            if obj.can_invalidate():
                if not self._link.put(name=obj.parent(name), op=OP_INVALIDATE, path=obj.real(name)):
                    log_err(self, 'sync_create failed, op=OP_INVALIDATE, name=%s' % name)
                    raise FuseOSError(EINVAL)
            elif obj.can_touch():
                if not self._link.put(name=obj.parent(name), op=OP_TOUCH, path=obj.real(name)):
                    log_err(self, 'sync_create failed, op=OP_TOUCH, name=%s' % name)
                    raise FuseOSError(EINVAL)
    
    def _sync_open(self, obj, uid, name, flags):
        if not self._shadow:
            if obj.can_invalidate():
                if obj.may_update(flags):
                    if not self._link.put(name=obj.parent(name), op=OP_INVALIDATE, path=obj.real(name)):
                        log_err(self, 'sync_open failed, op=INVALIDATE, name=%s' % name)
                        raise FuseOSError(EINVAL)
            elif obj.can_touch():
                if not self._link.put(name=obj.parent(name), op=OP_TOUCH, path=obj.real(name)):
                    log_err(self, 'sync_open failed, op=OP_TOUCH, name=%s' % name)
                    raise FuseOSError(EINVAL)
        elif obj.is_expired(uid, name):
            buf = self._link.put(name=obj.parent(name), op=OP_DIFF, label=obj.label, item=obj.child(name), buf=obj.signature(uid, name))
            obj.patch(uid, name, buf)
    
    def _sync_release(self, obj, uid, name, update=False):
        if self._shadow:
            if update:
                with open(obj.get_path(uid, name), 'r') as f:
                    buf = f.read()
                if not self._link.put(name=name, op=OP_SYNC, buf=buf):
                    log_err(self, 'sync_release failed, op=OP_SYNC, name=%s' % name)
                    raise FuseOSError(EINVAL)
    
    def _sync_unlink(self, obj, uid, name):
        if not self._shadow:
            if obj.can_invalidate() or obj.can_unlink():
                if not self._link.put(name=obj.parent(name), op=OP_INVALIDATE, path=obj.real(name)):
                    log_err(self, 'sync_unlink failed, op=OP_INVALIDATE, name=%s' % name)
                    raise FuseOSError(EINVAL)
    
    def _sync_enable(self, obj, uid, name):
        if not self._shadow:
            if obj.can_enable():
                if not self._link.put(name=name, op=OP_ENABLE, path=name):
                    log_err(self, 'sync_enable failed, op=OP_ENABLE, name=%s' % name)
                    raise FuseOSError(EINVAL)
    
    def _sync_disable(self, obj, uid, name):
        if not self._shadow:
            if obj.can_disable():
                if not self._link.put(name=name, op=OP_DISABLE, path=name):
                    log_err(self, 'sync_disable failed, op=OP_DISABLE, name=%s' % name)
                    raise FuseOSError(EINVAL)
    
    @named_lock
    @show_path
    def _invalidate(self, path):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to invalidate, cannot get obj')
            raise FuseOSError(EINVAL)
        obj.invalidate(uid, name)
    
    def _initialize(self, uid, name, mode, vertex, parent, freq, prof, hndl, filt, disp, typ, timeout):
        lo = mode & MODE_LO
        if lo:
            if not typ or (self._shadow and not self._manager.has_lo()):
                log_err(self, 'failed to initialize device')
                raise FuseOSError(EINVAL)
        
        link = mode & MODE_LINK
        if link:
            mode &= ~MODE_LINK
            
        if mode & MODE_CLONE and not parent:
            log_err(self, 'failed to initialize device, no parent')
            raise FuseOSError(EINVAL)
        
        if not mode & MODE_VIRT:
            timeout = None
        
        if mode & MODE_VIRT:
            typ = TYPE_VDEV
        
        if not prof:
            if typ:
                driver = load_driver(typ)
                if not driver:
                    log_err(self, 'failed to initialize device')
                    raise FuseOSError(EINVAL)
                if mode & MODE_CLONE:
                    mode = driver.get_mode() | MODE_CLONE
                else:
                    mode = driver.get_mode()
                freq = driver.get_freq()
                prof = driver.get_profile()
        
        self._data.initialize(uid, name)
        self._attr.initialize(uid, name, ATTR_MODE, mode)
        
        if freq:
            self._attr.initialize(uid, name, ATTR_FREQ, freq)
        
        if filt:
            self._attr.initialize(uid, name, ATTR_FILTER, filt)
        
        if hndl:
            self._attr.initialize(uid, name, ATTR_HANDLER, hndl)
        
        if prof:
            self._attr.initialize(uid, name, ATTR_PROFILE, prof)
        
        if mode & MODE_CLONE:
            self._attr.initialize(uid, name, ATTR_PARENT, parent)
        
        if disp:
            self._attr.initialize(uid, name, ATTR_DISPATCHER, disp)
        
        if timeout:
            self._attr.initialize(uid, name, ATTR_TIMEOUT, timeout)
        
        if vertex:
            if mode & MODE_CLONE:
                log_err(self, 'failed to initialize device, invalid vertex')
                raise FuseOSError(EINVAL)
            self._vertex.initialize(uid, name, vertex)
            for v in vertex:
                self._edge.initialize(uid, (v, name), hidden=True)
        
        if not self._shadow:
            if not link:
                if not self._link.mount(uid, name, mode, vertex, typ, parent, timeout):
                    log_err(self, 'failed to initialize device, cannot mount')
                    raise FuseOSError(EINVAL)
        else:
            if lo and not mode & MODE_CLONE:
                self._manager.create(device_name(typ, name, mode), init=False)
    
    def _mount_device(self, uid, name, mode, vertex, parent, freq=None, prof=None, hndl=None, filt=None, disp=None, typ=None, timeout=None):
        if not name:
            name = uuid.uuid4().hex
        
        if mode != None:
            link = mode & MODE_LINK
            self._initialize(uid, name, mode, vertex, parent, freq, prof, hndl, filt, disp, typ, timeout)
        else:
            link = None
        
        if self._shadow and not link:
            if not self._link.put(name=name, op=OP_ADD, mode=mode, freq=freq, prof=prof):
                log_err(self, 'failed to mount device, cannot link, op=OP_ADD')
                raise FuseOSError(EINVAL)
        
        return name
    
    def _mount(self, path, value):
        if not value:
            log_err(self, 'failed to mount, no value')
            raise FuseOSError(EINVAL)
        
        uid = self._get_uid(path)
        if not uid:
            log_err(self, 'failed to mount')
            raise FuseOSError(EINVAL)
        
        args = ast.literal_eval(value)
        if type(args) != dict:
            log_err(self, 'failed to mount')
            raise FuseOSError(EINVAL)
        
        typ = args.get('type')
        freq = args.get('freq')
        name = args.get('name')
        mode = args.get('mode')
        prof = args.get('prof')
        filt = args.get('filter')
        hndl = args.get('handler')
        vertex = args.get('vertex')
        parent = args.get('parent')
        timeout = args.get('timeout')
        disp = args.get('dispatcher')
        
        if freq and float(freq) > FREQ_MAX:
            log_err(self, 'failed to mount, invalid frequency')
            raise FuseOSError(EINVAL)
        
        if prof and type(prof) != dict:
            log_err(self, 'failed to mount, invalid profile')
            raise FuseOSError(EINVAL)
        
        if vertex:
            if type(vertex) != list:
                log_err(self, 'failed to mount, invalid vertex')
                raise FuseOSError(EINVAL)
            for i in vertex:
                if not self._check_uid(i):
                    log_err(self, 'failed to mount')
                    raise FuseOSError(EINVAL)
        
        if timeout and float(timeout) > TIMEOUT_MAX:
            log_err(self, 'failed to mount, invalid timeout')
            raise FuseOSError(EINVAL)
        
        self._mount_device(uid, name, mode, vertex, parent, freq, prof, hndl, filt, disp, typ, timeout)
    
    @named_lock
    def getattr(self, path, fh=None):
        obj, uid, name = self._parse(path)
        if not name:
            return _stat_dir
        if not obj:
            log_err(self, 'failed to getattr, no object')
            raise FuseOSError(EINVAL)
        return obj.getattr(uid, name)
    
    @named_lock
    @show_path
    def create(self, path, mode):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to create, no object')
            raise FuseOSError(EINVAL)
        self._sync_create(obj, uid, name)
        return obj.create(uid, name)
    
    @named_lock
    @show_path
    def readdir(self, path, fh):
        obj, uid, name = self._parse(path)
        res = []
        if obj:
            res = obj.readdir(uid, name)
        return res
    
    @named_lock
    @show_path
    def open(self, path, flags):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to open, no object')
            raise FuseOSError(EINVAL)
        self._sync_open(obj, uid, name, flags)
        return obj.open(uid, name, flags)
    
    @named_lock
    @show_path
    def release(self, path, fh):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to release, no object')
            raise FuseOSError(EINVAL)
        update = obj.release(uid, name, fh)
        self._sync_release(obj, uid, name, update)
    
    @named_lock
    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, 0)
        return os.write(fh, buf)
    
    @named_lock
    def read(self, path, size, offset, fh):
        os.lseek(fh, offset, 0)
        return os.read(fh, size)
    
    @named_lock
    @show_path
    def unlink(self, path):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to unlink, no object')
            raise FuseOSError(EINVAL)
        self._sync_unlink(obj, uid, name)
        obj.unlink(uid, name)
    
    def _create_device(self, path, op, attr):
        if self._shadow:
            return
        
        uid = self._get_uid(path)
        if not uid:
            log_err(self, 'failed to create device, invalid uid')
            raise FuseOSError(EINVAL)
        
        args = ast.literal_eval(attr)
        if type(args) != dict:
            log_err(self, 'failed to create device, invalid attr')
            raise FuseOSError(EINVAL)
        
        typ = args.get('type')
        mode = args.get('mode')
        parent = args.get('parent')
        timeout = args.get('timeout')
        
        if op == OP_CLONE:
            if not parent:
                log_err(self, 'failed to create device, no parent')
                raise FuseOSError(EINVAL)
            prof = self._attr.get_profile(uid, parent)
            typ = prof['type']
        
        if None == mode:
            if not typ:
                mode = MODE_VIRT
            else:
                mode = MODE_LO
        
        if op == OP_CREATE:
            if typ:
                mode |= MODE_VISI
        elif op == OP_CLONE:
            mode |= MODE_CLONE
        
        vertex = args.get('vertex')
        if vertex:
            if mode & MODE_CLONE:
                log_err(self, 'failed to create device, any cloned device does not have vertex')
                raise FuseOSError(EINVAL)
            if type(vertex) != list:
                log_err(self, 'failed to create device, invalid vertex')
                raise FuseOSError(EINVAL)
            for i in vertex:
                if not self._check_uid(i):
                    log_err(self, 'failed to create device, invalid vertex')
                    raise FuseOSError(EINVAL)
        
        if parent and not self._check_uid(parent):
            log_err(self, 'failed to create device, invalid parent')
            raise FuseOSError(EINVAL)
        
        return self._mount_device(uid, None, mode, vertex, parent, typ=typ, timeout=timeout)
    
    def _enable(self, path):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to enable, no object')
            raise FuseOSError(EINVAL)
        self._sync_enable(obj, uid, name)
    
    def _disable(self, path):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to disable, no object')
            raise FuseOSError(EINVAL)
        self._sync_disable(obj, uid, name)
    
    def _join(self, path, target):
        if not self._shadow:
            return
        obj, _, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to join, no object')
            raise FuseOSError(EINVAL)
        self._manager.guest.join(name, target)
    
    def _accept(self, path, target):
        if not self._shadow:
            return
        obj, _, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to accept, no object')
            raise FuseOSError(EINVAL)
        self._manager.guest.accept(name, target)
    
    def setxattr(self, path, name, value, options, position=0):
        if name == OP_INVALIDATE:
            self._invalidate(path)
        elif name == OP_MOUNT:
            self._mount(path, value)
        elif name == OP_ENABLE:
            self._enable(path)
        elif name == OP_DISABLE:
            self._disable(path)
        elif name == OP_JOIN:
            self._join(path, value)
        elif name == OP_ACCEPT:
            self._accept(path, value)
    
    @named_lock
    def _get_event(self, uid):
        ret = self._events.get(uid)
        if ret:
            del self._events[uid]
            return ret
        else:
            return ''
    
    @named_lock
    def _set_event(self, uid, event):
        self._events[uid] = event
    
    def _scan(self, path, args):
        result = ''
        if self._shadow or not args:
            return result
        obj, _, name = self._parse(path)
        if not obj or not obj.can_scan():
            return result
        if name:
            result = self._query.history.get(name, args)
        return result
    
    def _get_uid(self, path):
        if len(path) > PATH_MAX:
            log_err(self, 'failed to get uid')
            raise FuseOSError(EINVAL)
        
        if path[0] != '/':
            return
        
        field = path[1:].split('/') 
        if field[0]:
            return self._check_uid(field[0])
    
    def _poll(self, path):
        event = ''
        if self._shadow:
            return event
        uid = self._get_uid(path)
        if uid:
            event = self._get_event(uid)
            if not event:
                event = self._query.event.get(uid)
                self._set_event(uid, event)
        return event
    
    @named_lock
    def _get_result(self, path, op):
        if self._results.has_key(op):
            ret = self._results[op].get(path)
            if ret:
                del self._results[op][path]
                return ret
        return ''
    
    @named_lock
    def _set_result(self, path, op, result):
        if self._results.has_key(op):
            self._results[op].update({path:result})
        else:
            self._results.update({op:{path:result}})
    
    def getxattr(self, path, name, position=0):
        if name == OP_POLL:
            op = 'poll'
        elif name.startswith('scan:'):
            op = 'scan'
        elif name.startswith('clone:'):
            op = 'clone'
        elif name.startswith('create:'):
            op = 'create'
        elif name.startswith('combine:'):
            op = 'combine'
        else:
            return ''
        
        res = self._get_result(path, op)
        if res:
            return res
        
        if op == 'poll':
            res = self._poll(path)
        elif op == 'scan':
            res = self._scan(path, name[len('scan:'):])
        elif op == 'clone':
            res = self._create_device(path, OP_CLONE, name[len('clone:'):])
        elif op == 'create':
            res = self._create_device(path, OP_CREATE, name[len('create:'):])
        elif op == 'combine':
            res = self._create_device(path, OP_COMBINE, name[len('combine:'):])
        self._set_result(path, op, res)
        return res
    
    @named_lock
    @show_path
    def truncate(self, path, length, fh=None):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to truncate, no object')
            raise FuseOSError(EINVAL)
        obj.truncate(uid, name, length)
    
    @named_lock
    @show_path
    def readlink(self, path):
        obj, uid, name = self._parse(path)
        return obj.readlink(uid, name)
