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

import re
import os
import ast
import uuid
import time
import stat
from edge import Edge
from data import Data
from fuse import FUSE
from errno import EINVAL
from vertex import Vertex
from lib.util import DIR_MODE
from lib.lock import VDevLock
from path import VDEV_FS_UPDATE
from lib.log import log_err, log
from manager import VDevFSManager
from watcher import VDevWatcherPool
from fuse import FuseOSError, Operations
from conf.virtdev import VDEV_DFS_SERVERS, VDEV_FS_MOUNTPOINT
from dev.vdev import VDev, VDEV_MODE_VIRT, VDEV_MODE_VISI, VDEV_GET
from attr import Attr, VDEV_ATTR_MODE, VDEV_ATTR_PROFILE, VDEV_ATTR_HANDLER, VDEV_ATTR_MAPPER, VDEV_ATTR_DISPATCHER, VDEV_ATTR_FREQ
from oper import OP_LOAD, OP_POLL, OP_FORK, OP_MOUNT, OP_CREATE, OP_COMBINE, OP_INVALIDATE, OP_TOUCH, OP_ENABLE, OP_DISABLE, OP_DIFF, OP_SYNC, OP_ADD, OP_JOIN, OP_ACCEPT

_stat_dir = dict(st_mode=(stat.S_IFDIR | DIR_MODE), st_nlink=1)
_stat_dir['st_ctime'] = _stat_dir['st_mtime'] = _stat_dir['st_atime'] = time.time()

VDEV_PATH_MAX = 1024

def excl(func):
    def _excl(*args, **kwargs):
        self = args[0]
        path = args[1]
        lock = self._lock.acquire(path)
        try:
            return func(*args, **kwargs)
        finally:
            lock.release()
    return _excl

def show(func):
    def _show(*args, **kwargs):
        path = args[1]
        log('%s: path=%s' % (func.func_name, path))
        return func(*args, **kwargs)
    return _show

class VDevFS(Operations):
    def _check_mountpoint(self):
        os.system('umount %s 2>/dev/null' % VDEV_FS_MOUNTPOINT)
        if not os.path.exists(VDEV_FS_MOUNTPOINT):
            os.mkdir(VDEV_FS_MOUNTPOINT)
    
    def __init__(self, query=None):
        self._events = {}
        self._results = {}
        self._query = query
        watcher = VDevWatcherPool()
        if not query:
            self._shadow = True
            manager = VDevFSManager()
            self._edge = Edge(manager=manager)
            self._attr = Attr(manager=manager)
            self._vertex = Vertex(manager=manager)
            self._data = Data(self._vertex, self._edge, self._attr, watcher=watcher, manager=manager)
            
            from link import VDevFSUplink
            self._link = VDevFSUplink(manager)
        else:
            manager = None
            self._shadow = False
            router = query.router
            for i in VDEV_DFS_SERVERS:
                router.add_server('dfs', i)
            self._edge = Edge(router=router)
            self._vertex = Vertex(router=router)
            self._attr = Attr(watcher=watcher, router=router)
            self._data = Data(self._vertex, self._edge, self._attr, watcher=watcher, router=router)
            
            from link import VDevFSDownlink
            link = VDevFSDownlink(query)
            self._query.set_link(link)
            self._link = link
        
        self.manager = manager
        self._check_mountpoint()
        self._lock = VDevLock()
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
        if len(path) > VDEV_PATH_MAX:
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
                    log_err(self, 'failed to sync create, op=OP_INVALIDATE')
                    raise FuseOSError(EINVAL)
            elif obj.can_touch():
                if not self._link.put(name=obj.parent(name), op=OP_TOUCH, path=obj.real(name)):
                    log_err(self, 'failed to sync create, op=OP_TOUCH')
                    raise FuseOSError(EINVAL)
    
    def _sync_open(self, obj, uid, name, flags):
        if not self._shadow:
            if obj.can_invalidate():
                if obj.may_update(flags):
                    if not self._link.put(name=obj.parent(name), op=OP_INVALIDATE, path=obj.real(name)):
                        log_err(self, 'failed to sync open, op=INVALIDATE')
                        raise FuseOSError(EINVAL)
            elif obj.can_touch():
                if not self._link.put(name=obj.parent(name), op=OP_TOUCH, path=obj.real(name)):
                    log_err(self, 'failed to sync open, op=OP_TOUCH')
                    raise FuseOSError(EINVAL)
        elif obj.is_expired(uid, name):
            buf = self._link.put(name=obj.parent(name), op=OP_DIFF, label=obj.label, item=obj.child(name), buf=obj.signature(uid, name))
            obj.patch(uid, name, buf)
    
    def _sync_release(self, obj, uid, name, flags):
        if self._shadow:
            if flags and flags & VDEV_FS_UPDATE:
                with open(obj.get_path(uid, name), 'r') as f:
                    buf = f.read()
                if not self._link.put(name=name, op=OP_SYNC, buf=buf):
                    log_err(self, 'failed to sync release, op=OP_SYNC')
                    raise FuseOSError(EINVAL)
    
    def _sync_unlink(self, obj, uid, name):
        if not self._shadow:
            if obj.can_invalidate() or obj.can_unlink():
                if not self._link.put(name=obj.parent(name), op=OP_INVALIDATE, path=obj.real(name)):
                    log_err(self, 'failed to sync unlink, op=OP_INVALIDATE')
                    raise FuseOSError(EINVAL)
    
    def _sync_enable(self, obj, uid, name):
        if not self._shadow:
            if obj.can_enable():
                if not self._link.put(name=name, op=OP_ENABLE, path=name):
                    log_err(self, 'failed to sync enable, op=OP_ENABLE')
                    raise FuseOSError(EINVAL)
    
    def _sync_disable(self, obj, uid, name):
        if not self._shadow:
            if obj.can_disable():
                if not self._link.put(name=name, op=OP_DISABLE, path=name):
                    log_err(self, 'failed to sync disable, op=OP_DISABLE')
                    raise FuseOSError(EINVAL)
    
    @excl
    @show
    def _invalidate(self, path):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to invalidate, no object')
            raise FuseOSError(EINVAL)
        obj.invalidate(uid, name)
    
    def _mount_device(self, uid, name, mode, vertex, freq=None, profile=None, handler=None, mapper=None, dispatcher=None, parent=None):
        if mode != None:
            self._data.initialize(uid, name)
            self._attr.initialize(uid, name, {VDEV_ATTR_MODE:mode})
            
            if not profile and mode & VDEV_MODE_VIRT:
                profile = VDev().d_profile
            
            if freq:
                self._attr.initialize(uid, name, {VDEV_ATTR_FREQ:freq})
            
            if profile:
                self._attr.initialize(uid, name, {VDEV_ATTR_PROFILE:profile})
            
            if mapper:
                self._attr.initialize(uid, name, {VDEV_ATTR_MAPPER:mapper})
            
            if handler:
                self._attr.initialize(uid, name, {VDEV_ATTR_HANDLER:handler})
            
            if dispatcher:
                self._attr.initialize(uid, name, {VDEV_ATTR_DISPATCHER:dispatcher})
            
            if vertex:
                self._vertex.initialize(uid, name, vertex)
                for v in vertex:
                    self._edge.initialize(uid, (v, name), hidden=True)
            
            if not self._shadow and mode & VDEV_MODE_VIRT:
                if vertex:
                    src = vertex[0]
                elif parent:
                    src = parent
                else:
                    src = None
                if not self._link.clone(uid, src, name, mode, vertex):
                    log_err(self, 'failed to mount device, cannot clone')
                    raise FuseOSError(EINVAL)
        
        if self._shadow and (mode == None or not (mode & VDEV_MODE_VIRT)):
            if not self._link.put(name=name, op=OP_ADD, mode=mode, freq=freq, profile=profile):
                log_err(self, 'failed to mount device, cannot link, op=OP_ADD')
                raise FuseOSError(EINVAL)
    
    def _mount(self, path, value):
        if not value:
            log_err(self, 'failed to mount, no value')
            raise FuseOSError(EINVAL)
        
        uid = self._get_uid(path)
        if not uid:
            log_err(self, 'failed to mount')
            raise FuseOSError(EINVAL)
        
        args = ast.literal_eval(value)
        if type(args) != dict or not args.has_key('name'):
            log_err(self, 'failed to mount')
            raise FuseOSError(EINVAL)
        
        name = self._check_uid(args['name'])
        if not name:
            log_err(self, 'failed to mount')
            raise FuseOSError(EINVAL)
        
        freq = args.get('freq')
        mapper = args.get('mapper')
        handler = args.get('handler')
        dispatcher = args.get('dispatcher')
            
        vertex = args.get('vertex')
        if vertex:
            if type(vertex) != list:
                log_err(self, 'failed to mount, invalid vertex')
                raise FuseOSError(EINVAL)
            for i in vertex:
                if not self._check_uid(i):
                    log_err(self, 'failed to mount')
                    raise FuseOSError(EINVAL)
        
        profile = args.get('profile')
        if profile and type(profile) != dict:
            log_err(self, 'failed to mount, invalid profile')
            raise FuseOSError(EINVAL)
            
        mode = args.get('mode')
        if mode == None:
            if not self._shadow:
                if not profile and vertex:
                    mode = VDEV_MODE_VIRT
                else:
                    log_err(self, 'failed to mount, invalid mode')
                    raise FuseOSError(EINVAL)
            else:
                if profile:
                    log_err(self, 'failed to mount, invalid mode')
                    raise FuseOSError(EINVAL)
        
        self._mount_device(uid, name, mode, vertex, freq, profile, handler, mapper, dispatcher)
    
    @excl
    def getattr(self, path, fh=None):
        obj, uid, name = self._parse(path)
        if not name:
            return _stat_dir
        if not obj:
            log_err(self, 'failed to getattr, no object')
            raise FuseOSError(EINVAL)
        return obj.getattr(uid, name)
    
    @excl
    @show
    def create(self, path, mode):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to create, no object')
            raise FuseOSError(EINVAL)
        self._sync_create(obj, uid, name)
        return obj.create(uid, name)
    
    @excl
    @show                        
    def readdir(self, path, fh):
        obj, uid, name = self._parse(path)
        res = []
        if obj:
            res = obj.readdir(uid, name)
        return res
    
    @excl
    @show
    def open(self, path, flags):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to open, no object')
            raise FuseOSError(EINVAL)
        self._sync_open(obj, uid, name, flags)
        return obj.open(uid, name, flags)
    
    @excl
    @show
    def release(self, path, fh):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to release, no object')
            raise FuseOSError(EINVAL)
        flags = obj.release(uid, name, fh)
        self._sync_release(obj, uid, name, flags)
    
    @excl
    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, 0)
        return os.write(fh, buf)
    
    @excl
    def read(self, path, size, offset, fh):
        os.lseek(fh, offset, 0)
        return os.read(fh, size)
    
    @excl
    @show
    def unlink(self, path):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to unlink, no object')
            raise FuseOSError(EINVAL)
        self._sync_unlink(obj, uid, name)
        obj.unlink(uid, name)
    
    def _create(self, path, op, attr):
        if self._shadow:
            return
        parent = None
        vertex = None
        mode = VDEV_MODE_VIRT
        uid = self._get_uid(path)
        if not uid:
            log_err(self, 'failed to create device')
            raise FuseOSError(EINVAL)
        
        if op == OP_CREATE:
            mode |= VDEV_MODE_VISI
        
        args = ast.literal_eval(attr)
        if type(args) != dict or not args.has_key('name'):
            log_err(self, 'failed to create device')
            raise FuseOSError(EINVAL)
        name = args['name']
        if op != OP_FORK:
            if not args.has_key('vertex'):
                log_err(self, 'failed to create device')
                raise FuseOSError(EINVAL)
            vertex = args['vertex']
            if type(vertex) != list:
                log_err(self, 'failed to create device')
                raise FuseOSError(EINVAL)
            for i in vertex:
                if not self._check_uid(i):
                    log_err(self, 'failed to create device')
                    raise FuseOSError(EINVAL)
        else:
            if not args.has_key('parent'):
                log_err(self, 'failed to create device')
                raise FuseOSError(EINVAL)
            parent = self._check_uid(args['parent'])
            if not parent:
                log_err(self, 'failed to create device, cannot fork')
                raise FuseOSError(EINVAL)

        self._mount_device(uid, name, mode=mode, vertex=vertex, parent=parent)
    
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
        self.manager.guest.join(name, target)
    
    def _accept(self, path, target):
        if not self._shadow:
            return
        obj, _, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to accept, no object')
            raise FuseOSError(EINVAL)
        self.manager.guest.accept(name, target)
    
    def setxattr(self, path, name, value, options, position=0):
        if name == OP_INVALIDATE:
            self._invalidate(path)
        elif name == OP_MOUNT:
            self._mount(path, value)
        elif name == OP_FORK or name == OP_CREATE or name == OP_COMBINE:
            self._create(path, name, value)
        elif name == OP_ENABLE:
            self._enable(path)
        elif name == OP_DISABLE:
            self._disable(path)
        elif name == OP_JOIN:
            self._join(path, value)
        elif name == OP_ACCEPT:
            self._accept(path, value)
    
    @excl
    def _get_event(self, uid):
        ret = self._events.get(uid)
        if ret:
            del self._events[uid]
            return ret
        else:
            return ''
    
    @excl
    def _set_event(self, uid, event):
        self._events[uid] = event
    
    @excl
    def _get_result(self, path):
        ret = self._results.get(path)
        if ret:
            del self._results[path]
            return ret
        else:
            return ''
    
    @excl
    def _set_result(self, path, result):
        self._results[path] = result
    
    def _scan(self, path, query):
        result = ''
        if self._shadow or not query:
            return result
        obj, _, name = self._parse(path)
        if not obj or not obj.can_scan():
            return result
        if name:
            path = os.path.join(path, query)
            result = self._get_result(path)
            if not result:
                result = self._query.history_get(name, query)
                self._set_result(path, result)
        return result
    
    def _get_uid(self, path):
        if len(path) > VDEV_PATH_MAX:
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
                event = self._query.event_get(uid)
                self._set_event(uid, event)
        return event
    
    def _is_query(self, name):
        if re.match('[0-9a-zA-Z]+(_[0-9a-zA-Z]+)+', name):
            return True
    
    def _load(self, path):
        obj, _, name = self._parse(path)
        if not obj or not obj.can_load():
            log_err(self, 'failed to load')
            raise FuseOSError(EINVAL)
        if self.manager:
            for device in self.manager:
                d = device.find(name)
                if d:
                    return d.proc(name, VDEV_GET)
        return ''
        
    def getxattr(self, path, name, position=0):
        if name == OP_LOAD:
            return self._load(path)
        elif name == OP_POLL:
            return self._poll(path)
        elif self._is_query(name):
            return self._scan(path, name)
        else:
            return ''
    
    @excl
    @show
    def truncate(self, path, length, fh=None):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to truncate, no object')
            raise FuseOSError(EINVAL)
        obj.truncate(uid, name, length)
    
    @excl
    @show
    def readlink(self, path):
        obj, uid, name = self._parse(path)
        return obj.readlink(uid, name)
    