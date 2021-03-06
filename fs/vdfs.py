# vdfs.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import ast
import uuid
import time
import stat
from attr import Attr
from edge import Edge
from data import Data
from vrtx import Vrtx
from lib.types import *
from lib.modes import *
from lib.fields import *
from errno import EINVAL
from lib.attributes import *
from lib.operations import *
from conf.log import LOG_VDFS
from lib.loader import Loader
from lib.lock import NamedLock
from dev.manager import Manager
from lib.log import log_debug, log_err
from conf.virtdev import EXPOSE, HISTORY
from fuse import FuseOSError, Operations
from dev.interface.lo import device_name
from dev.driver import FREQ_MAX, load_driver
from lib.util import DIR_MODE, named_lock, named_lock_lock, named_lock_unlock

PATH_MAX = 1024
TIMEOUT_MAX = 600 # seconds

RETRY_MAX = 2
RETRY_INTERVAL = 5 # seconds

STAT_DIR = dict(st_mode=(stat.S_IFDIR | DIR_MODE), st_nlink=1)
STAT_DIR['st_ctime'] = STAT_DIR['st_mtime'] = STAT_DIR['st_atime'] = time.time()

def show_path(func):
    def _show_path(*args, **kwargs):
        self = args[0]
        path = args[1]
        self._log('%s, path=%s' % (func.func_name, str(path)))
        return func(*args, **kwargs)
    return _show_path

class VDFS(Operations):
    def __init__(self, query=None, router=None):
        self._events = {}
        self._results = {}
        self._query = query
        if not query:
            self._shadow = True
            manager = Manager()
            self._edge = Edge(core=manager.core)
            self._vrtx = Vrtx(core=manager.core)
            self._attr = Attr(core=manager.core)
            self._data = Data(self._vrtx, self._edge, self._attr, core=manager.core)

            from lib.link import Uplink
            self._link = Uplink(manager)
        else:
            manager = None
            self._shadow = False
            self._edge = Edge(router=router)
            self._vrtx = Vrtx(router=router)
            self._attr = Attr(router=router, rdonly=False)
            self._data = Data(self._vrtx, self._edge, self._attr, router=router, rdonly=False)

            from lib.link import Downlink
            link = Downlink(query)
            self._query.link = link
            self._link = link

        self._manager = manager
        self._lock = NamedLock()
        if manager:
            manager.start()

    def _log(self, text):
        if LOG_VDFS:
            log_debug(self, text)

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
            self._log('failed to parse')
            raise FuseOSError(EINVAL)

        if path == '/' or path[:2] == '/.':
            return (None, None, None)

        field = path[1:].split('/')
        uid = self._check_uid(field[0])
        if not uid:
            self._log('failed to parse, invalid uid, path=%s' % str(path))
            raise FuseOSError(EINVAL)

        name = ''
        total = len(field)
        if total == 1:
            obj = self._data
        else:
            if field[1] == FIELD_VRTX:
                obj = self._vrtx
            elif field[1] == FIELD_EDGE:
                obj = self._edge
            elif field[1] == FIELD_ATTR:
                obj = self._attr
            else:
                if total != 2:
                    self._log('failed to parse, invalid path, path=%s' % str(path))
                    raise FuseOSError(EINVAL)

                name = self._check_name(field[1])
                if not name:
                    self._log('failed to parse, invalid path, path=%s' % str(path))
                    raise FuseOSError(EINVAL)

                obj = self._data

            if total > 2:
                if obj == self._attr:
                    if total > 4:
                        self._log('failed to parse, invalid path, path=%s' % str(path))
                        raise FuseOSError(EINVAL)

                    name = self._check_name(field[2])
                    if not name:
                        self._log('failed to parse, invalid path, path=%s' % str(path))
                        raise FuseOSError(EINVAL)

                    if total == 4:
                        name = os.path.join(name, field[3])
                else:
                    name = self._check_name(field[-1])
                    if not name:
                        self._log('failed to parse, invalid path, path=%s' % str(path))
                        raise FuseOSError(EINVAL)

                    if total >= 4:
                        parent = self._check_name(field[-2])
                        name = os.path.join(parent, name)

        return (obj, uid, name)

    def _launch(self, func, **args):
        cnt = RETRY_MAX
        while cnt >= 0:
            try:
                ret = func(**args)
                if ret:
                    return ret
            except:
                pass
            cnt -= 1
            if cnt >= 0:
                time.sleep(RETRY_INTERVAL)

    def _init(self, uid, name, mode, vrtx, parent, freq, prof, hndl, filt, disp, typ, timeout):
        if prof:
            if not typ:
                typ = prof.get('type')
            elif typ != prof.get('type'):
                log_err(self, 'failed to initialize, invalid type, type=%' % str(typ))
                raise FuseOSError(EINVAL)

        if not typ:
            log_err(self, 'failed to initialize, no type, name=%s' % str(name))
            raise FuseOSError(EINVAL)

        link = mode & MODE_LINK
        if link:
            mode &= ~MODE_LINK

        if mode & MODE_CLONE and not parent:
            log_err(self, 'failed to initialize, no parent, name=%s' % str(name))
            raise FuseOSError(EINVAL)

        if not mode & MODE_VIRT:
            timeout = None

        if not prof:
            if self._shadow:
                driver = load_driver(typ)
                if not driver:
                    log_err(self, 'failed to initialize, cannot load driver %s, name=%s' % (typ, str(name)))
                    raise FuseOSError(EINVAL)

                if mode & MODE_CLONE:
                    mode = driver.get_mode() | MODE_CLONE
                else:
                    mode = driver.get_mode()
                freq = driver.get_freq()
                prof = driver.get_profile()
            else:
                prof = {'type':typ}

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

        if disp:
            self._attr.initialize(uid, name, ATTR_DISPATCHER, disp)

        if timeout:
            self._attr.initialize(uid, name, ATTR_TIMEOUT, timeout)

        if mode & MODE_CLONE:
            self._attr.initialize(uid, name, ATTR_PARENT, parent)

        if vrtx:
            if mode & MODE_CLONE:
                log_err(self, 'failed to initialize, cannot set vertex for a cloned device, name=%s' % str(name))
                raise FuseOSError(EINVAL)

            self._vrtx.initialize(uid, name, vrtx)
            for v in vrtx:
                self._edge.initialize(uid, (v, name), hidden=True)

        if not self._shadow:
            if not link:
                if not self._launch(self._link.mount, uid=uid, name=name, mode=mode, vrtx=vrtx, typ=typ, parent=parent, timeout=timeout):
                    log_err(self, 'failed to initialize, cannot mount, name=%s' % str(name))
                    raise FuseOSError(EINVAL)
        else:
            if link and not mode & MODE_CLONE:
                self._manager.create(device_name(typ, name, mode), init=False)

    def _do_mount(self, uid, name, mode, vrtx, parent, freq=None, prof=None, hndl=None, filt=None, disp=None, typ=None, timeout=None):
        if not name:
            name = uuid.uuid4().hex

        if self._shadow and (mode == None or not mode & MODE_LINK):
            if mode != None:
                mode |= MODE_LINK

            if not self._launch(self._link.put, name=name, op=OP_ADD, mode=mode, freq=freq, prof=prof):
                log_err(self, 'failed to mount, link error, name=%s' % str(name))
                raise FuseOSError(EINVAL)

        if mode != None:
            self._init(uid, name, mode, vrtx, parent, freq, prof, hndl, filt, disp, typ, timeout)

        return name

    def _mount(self, path, value):
        if not value:
            log_err(self, 'failed to mount')
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
        vrtx = args.get('vertex')
        hndl = args.get('handler')
        parent = args.get('parent')
        timeout = args.get('timeout')
        disp = args.get('dispatcher')

        if freq and float(freq) > FREQ_MAX:
            log_err(self, 'failed to mount, invalid frequency, name=%s' % str(name))
            raise FuseOSError(EINVAL)

        if prof and type(prof) != dict:
            log_err(self, 'failed to mount, invalid profile, name=%s' % str(name))
            raise FuseOSError(EINVAL)

        if vrtx:
            if type(vrtx) != list:
                log_err(self, 'failed to mount, invalid vertex, name=%s' % str(name))
                raise FuseOSError(EINVAL)

            for i in vrtx:
                if not self._check_uid(i):
                    log_err(self, 'failed to mount, name=%s' % str(name))
                    raise FuseOSError(EINVAL)

        if timeout and float(timeout) > TIMEOUT_MAX:
            log_err(self, 'failed to mount, invalid timeout, name=%s' % str(name))
            raise FuseOSError(EINVAL)

        self._do_mount(uid, name, mode, vrtx, parent, freq, prof, hndl, filt, disp, typ, timeout)

    def getattr(self, path, fh=None):
        obj, uid, name = self._parse(path)
        if not name:
            return STAT_DIR
        if not obj:
            log_err(self, 'failed to getattr, no object, name=%s' % str(name))
            raise FuseOSError(EINVAL)
        return obj.getattr(uid, name)

    def _check_file(self, obj, name, flags=0, create=False):
        if not self._shadow:
            if obj.can_invalidate():
                if create or obj.may_update(flags):
                    self._log('start to invalidate, name=%s' % name)
                    if not self._launch(self._link.put, name=obj.parent(name), op=OP_INVALIDATE, path=obj.real(name)):
                        log_err(self, 'failed to check, cannot invalidate %s' % str(name))
                        raise FuseOSError(EINVAL)
            elif obj.can_touch():
                self._log('start to touch, name=%s' % name)
                if not self._launch(self._link.put, name=obj.parent(name), op=OP_TOUCH, path=obj.real(name)):
                    log_err(self, 'failed to check, cannot touch %s' % str(name))
                    raise FuseOSError(EINVAL)

    @named_lock_lock
    def _do_create(self, path, obj, uid, name):
        return obj.create(uid, name)

    def create(self, path, mode):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to create, no object, name=%s' % str(name))
            raise FuseOSError(EINVAL)
        self._check_file(obj, name, create=True)
        self._log('create, path=%s' % path)
        return self._do_create(path, obj, uid, name)

    def _update(self, obj, uid, name):
        if self._shadow:
            if obj.is_expired(uid, name):
                buf = self._launch(self._link.put, name=obj.parent(name), op=OP_GET, field=obj.field, item=obj.child(name), buf=obj.signature(uid, name))
                if buf:
                    obj.patch(uid, name, buf)

    @named_lock_lock
    def _do_open(self, path, flags, obj, uid, name):
        self._update(obj, uid, name)
        return obj.open(uid, name, flags)

    def open(self, path, flags):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to open, no object, name=%s' % str(name))
            raise FuseOSError(EINVAL)
        self._check_file(obj, name, flags=flags)
        self._log('open, path=%s' % path)
        return self._do_open(path, flags, obj, uid, name)

    @named_lock_unlock
    def _do_release(self, path, fh, obj, uid, name):
        if obj.release(uid, name, fh):
            try:
                obj.commit(uid, name)
            except:
                obj.discard(uid, name)

    def release(self, path, fh):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to release, no object, name=%s' % str(name))
            raise FuseOSError(EINVAL)
        self._do_release(path, fh, obj, uid, name)
        self._log('release, path=%s' % path)

    @show_path
    def readdir(self, path, fh):
        obj, uid, name = self._parse(path)
        res = []
        if obj:
            res = obj.readdir(uid, name)
        return res

    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, 0)
        return os.write(fh, buf)

    def read(self, path, size, offset, fh):
        os.lseek(fh, offset, 0)
        return os.read(fh, size)

    def _do_unlink(self, obj, uid, name):
        if not self._shadow:
            if obj.can_invalidate() or obj.can_unlink():
                if not self._launch(self._link.put, name=obj.parent(name), op=OP_INVALIDATE, path=obj.real(name)):
                    log_err(self, 'failed to unlink, link error, name=%s' % str(name))
                    raise FuseOSError(EINVAL)

    @show_path
    def unlink(self, path):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to unlink, no object, name=%s' % str(name))
            raise FuseOSError(EINVAL)
        self._do_unlink(obj, uid, name)
        obj.unlink(uid, name)

    def _create_device(self, path, op, attr):
        if self._shadow:
            return

        uid = self._get_uid(path)
        if not uid:
            log_err(self, 'failed to create device, no uid')
            raise FuseOSError(EINVAL)

        args = ast.literal_eval(attr)
        if type(args) != dict:
            log_err(self, 'failed to create device, invalid attr')
            raise FuseOSError(EINVAL)

        typ = args.get('type')
        mode = args.get('mode')
        parent = args.get('parent')
        timeout = args.get('timeout')

        if None == mode:
            mode = MODE_VIRT

        if op == OP_CLONE:
            if not parent:
                log_err(self, 'failed to create device, no parent')
                raise FuseOSError(EINVAL)

            prof = Loader(uid).get_profile(parent)
            typ = prof['type']
            mode |= MODE_CLONE

        if not typ:
            typ = VDEV

        vrtx = args.get('vertex')
        if vrtx:
            if mode & MODE_CLONE:
                log_err(self, 'failed to create device, invalid mode')
                raise FuseOSError(EINVAL)

            if type(vrtx) != list:
                log_err(self, 'failed to create device, invalid vertex')
                raise FuseOSError(EINVAL)

            for i in vrtx:
                if not self._check_uid(i):
                    log_err(self, 'failed to create device, invalid vertex')
                    raise FuseOSError(EINVAL)

        if parent and not self._check_uid(parent):
            log_err(self, 'failed to create device, invalid parent, parent=%s' % str(parent))
            raise FuseOSError(EINVAL)

        return self._do_mount(uid, None, mode, vrtx, parent, typ=typ, timeout=timeout)

    def _do_enable(self, obj, uid, name):
        if not self._shadow:
            if obj.can_enable():
                if not self._launch(self._link.put, name=name, op=OP_ENABLE, path=name):
                    log_err(self, 'failed to enable, link error, name=%s' % str(name))
                    raise FuseOSError(EINVAL)

    def _enable(self, path):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to enable, no object, name=%s' % str(name))
            raise FuseOSError(EINVAL)
        self._do_enable(obj, uid, name)

    def _do_disable(self, obj, uid, name):
        if not self._shadow:
            if obj.can_disable():
                if not self._launch(self._link.put, name=name, op=OP_DISABLE, path=name):
                    log_err(self, 'failed to disable, link error, name=%s' % str(name))
                    raise FuseOSError(EINVAL)

    def _disable(self, path):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to disable, no object, name=%s' % str(name))
            raise FuseOSError(EINVAL)
        self._do_disable(obj, uid, name)

    def _join(self, path, target):
        if not self._shadow:
            return
        obj, _, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to join, no object, name=%s' % str(name))
            raise FuseOSError(EINVAL)
        self._manager.guest.join(name, target)

    def _accept(self, path, target):
        if not self._shadow:
            return
        obj, _, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to accept, no object, name=%s' % str(name))
            raise FuseOSError(EINVAL)
        self._manager.guest.accept(name, target)

    @named_lock
    def _invalidate(self, path):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to invalidate, no object, name=%s' % str(name))
            raise FuseOSError(EINVAL)
        obj.invalidate(uid, name)

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

    def _scan(self, path):
        result = ''
        if not HISTORY or self._shadow:
            return result
        obj, uid, name = self._parse(path)
        if not obj or not obj.can_scan():
            return result
        if name:
            result = self._query.history.get(uid, name)
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
        if name.startswith(OP_POLL):
            op = OP_POLL
        elif name.startswith(OP_SCAN):
            op = OP_SCAN
        elif name.startswith(OP_CLONE):
            op = OP_CLONE
        elif name.startswith(OP_CREATE):
            op = OP_CREATE
        elif name.startswith(OP_COMBINE):
            op = OP_COMBINE
        else:
            return ''
        res = self._get_result(path, op)
        if res:
            return res
        if op == OP_POLL:
            res = self._poll(path)
        elif op == OP_SCAN:
            res = self._scan(path)
        elif op == OP_CLONE:
            res = self._create_device(path, OP_CLONE, name[len(OP_CLONE) + 1:])
        elif op == OP_CREATE:
            res = self._create_device(path, OP_CREATE, name[len(OP_CREATE) + 1:])
        elif op == OP_COMBINE:
            res = self._create_device(path, OP_COMBINE, name[len(OP_COMBINE) + 1:])
        self._set_result(path, op, res)
        return res

    @show_path
    def truncate(self, path, length, fh=None):
        if fh:
            os.ftruncate(fh, length)
        else:
            obj, uid, name = self._parse(path)
            if not obj:
                log_err(self, 'failed to truncate, no object, name=%s' % str(name))
                raise FuseOSError(EINVAL)
            obj.truncate(uid, name, length)

    @show_path
    def readlink(self, path):
        obj, uid, name = self._parse(path)
        if not obj:
            log_err(self, 'failed to read link, no object, name=%s' % str(name))
            raise FuseOSError(EINVAL)
        return obj.readlink(uid, name)
