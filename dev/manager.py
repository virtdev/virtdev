# manager.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import re
import time
import shelve
from json import dumps
from lib import channel
from lib import notifier
from proc.core import Core
from proc.proc import Proc
from lib.lock import NamedLock
from lib.request import Request
from lib.modes import MODE_VISI
from conf.defaults import DEBUG
from lib.daemon import VDevDaemon
from threading import Lock, Thread
from conductor import Conductor, conductor
from lib.operations import OP_OPEN, OP_CLOSE
from lib.log import log_err, log_get, log_debug
from conf.virtdev import LO, BT, USB, FS, EXPOSE, COMP
from conf.defaults import PROC_ADDR, FILTER_PORT, HANDLER_PORT, DISPATCHER_PORT
from lib.util import USERNAME_SIZE, PASSWORD_SIZE, get_node, get_name, lock, named_lock, get_conf_path, get_mnt_path

SAFE = True
LOGIN_RETRY_MAX = 5
ALLOC_RETRY_MAX = 5
LOGIN_RETRY_INTERVAL = 5 # seconds
ALLOC_RETRY_INTERVAL = 5 # seconds

class DeviceManager(object):
    def __init__(self):
        self._lock = NamedLock()

    @named_lock
    def open(self, name):
        for parent in conductor.devices:
            device = parent.find(name)
            if device:
                device.proc(name, OP_OPEN)
                return

    @named_lock
    def close(self, name):
        for parent in conductor.devices:
            device = parent.find(name)
            if device:
                device.proc(name, OP_CLOSE)
                return

    def add(self, name, mode=None, freq=None, prof=None):
        return conductor.request.device.add(node=get_node(), addr=conductor.addr, name=name, mode=mode, freq=freq, prof=prof)

    def put(self, name, buf):
        return conductor.request.device.put(name=name, buf=buf)

    def find(self, name):
        return conductor.request.device.find(name=name)

    def get(self, name, field, item, buf):
        return conductor.request.device.get(name=name, field=field, item=item, buf=buf)

    def delete(self, name):
        return conductor.request.device.delete(node=get_node(), name=name)

class GuestManager(object):
    def __init__(self):
        self._lock = Lock()

    @lock
    def join(self, dest, src):
        return conductor.request.guest.join(user=conductor.user, dest=dest, src=src)

    @lock
    def accept(self, dest, src):
        return conductor.request.guest.accept(user=conductor.user, dest=dest, src=src)

    @lock
    def drop(self, dest, src):
        return conductor.request.guest.drop(dest=dest, src=src)

class NodeManager(object):
    def __init__(self):
        self._lock = Lock()

    @lock
    def find(self, user, node):
        return conductor.request.node.find(user=user, node=node)

class ChannelManager(object):
    def __init__(self):
        self._lock = NamedLock()

    def _allocate_channel_safe(self, uid, addr, key):
        try:
            channel.allocate(uid, addr, key)
            return True
        except:
            log_debug(self, "failed to allocate channel, uid=%s, addr=%s" % (str(uid), str(addr)))
            conductor.remove_key(uid, node)
            conductor.remove_device(name)
            return False

    def _allocate_channel(self, uid, addr, key):
        channel.allocate(uid, addr, key)
        return True

    def _allocate(self, name):
        uid, node, addr = conductor.get_device(name)
        if not uid:
            log_err(self, 'failed to allocate, no uid, name=%s' % str(name))
            conductor.remove_device(name)
            return False
        key = conductor.get_key(uid, node)
        if not key:
            log_err(self, 'failed to allocate, no key, name=%s' % str(name))
            conductor.remove_key(uid, node)
            return False
        if DEBUG and not SAFE:
            return self._allocate_channel(uid, addr, key)
        else:
            return self._allocate_channel_safe(uid, addr, key)

    def _allocate_safe(self, name):
        try:
            return self._allocate(name)
        except:
            log_debug(self, 'failed to allocate, name=%s' % str(name))

    @named_lock
    def allocate(self, name):
        cnt = ALLOC_RETRY_MAX
        while cnt >= 0:
            if DEBUG or not SAFE:
                if self._allocate(name):
                    return
            else:
                if self._allocate_safe(name):
                    return
            cnt -= 1
            if cnt >= 0:
                time.sleep(ALLOC_RETRY_INTERVAL)
        log_err(self, 'failed to allocate')
        raise Exception(log_get(self, 'failed to allocate'))

    @named_lock
    def free(self, name):
        _, _, addr = conductor.get_device(name)
        channel.free(addr)

    @named_lock
    def put(self, dest, src, **args):
        token = conductor.token
        uid, _, addr = conductor.get_device(dest)
        if uid != conductor.uid:
            uid = src
        args.update({'dest': dest, 'src':src})
        channel.put(uid, addr, 'put', args, token)

class MemberManager(object):
    def __init__(self):
        self._lock = Lock()
        self._path = os.path.join(get_conf_path(), get_name(conductor.uid, get_node()))

    @lock
    def list(self):
        d = shelve.open(self._path)
        try:
            keys = d.keys()
            if len(keys) > 0:
                i = keys[0]
                res = dumps({'name':i, 'user':d[i]['user'], 'node':d[i]['node'], 'state':d[i]['state']})
                for j in range(1, len(keys)):
                    i = keys[j]
                    res += ';' + dumps({'name':i, 'user':d[i]['user'], 'node':d[i]['node'], 'state':d[i]['state']})
                return res
        finally:
            d.close()

    @lock
    def update(self, item):
        if type(item) != dict or len(item) != 1:
            log_err(self, 'failed to update, invalid type')
            return
        d = shelve.open(self._path)
        try:
            d.update(item)
        finally:
            d.close()

    @lock
    def delete(self, name):
        name = str(name)
        d = shelve.open(self._path)
        try:
            if d.has_key(name):
                del d[name]
        finally:
            d.close()

class Manager(object):
    def __init__(self):
        self._lo = None
        self._bt = None
        self.uid = None
        self.key = None
        self._usb = None
        self.addr = None
        self.guest = None
        self.token = None
        self.devices = []
        self.device = None
        self.channel = None
        self._daemon = None
        self._filter = None
        self._ready = False
        self._handler = None
        self._active = False
        self._listener = None
        self._dispatcher = None
        self._init()

    def _init(self):
        if not FS:
            return
        self._init_user()
        self._init_proc()
        self._init_manager()
        self._init_core()
        self._init_devices()
        if EXPOSE:
            self._init_daemon()

    def _init_proc(self):
        self._filter = Proc(self, PROC_ADDR, FILTER_PORT)
        self._handler = Proc(self, PROC_ADDR, HANDLER_PORT)
        self._dispatcher = Proc(self, PROC_ADDR, DISPATCHER_PORT)
        self._filter.start()
        self._handler.start()
        self._dispatcher.start()

    def _init_manager(self):
        Conductor().create(self)
        self.node = NodeManager()
        self.guest = GuestManager()
        self.device = DeviceManager()
        self.member = MemberManager()
        self.channel = ChannelManager()

    def _init_daemon(self):
        self._daemon = VDevDaemon(self)
        self._daemon.start()

    def _init_devices(self):
        if BT:
            from interface.bt import Bluetooth
            self._bt = Bluetooth(self.uid, self.core)
            self.devices.append(self._bt)

        if LO:
            from interface.lo import Lo
            self._lo = Lo(self.uid, self.core)
            self.devices.append(self._lo)

        if USB:
            from interface.usb import USBSerial
            self._usb = USBSerial(self.uid, self.core)
            self.devices.append(self._usb)

    def _init_core(self):
        self.core = Core(self)

    def _init_user(self):
        user, password = self._get_password()
        if not user or not password:
            log_err(self, 'failed to initialize, no password')
            raise Exception(log_get(self, 'failed to initialize'))

        try:
            uid, addr, token, key = self._login(user, password)
        except:
            log_err(self, 'failed to initialize')
            raise Exception(log_get(self, 'failed to initialize'))

        if not uid:
            log_err(self, 'failed to initialize, no uid')
            raise Exception(log_get(self, 'failed to initialize'))

        self.uid = uid
        self.key = key
        self.addr = addr
        self.user = user
        self.token = token

    def _get_password(self):
        path = os.path.join(get_conf_path(), 'user')
        if os.path.exists(path):
            d = shelve.open(path)
            try:
                user = d['user']
                password = d['password']
                return (user, password)
            finally:
                d.close()
        else:
            from conf.user import USER, PASSWORD
            return (USER, PASSWORD)

    def _login(self, user, password):
        length = len(user)
        if length > 0 and length < USERNAME_SIZE and re.match('^[0-9a-zA-Z]+$', user):
            name = user + (USERNAME_SIZE - length) * '*'
        else:
            log_err(self, 'failed to login, invalid user name')
            return

        if len(password) != PASSWORD_SIZE:
            log_err(self, 'failed to login, invalid password')
            return

        if EXPOSE:
            mode = MODE_VISI
        else:
            mode = 0

        cnt = LOGIN_RETRY_MAX
        while cnt >= 0:
            try:
                req = Request(name, password)
                res = req.user.login(node=get_node(), mode=mode)
                if res:
                    log_debug(self, 'login, uid=%s, addr=%s' % (res['uid'], res['addr']))
                    if not channel.has_network(res['addr']):
                        return (res['uid'], res['addr'], res['token'], res['key'])
            except:
                pass
            cnt -= 1
            if cnt >= 0:
                time.sleep(LOGIN_RETRY_INTERVAL)
        log_err(self, 'failed to login')

    def _start(self):
        while not self.uid:
            time.sleep(0.1)
        path = get_mnt_path(self.uid)
        while not os.path.exists(path):
            time.sleep(0.1)
        for device in self.devices:
            device.start()
        self._ready = True

    def start(self):
        if not self._active:
            self._active = True
            Thread(target=self._start).start()

    def chkaddr(self, name):
        if name:
            uid, node, addr = conductor.get_device(name)
            key = conductor.get_key(uid, node)
            if key:
                return (addr, key)

    def chknode(self):
        if self._ready:
            return (get_node(), self.addr)
        else:
            return ('', '')

    def notify(self, op, buf):
        notifier.notify(op, buf)

    def has_lo(self):
        return self._lo != None

    def create(self, device, init):
        if self._lo:
            return self._lo.create(device, init)

    @property
    def compute_unit(self):
        if COMP:
            if self._lo:
                return self._lo.compute_unit
