# util.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import ast
import sys
import uuid
import psutil
import struct
import getpass
import subprocess
import collections
from signal import SIGKILL
from socket import inet_aton
from datetime import datetime
from conf.log import LOG_LOCK
from lib.log import log_err, log
from pynetlinux.ifconfig import Interface
from SocketServer import ThreadingTCPServer
from lib.fields import FIELDS, FIELD_DATA, DATA, ATTR
from netifaces import AF_INET, interfaces, ifaddresses

_bin = subprocess.check_output(['readlink', '-f', sys.argv[0]])
_path = os.path.dirname(_bin)
_dir = os.path.dirname(_path)
sys.path.append(_dir)

from conf.env import *
from conf.virtdev import *
from conf.log import LOG_UTIL

UID_SIZE = 32
TOKEN_SIZE = 32
PASSWORD_SIZE = 32
USERNAME_SIZE = UID_SIZE

CHKPID_RETRY_MAX = 5
CHKPID_INTERVAL = 3 # seconds

DIR_MODE = 0o755
FILE_MODE = 0o644

DEVNULL = open(os.devnull, 'wb')

if LOG_UTIL:
    OUTPUT = None
else:
    OUTPUT = DEVNULL

INFO = ['mode', 'type', 'freq', 'spec']

_node = None
_ifaddr = None
_mnt = PATH_MNT
_var = PATH_VAR
_conf = PATH_CONF

def _log_lock(text):
    if LOG_LOCK:
        log('Lock: %s' % str(text))

def set_mnt_path(path):
    if not path:
        raise Exception('failed to set mnt path')
    global _mnt
    _mnt = path

def set_var_path(path):
    if not path:
        raise Exception('failed to set var path')
    global _var
    _var = path

def set_edgenode():
    from conf.defaults import EDGENODE_MNT, EDGENODE_VAR
    set_mnt_path(EDGENODE_MNT)
    set_var_path(EDGENODE_VAR)

def get_dir():
    return _dir

def readlink(path):
    if path.startswith('..'):
        home = subprocess.check_output(['readlink', '-f', '..'])
        path = path[2:]
    elif path.startswith('.'):
        home = subprocess.check_output(['readlink', '-f', path[0]])
        path = path[1:]
    elif path.startswith('~'):
        user = getpass.getuser()
        if user == 'root':
            home = '/root'
        else:
            home = os.path.join('/home', user)
        path = path[1:]
    else:
        if not path.startswith('/'):
            return os.path.join(get_dir(), path)
        else:
            return path

    if not path.startswith('/'):
        raise Exception('Error: failed to read link')

    path = path[1:]
    if path.startswith('.') or path.startswith('/'):
        raise Exception('Error: failed to read link')

    return os.path.join(home, path)

def get_conf_path():
    global _conf
    path = readlink(_conf)
    if _conf != path:
        _conf = path
    return path

def get_mnt_path(uid=None, name=None):
    global _mnt
    path = readlink(_mnt)
    if _mnt != path:
        _mnt = path
    if uid:
        path = os.path.join(path, uid)
        if name:
            path = os.path.join(path, name)
    return path

def get_var_path(uid=None):
    global _var
    path = readlink(_var)
    if _var != path:
        _var = path
    if uid:
        path = os.path.join(path, uid)
    return path

def get_network(ifname):
    iface = ifaddresses(ifname)[AF_INET][0]
    addr = struct.unpack("I", inet_aton(iface['addr']))[0]
    mask = struct.unpack("I", inet_aton(iface['netmask']))[0]
    return (addr, mask)

def get_ifaces():
    f = lambda x:x != 'lo' and ifaddresses(x).has_key(AF_INET)
    return filter(f, interfaces())

def get_networks():
    return map(get_network, get_ifaces())

def get_node():
    global _node
    if not _node:
        _node = '%x' % uuid.getnode()
    return _node

def get_name(ns, parent, child=None):
    if child == None:
        child = ''
    return uuid.uuid5(uuid.UUID(ns), os.path.join(str(parent), str(child))).hex

def get_temp(path):
    return path + '~'

def get_devices(uid, name='', field='', sort=False):
    if not name and not field:
        path = get_mnt_path(uid)
    else:
        if not field:
            field = FIELD_DATA
        elif not FIELDS.get(field):
            log_err(None, 'invalid filed %s' % str(field))
            return
        mnt = get_mnt_path(uid)
        path = os.path.join(mnt, field, name)
    if not os.path.exists(path):
        return
    if not sort:
        return os.listdir(path)
    else:
        key = lambda f: os.stat(os.path.join(path, f)).st_mtime
        return sorted(os.listdir(path), key=key)

def gen_uid():
    return uuid.uuid4().hex

def gen_key():
    return uuid.uuid4().hex

def gen_token():
    return uuid.uuid4().hex

def get_cmd(op, args):
    return op + ':' + str(args)

def zmqaddr(addr, port):
    return 'tcp://%s:%d' % (str(addr), int(port))

def ifaddr(ifname=IFNAME):
    global _ifaddr
    if ifname == IFNAME and _ifaddr:
        return _ifaddr
    else:
        iface = ifaddresses(ifname)[AF_INET][0]
        addr = iface['addr']
        if ifname == IFNAME:
            _ifaddr = addr
        return addr

def ifdown(ifname):
    Interface(ifname).down()

def unicode2str(buf):
    if isinstance(buf, basestring):
        return str(buf)
    elif isinstance(buf, collections.Mapping):
        return dict(map(unicode2str, buf.iteritems()))
    elif isinstance(buf, collections.Iterable):
        return type(buf)(map(unicode2str, buf))
    else:
        return buf

def str2tuple(s):
    items = str(s).split('|')
    return tuple((i for i in items))

def tuple2str(v):
    return reduce(lambda x, y: x + '|' + y, v)

def lock(func):
    def _lock(*args, **kwargs):
        self = args[0]
        self._lock.acquire()
        try:
            return func(*args, **kwargs)
        finally:
            self._lock.release()
    return _lock

def named_lock(func):
    def _named_lock(*args, **kwargs):
        self = args[0]
        name = args[1]
        _log_lock('lock, name=%s' % str(name))
        self._lock.acquire(name)
        try:
            return func(*args, **kwargs)
        finally:
            self._lock.release(name)
            _log_lock('unlock, name=%s' % str(name))
    return _named_lock

def named_lock_lock(func):
    def _named_lock_lock(*args, **kwargs):
        self = args[0]
        name = args[1]
        ret = None
        finish = False
        _log_lock('lock, name=%s' % str(name))
        self._lock.acquire(name)
        try:
            ret = func(*args, **kwargs)
            finish = True
        finally:
            if not finish:
                self._lock.release(name)
        return ret
    return _named_lock_lock

def named_lock_unlock(func):
    def _named_lock_unlock(*args, **kwargs):
        self = args[0]
        name = args[1]
        try:
            return func(*args, **kwargs)
        finally:
            self._lock.release(name)
            _log_lock('unlock, name=%s' % str(name))
    return _named_lock_unlock

def edge_lock(func):
    def _edge_lock(*args, **kwargs):
        self = args[0]
        edge = args[1]
        self._lock.acquire(edge[0])
        try:
            return func(*args, **kwargs)
        finally:
            self._lock.release(edge[0])
    return _edge_lock

def close_port(port):
    cmd = 'lsof -i:%d -Fp | cut -c2- | xargs --no-run-if-empty kill -9 2>/dev/null' % port
    os.system(cmd)

def update_device(query, uid, node, addr, name):
    query.device.put(name, {'uid':uid, 'addr':addr, 'node':node})
    query.member.delete(uid, (name,))
    query.member.put(uid, (name, node))

def save_device(manager, name, buf):
    if type(buf) != str and type(buf) != unicode:
        buf = str(buf)
    path = os.path.join(get_var_path(manager.uid), DATA, name)
    with open(path, 'wb') as f:
        f.write(buf)
    manager.device.put(name, buf)

def device_info(buf):
    try:
        info = ast.literal_eval(buf)
        if type(info) != dict:
            return
        for i in info:
            for j in info[i]:
                if j not in INFO:
                    return
        return info
    except:
        pass

def is_local(uid, name):
    path = os.path.join(get_var_path(uid), ATTR, name)
    return os.path.exists(path)

def start_servers(servers):
    for srv in servers:
        srv.start()

def wait_servers(servers):
    for srv in servers:
        srv.join()

def stop_servers(ports):
    for port in ports:
        close_port(port)

def create_server(addr, port, handler):
    server = ThreadingTCPServer((addr, port), handler, bind_and_activate=False)
    server.allow_reuse_address = True
    server.server_bind()
    server.server_activate()
    server.serve_forever()

def server_info(addresses, area=None):
    if not addresses or type(addresses) != list:
        return
    length = len(addresses)
    if not area:
        t = 0.0
        area = [0] * length
    else:
        if type(area) != list or len(area) != length:
            return
        t = (datetime.utcnow() - datetime(1970, 1, 1)).total_seconds()
    j = 0
    cnt = -1
    pos = []
    grp = []
    grp_sz = []
    left = area[0]
    if length == 1:
        pos = [0]
        peer = [1]
    else:
        for i in range(length):
            if area[i] != left:
                left = area[i]
                grp_sz.append(cnt + 1)
                cnt = 0
                j += 1
            else:
                cnt += 1
            if i == length - 1:
                grp_sz.append(cnt + 1)
            pos.append(cnt)
            grp.append(j)
        peer = map(lambda x: grp_sz[x], grp)
    return map(lambda a, b, c, d, e: (t, int(a), b, length, c, d, e), area, range(length), pos, peer, addresses)

def popen(*args):
    if OUTPUT:
        return subprocess.Popen(args, stderr=OUTPUT, stdout=OUTPUT).pid
    else:
        return subprocess.Popen(args).pid

def call(*args):
    if OUTPUT:
        subprocess.call(args, stderr=OUTPUT, stdout=OUTPUT)
    else:
        subprocess.call(args)

def pkill(name):
    os.system('killall -9 %s 2>/dev/null' % name)

def sigkill(pid):
    os.kill(pid, SIGKILL)

def status_zobie(pid):
    return psutil.Process(pid).status() == psutil.STATUS_ZOMBIE

def chkpid(pid):
    for i in range(CHKPID_RETRY_MAX + 1):
        try:
            if status_zobie(pid):
                return False
            else:
                return True
        except:
            if i != CHKPID_RETRY_MAX:
                time.sleep(CHKPID_INTERVAL)
    return False

def mkdir(path):
    os.system('mkdir -p %s' % path)

def _touch(path):
    fd = os.open(path, os.O_CREAT)
    os.close(fd)

def touch(path):
    try:
        _touch(path)
    except:
        _touch(path)
