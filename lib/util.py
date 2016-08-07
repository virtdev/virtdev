#      util.py
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

import os
import ast
import sys
import uuid
import xattr
import psutil
import struct
import commands
import subprocess
import collections
from log import log_err
from signal import SIGKILL
from socket import inet_aton
from datetime import datetime
from pynetlinux.ifconfig import Interface
from SocketServer import ThreadingTCPServer
from operations import OP_MOUNT, OP_INVALIDATE
from fields import FIELDS, FIELD_DATA, DATA, ATTR
from netifaces import AF_INET, interfaces, ifaddresses

_bin = commands.getoutput('readlink -f %s' % sys.argv[0])
_path = os.path.dirname(_bin)
_dir = os.path.dirname(_path)
sys.path.append(_dir)

from conf.log import LOG_UTIL
from conf.virtdev import IFNAME
from conf.env import PATH_MNT, PATH_VAR

UID_SIZE = 32
TOKEN_SIZE = 32
PASSWORD_SIZE = 32
USERNAME_SIZE = UID_SIZE

DIR_MODE = 0o755
FILE_MODE = 0o644

if LOG_UTIL:
    OUTPUT = None
else:
    OUTPUT = open(os.devnull, 'wb')
INFO = ['mode', 'type', 'freq', 'spec']

_node = None
_ifaddr = None

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
        path = os.path.join(PATH_MNT, uid)
    else:
        if not field:
            field = FIELD_DATA
        elif not FIELDS.get(field):
            log_err(None, 'invalid filed %s' % str(field))
            return
        path = os.path.join(PATH_MNT, uid, field, name)
    if not os.path.exists(path):
        return
    if not sort:
        return os.listdir(path)
    else:
        key = lambda f: os.stat(os.path.join(path, f)).st_mtime
        return sorted(os.listdir(path), key=key)

def get_dir():
    return _dir

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
        self._lock.acquire(name)
        try:
            return func(*args, **kwargs)
        finally:
            self._lock.release(name)
    return _named_lock

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

def mount(uid, attr):
    path = os.path.join(PATH_MNT, uid)
    xattr.setxattr(path, OP_MOUNT, str(attr))

def mount_device(uid, name, mode, freq, prof):
    attr = {}
    attr.update({'name':name})
    attr.update({'mode':mode})
    attr.update({'freq':freq})
    attr.update({'prof':prof})
    mount(uid, attr)

def update_device(query, uid, node, addr, name):
    query.device.put(name, {'uid':uid, 'addr':addr, 'node':node})
    query.member.delete(uid, (name,))
    query.member.put(uid, (name, node))

def device_sync(manager, name, buf):
    if type(buf) != str and type(buf) != unicode:
        buf = str(buf)
    path = os.path.join(PATH_VAR, manager.uid, DATA, name)
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

def touch(path):
    call('touch', path)

def invalidate(path):
    xattr.setxattr(path, OP_INVALIDATE, "", symlink=True)

def is_local(uid, name):
    path = os.path.join(PATH_VAR, uid, ATTR, name)
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
