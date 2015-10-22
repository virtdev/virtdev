#      util.py
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
import imp
import uuid
import xattr
import struct
import collections
from datetime import datetime
from pynetlinux import ifconfig
from op import OP_MOUNT, OP_INVALIDATE
from netifaces import AF_INET, interfaces, ifaddresses

import sys
sys.path.append('..')
from conf.virtdev import IFNAME
from conf.path import PATH_MOUNTPOINT

UID_SIZE = 32
TOKEN_SIZE = 32
PASSWORD_SIZE = 32
USERNAME_SIZE = UID_SIZE
INFO_FIELDS = ['mode', 'type', 'freq', 'spec']

DIR_MODE = 0o755
FILE_MODE = 0o644

USER_DOMAIN = 'U'
DEVICE_DOMAIN = 'D'

DEVNULL = open(os.devnull, 'wb')
PATH_DRIVER = os.path.join(os.getcwd(), 'drivers')

_ifaddr = None

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

def hash_name(name):
    length = len(name)
    if length > 1:
        return (ord(name[-2]) << 8) + ord(name[-1])
    elif length == 1:
        return ord(name[-1])
    else:
        return 0

def maskaddr(ifname):
    iface = ifaddresses(ifname)[AF_INET][0]
    address = iface['addr']
    addr = address.split('.')
    netmask = iface['netmask']
    mask = netmask.split('.')
    res = ''
    for i in range(len(addr) - 1):
        if mask[i] == '255':
            res += addr[i] + '.'
        else:
            break
    return res

def ifaces():
    f = lambda x:x != 'lo' and ifaddresses(x).has_key(AF_INET)
    return filter(f, interfaces())

def netaddresses(mask=False):
    if not mask:
        return map(ifaddr, ifaces())
    else:
        return map(maskaddr, ifaces())

def srv_start(srv_list):
    for srv in srv_list:
        srv.start()

def srv_join(srv_list):
    for srv in srv_list:
        srv.join()

def str2tuple(s):
    items = str(s).split('|')
    return tuple((i for i in items))

def tuple2str(v):
    return reduce(lambda x, y: x + '|' + y, v)

def send_pkt(sock, buf):
    head = struct.pack('I', len(buf))
    sock.sendall(head)
    if buf:
        sock.sendall(buf)

def recv_bytes(sock, length):
    ret = []
    while length > 0:
        buf = sock.recv(min(length, 2048))
        if not buf:
            raise Exception('failed to receive')
        ret.append(buf)
        length -= len(buf) 
    return ''.join(ret)

def recv_pkt(sock):
    head = recv_bytes(sock, 4)
    if not head:
        return ''
    length = struct.unpack('I', head)[0]
    return recv_bytes(sock, length)

def close_port(port):
    cmd = 'lsof -i:%d -Fp | cut -c2- | xargs --no-run-if-empty kill -9' % port
    os.system(cmd)

def get_node():
    return '%x' % uuid.getnode()

def get_name(ns, parent, child=None):
    if child == None:
        child = ''
    return uuid.uuid5(uuid.UUID(ns), os.path.join(str(parent), str(child))).hex

def cat(*items):
    ret = ''
    if len(items):
        ret = str(items[0])
        for i in range(1, len(items)):
            ret += ':%s' % str(items[i])
    return ret

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

def mount(uid, attr):
    path = os.path.join(PATH_MOUNTPOINT, uid)
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
    query.member.remove(uid, (name,))
    query.member.put(uid, (name, node))

def load_driver(typ, name=None):
    try:
        driver_name = typ.lower()
        module = imp.load_source(typ, os.path.join(PATH_DRIVER, driver_name, '%s.py' % driver_name))
        if module and hasattr(module, typ):
            driver = getattr(module, typ)
            if driver:
                return driver(name=name)
    except:
        pass

def device_info(buf):
    try:
        info = ast.literal_eval(buf)
        if type(info) != dict:
            return
        for i in info:
            for j in info[i].keys():
                if j not in INFO_FIELDS:
                    return
        return info
    except:
        pass

def server_list(addr, area=None):
    if not addr or type(addr) != list:
        return
    length = len(addr)
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
    return map(lambda a, b, c, d, e: (t, int(a), b, length, c, d, e), area, range(length), pos, peer, addr)

def unicode2str(buf):
    if isinstance(buf, basestring):
        return str(buf)
    elif isinstance(buf, collections.Mapping):
        return dict(map(unicode2str, buf.iteritems()))
    elif isinstance(buf, collections.Iterable):
        return type(buf)(map(unicode2str, buf))
    else:
        return buf

def path2temp(path):
    return path + '~'

def invalidate(path):
    xattr.setxattr(path, OP_INVALIDATE, "", symlink=True)
    
def cmd(op, args):
    return op + ':' + str(args)
