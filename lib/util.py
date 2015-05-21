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
from op import OP_MOUNT
from netifaces import interfaces, ifaddresses, AF_INET

import sys
sys.path.append('..')
from conf.virtdev import IFNAME, MOUNTPOINT

UID_SIZE = 32
TOKEN_SIZE = 32
PASSWORD_SIZE = 32
USERNAME_SIZE = UID_SIZE

DEFAULT_NAME = '__anon__'
DEFAULT_UID = '0' * UID_SIZE
DEFAULT_TOKEN = '1' * TOKEN_SIZE
INFO_FIELDS = ['mode', 'type', 'freq', 'range']

DIR_MODE = 0o755
FILE_MODE = 0o644

DEVNULL = open(os.devnull, 'wb')
DRIVER_PATH = os.path.join(os.getcwd(), 'drivers')

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
    if None == child:
        child = ''
    return uuid.uuid5(uuid.UUID(ns), os.path.join(str(parent), str(child))).hex

def dev_name(uid, node=None):
    if not node:
        node = get_node()
    ns = '%032x' % int(node, 16)
    return get_name(ns, str(uid), 'vdev')

def split(s):
    return str(s).split(":")

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

def mount_device(uid, name, mode, freq, prof):
    attr = {}
    attr.update({'name':name})
    attr.update({'mode':mode})
    attr.update({'freq':freq})
    attr.update({'prof':prof})
    path = os.path.join(MOUNTPOINT, uid)
    xattr.setxattr(path, OP_MOUNT, str(attr))

def update_device(query, uid, node, addr, name):
    query.device.put(name, {'uid':uid, 'addr':addr, 'node':node})
    query.member.remove(uid, (name,))
    query.member.put(uid, (name, node))

def load_driver(typ, name=None):
    try:
        module = imp.load_source(typ, os.path.join(DRIVER_PATH, '%s.py' % typ.lower()))
        if module and hasattr(module, typ):
            driver = getattr(module, typ)
            if driver:
                return driver(name=name)
    except:
        pass
    
def info(typ, mode=0, freq=None, rng=None):
    ret = {'type':typ, 'mode':mode}
    if freq:
        ret.update({'freq':freq})
    if range:
        ret.update({'range':rng})
    return ret

def check_info(buf):
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
