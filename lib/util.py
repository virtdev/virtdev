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
import uuid
import struct
from netifaces import interfaces, ifaddresses, AF_INET

import sys
sys.path.append('..')
from conf.virtdev import VDEV_IFNAME

UID_SIZE = 32
TOKEN_SIZE = 32
PASSWORD_SIZE = 32
USERNAME_SIZE = UID_SIZE

DEFAULT_UID = '0' * UID_SIZE
DEFAULT_TOKEN = '1' * TOKEN_SIZE
DEFAULT_NAME = '__anon__'

DIR_MODE = 0o755
FILE_MODE = 0o644
VDEV_FLAG_SPECIAL = 0x0001

_default_addr = None

def zmqaddr(addr, port):
    return 'tcp://%s:%d' % (str(addr), int(port))

def ifaddr(ifname=VDEV_IFNAME):
    global _default_addr
    if ifname == VDEV_IFNAME and _default_addr:
        return _default_addr
    else:
        iface = ifaddresses(ifname)[AF_INET][0]
        addr = iface['addr']
        if ifname == VDEV_IFNAME:
            _default_addr = addr
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

def service_start(*args):
    for srv in args:
        srv.start()

def service_join(*args):
    for srv in args:
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

def _recv(sock, length):
    ret = []
    while length > 0:
        buf = sock.recv(min(length, 2048))
        if not buf:
            raise Exception('failed to receive')
        ret.append(buf)
        length -= len(buf) 
    return ''.join(ret)

def recv_pkt(sock):
    head = _recv(sock, 4)
    if not head:
        return ''
    length = struct.unpack('I', head)[0]
    return _recv(sock, length)

def close_port(port):
    cmd = 'lsof -i:%d -Fp | cut -c2- | xargs --no-run-if-empty kill -9' % port
    os.system(cmd)

def get_node():
    return '%x' % uuid.getnode()

def get_name(ns, parent, child=''):
    return uuid.uuid5(uuid.UUID(ns), os.path.join(parent, child)).hex

def vdev_name(uid, node=None):
    if node:
        if type(node) == str or type(node) == unicode:
            node = int(node, 16)
    else:
        node = uuid.getnode()
    ns = '%032x' % node
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
