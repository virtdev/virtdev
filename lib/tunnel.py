#      tunnel.py
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
import time
import crypto
import socket
import psutil
import signal
from log import log
from subprocess import Popen
from util import DEFAULT_UID, DEFAULT_TOKEN, ifaddr, send_pkt, recv_pkt
from conf.virtdev import VDEV_SUPERNODE_PORT, VDEV_SUPERNODES, VDEV_FS_PORT

NETSIZE = 30
NETMASK = '255.255.255.224'
PATH = '/etc/dhcp/dhcpd.conf'
DEVNULL = open(os.devnull, 'wb')
RETRY_MAX = 4
SLEEP_TIME = 0.3 # seconds

def _split(addr):
    return addr.split(':')

def _iface(addr): 
    name = ''
    fields = _split(addr)[1].split('.')
    for i in range(1, 4):
        name += '%02x' % int(fields[i])
    return name

def _tunnel(addr):
    grp, addr = _split(addr)
    if int(grp) > 255:
        raise Exception('invalid address')
    name = '%02x' % int(grp)
    fields = addr.split('.')
    for i in range(1, 4):
        name += '%02x' % int(fields[i])
    return name

def _chkiface(addr):
    address = None
    for _ in range(RETRY_MAX):
        try:
            ifname = _iface(addr)
            address = ifaddr(ifname)
            if address:
                return address
            time.sleep(SLEEP_TIME)
        except:
            time.sleep(SLEEP_TIME)
    log('tunnel: failed to check')      
    raise Exception('tunnel: failed to check')

def _check_pid(pid):
    for _ in range(RETRY_MAX):
        try:
            if psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
                return False
            else:
                time.sleep(SLEEP_TIME)
                return True
        except:
            pass
    return False

def _run_path(addr):
    return '/var/run/tunnel-%s.pid' % _iface(addr)

def _supernode(addr):
    code = 0
    odd = 1
    length = len(VDEV_SUPERNODES)
    if length >= (1 << 16):
        log('tunnel: too much supernodes')
        raise Exception('tunnel: too much supernodes')
    for i in addr:
        if odd:
            code ^= ord(i)
            odd = 0
        else:
            code ^= ord(i) << 8
            odd = 1
    return '%s:%d' % (VDEV_SUPERNODES[code % length], VDEV_SUPERNODE_PORT)

def _is_gateway(addr):
    address = ifaddr(_iface(addr))
    fields = address.split('.')
    return int(fields[3]) == 1

def send(sock, buf):
    send_pkt(sock, buf)

def recv(sock):
    return recv_pkt(sock)

def addr2ip(addr):
    return _split(addr)[1]

def get_reserved_address(addr):
    ip = addr2ip(addr)
    fields = ip.split('.')
    n = int(fields[3])
    return '%s.%s.%s.%d' % (fields[0], fields[1], fields[2], n + NETSIZE - 1)

def create(addr, key):
    cfg = []
    address = _split(addr)[1]
    fields = address.split('.')
    n = int(fields[3])
    subnet = '%s.%s.%s.%d' % (fields[0], fields[1], fields[2], n - 1)
    start = '%s.%s.%s.%d' % (fields[0], fields[1], fields[2], n + 1)
    end = '%s.%s.%s.%d' % (fields[0], fields[1], fields[2], n + NETSIZE - 2)
    broadcast = '%s.%s.%s.%d' % (fields[0], fields[1], fields[2], n + NETSIZE)
    cfg.append('ddns-update-style none;\n')
    cfg.append('default-lease-time 14400;\n')
    cfg.append('max-lease-time 36000;\n')
    cfg.append('subnet %s netmask %s {\n' % (subnet, NETMASK))
    cfg.append('  range %s %s;\n' % (start, end))
    cfg.append('  option subnet-mask %s;\n' % NETMASK) 
    cfg.append('  option broadcast-address %s;\n' % broadcast)
    cfg.append('}\n')
    with open(PATH, 'w') as f:
        f.writelines(cfg)
    pid = Popen(['edge', '-r', '-d', _iface(addr), '-a', address, '-s', NETMASK, '-c', _tunnel(addr), '-k', key, '-l', _supernode(addr)], stdout=DEVNULL, stderr=DEVNULL).pid
    with open(_run_path(addr), 'w') as f:
        f.write(str(pid))
    os.system('killall -q dhcpd')
    os.system('dhcpd -q')
    return _chkiface(addr)

def connect(addr, key, static=False):
    if not static:
        address = '0.0.0.0'
    else:
        address = get_reserved_address(addr)
    pid = Popen(['edge', '-r', '-d', _iface(addr), '-a', address, '-s', NETMASK, '-c', _tunnel(addr), '-k', key, '-l', _supernode(addr)], stdout=DEVNULL, stderr=DEVNULL).pid
    if not _check_pid(pid):
        log('tunnel: failed to connect')
        raise Exception('tunnel: failed to connect')
    with open(_run_path(addr), 'w') as f:
        f.write(str(pid))
    if not static:
        os.system('dhclient -q %s' % _iface(addr))
    return _chkiface(addr)

def release(addr, force=False):
    if not force:
        if _is_gateway(addr):
            return
    path = _run_path(addr)
    with open(path, 'r') as f:
        pid = int(f.readlines()[0].strip())
    try:
        if psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
            log('tunnel: failed to release')
            return
    except:
        log('tunnel: failed to release')
        return
    os.kill(pid, signal.SIGKILL)
    os.remove(path)

def disconnect(addr, force=False):
    release(addr, force)

def exist(addr):
    return os.path.exists(_run_path(addr))

def put(addr, op, args, uid=DEFAULT_UID, token=DEFAULT_TOKEN):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((addr, VDEV_FS_PORT))
    try:
        req = {'op':op, 'args':args}
        msg = crypto.pack(uid, req, token)
        send(sock, msg)
        ret = recv(sock)
        return crypto.unpack(uid, ret, token)
    finally:
        sock.close()
