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
from log import log
from lock import VDevLock
from pool import VDevPool
from queue import VDevQueue
from subprocess import Popen, call
from conf.virtdev import SUPERNODE_PORT, SUPERNODES, CONDUCTOR_PORT, RUN_PATH
from util import DEVNULL, DEFAULT_UID, DEFAULT_TOKEN, ifaddr, send_pkt, recv_pkt, split, named_lock

NETSIZE = 30
QUEUE_LEN = 2
POOL_SIZE = 64
RETRY_MAX = 50
CREATE_MAX = 2
SLEEP_TIME = 0.1 # seconds
TOUCH_TIMEOUT = 1 # seconds
DHCP_CLEAN = True
NETMASK = '255.255.255.224'
PATH = '/etc/dhcp/dhcpd.conf'

class TunnelQueue(VDevQueue):
    def __init__(self):
        VDevQueue.__init__(self, QUEUE_LEN)
        
    def _proc(self, buf):
        put(*buf)

class TunnelPool(object):
    def __init__(self):
        self._pool = VDevPool()
        for _ in range(POOL_SIZE):
            self._pool.add(TunnelQueue())
    
    def push(self, buf):
        self._pool.push(buf)

class Tunnel(object):
    def __init__(self):
        self._tunnels = {}
        self._lock = VDevLock()
    
    def _get_path(self, addr):
        return os.path.join(RUN_PATH, 'tunnel-%s.pid' % self._get_iface(addr))
    
    def _get_reserved_address(self, addr):
        ip = self.addr2ip(addr)
        fields = ip.split('.')
        n = int(fields[3])
        return '%s.%s.%s.%d' % (fields[0], fields[1], fields[2], n + NETSIZE - 1)
    
    def _get_iface(self, addr): 
        name = ''
        fields = split(addr)[1].split('.')
        for i in range(1, 4):
            name += '%02x' % int(fields[i])
        return name
    
    def _get_tunnel(self, addr):
        grp, addr = split(addr)
        if int(grp) > 255:
            raise Exception('invalid address')
        name = '%02x' % int(grp)
        fields = addr.split('.')
        for i in range(1, 4):
            name += '%02x' % int(fields[i])
        return name
    
    def _get_supernode(self, addr):
        odd = 1
        code = 0
        length = len(SUPERNODES)
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
        return '%s:%d' % (SUPERNODES[code % length], SUPERNODE_PORT)
    
    def _is_gateway(self, addr):
        address = ifaddr(self._get_iface(addr))
        fields = address.split('.')
        return int(fields[3]) == 1
    
    def _check_pid(self, pid):
        for _ in range(RETRY_MAX):
            try:
                if psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
                    return False
                else:
                    return True
            except:
                time.sleep(SLEEP_TIME)
        return False
    
    def addr2ip(self, addr):
        return split(addr)[1]
    
    def _connect(self, addr, key, static):
        if not static:
            address = '0.0.0.0'
        else:
            address = self._get_reserved_address(addr)
        pid = Popen(['edge', '-r', '-d', self._get_iface(addr), '-a', address, '-s', NETMASK, '-c', self._get_tunnel(addr), '-k', key, '-l', self._get_supernode(addr)], stdout=DEVNULL, stderr=DEVNULL).pid
        if not self._check_pid(pid):
            log('tunnel: failed to connect')
            raise Exception('tunnel: failed to connect')
        with open(self._get_path(addr), 'w') as f:
            f.write(str(pid))
        if not static:
            call(['dhclient', '-q', self._get_iface(addr)], stderr=DEVNULL, stdout=DEVNULL)
        return self._chkiface(addr)
    
    def _touch(self, addr):
        ip = self.addr2ip(addr)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(TOUCH_TIMEOUT)
        try:
            sock.connect((ip, CONDUCTOR_PORT))
            sock.close()
            return True
        except:
            pass
    
    @named_lock
    def connect(self, addr, key, static, touch):
        if self._tunnels.has_key(addr):
            if touch:
                if not self._touch(addr):
                    raise Exception('failed to connect')
            self._tunnels[addr] += 1
        else:
            self._connect(addr, key, static)
            if touch:
                if not self._touch(addr):
                    self._disconnect(addr, True)
                    raise Exception('failed to connect')
            self._tunnels[addr] = 1
    
    def _disconnect(self, addr, force):
        if not force:
            if self._is_gateway(addr):
                return
        path = self._get_path(addr)
        with open(path, 'r') as f:
            pid = int(f.readlines()[0].strip())
        call(['ifconfig', self._get_iface(addr), 'down'], stderr=DEVNULL, stdout=DEVNULL)
        call(['kill', '-9', str(pid)], stderr=DEVNULL, stdout=DEVNULL)
        os.remove(path)
    
    @named_lock
    def disconnect(self, addr, force):
        cnt = self._tunnels.get(addr)
        if cnt and cnt > 0:
            self._tunnels[addr] -= 1
            if self._tunnels[addr] == 0:
                del self._tunnels[addr]
                self._disconnect(addr, force)
    
    @named_lock
    def exists(self, addr):
        return os.path.exists(self._get_path(addr))
    
    def _chkiface(self, addr):
        address = None
        for _ in range(RETRY_MAX):
            try:
                ifname = self._get_iface(addr)
                address = ifaddr(ifname)
                if address:
                    return address
                time.sleep(SLEEP_TIME)
            except:
                time.sleep(SLEEP_TIME)
        log('tunnel: failed to check iface')      
        raise Exception('tunnel: failed to check iface')
    
    def _create(self, addr, key):
        cfg = []
        address = split(addr)[1]
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
        pid = Popen(['edge', '-r', '-d', self._get_iface(addr), '-a', address, '-s', NETMASK, '-c', self._get_tunnel(addr), '-k', key, '-l', self._get_supernode(addr)], stdout=DEVNULL, stderr=DEVNULL).pid
        if DHCP_CLEAN:
            call(['killall', '-9', 'dhcpd'], stderr=DEVNULL, stdout=DEVNULL)
        call(['dhcpd', '-q'], stderr=DEVNULL, stdout=DEVNULL)
        return pid

    @named_lock
    def create(self, addr, key):
        for _ in range(CREATE_MAX):
            pid = self._create(addr, key)
            ret = self._chkiface(addr)
            if ret:
                with open(self._get_path(addr), 'w') as f:
                    f.write(str(pid))
                return ret
            else:
                call(['kill', '-9', str(pid)], stderr=DEVNULL, stdout=DEVNULL)

tunnel = Tunnel()
pool = TunnelPool()

def send(sock, buf):
    send_pkt(sock, buf)

def recv(sock):
    return recv_pkt(sock)

def addr2ip(addr):
    return tunnel.addr2ip(addr)

def create(addr, key):
    return tunnel.create(addr, key)

def connect(addr, key, static=False, touch=False):
    tunnel.connect(addr, key, static, touch)

def release(addr, force=False):
    tunnel.disconnect(addr, force)

def disconnect(addr, force=False):
    tunnel.disconnect(addr, force)

def exist(addr):
    tunnel.exists(addr)

def put(ip, op, args, uid=DEFAULT_UID, token=DEFAULT_TOKEN):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, CONDUCTOR_PORT))
    try:
        req = {'op':op, 'args':args}
        msg = crypto.pack(uid, req, token)
        send(sock, msg)
    finally:
        sock.close()

def push(ip, op, args, uid=DEFAULT_UID, token=DEFAULT_TOKEN):
    pool.push((ip, op, args, uid, token))
    
def clean():
    path = os.path.join(RUN_PATH, 'tunnel-*')
    call(['killall', '-9', 'edge'], stderr=DEVNULL, stdout=DEVNULL)
    call(['rm', '-f', path], stderr=DEVNULL, stdout=DEVNULL)
