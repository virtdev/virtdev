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
import socket
import psutil
import signal
import package
from log import log
from pool import Pool
from queue import Queue
from lock import NamedLock
from conf.path import PATH_RUN
from subprocess import Popen, call
from supernode import get_supernode
from conf.virtdev import CONDUCTOR_PORT
from pynetlinux.ifconfig import Interface
from util import DEVNULL, ifaddr, send_pkt, recv_pkt, named_lock

NETSIZE = 30
QUEUE_LEN = 2
POOL_SIZE = 0
RETRY_MAX = 5
TUNNEL_MAX = 4096
SLEEP_TIME = 2 # seconds
TOUCH_RETRY = 0
CREATE_RETRY = 1
TOUCH_TIMEOUT = 10 # seconds
TOUCH_INTERVAL = 5 # seconds
CHECK_INTERVAL = 0.1 #seconds

PATH = '/etc/dhcp/dhcpd.conf'
NETMASK = '255.255.255.224'
CLEAN_DHCP = True

class TunnelQueue(Queue):
    def __init__(self):
        Queue.__init__(self, QUEUE_LEN)
        
    def proc(self, buf):
        put(*buf)

class TunnelPool(object):
    def __init__(self):
        self._pool = Pool()
        for _ in range(POOL_SIZE):
            self._pool.add(TunnelQueue())
    
    def push(self, buf):
        self._pool.push(buf)

class Tunnel(object):
    def __init__(self):
        self._tunnels = {}
        self._lock = NamedLock()
    
    def _get_path(self, addr):
        return os.path.join(PATH_RUN, 'tunnel-%s.pid' % self._get_iface(addr))
    
    def _get_reserved_address(self, addr):
        fields = addr.split('.')
        n = int(fields[3])
        return '%s.%s.%s.%d' % (fields[0], fields[1], fields[2], n + NETSIZE - 1)
    
    def _get_iface(self, addr): 
        name = ''
        fields = addr.split('.')
        for i in range(1, 4):
            name += '%02x' % int(fields[i])
        return name
    
    def _get_tunnel(self, addr):
        return self._get_iface(addr)
    
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
    
    def _do_connect(self, addr, key, static, supernode):
        if not static:
            address = '0.0.0.0'
        else:
            address = self._get_reserved_address(addr)
        pid = Popen(['edge', '-r', '-d', self._get_iface(addr), '-a', address, '-s', NETMASK, '-c', self._get_tunnel(addr), '-k', key, '-l', supernode], stdout=DEVNULL, stderr=DEVNULL).pid
        if not self._check_pid(pid):
            log('tunnel: failed to connect')
            raise Exception('tunnel: failed to connect')
        with open(self._get_path(addr), 'w') as f:
            f.write(str(pid))
        if not static:
            call(['dhclient', '-q', self._get_iface(addr)], stderr=DEVNULL, stdout=DEVNULL)
        return self._chkiface(addr)
    
    def _touch(self, addr):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(TOUCH_TIMEOUT)
        cnt = TOUCH_RETRY
        while True:
            try:
                sock.connect((addr, CONDUCTOR_PORT))
                sock.close()
                return True
            except:
                pass
            cnt -= 1
            if cnt >= 0:
                time.sleep(TOUCH_INTERVAL)
            else:
                break
    
    def _release(self, addr, force):
        total = self._tunnels.get(addr)
        if total != None and total <= 0:
            self._disconnect(addr, force)
            del self._tunnels[addr]
    
    @named_lock
    def release(self, addr, force):
        self._relase(addr, force)
    
    @named_lock
    def _connect(self, addr, key, static, touch, supernode):
        if self._tunnels.has_key(addr):
            if touch:
                if not self._touch(addr):
                    raise Exception('tunnel: failed to connect')
            self._tunnels[addr] += 1
        else:
            self._do_connect(addr, key, static, supernode)
            if touch:
                if not self._touch(addr):
                    self._disconnect(addr, True)
                    raise Exception('tunnel: failed to connect')
            self._tunnels[addr] = 1
    
    def _check_tunnel(self, addr, force=False):
        if len(self._tunnels) < TUNNEL_MAX:
            return
        while True:
            keys = self._tunnels.keys()
            for i in keys:
                total = self._tunnels.get(i)
                if i != addr and total != None and total <= 0:
                    self.release(i, force)
                    if len(self._tunnels) < TUNNEL_MAX:
                        return
            time.sleep(CHECK_INTERVAL)
            
    def connect(self, addr, key, static, touch, supernode):
        if static:
            self._check_tunnel(addr, force=True)
        else:
            self._check_tunnel(addr)
        self._connect(addr, key, static, touch, supernode)
        
    
    def _disconnect(self, addr, force):
        if not force and self._is_gateway(addr):
            return
        path = self._get_path(addr)
        if not os.path.exists(path):
            return
        with open(path, 'r') as f:
            pid = int(f.readlines()[0].strip())
        iface = Interface(self._get_iface(addr))
        iface.down()
        os.kill(pid, signal.SIGKILL)
        os.remove(path)
    
    @named_lock
    def disconnect(self, addr, force):
        if self._tunnels.get(addr) != None:
            if self._tunnels[addr] > 0:
                self._tunnels[addr] -= 1
            if force:
                self._release(addr, force)
    
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
    
    def _create(self, addr, key, supernode):
        cfg = []
        fields = addr.split('.')
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
        pid = Popen(['edge', '-r', '-d', self._get_iface(addr), '-a', addr, '-s', NETMASK, '-c', self._get_tunnel(addr), '-k', key, '-l', supernode], stdout=DEVNULL, stderr=DEVNULL).pid
        if CLEAN_DHCP:
            call(['killall', '-9', 'dhcpd'], stderr=DEVNULL, stdout=DEVNULL)
        call(['dhcpd', '-q'], stderr=DEVNULL, stdout=DEVNULL)
        return pid
    
    def _try_create(self, addr, key, supernode):
        try:
            pid = self._create(addr, key, supernode)
            ret = self._chkiface(addr)
            if ret:
                with open(self._get_path(addr), 'w') as f:
                    f.write(str(pid))
                return ret
            os.kill(pid, signal.SIGKILL)
        except:
            pass
    
    @named_lock
    def create(self, addr, key, supernode):
        for _ in range(CREATE_RETRY + 1):
            ret = self._try_create(addr, key, supernode)
            if ret:
                return ret

tunnel = Tunnel()

if POOL_SIZE:
    pool = TunnelPool()
else:
    pool = None

def send(sock, buf):
    send_pkt(sock, buf)

def recv(sock):
    return recv_pkt(sock)

def create(uid, addr, key):
    supernode = get_supernode(uid)
    return tunnel.create(addr, key, supernode)

def connect(uid, addr, key, static=False, touch=False):
    supernode = get_supernode(uid)
    tunnel.connect(addr, key, static, touch, supernode)

def release(addr, force=True):
    tunnel.release(addr, force)

def disconnect(addr, force=False):
    tunnel.disconnect(addr, force)

def exist(addr):
    tunnel.exists(addr)

def put(uid, addr, op, args, token):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((addr, CONDUCTOR_PORT))
    try:
        req = {'op':op, 'args':args}
        buf = package.pack(uid, req, token)
        send(sock, buf)
    finally:
        sock.close()

def push(uid, addr, op, args, token):
    if POOL_SIZE:
        pool.push((uid, addr, op, args, token))
    else:
        put(uid, addr, op, args, token)

def clean():
    path = os.path.join(PATH_RUN, 'tunnel-*')
    call(['killall', '-9', 'edge'], stderr=DEVNULL, stdout=DEVNULL)
    call(['rm', '-f', path], stderr=DEVNULL, stdout=DEVNULL)
