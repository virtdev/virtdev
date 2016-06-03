#      channel.py (n2n)
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
import time
import struct
import socket
import psutil
import signal
from threading import Thread
from datetime import datetime
from lib.lock import NamedLock
from conf.log import LOG_CHANNEL
from subprocess import Popen, call
from websocket import create_connection
from conf.path import PATH_RUN, PATH_DHCP
from pynetlinux.ifconfig import Interface
from lib.log import log_debug, log_err, log_get
from lib.util import DEVNULL, ifaddr, named_lock
from conf.virtdev import CONDUCTOR_PORT, BRIDGE_PORT

NETSIZE = 30
CHANNEL_MAX = 256
CHECK_RETRY = 7
CREATE_RETRY = 3
CONNECT_RETRY = 3

IDLE_TIME = 600 # seconds
WAIT_INTERVAL = 1 # seconds
CHECK_INTERVAL = 3 # seconds
CONNECT_INTERVAL = 5 # seconds
RECYCLE_INTERVAL = 60 # seconds

RECYCLE = False
CLEAN_DHCP = True
KEEP_CONNECTION = True

NETMASK = '255.255.255.224'

class Channel(object):
    def __init__(self):
        self._ts = {}
        self._conn = {}
        self._channels = {}
        self._lock = NamedLock()
        if KEEP_CONNECTION and RECYCLE:
            Thread(target=self._recycle).start()
    
    def _log(self, text):
        if LOG_CHANNEL:
            log_debug(self, text)
    
    def _get_path(self, addr):
        return os.path.join(PATH_RUN, 'channel-%s.pid' % self._get_iface(addr))
    
    def _get_reserved_address(self, addr):
        fields = addr.split('.')
        n = int(fields[3])
        return '%s.%s.%s.%d' % (fields[0], fields[1], fields[2], n + NETSIZE - 1)
    
    def _get_iface(self, addr): 
        return '%08x' % struct.unpack('I', socket.inet_aton(addr))[0]
    
    def _get_channel(self, addr):
        return self._get_iface(addr)
    
    def _get_url(self, addr):
        return "ws://%s:%d/" % (addr, CONDUCTOR_PORT)
    
    def _is_gateway(self, addr):
        address = ifaddr(self._get_iface(addr))
        fields = address.split('.')
        return int(fields[3]) == 1
    
    def _check_pid(self, pid):
        for i in range(CHECK_RETRY + 1):
            try:
                if psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
                    return False
                else:
                    return True
            except:
                if i != CHECK_RETRY:
                    time.sleep(CHECK_INTERVAL)
        return False
    
    def _chkiface(self, addr):
        for i in range(CHECK_RETRY + 1):
            try:
                iface = self._get_iface(addr)
                address = ifaddr(iface)
                if address:
                    return address
                elif i != CHECK_RETRY:
                    time.sleep(CHECK_INTERVAL)
            except:
                if i != CHECK_RETRY:
                    time.sleep(CHECK_INTERVAL)
        log_err(self, 'failed to check iface')      
        raise Exception(log_get(self, 'failed to check iface'))
    
    def _create(self, addr, key, bridge):
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
        with open(PATH_DHCP, 'w') as f:
            f.writelines(cfg)
        bridge = '%s:%d' % (bridge, BRIDGE_PORT)
        iface = self._get_iface(addr)
        channel = self._get_channel(addr)
        pid = Popen(['edge', '-r', '-d', iface, '-a', addr, '-s', NETMASK, '-c', channel, '-k', key, '-l', bridge], stdout=DEVNULL, stderr=DEVNULL).pid
        if CLEAN_DHCP:
            call(['killall', '-9', 'dhcpd'], stderr=DEVNULL, stdout=DEVNULL)
        call(['dhcpd', '-q'], stderr=DEVNULL, stdout=DEVNULL)
        return pid
    
    def _try_create(self, addr, key, bridge):
        try:
            pid = self._create(addr, key, bridge)
            ret = self._chkiface(addr)
            if ret:
                with open(self._get_path(addr), 'w') as f:
                    f.write(str(pid))
                return ret
            else:
                os.kill(pid, signal.SIGKILL)
        except:
            pass
    
    def create(self, addr, key, bridge):
        for _ in range(CREATE_RETRY + 1):
            ret = self._try_create(addr, key, bridge)
            if ret:
                return ret
    
    def _create_connection(self, addr):
        url = self._get_url(addr)
        for i in range(CONNECT_RETRY + 1):
            try:
                conn = create_connection(url)
                break
            except:
                if i != CONNECT_RETRY:
                    time.sleep(CONNECT_INTERVAL)
        if not conn:
            self._disconnect(addr)
            raise Exception(log_get(self, 'failed to create connection, addr=%s' % str(addr)))
        self._conn.update({addr:conn})
        self._log('connection=%s' % url)
        return conn
    
    def _do_connect(self, addr, key, static, bridge):
        if not static:
            address = '0.0.0.0'
        else:
            address = self._get_reserved_address(addr)
        bridge = '%s:%d' % (bridge, BRIDGE_PORT)
        iface = self._get_iface(addr)
        channel = self._get_channel(addr)
        pid = Popen(['edge', '-r', '-d', iface, '-a', address, '-s', NETMASK, '-c', channel, '-k', key, '-l', bridge], stdout=DEVNULL, stderr=DEVNULL).pid
        if not self._check_pid(pid):
            log_err(self, 'failed to connect')
            raise Exception(log_get(self, 'failed to connect'))
        with open(self._get_path(addr), 'w') as f:
            f.write(str(pid))
        if not static:
            call(['dhclient', '-q', iface], stderr=DEVNULL, stdout=DEVNULL)
        self._chkiface(addr)
        if KEEP_CONNECTION:
            self._create_connection(addr)
    
    def _verify(self, addr):
        if not KEEP_CONNECTION:
            self._create_connection(addr)
        return True
    
    @named_lock
    def _connect(self, addr, key, static, verify, bridge):
        if self._channels.has_key(addr):
            if verify:
                if not self._verify(addr):
                    log_err(self, 'failed to connect')
                    raise Exception(log_get(self, 'failed to connect'))
            self._channels[addr] += 1
        else:
            self._do_connect(addr, key, static, bridge)
            if verify:
                if not self._verify(addr):
                    self._disconnect(addr)
                    log_err(self, 'failed to connect')
                    raise Exception(log_get(self, 'failed to connect'))
            self._channels[addr] = 1
        if KEEP_CONNECTION and RECYCLE:
            self._ts[addr] = datetime.utcnow()
    
    def _release_connection(self, addr):
        if self._conn.has_key(addr):
            conn = self._conn.pop(addr)
            conn.close()
    
    def _disconnect(self, addr):
        if self._is_gateway(addr):
            return
        if KEEP_CONNECTION:
            self._release_connection(addr)
        path = self._get_path(addr)
        if not os.path.exists(path):
            return
        with open(path, 'r') as f:
            pid = int(f.readlines()[0].strip())
        Interface(self._get_iface(addr)).down()
        os.kill(pid, signal.SIGKILL)
        os.remove(path)
    
    def _do_release(self, addr):
        self._disconnect(addr)
        del self._channels[addr]
        if KEEP_CONNECTION and RECYCLE:
            if self._ts.has_key(addr):
                del self._ts[addr]
    
    @named_lock
    def _release(self, addr):
        total = self._channels.get(addr)
        if total != None and total <= 0:
            self._do_release(addr)
    
    @named_lock
    def _do_recycle(self, addr):
        ts = self._ts.get(addr)
        if ts:
            t = datetime.utcnow()
            if (t - ts).total_seconds() >= IDLE_TIME:
                self._release_connection(addr)
                del self._ts[addr]
    
    def _recycle(self):
        while True:
            time.sleep(RECYCLE_INTERVAL)
            channels = self._ts.keys()
            for addr in channels:
                self._do_recycle(addr)
    
    def _wait(self):
        while True:
            channels = self._channels.keys()
            for addr in channels:
                self._release(addr)
                if len(self._channels) < CHANNEL_MAX:
                    return
            time.sleep(WAIT_INTERVAL)
    
    def _can_connect(self, addr):
        if len(self._channels) >= CHANNEL_MAX and addr not in self._channels:
            self._wait()
        return True
    
    def connect(self, addr, key, static, verify, bridge):
        if self._can_connect(addr):
            self._connect(addr, key, static, verify, bridge)
    
    @named_lock
    def disconnect(self, addr, release):
        if self._channels.has_key(addr):
            if self._channels[addr] > 0:
                self._channels[addr] -= 1
                if release and self._channels[addr] == 0:
                    self._do_release(addr)
    
    @named_lock
    def put(self, addr, buf):
        conn = self._conn.get(addr)
        if not conn:
            conn = self._create_connection(addr)
        if conn:
            conn.send(buf)
            if not KEEP_CONNECTION:
                self._release_connection(addr)
            elif RECYCLE:
                self._ts[addr] = datetime.utcnow()
        else:
            log_err(self, 'no connection')
            raise Exception(log_get(self, 'no connection'))
    
    @named_lock
    def exist(self, addr):
        return os.path.exists(self._get_path(addr))
    
    def clean(self):
        path = os.path.join(PATH_RUN, 'channel-*')
        call(['killall', '-9', 'edge'], stderr=DEVNULL, stdout=DEVNULL)
        call(['rm', '-f', path], stderr=DEVNULL, stdout=DEVNULL)
