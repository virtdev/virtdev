#      channel.py (n2n)
#      
#      Copyright (C) 2015 Yi-Wei Ci <ciyiwei@hotmail.com>
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
import psutil
import signal
from conf.path import PATH_RUN
from lib.lock import NamedLock
from subprocess import Popen, call
from lib.log import log_err, log_get
from websocket import create_connection
from pynetlinux.ifconfig import Interface
from lib.util import DEVNULL, ifaddr, named_lock
from conf.virtdev import CONDUCTOR_PORT, BRIDGE_PORT

NETSIZE = 30
RETRY_MAX = 5
SLEEP_TIME = 2 # seconds
CONNECT_MAX = 1
CHANNEL_MAX = 4096
CREATE_RETRY = 1
CHECK_INTERVAL = 0.1 #seconds
CONNECT_INTERVAL = 5 # seconds

PATH = '/etc/dhcp/dhcpd.conf'
NETMASK = '255.255.255.224'
CLEAN_DHCP = True

class Channel(object):
    def __init__(self):
        self._channels = {}
        self._lock = NamedLock()
    
    def _get_path(self, addr):
        return os.path.join(PATH_RUN, 'channel-%s.pid' % self._get_iface(addr))
    
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
    
    def _get_channel(self, addr):
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
        if not address:
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
        with open(PATH, 'w') as f:
            f.writelines(cfg)
        bridge = '%s:%d' % (bridge, BRIDGE_PORT)
        pid = Popen(['edge', '-r', '-d', self._get_iface(addr), '-a', addr, '-s', NETMASK, '-c', self._get_channel(addr), '-k', key, '-l', bridge], stdout=DEVNULL, stderr=DEVNULL).pid
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
            os.kill(pid, signal.SIGKILL)
        except:
            pass
    
    def create(self, addr, key, bridge):
        for _ in range(CREATE_RETRY + 1):
            ret = self._try_create(addr, key, bridge)
            if ret:
                return ret
    
    def clean(self):
        path = os.path.join(PATH_RUN, 'channel-*')
        call(['killall', '-9', 'edge'], stderr=DEVNULL, stdout=DEVNULL)
        call(['rm', '-f', path], stderr=DEVNULL, stdout=DEVNULL)
    
    def _do_connect(self, addr, key, static, bridge):
        if not static:
            address = '0.0.0.0'
        else:
            address = self._get_reserved_address(addr)
        bridge = '%s:%d' % (bridge, BRIDGE_PORT)
        pid = Popen(['edge', '-r', '-d', self._get_iface(addr), '-a', address, '-s', NETMASK, '-c', self._get_channel(addr), '-k', key, '-l', bridge], stdout=DEVNULL, stderr=DEVNULL).pid
        if not self._check_pid(pid):
            log_err(self, 'failed to connect')
            raise Exception(log_get(self, 'failed to connect'))
        with open(self._get_path(addr), 'w') as f:
            f.write(str(pid))
        if not static:
            call(['dhclient', '-q', self._get_iface(addr)], stderr=DEVNULL, stdout=DEVNULL)
        return self._chkiface(addr)
    
    def _exist(self, addr):
        addr = "ws://%s:%d" % (addr, CONDUCTOR_PORT)
        for _ in range(CONNECT_MAX):
            try:
                ws = create_connection(addr)
                return True
            except:
                time.sleep(CONNECT_INTERVAL)
            finally:
                ws.close()
    
    def _do_release(self, addr):
        self._disconnect(addr)
        del self._channels[addr]
    
    def _release(self, addr):
        total = self._channels.get(addr)
        if total != None and total <= 0:
            self._do_release(addr)
    
    def _recycle(self):
        while True:
            channels = self._channels.keys()
            for addr in channels:
                self._release(addr)
                if len(self._channels) < CHANNEL_MAX:
                    return
            time.sleep(CHECK_INTERVAL)
    
    @named_lock
    def _connect(self, addr, key, static, verify, bridge):
        if self._channels.has_key(addr):
            if verify:
                if not self._exist(addr):
                    log_err(self, 'failed to connect')
                    raise Exception(log_get(self, 'failed to connect'))
            self._channels[addr] += 1
        else:
            self._do_connect(addr, key, static, bridge)
            if verify:
                if not self._exist(addr):
                    self._disconnect(addr)
                    log_err(self, 'failed to connect')
                    raise Exception(log_get(self, 'failed to connect'))
            self._channels[addr] = 1
    
    def connect(self, addr, key, static, verify, bridge):
        if len(self._channels) >= CHANNEL_MAX and addr not in self._channels:
            self._recycle()
        self._connect(addr, key, static, verify, bridge)
    
    def _disconnect(self, addr):
        if self._is_gateway(addr):
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
    def disconnect(self, addr, release):
        if self._channels.has_key(addr):
            if self._channels[addr] > 0:
                self._channels[addr] -= 1
                if release and self._channels[addr] == 0:
                    self._do_release(addr)
    
    @named_lock
    def put(self, addr, buf):
        addr = "ws://%s:%d" % (addr, CONDUCTOR_PORT)
        ws = create_connection(addr)
        try:
            ws.send(buf)
        finally:
            ws.close()
            
    @named_lock
    def exist(self, addr):
        return os.path.exists(self._get_path(addr))
    
