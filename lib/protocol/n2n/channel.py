# channel.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import time
import struct
import socket
from threading import Thread
from datetime import datetime
from lib.lock import NamedLock
from conf.conf import CONF_DHCP
from conf.log import LOG_CHANNEL
from conf.virtdev import BRIDGE_PORT
from lib.ws import ws_connect, ws_addr
from conf.defaults import CONDUCTOR_PORT
from lib.log import log_debug, log_err, log_get
from lib.util import call, popen, ifaddr, named_lock, get_networks, pkill, sigkill, status_zobie, ifdown

NETSIZE = 30
CHANNEL_MAX = 256
CHECK_RETRY = 7
CREATE_RETRY = 2
CONNECT_RETRY = 2

IDLE_TIME = 600 # seconds
WAIT_INTERVAL = 1 # seconds
CHECK_INTERVAL = 3 # seconds
CONNECT_INTERVAL = 5 # seconds
RECYCLE_INTERVAL = 60 # seconds

RECYCLE = False
CLEAN_DHCP = True
KEEP_CONNECTION = True
NETMASK = '255.255.255.224'
ADAPTER_NAME = 'edge'

class Channel(object):
    def __init__(self):
        self._ts = {}
        self._pid = {}
        self._conn = {}
        self._alloc = {}
        self._channels = {}
        self._lock = NamedLock()
        if KEEP_CONNECTION and RECYCLE:
            Thread(target=self._recycle).start()
    
    def _log(self, text):
        if LOG_CHANNEL:
            log_debug(self, text)
    
    def _get_gateway(self, addr):
        fields = addr.split('.')
        n = int(fields[3])
        return '%s.%s.%s.%d' % (fields[0], fields[1], fields[2], n + NETSIZE - 1)
    
    def _get_iface(self, addr): 
        return '%08x' % struct.unpack('I', socket.inet_aton(addr))[0]
    
    def _get_channel(self, addr):
        return self._get_iface(addr)
    
    def _get_url(self, addr):
        return ws_addr(addr, CONDUCTOR_PORT)
    
    def _can_release(self, addr):
        address = ifaddr(self._get_iface(addr))
        fields = address.split('.')
        return int(fields[3]) != 1
    
    def _check_pid(self, pid):
        for i in range(CHECK_RETRY + 1):
            try:
                if status_zobie(pid):
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
        with open(CONF_DHCP, 'w') as f:
            f.writelines(cfg)
        bridge = '%s:%d' % (bridge, BRIDGE_PORT)
        iface = self._get_iface(addr)
        channel = self._get_channel(addr)
        pid = popen(ADAPTER_NAME, '-r',
                    '-d', iface,
                    '-a', addr,
                    '-s', NETMASK,
                    '-c', channel,
                    '-k', key,
                    '-l', bridge)
        if CLEAN_DHCP:
            pkill('dhcpd')
        call('dhcpd', '-q')
        return pid
    
    def _try_create(self, addr, key, bridge):
        try:
            pid = self._create(addr, key, bridge)
            ret = self._chkiface(addr)
            if ret:
                self._pid[addr] = pid
                return ret
            else:
                sigkill(pid)
        except:
            pass
    
    def create(self, addr, key, bridge):
        for _ in range(CREATE_RETRY + 1):
            ret = self._try_create(addr, key, bridge)
            if ret:
                return ret
        
    def _do_release_connection(self, addr):
        if self._conn.has_key(addr):
            conn = self._conn.pop(addr)
            conn.close()
    
    def _release_connection(self, addr):
        if self._can_release(addr):
            if KEEP_CONNECTION:
                self._do_release_connection(addr)
            
            if self._pid.has_key(addr):
                ifdown(self._get_iface(addr))
                sigkill(self._pid[addr])
                del self._pid[addr]
    
    @named_lock
    def _do_recycle(self, addr):
        ts = self._ts.get(addr)
        if ts:
            t = datetime.utcnow()
            if (t - ts).total_seconds() >= IDLE_TIME:
                self._do_release_connection(addr)
                del self._ts[addr]
    
    def _recycle(self):
        while True:
            time.sleep(RECYCLE_INTERVAL)
            channels = self._ts.keys()
            for addr in channels:
                self._do_recycle(addr)
    
    def _do_disconnect(self, addr):
        self._release_connection(addr)
        del self._channels[addr]
        if KEEP_CONNECTION and RECYCLE:
            if self._ts.has_key(addr):
                del self._ts[addr]
    
    @named_lock
    def _disconnect(self, addr):
        total = self._channels.get(addr)
        if total != None and total <= 0:
            self._do_disconnect(addr)
    
    @named_lock
    def disconnect(self, addr, release):
        if self._channels.has_key(addr):
            if self._channels[addr] > 0:
                self._channels[addr] -= 1
                if release and self._channels[addr] == 0:
                    self._do_disconnect(addr)
    
    def _wait(self):
        while True:
            channels = self._channels.keys()
            for addr in channels:
                self._disconnect(addr)
                if len(self._channels) < CHANNEL_MAX:
                    return
            time.sleep(WAIT_INTERVAL)
    
    def _create_connection(self, addr):
        url = self._get_url(addr)
        for i in range(CONNECT_RETRY + 1):
            try:
                conn = ws_connect(url)
                break
            except:
                if i != CONNECT_RETRY:
                    time.sleep(CONNECT_INTERVAL)
        if not conn:
            self._release_connection(addr)
            raise Exception(log_get(self, 'failed to create connection, addr=%s' % str(addr)))
        self._conn.update({addr:conn})
        self._log('connection=%s' % url)
        return conn
    
    def _do_connect(self, addr, key, bridge, gateway):
        if not gateway:
            address = '0.0.0.0'
        else:
            address = self._get_gateway(addr)
        bridge = '%s:%d' % (bridge, BRIDGE_PORT)
        iface = self._get_iface(addr)
        channel = self._get_channel(addr)
        pid = popen(ADAPTER_NAME, '-r',
                    '-d', iface,
                    '-a', address,
                    '-s', NETMASK,
                    '-c', channel,
                    '-k', key,
                    '-l', bridge)
        if not self._check_pid(pid):
            log_err(self, 'failed to connect')
            raise Exception(log_get(self, 'failed to connect'))
        self._pid[addr] = pid
        if not gateway:
            call('dhclient', '-q', iface)
        self._chkiface(addr)
        if KEEP_CONNECTION:
            self._create_connection(addr)
    
    @named_lock
    def _connect(self, addr, key, bridge, gateway):
        if self._channels.has_key(addr):
            self._channels[addr] += 1
        else:
            self._do_connect(addr, key, bridge, gateway)
            self._channels[addr] = 1
        if KEEP_CONNECTION and RECYCLE:
            self._ts[addr] = datetime.utcnow()
    
    def connect(self, addr, key, bridge, gateway):
        if len(self._channels) >= CHANNEL_MAX and addr not in self._channels:
            self._wait()
        self._connect(addr, key, bridge, gateway)
    
    def _send(self, addr, buf):
        conn = self._conn.get(addr)
        if not conn:
            conn = self._create_connection(addr)
        if conn:
            conn.send(buf)
            if not KEEP_CONNECTION:
                self._do_release_connection(addr)
            elif RECYCLE:
                self._ts[addr] = datetime.utcnow()
        else:
            log_err(self, 'failed to send, no connection')
            raise Exception(log_get(self, 'failed to send, no connection'))
    
    @named_lock
    def send(self, addr, buf):
        self._send(addr, buf)
    
    def _allocate(self, addr):
        if self._alloc.has_key(addr):
            log_err(self, 'failed to allocate')
            raise Exception(log_get(self, 'failed to allocate'))
        key = self._alloc[addr]['key']
        bridge = self._alloc[addr]['bridge']
        self._do_connect(addr, key, bridge, False)
        self._alloc[addr]['active'] = True 
    
    @named_lock
    def allocate(self, addr, key, bridge):
        if self._alloc.has_key(addr):
            self._free(addr)
        self._alloc[addr] = {'key':key, 'bridge':bridge, 'active':False}
    
    def _free(self, addr):
        self._do_disconnect(addr)
        del self._alloc[addr]
    
    @named_lock
    def free(self, addr):
        if self._alloc.has_key(addr):
            self._free(addr)
    
    @named_lock
    def put(self, addr, buf):
        if not self._alloc.has_key(addr):
            log_err(self, 'failed to put, no channel')
            raise Exception(log_get(self, 'failed to put'))
        if not self._alloc[addr]['active']:
            self._allocate(addr)
        try:
            self._send(addr, buf)
        except:
            self._do_disconnect(addr)
            self._allocate(addr)
            self._send(addr, buf)
    
    @named_lock
    def exist(self, addr):
        return self._pid.has_key(addr)
    
    def clean(self):
        pkill(ADAPTER_NAME)
    
    def has_network(self, addr):
        networks = get_networks()
        address = struct.unpack("I", socket.inet_aton(addr))[0]
        for n in networks:
            network, mask = n
            if address & mask == network:
                return True
        return False
    
    def initialize(self):
        pass
