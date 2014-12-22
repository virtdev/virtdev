#      dhcp.py
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

import random
from lib.util import cat, split
from db import CounterDB, AddressDB
from lib.log import log_err, log_get

VDEV_DHCP_F1_SIZE = 255 * 8
VDEV_DHCP_F2_SIZE = 8
VDEV_DHCP_F3_SIZE = 32

VDEV_DHCP_RETRY_MAX = 3
VDEV_DHCP_GROUP_MAX = 32
VDEV_DHCP_HOST_MAX = 255 * 255 * 8

class VDevDHCP(object):
    def __init__(self):
        self._counter = CounterDB()
        self._address = AddressDB()
        for i in range(VDEV_DHCP_GROUP_MAX):
            self._counter.set(str(i), 0)
    
    def _index2addr(self, index):
        if index >= VDEV_DHCP_HOST_MAX:
            log_err(self, 'invalid index')
            raise Exception(log_get(self, 'invalid index'))
        tmp = index % VDEV_DHCP_F1_SIZE
        f1 = int(index / VDEV_DHCP_F1_SIZE)
        f2 = int(tmp / VDEV_DHCP_F2_SIZE)
        f3 = (tmp % VDEV_DHCP_F2_SIZE) * VDEV_DHCP_F3_SIZE + 1
        return '10.%d.%d.%d' % (f1, f2, f3)
    
    def _check_addr(self, addr, networks):
        if not addr:
            return False
        field = addr.split('.')
        net = '%s.%s.%s.' % (field[0], field[1], field[2])
        len_net = len(net)
        for n in networks:
            length = len(n)
            if len_net >= length and net[0:length] == n:
                return False
        return True
    
    def _check_group(self):
        start = random.randint(0, VDEV_DHCP_GROUP_MAX - 1)
        for i in range(VDEV_DHCP_GROUP_MAX):
            grp = (start + i) % VDEV_DHCP_GROUP_MAX
            cnt = self._counter.get(str(grp))
            if cnt < VDEV_DHCP_HOST_MAX:
                start = grp * int(VDEV_DHCP_HOST_MAX / VDEV_DHCP_GROUP_MAX)
                index = (start + cnt) % VDEV_DHCP_HOST_MAX
                return (str(grp), index)
        return (None, None)
    
    def _check_gaddr(self, uid, node, gaddr, group=None):
        name = cat(uid, node)
        self._address.put(gaddr, name)
        res = self._address.get(gaddr, first=True)
        if res == name:
            if group:
                self._counter.put(group, 1)
            return True
    
    def allocate(self, uid, node, networks=None):
        cnt = 0
        while cnt < VDEV_DHCP_RETRY_MAX:
            group, index = self._check_group()
            if group:
                addr = self._index2addr(index)
                gaddr = cat(group, addr)
            else:
                gaddr = self._address.recycle()
                addr = split(gaddr)[1]
            if self._check_addr(addr, networks):
                if self._check_gaddr(uid, node, gaddr, group):
                    return gaddr
                else:
                    gaddr = self._address.recycle()
                    if gaddr:
                        addr = split(gaddr)[1]
                        if self._check_addr(addr, networks):
                            if self._check_gaddr(uid, node, gaddr):
                                return gaddr
            cnt += 1
    
    def free(self, address):
        self._address.unset(address)
    
    def refresh(self, address):
        self._address.refresh(address)
    