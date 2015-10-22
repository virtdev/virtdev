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

from random import randint

F1_SIZE = 255 * 8
F2_SIZE = 8
F3_SIZE = 32

HOST_MAX = 255 * 255 * 8
RETRY_MAX = 3

class DHCP(object):    
    def _gen_addr(self):
        index = randint(0, HOST_MAX - 1)
        tmp = index % F1_SIZE
        f1 = int(index / F1_SIZE)
        f2 = int(tmp / F2_SIZE)
        f3 = (tmp % F2_SIZE) * F3_SIZE + 1
        return '10.%d.%d.%d' % (f1, f2, f3)
    
    def _check_addr(self, addr, networks):
        field = addr.split('.')
        net = '%s.%s.%s.' % (field[0], field[1], field[2])
        len_net = len(net)
        for n in networks:
            length = len(n)
            if len_net >= length and net[0:length] == n:
                return False
        return True
    
    def allocate(self, uid, node, networks=None):
        for _ in range(RETRY_MAX):
            addr = self._gen_addr()
            if self._check_addr(addr, networks):
                return addr
