#      resolv.py (n2n)
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

class Resolv(object):    
    def _gen_addr(self):
        index = randint(0, HOST_MAX - 1)
        tmp = index % F1_SIZE
        f1 = int(index / F1_SIZE)
        f2 = int(tmp / F2_SIZE)
        f3 = (tmp % F2_SIZE) * F3_SIZE + 1
        return '10.%d.%d.%d' % (f1, f2, f3)
    
    def get_addr(self, uid, node):
        return self._gen_addr()
