#      poll.py
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
import xattr

import sys
sys.path.append('..')
from fs.oper import OP_POLL
from conf.virtdev import VDEV_FS_MOUNTPOINT

def usage():
    print 'poll.py [uid]'

if __name__ == '__main__':
    argc = len(sys.argv)
    if argc != 2:
        usage()
        sys.exit()
    uid = sys.argv[1]
    path = os.path.join(VDEV_FS_MOUNTPOINT, uid)
    ret = xattr.getxattr(path, OP_POLL)
    print '%s' % str(ret)
    