#      emitter.py
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
#      MA 02110-1301, USA

import sys
sys.path.append('..')
from lib.router import VDevRouter
from event.event import VDevEventEmitter
from conf.virtdev import VDEV_DB_SERVERS

def usage():
    print 'emitter.py [uid] [device id]'

if __name__ == '__main__':
    argc = len(sys.argv)
    if argc != 3:
        usage()
        sys.exit()
    router = VDevRouter()
    for i in VDEV_DB_SERVERS:
        router.add_server('event', i)
    uid = sys.argv[1]
    name = sys.argv[2]
    VDevEventEmitter(router).put(uid, name)
