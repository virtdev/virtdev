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
#      MA 02110-1301, USA.

import zerorpc
from lib.log import log_err
from lib.util import zmqaddr
from conf.virtdev import EVENT_COLLECTOR_PORT

class VDevEventEmitter(object):
    def __init__(self, router):
        self._router = router
    
    def put(self, uid, name):
        addr = self._router.get('event', uid)
        cli = zerorpc.Client()
        cli.connect(zmqaddr(addr, EVENT_COLLECTOR_PORT))
        try:
            cli.put(uid, name)
        except:
            log_err(self, 'failed to put')
        finally:
            cli.close()
    