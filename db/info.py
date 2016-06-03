#      info.py
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

import zerorpc
from lib.util import zmqaddr
from lib.log import log_debug
from conf.log import LOG_INFO
from conf.virtdev import MASTER_ADDR, MASTER_PORT

def _log(text):
    if LOG_INFO:
        log_debug('Info', text)

def get_mappers(domain):
    c = zerorpc.Client()
    c.connect(zmqaddr(MASTER_ADDR, MASTER_PORT))
    try:
        mappers = c.get_mappers(domain)
        _log('domain=%s, mappers=%s' % (domain, str(mappers)))
        return mappers
    finally:
        c.close()

def get_finders(domain):
    c = zerorpc.Client()
    c.connect(zmqaddr(MASTER_ADDR, MASTER_PORT))
    try:
        finders = c.get_finders(domain)
        _log('domain=%s, finders=%s' % (domain, str(finders)))
        return finders
    finally:
        c.close()

def get_servers(domain):
    c = zerorpc.Client()
    c.connect(zmqaddr(MASTER_ADDR, MASTER_PORT))
    try:
        servers = c.get_servers(domain)
        _log('domain=%s, servers=%s' % (domain, str(servers)))
        return servers
    finally:
        c.close()
