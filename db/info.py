# info.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import zerorpc
from lib.util import zmqaddr
from lib.log import log_debug
from conf.log import LOG_INFO
from conf.route import MASTER_ADDR, MASTER_PORT

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
