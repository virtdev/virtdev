# marker.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from router import Router
from datetime import datetime
from conf.log import LOG_MARK
from lib.domains import DOMAIN_USR, DOMAIN_DEV
from lib.log import log_debug, log_err, log_get
from conf.route import USR_SERVERS, DEV_SERVERS
from interface.commondb import CommonDB, VAL_NAME

USR_MARK = 'usrmark'
DEV_MARK = 'devmark'

class Marker(object):
    def __init__(self):
        self._usr_db = CommonDB(name=USR_MARK, router=Router(servers=USR_SERVERS))
        self._dev_db = CommonDB(name=DEV_MARK, router=Router(servers=DEV_SERVERS))
    
    def _log(self, text):
        if LOG_MARK:
            log_debug(self, text)
    
    def mark(self, name, domain, area):
        if domain == DOMAIN_USR:
            db = self._usr_db
        elif domain == DOMAIN_DEV:
            db = self._dev_db
        else:
            log_err(self, 'invalid domain')
            raise Exception(log_get(self, 'invalid domain'))
        coll = db.collection(name)
        conn = db.connection(coll)
        t = (datetime.utcnow() - datetime(1970, 1, 1)).total_seconds()
        db.put(conn, name, {'$set':{VAL_NAME:(t, area)}}, create=True)
        self._log('name=%s, domain=%s' % (str(name), str(domain)))
