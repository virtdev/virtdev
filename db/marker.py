#      marker.py
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


from router import Router
from datetime import datetime
from conf.log import LOG_MARK
from lib.domains import DOMAIN_USR, DOMAIN_DEV
from lib.log import log_debug, log_err, log_get
from interface.commondb import CommonDB, VAL_NAME
from conf.virtdev import USR_SERVERS, DEV_SERVERS

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
