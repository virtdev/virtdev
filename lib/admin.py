#      admin.py
#      
#      Copyright (C) 2015 Yi-Wei Ci <ciyiwei@hotmail.com>
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

import uuid
from domains import *
from db.userdb import UserDB
from db.marker import Marker
from db.router import Router
from conf.virtdev import META_SERVERS, AREA_CODE

class User(object):
    def __init__(self):
        router = Router(META_SERVERS, sync=False)
        self._user = UserDB(router)
        self._marker = Marker()
    
    def add(self, user, password, uid):
        ret = self._user.get(user, 'uid')
        if not ret:
            self._user.put(user, password=password, uid=uid)
            self._marker.mark(uid, DOMAIN_USR, AREA_CODE)
            return True
    
    def get_password(self, user):
        return self._user.get(user, 'password')

_user = User()

def create_user(user, password):
    uid = uuid.uuid4().hex
    if not _user.add(user, password, uid):
        raise Exception('Error: failed to create user %s' % user)

def get_password(user):
    return _user.get_password(user)
