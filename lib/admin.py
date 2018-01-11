# admin.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import uuid
from lib.types import *
from db.user import User
from conf.route import AREA
from db.marker import Marker
from db.router import Router
from conf.virtdev import META_SERVERS

class UserInfo(object):
    def __init__(self):
        router = Router(META_SERVERS, sync=False)
        self._user = User(router)
        self._marker = Marker()

    def add(self, user, password, uid):
        ret = self._user.get(user, 'uid')
        if not ret:
            self._user.put(user, password=password, uid=uid)
            self._marker.mark(uid, DOMAIN_USR, AREA)
            return True

    def get_password(self, user):
        return self._user.get(user, 'password')

_user_info = UserInfo()

def create_user(user, password):
    uid = uuid.uuid4().hex
    if not _user_info.add(user, password, uid):
        raise Exception('Error: failed to create user %s' % user)

def get_password(user):
    return _user_info.get_password(user)
