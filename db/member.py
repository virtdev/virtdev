#      member.py
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

from threading import Lock
from conf.log import LOG_MEMBER
from lib.log import log_debug, log_err

class Member(object):
    def __init__(self):
        self.__members = []
        self.__lock = Lock()
    
    def _log(self, text):
        if LOG_MEMBER:
            log_debug(self, text)
    
    def get(self, pos):
        if pos >= 0 and pos < len(self.__members):
            return self.__members[pos]
    
    def length(self):
        return len(self.__members)
    
    def set_members(self, members, sort=False):
        if not members or type(members) != list:
            log_err(self, 'invalid members')
            return
        self._log('set_members %s' % str(members))
        self.__lock.acquire()
        try:
            self.__members = members
            if sort:
                self.__members.sort()
        finally:
            self.__lock.release()
    
    def check_members(self, members, pos):
        if not members or type(members) != list:
            log_err(self, 'invalid members')
            return
        self._log('check_members %s (pos=%d)' % (str(members), pos))
        self.__lock.acquire()
        try:
            if len(self.__members) != pos:
                return
            for i in members:
                if i in self.__members:
                    return
            return True
        finally:
            self.__lock.release()
    
    def add_members(self, members, pos, sort=False):
        if not members or type(members) != list:
            log_err(self, 'invalid members')
            return
        self._log('add_members %s (pos=%d)' % (str(members), pos))
        self.__lock.acquire()
        try:
            if len(self.__members) != pos:
                return
            for i in members:
                if i in self.__members:
                    return
            self.__members += members
            if sort:
                self.__members.sort()
            return True
        finally:
            self.__lock.release()
    
    def get_members(self, pos):
        self._log('get_members (pos=%d)' % pos)
        self.__lock.acquire()
        try:
            if pos < len(self.__members):
                return self.__members[pos:]
        finally:
            self.__lock.release()
