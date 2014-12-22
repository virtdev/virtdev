#      recognizer.py
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

from aop import VDevAnonOper

class Recognizer(VDevAnonOper):
    def __init__(self, identity):
        self._identity = identity
    
    def __str__(self):
        return 'REC_%d' % self._identity
    
    def recognize(self, args):
        print 'recognize: args=%s' % args.keys()
        return True
    
    def put(self, buf):
        args = self._get_args(buf)
        if not args:
            return
        if self.recognize(args):
            return {'Enable':'True'}
    