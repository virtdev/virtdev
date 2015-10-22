#      log.py
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

from datetime import datetime
from conf.virtdev import DEBUG, LOG_ERR

def _get_name(obj):
    return obj.__class__.__name__

def log_get(obj, text):
    return _get_name(obj) + ': ' + str(text)

def log(text, time=False, force=False):
    if DEBUG or force:
        if time:
            print(str(text) + '  %s' % str(datetime.utcnow()))
        else:
            print(str(text))

def log_err(obj, text):
    if LOG_ERR:
        if obj:
            text = log_get(obj, text)
        log(text, time=True, force=True)
