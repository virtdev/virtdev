#      facerec.py
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

import os
from base64 import encodestring

import sys
sys.path.append('..')
from drivers.facerec import FaceRec

PATH = '/opt/images/face.jpg'

if __name__ == '__main__':
    if not os.path.exists(PATH):
        print('FaceRec: cannot find %s' % PATH)
        sys.exit()
    with open(PATH) as f:
        buf = f.read()
    image = encodestring(buf)
    rec = FaceRec()
    ret = rec.recognize(image)
    print('FaceRec: ret=%s' % str(ret))
