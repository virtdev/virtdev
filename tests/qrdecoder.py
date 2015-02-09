#      qrdecoder.py
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

import Image
import pyqrcode
from base64 import encodestring

import sys
sys.path.append('..')
from anon.qrdecoder import QRDecoder

PATH_PNG = '/tmp/qr.png'
PATH_JPG = '/tmp/qr.jpg'

if __name__ == '__main__':
    qr = pyqrcode.create('hello')
    qr.png(PATH_PNG, scale=6)
    image = Image.open(PATH_PNG)
    image.save(PATH_JPG)
    dec = QRDecoder()
    with open(PATH_JPG) as f:
        buf = f.read()
    image = encodestring(buf)
    ret = dec.decode(image)
    print('QRDecoder: ret=%s' % str(ret))
