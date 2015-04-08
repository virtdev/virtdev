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

import zbar
import Image
from StringIO import StringIO
from base64 import decodestring
from dev.driver import VDevDriver
from lib.mode import MODE_IN, MODE_VISI

DEBUG_QRDECODER = False

class QRDecoder(VDevDriver):    
    def _decode(self, image):
        buf = decodestring(image)
        if buf:
            f = StringIO(buf)
            src = Image.open(f)
            w, h = src.size
            raw = src.tostring()
            scanner = zbar.ImageScanner()
            scanner.parse_config('enable')
            img = zbar.Image(w, h, 'Y800', raw)
            scanner.scan(img)
            for symbol in img:
                if str(symbol.type) == 'QRCODE':
                    return str(symbol.data).lower()
    
    def info(self):
        return {'mode': MODE_IN | MODE_VISI}
    
    def put(self, buf):
        args = self.get_args(buf)
        if args and type(args) == dict:
            image = args.get('File')
            if image:
                url = self._decode(image)
                if url:
                    if DEBUG_QRDECODER:
                        print('QRDecoder: url=%s' % url)
                    ret = {'URL':url}
                    name = args.get('Name')
                    if name:
                        ret.update({'Name':name})
                    timer = args.get('Timer')
                    if timer:
                        ret.update({'Timer':timer})
                    return ret
