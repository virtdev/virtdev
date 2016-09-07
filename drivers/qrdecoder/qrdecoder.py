# qrdecoder.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import zbar
import Image
from base64 import b64decode
from StringIO import StringIO
from dev.driver import Driver, wrapper

PRINT = False

class QRDecoder(Driver):    
    def _decode(self, image):
        buf = b64decode(image)
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
                    url = str(symbol.data).lower()
                    if PRINT:
                        print('QRDecoder: url=%s' % url)
                    return url
    
    @wrapper
    def put(self, *args, **kwargs):
            image = kwargs.get('content')
            if image:
                url = self._decode(image)
                if url:
                    return {'url':url}
