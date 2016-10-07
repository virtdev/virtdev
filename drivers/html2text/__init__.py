#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import html2text
from dev.driver import Driver, wrapper
from base64 import b64encode, b64decode

PRINT = False
IGNORE_LINKS = True
IGNORE_IMAGES = True

class HTML2Text(Driver):    
    def _convert(self, html):
        buf = b64decode(html)
        if buf:
            try:
                temp = buf.decode(errors='ignore')
                h = html2text.HTML2Text()
                if IGNORE_LINKS:
                    h.ignore_links = True
                if IGNORE_IMAGES:
                    h.ignore_images = True
                res = h.handle(temp)
                if res:
                    return res.encode('utf-8')
            except:
                if PRINT:
                    print('HTML2Text: failed to convert')
    
    @wrapper
    def put(self, *args, **kwargs):
        html = kwargs.get('content')
        if html:
            text = self._convert(html)
            if text:
                return {'content':b64encode(text)}
