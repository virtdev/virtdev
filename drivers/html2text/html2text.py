# html2text.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import html2text
from base64 import b64encode, b64decode
from dev.driver import Driver, check_output

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
    
    @check_output
    def put(self, args):
        html = args.get('content')
        if html:
            text = self._convert(html)
            if text:
                return {'content':b64encode(text)}
