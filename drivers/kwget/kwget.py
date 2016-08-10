# kwget.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
from RAKE import Rake
from base64 import b64decode
from dev.driver import Driver, check_output

PRINT = False
STOPWORDS = 'stoplist'

class KWGet(Driver):
    def setup(self):
        path = os.path.join(os.path.dirname(__file__), STOPWORDS)
        self._rake = Rake(path)
    
    def _get_keywords(self, text):
        buf = b64decode(text)
        keywords = self._rake.run(buf)
        if PRINT:
            print('KWGet: keywords=%s' % str(keywords))
        return keywords
    
    @check_output
    def put(self, args):
        text = args.get('content')
        keywords = self._get_keywords(text)
        if keywords:
            return {'keywords':keywords}
