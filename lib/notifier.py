# notifier.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import zerorpc
from lib.util import zmqaddr
from conf.defaults import NOTIFIER_ADDR, NOTIFIER_PORT

def notify(op, buf):
    cli = zerorpc.Client()
    cli.connect(zmqaddr(NOTIFIER_ADDR, NOTIFIER_PORT))
    try:
        cli.push(op, buf)
    finally:
        cli.close()
