import ctypes
import inspect
import threading
from lib.log import log_debug
from threading import Event, Thread

def _wakeup(th, ev):
    th.join()
    ev.set()

def _async_raise(tid, exctype):
    if not inspect.isclass(exctype):
        raise TypeError("Only types can be raised (not instances)")
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("invalid tasklet id")
    elif res != 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, 0)
        raise SystemError("PyThreadState_SetAsyncExc failed")

class Tasklet(Thread):
    def __init__(self, target, args, parent=None):
        Thread.__init__(self)
        self._parent = parent
        self._target = target
        self._args = args
        self._ret = None

    def _log(self, text):
        if self._parent:
            log_debug(self._parent, "Tasklet=>%s" % str(text))
        else:
            log_debug(self, text)

    def run(self):
        self._ret = self._target(*self._args)

    def _get_tid(self):
        if self.isAlive():
            if hasattr(self, "_thread_id"):
                return self._thread_id
            else:
                for tid, tobj in threading._active.items():
                    if tobj is self:
                        self._thread_id = tid
                        return tid

    def raise_exc(self, exctype):
        tid = self._get_tid()
        if tid:
            _async_raise(tid, exctype)

    def terminate(self):
        self.raise_exc(SystemExit)

    def wait(self, timeout):
        ev = Event()
        self.start()
        threading.Thread(target=_wakeup, args=(self, ev)).start()
        if not ev.wait(timeout) and self.isAlive():
            self._log('timeout (%ss)' % str(timeout))
            self.terminate()
        else:
            return self._ret
