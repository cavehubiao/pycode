import threading
import time

class CSyncObj:
    """Simple sync obj"""
    def __init__(self, container):
        self.obj = container
        self.lock = threading.Lock()
        if type(self.obj) is list:
            self.popone_func = getattr(self.obj, "pop")
            self.pushone_func = getattr(self.obj, "append")

    def popone(self):
        self.lock.acquire()
        ret = self.popone_func()
        self.lock.release()
        return ret
    
    def pushone(self, elem):
        self.lock.acquire()
        self.pushone_func(elem)
        self.lock.release()


class CThread:
    """Simple threading use"""
    def __init__(self, thread_num, thread_func, unlock_args = None, lock_args = None):
        self.thread_num = thread_num
        self.thread_func = thread_func
        self.unlock_args = unlock_args
        self.lock_rawargs = lock_args
        self.thread_list = []
        self.lock_args = []

    def prepare(self):
        for item in self.lock_rawargs:
            self.lock_args.append(CSyncObj(item))

    def run(self):
        self.prepare()

        params = []
        if self.unlock_args is not None:
            params += self.unlock_args
        if self.lock_args is not None:
            params += self.lock_args

        for x in range(self.thread_num):
            tfd = threading.Thread(target = self.thread_func, args = tuple(params))
            self.thread_list.append(tfd)

        for tfd in self.thread_list:
            tfd.start()

        for tfd in self.thread_list:
            tfd.join()
