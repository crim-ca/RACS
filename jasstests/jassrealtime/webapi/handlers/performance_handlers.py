import json, os
import traceback
from http import HTTPStatus
from uuid import uuid1
import time
import multiprocessing as mp

from jassrealtime.webapi.handlers.base_handler import BaseHandler

def do_work(nbSteps :int):
    start = time.time()
    i = nbSteps
    while (i > 0):
        i = i - 1
    end = time.time()
    return "long,%s,%s,%s" % (start,end,(end - start))

class LongRunningGetHandler(BaseHandler):
    def get(self,number:float):
        res = do_work(100000000)
        self.write_and_set_status(res,HTTPStatus.OK)

"""
Does NOT WOKR
class LongRunningSubProcessHandler(BaseHandler):
    def get(self,number:float):
        pool = mp.Pool()
        res = pool.map_async(do_work,(1000000,))
        res.wait()
        self.write_and_set_status(res.get(),HTTPStatus.OK)

"""
class ShortRunningGetHandler(BaseHandler):
    def get(self,number:float):
        res = do_work(10000)
        self.write_and_set_status(res, HTTPStatus.OK)

