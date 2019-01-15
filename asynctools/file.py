import asyncio
import aioredis
# from mroylib.config import Config
import os
import aiofiles
from bs4 import BeautifulSoup as Bs
from base64 import b64decode, b64encode
from functools import partial
from termcolor import colored
import pickle
import time
from redis import Redis
import logging
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import TimeoutError


encoder = lambda x: b64encode(pickle.dumps(x)).decode()
decoder = lambda x: pickle.loads(b64decode(x))


async def aio_db_save(id, hand, data,loop ):
    m = {}
    redis = await aioredis.create_redis(
        'redis://localhost', db=6, loop=loop)
    if isinstance(data, dict) and 'error' in data:
        m = data
        soup = None
    else:
        soup = Bs(data, 'lxml')   
    
    selector = hand['selector']
    if selector and soup:
        m['tag'] = []
        for select_id in selector:
            if not select_id.strip():continue
            for s in soup.select(select_id):
                w = s.attrs
                w['xml'] = s.decode()
                m['tag'].append(w)
    else:
        m['html'] = data 
    await redis.set(id, encoder(m))
    redis.close()
    await redis.wait_closed()


class RedisListener:

    exe = ThreadPoolExecutor(256)
    ok = set()    
    handler = dict()
    running_handle = []
    def __init__(self,db=0, host='localhost', loop=None, timeout=10):
        #if not loop:
        #    loop = asyncio.get_event_loop()
        self.loop = loop    
        self.host = host
        self.redis_db = db
        self.handler = {}
        self.runtime_gen = self.runtime()
        self.id = None
        self.timeout = timeout

    def regist(self,key,func, **kargs):
        f = partial(func, **kargs)
        self.handler[key.encode()] = f

    def clear_db(self):
        r = Redis(host=self.host, db=self.redis_db)
        r.flushdb()

    def runtime(self):
        r = Redis(host=self.host, db=self.redis_db)
        while 1:
            keys = r.keys()
            got_key = []
            handler = self.handler
            for k in handler:
                if k in keys:
                    got_key.append(k)
                    
            for kk in got_key:        
                fun = handler.pop(kk)
                arg = decoder(r.get(kk))
                # logging.info("handle -> " + kk.decode())
                #import pdb; pdb.set_trace()
                fun(arg)
                #self.__class__.exe.submit(fun, arg)
                r.delete(kk)
            yield
    
    def finish(self,fun, arg, key):
        def _finish(res):
            # print("real finish")
            self.__class__.ok.add(key)
        
            
        fut = self.__class__.exe.submit(fun, arg)
        fut.add_done_callback(_finish)
        self.__class__.running_handle.append(fut)
        #fut.result(timeout=self.timeout)
        # except TimeoutError as te:
            # logging.error(colored("[!] : %s Timeout" % key))
    
    def _run_loop(self, sec):
        r = Redis(host=self.host, db=self.redis_db)
        #r.flushdb()
        st = time.time()
        turn = 0
        try:
            while 1:
                oks = self.__class__.ok
                handler = self.handler
                got_key = []

                et = time.time()
                if et - st > sec:
                    break
                for k in handler:
                    if isinstance(k, str):
                        key = k.encode()
                    else:
                        key = k

                    if key in r.keys():
                        got_key.append(key)

                for i,kk in enumerate(got_key):        
                    if kk in handler:
                        fun = handler.get(kk)
                    else:
                        fun = None
                        continue

                    arg_tmp = r.get(kk)
                    if not arg_tmp:continue
                    arg = decoder(arg_tmp)

                    # finish will load function to deal data from redis.
                    self.finish(fun, arg, kk)
                    r.delete(kk)
                
                if got_key:
                    # print("got_key")
                    # to stop this listener thread
                    break
                time.sleep(0.4)
                turn += 1
                # print("wait :%d" % turn )
        except Exception as e:
            logging.exception(e)
        
        # finally:
        #    if len(self.__class__.running_handle) > 40:
        #         r_hs =[]
        #         for f in self.__class__.running_handle:
        #             r_hs.append(f)
        #             try:
        #                 [f.result(timeout=self.timeout) ]
        #             except TimeoutError:
        #                 pass
        #         [self.__class__.running_handle.remove(i) for i in r_hs if i in self.__class__.running_handle]
        #         logging.info(colored("(x): %d " % len(self.__class__.running_handle),'green', attrs=['bold']))
        

    def run_loop(self, sec):
        self.__class__.exe.submit(self._run_loop, sec)

    def __iter__(self):
        return self.runtime_gen

    def __next__(self):
        return next(self.runtime_gen)

#loop.run_until_complete(go())
