import asyncio

# from mroylib.config import Config
import os

from bs4 import BeautifulSoup as Bs
from base64 import b64decode, b64encode
from functools import partial
from termcolor import colored
import pickle
import time
import datetime
from redis import Redis
import logging
import json
import re
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import TimeoutError

import asyncio
try:
    from aioelasticsearch import Elasticsearch
except ImportError:
    pass

try:
    import aiofiles
except ImportError:
    pass
try:
    import aioredis
except ImportError:
    pass

encoder = lambda x: b64encode(pickle.dumps(x)).decode()
decoder = lambda x: pickle.loads(b64decode(x))



async def save_to_es(id, hand, data, loop ):
    host= hand.get('es_host','localhost:9200')
    index = hand.get('es_index', 'es-main')
    doc_type = hand.get('es_type', 'es-doc')
    filter = hand.get('es_filter')
    type = hand.get('type')
    if type == 'json':
        data = json.loads(data)
    
    if filter:
        if type == 'json':
            filter_d = json.loads(filter)
            
            for k in filter_d:
                vv = filter_d[k]
                if isinstance(vv, list):
                    if data.get(k) in vv:
                        logging.info(colored("Filter:  %s from data: {}".format(data) % id, 'yellow', attrs=['bold']))
                        return
                else:
                    if data.get(k) == vv:
                        logging.info(colored("Filter:  %s from data: {}".format(data) % id, 'yellow', attrs=['bold']))
                        return
            
        else:
            if re.search(filter.encode('utf-8'), data):
                logging.info(colored("Filter:  %s from data: {}".format(data[:100]) % id, 'yellow', attrs=['bold']))
                return
    try:
        data = json.loads(data)
    except json.JSONDecodeError:
        pass
    async with Elasticsearch([i for i in host.split(",")]) as es:
        ret = await es.create(index,doc_type,id,data)
        return ret


async def save_to_redis(id, hand, data,loop ):
    m = {}
    redis = await aioredis.create_redis(
        'redis://localhost', db=6, loop=loop)
    if isinstance(data, dict) and 'error' in data:
        m = data
        soup = None
    elif data:
        soup = Bs(data, 'lxml')   
    else:
        soup = None
    
    selector = hand['selector']
    m['html'] = data
    if len(selector) > 0 and soup:
        m['tag'] = []
        for select_id in selector:
            if not select_id.strip():continue
            for s in soup.select(select_id):
                w = s.attrs
                w['xml'] = s.decode()
                m['tag'].append(w)
    await redis.set(id, encoder(m))
    redis.close()
    await redis.wait_closed()

async def aio_db_save(id, hand, data,loop ):
    if hand['db_to_save'] == 'redis':
        await save_to_redis(id, hand, data, loop)
    elif hand['db_to_save'] == 'es':
        await save_to_es(id, hand, data, loop)
    else:
        pass

class RedisListener:

    exe = ThreadPoolExecutor(64)
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

class Session(RedisListener):

    def __init__(self, name=None, host='localhost', db=7, timeout=10,loop=None):
        super().__init__(db=db, host=host, loop=loop, timeout=timeout)
        self.name = name
        self.loop = loop
        if not name:
            self.name = str(int(time.time()))
    
    def _buld_many(self, index, type, datas):
        p = {'index': {'_index': index, '_type': type}}
        body = []
        for data in datas:
            if isinstance(data, (tuple, list,)):
                p['index']['_id'] = data[0]
                v = data[1]
            elif isinstance(data, dict):
                v = data
            else:
                logging.warn(colored("include error type in data: {}".format(data), 'red'))
                continue

            if 'timestamp' not in v:
                data['timestamp'] = datetime.datetime.now()
            body.append(p)
            body.append(v)
        return '\n'.join([json.dumps(i) for i in body])

        
    async def bulk(self, index, type, data, id=None):
        if not isinstance(data, dict):
            return
        k = self.name + "_" + index + "_" + type
        k2 = index + "_" + type 
        redis = await aioredis.create_redis(
            'redis://localhost', db=7, loop=self.loop)
        
        d = self._buld_many(index, type, [data])
        await redis.append(k, d + "\n")
        await redis.hset(self.name, k2,  )

    def __exit__(self):
        pass