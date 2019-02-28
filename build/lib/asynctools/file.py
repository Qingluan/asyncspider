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
    sess = Session(name=hand['session_name'], loop=loop)
    tp = hand.get('type','')
    if tp == 'json':
        data = json.loads(data)
    if hand['db_to_save'] == 'redis':
        await save_to_redis(id, hand, data, loop)
    elif hand['db_to_save'] == 'es':
        if sess.es_filter(hand, data):
            # logging.info(colored("%s" % type(data)))
            await sess.bulk(data, index=None, type=None, id=id)
        # await save_to_es(id, hand, data, loop)
    else:
        logging.error(colored("no suported type: %s" % tp, 'red'))

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

class Session:
    def __init__(self, name=None, index=None,type=None,host='localhost', db=7, timeout=10,loop=None):
        self.name = name
        self.loop = loop
        self.host = host
        self.timeout = timeout
        self.db = db
        if not name:
            self.name = str(int(time.time()))
        

    @classmethod
    def load_session(cls, name, index='', type=''):
        r = Redis(db=7, decode_responses='utf-8')
        if not name in r.hkeys('sess-manager'):
            logging.warn('no such session and create it')
            l = cls(name)
            l.init(index=index, type=type)
        else:
            return  cls(name)

    @classmethod
    def list_sessions(cls):
        r = Redis(db=7, decode_responses='utf-8')
        return r.hkeys("sess-manager")

    def status(self):
        r = Redis(db=self.db, decode_responses='utf-8')
        size = r.hget(self.name + "-es", 'cache')
        doc = r.hget(self.name + "-es", 'doc')
        return  size, doc

    def init(self, index='', type=''):
        r = Redis(db=7, decode_responses='utf-8')
        r.hset('sess-manager', self.name, 'init')
        r.hset(self.name+"-es", 'cache', 0)
        r.hset(self.name+"-es", 'doc', index + '|' + type)
        # r.hset(self.name + "-http", key, value)
    
    def __getitem__(self, k):
        r = Redis(db=7, decode_responses='utf-8')
        if isinstance(k, int):
            return  [pickle.loads(b64decode(i)) for i in r.lrange(self.name+ "-datas", k, k)][0]
        elif isinstance(k, slice):
            return  [pickle.loads(b64decode(i)) for i in r.lrange(self.name+ "-datas", k.start, k.stop)]
        elif isinstance(k, str):
            return  r.hget(self.name + "-es", k)

    @classmethod
    def es_flush(cls, name):
        c = cls(name)
        loop = asyncio.get_event_loop()
        logging.info("save to es: %s" % c['cache'])
        return loop.run_until_complete(asyncio.gather(c.save_to_es(loop=loop)))

    def set_es_index_type(self, index, type):
        r = Redis(db=7, decode_responses='utf-8')
        r.hset(self.name+"-es", 'doc', '|'.join([index, type]))

    def es_filter(self, hand ,data):
        filter_ = hand.get('es_filter')
        if not filter_:return  True
        if hand.get('type') == 'json':
            filter_ = json.loads(filter_)
            for k in filter_:
                vv = filter_[k]
                if isinstance(vv, list):
                    if data.get(k) in vv:
                        logging.info(colored("Filter:  %s from data: {}".format(data) % id, 'yellow', attrs=['bold']))
                        return False
                else:
                    if data.get(k) == vv:
                        logging.info(colored("Filter:  %s from data: {}".format(data) % id, 'yellow', attrs=['bold']))
                        return False
            
        else:
            if re.search(filter_.encode('utf-8'), data):
                logging.info(colored("Filter:  %s from data: {}".format(data[:100]) % id, 'yellow', attrs=['bold']))
                return False
        return  True
    

    @classmethod
    def destroy(cls, name):
        r = Redis(db=7, decode_responses='utf-8')
        if name in r.hkeys('sess-manager'):
            r.delete(name + "-es")
            r.delete(name+ "-http")
            r.delete(name+ "-datas")
            r.delete(name)
            r.hdel('sess-manager', name)

    @classmethod
    async def trace(cls, name, link, ok=True):
        """
        set the res if ok
        """
        redis = await aioredis.create_redis('redis://localhost', db=7)
        logging.info("trace: %s" % colored(link, 'green'))
        await redis.hset(name + "-http", link, str(ok))
        redis.close()

    def status_links(self):
        redis =  Redis(host='localhost', db=7, decode_responses='utf-8')
        return  redis.hgetall(self.name+"-http")


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
        return body

    async def save_to_es(self, datas=None, loop=None):
        if not loop:
            loop = asyncio.get_event_loop()
        async with Elasticsearch([i for i in self.host.split(",")]) as es:
            if not datas:
                redis = await aioredis.create_redis('redis://localhost', db=7, loop=loop)
                    # index, type = await redis.hget(self.name, 'doc', encoding='utf-8').split("|")
                    # k2 = index + "_" + type
                logging.info("get index type from db")
                _datas = await self.tmp_from_redis()
                datas = []
                for h,v in _datas:
                    datas.append(h)
                    datas.append(v)
                
                # await 
                redis.close()

            res = await es.bulk(datas)
            if not  res['errors']:
                redis = await aioredis.create_redis('redis://localhost',db=7,loop=self.loop)
                await redis.delete(self.name+"-datas")
                await redis.hset(self.name+"-es",'cache', 0)
                redis.close()
            else:
                logging.error(colored('{}'.format(res), 'red'))
            return res

    async def tmp_from_redis(self, start=0, end=-1):
        redis = await aioredis.create_redis('redis://localhost', db=7)
        data =  await redis.lrange(self.name + "-datas", start, end)
        redis.close()
        return [pickle.loads(b64decode(i)) for i in  data]
    
    async def tmp_to_redis(self, data):
        redis = await aioredis.create_redis('redis://localhost', db=7)
        data = b64encode(pickle.dumps(data))
        res =  await redis.lpush(self.name+"-datas", data)
        redis.close()
        return res

    async def bulk(self,  data, index=None, type=None, id=None):
        if not isinstance(data, dict):
            return
        redis = await aioredis.create_redis(
            'redis://localhost', db=7, loop=self.loop)

        if not index:
            index, _ = (await redis.hget(self.name + "-es", 'doc', encoding='utf-8')).split("|")
        if not type:
            _, type  = (await redis.hget(self.name + "-es", 'doc', encoding='utf-8')).split("|")

        k = self.name + "_" + index + "_" + type
        k2 = index + "_" + type
        

        d = self._buld_many(index, type, [data])
        # await redis.append(k, d + "\n")
        size = await self.tmp_to_redis(d)
        # await redis.hset(self.name, 'save', now + 1)
        if size > 1024:
            datas = await self.tmp_from_redis()
            q = []
            for h,v in datas:
                q.append(h)
                q.append(v)
            await self.save_to_es(q)
        else:
            await redis.hset(self.name+"-es",'cache', size)
        # await redis.close()
        redis.close()

    def __exit__(self):
        size,doc = self.status()
        if int(size) > 0:
            if not self.loop:
                loop = asyncio.get_event_loop()
            else:
                loop = self.loop
            logging.info("exit and save to es")
            loop.run_until_complete(self.save_to_es(loop=loop))

