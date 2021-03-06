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
from getpass import getpass
from tqdm import tqdm
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import TimeoutError

from .chains import  get_code

import asyncio
try:
    import inspect
    from aioelasticsearch import Elasticsearch
    from aioelasticsearch.helpers import Scan
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
    soup = None
    if isinstance(data, dict) and 'error' in data:
        m = data
        soup = None
    elif data:
        if isinstance(data, (str, bytes,)):
            soup = Bs(data, 'lxml')
        elif isinstance(data, list):
            m['html'] = data       
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
        try:
            data = json.loads(data)
        except TypeError:
            logging.error(colored(data, 'red'))
            return
    if 'chains' in hand:
        c = hand['chains']
        logging.info("--- chains --- turn:%d | order: %d" %(c.turn, c.order))
        
        try:
            data = hand['chains'].end_handler(data)
            if not data:
                return
        except Exception as e:
            logging.error(colored(e, 'red'))
            return


    if hand['db_to_save'] == 'redis':
        await save_to_redis(id, hand, data, loop)
    elif hand['db_to_save'] == 'es':
        if isinstance(data, list):
            # for _data in data:
                # logging.debug(str(_data))
            await sess.bulk(data, index=None, type=None)
            return
        elif not isinstance(data, dict):
            data = {'raw':data}
            # logging.info(colored("%s" % type(data)))
        if not sess.es_filter(hand, data):return
        await sess.bulk(data, index=None, type=None)
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
        self._duplicate = {}
        if not name:
            self.name = str(int(time.time()))
        

    @classmethod
    def load_session(cls, name, index='', type='', host='localhost'):
        r = Redis(db=7, decode_responses='utf-8')
        if not name in r.hkeys('sess-manager'):
            logging.warn('no such session and create it')

            l = cls(name, host=host)
            l.init(index=index, type=type, host=host)
            return cls(name, host=host)
        else:
            host = r.hget(name+'-es', 'hosts')
            return  cls(name, host=host)

    @classmethod
    def change_es_host(cls,name, host):
        r = Redis(db=7, decode_responses='utf-8')
        r.hset(name+'-es', 'hosts', host)


    @classmethod
    def list_sessions(cls):
        r = Redis(db=7, decode_responses='utf-8')
        return r.hkeys("sess-manager")

    def status_chains(self):
        r = Redis(db=7, decode_responses='utf-8')
        return r.hgetall(self.name + "-chains")

    def status(self):
        r = Redis(db=self.db, decode_responses='utf-8')
        size = r.hget(self.name + "-es", 'cache')
        doc = r.hget(self.name + "-es", 'doc')
        code = r.hget(self.name + "-es", 'code')
        if code:
            code = b64decode(code.encode('utf-8'))
        return  size,doc, code
    
    def all_status(self):
        r = Redis(db=7, decode_responses='utf-8') 
        base = r.hgetall(self.name + "-es")
        for k in base:
            v = base[k]
            if v.endswith('=='):
                v = b64decode(v.encode('utf-8')).decode('utf-8')
                base[k] = v
        chains_status = r.hgetall(self.name + "-chains")
        goods = 0
        
        links_status = r.hgetall(self.name + "-http")
        for i in links_status:
            if links_status[i] == "True":
                goods +=1
    
        base['http'] = '%d/%d' % (goods, len(links_status))
        base['chains'] = chains_status
        return base


    def init(self, index='', type='', host=''):
        r = Redis(db=7, decode_responses='utf-8')
        r.hset('sess-manager', self.name, 'init')
        r.hset(self.name+"-es", 'cache', 0)
        r.hset(self.name+"-es", 'hosts', self.host)
        if not index:
            index = self.name.lower()
        if not type:
            type = self.name.lower()
        if not host:
            r.hset(self.name+'-es', 'hosts', host)
        r.hset(self.name+"-es", 'doc', index + '|' + type)
        # r.hset(self.name + "-http", key, value)

    def clear_listener(self):
        r = Redis(db=7, decode_responses='utf-8')
        r.hset(self.name + "-es", 'code', '')

    def clear_data(self):
        r = Redis(db=7, decode_responses='utf-8')
        if self.name in r.hkeys('sess-manager'):
            r.hset(self.name+"-es", 'cache', 0)
            # r.hset(self.name+"-es", 'doc', index + '|' + type)
            r.delete(self.name + '-datas')
            r.delete(self.name + '-datas-bak')
            r.delete(self.name + '-http')
            r.delete(self.name + '-chains')
    
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
        c = cls.load_session(name)
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
            r.delete(name + "-http")
            r.delete(name + "-chains")
            r.delete(name + "-datas")
            r.delete(name)
            r.hdel('sess-manager', name)

    @classmethod
    async def trace_before(cls, name, link):
        await cls.trace(name, link, ok=False)        

    @classmethod
    async def trace(cls, name, link, ok=True):
        """
        set the res if ok
        """
        redis = await aioredis.create_redis('redis://localhost', db=7)
        logging.debug("trace: %s" % colored(link, 'green'))
        await redis.hset(name + "-http", link, str(ok))
        redis.close()

    def status_links(self):
        redis =  Redis(host='localhost', db=7, decode_responses='utf-8')
        return  redis.hgetall(self.name+"-http")

    def ready_save(self):
        r = Redis(db=7, decode_responses='utf-8')
        r.hset("sess-manager", self.name, "init")


    def _buld_many(self, index, type, datas, id=None, handle='index'):
        body = []
        for data in datas:
            p = {handle: {'_index': index, '_type': type}}
            _b = []
            if isinstance(data, dict) and handle in ('index','create',):
                v = data
                if 'timestamp' not in v:
                    data['timestamp'] = datetime.datetime.now()
                if id and id in data:
                    p[handle]['_id'] = data[id]

                _b = [p,v]
            elif handle == 'delete':
                p[handle]['_id'] = data
                _b = [p]
            else:
                logging.warn(colored("include error type in data: {}".format(data), 'red'))
                continue

            body.append(_b)

        return body


    async def just_bulk(self, datas):
        async with Elasticsearch([i for i in self.host.split(",")]) as es:
            d = []
            with tqdm(total=len(datas)) as pbar:
                for i,v in enumerate(datas):
                    if i > 0 and i % 1024 == 0:
                        await es.bulk(d)
                        pbar.update(1024)
                        if isinstance(v, list):
                            d = v
                        else:
                            d = [v]
                    else:
                        if isinstance(v ,list):
                            d += v
                        else:
                            d.append(v)


    async def save_to_es(self, datas=None, loop=None):
        if not loop:
            loop = asyncio.get_event_loop()
        print("->, es:", self.host)
        redis = await aioredis.create_redis('redis://localhost',db=7,loop=self.loop)
        host = await redis.hget(self.name + "-es", "hosts")
        self.host = host.decode('utf-8')
        async with Elasticsearch([i for i in self.host.split(",")]) as es:
            if not datas:
                    # index, type = await redis.hget(self.name, 'doc', encoding='utf-8').split("|")
                    # k2 = index + "_" + type
                logging.info("get index type from db")
                _datas = await self.tmp_from_redis()
                datas = []
                for h,v in _datas:
                    datas.append(h)
                    datas.append(v)

            res = await es.bulk(datas)
            await redis.hset("sess-manager", self.name, "init")
            if not  res['errors']:
                si = await redis.llen(self.name+"-datas")                    
                # bakdata =  await redis.lrange(self.name + "-datas-bak", 0, -1)
                # si = await redis.lpush(self.name+ '-datas', *bakdata)
                # await redis.delete(self.name + "-datas-bak")

                await redis.hset(self.name+"-es",'cache', si)
                logging.info(colored('save ok ', 'green'))
                redis.close()
            else:
                # errors = [i for i in res['items'] if i['index']['status'] != 201]
                # if len(errors) * 100 / len( res[0]['items']) == 0:
                si = await redis.llen(self.name+"-datas")                    
                
                # bakdata =  await redis.lrange(self.name + "-datas-bak", 0, -1)
                # si = await redis.lpush(self.name+ '-datas', *bakdata)
                # await redis.delete(self.name + "-datas-bak")

                await redis.hset(self.name+"-es",'cache', si)
                logging.info(colored('save ok ', 'green'))
                redis.close()
                logging.error(colored('{}'.format(res), 'red'))
            return res

    async def tmp_from_redis(self, start=0, end=-1):
        redis = await aioredis.create_redis('redis://localhost', db=7)
        data =  await redis.lrange(self.name + "-datas", start, end)
        redis.close()
        return [pickle.loads(b64decode(i)) for i in  data]
    
    # async def rm_flush(self, redis, datas)

    async def tmp_to_redis(self, data, bak=False):
        redis = await aioredis.create_redis('redis://localhost', db=7)
        data = [b64encode(pickle.dumps(i)) for i in data]
        if not bak:
            res =  await redis.lpush(self.name+"-datas", *data)
        else:
            res =  await redis.lpush(self.name+"-datas-bak", *data)

        
        l = await redis.llen(self.name + "-datas")
        redis.close()
        return l
 
    def sync_es_range(self, *keys, call=None, **query):
        loop = asyncio.get_event_loop()
        index,tp = self.status()[1].split("|")
        return loop.run_until_complete(self.es_range(index, tp, *keys, call=call, **query))
 
    def _collection_duplicate(self, field, doc):
        v = doc['_source'].get(field)
        if v in self._duplicate:
            self._duplicate[v].append(doc['_id'])
        else:
            self._duplicate[v] = []

    def sync_es_duplicate(self, field):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.es_duplicate(field))
        return self._duplicate

    async def es_duplicate(self, field):
        filter_func = partial(self._collection_duplicate, field)
        index, tp = self.status()[1].split("|")
        await self.es_range(index, tp, call=filter_func)

    async def es_range(self, index, tp, *keys, call=None, **query):
        async with Elasticsearch([self.host]) as es:
            async with Scan(
                es,
                index=index,
                doc_type=tp,
                query=query,
            ) as scan:

                res = []
                count = await es.count(index=index)
                count = count['count']
                progressbar = tqdm(desc="scan all elasticsearch", total=count)
                ic = 0
                si = count / 1000
                async for doc in scan:
                    ic += 1
                    if ic > 0 and ic % 1000 ==0:
                        progressbar.update(si)
                    if call:
                        call(doc)
                    else:
                        dd = {}
                        for k in keys:
                            km = k.split(':')
                            v = doc
                            for kk in km:
                                v = v.get(kk)
                                if not v:break
                            dd[k] =v
                        res.append(dd)
                progressbar.close()
                return res


    async def bulk(self,  data, index=None, type=None):
        if not isinstance(data, (dict,list,)):
            return
        redis = await aioredis.create_redis(
            'redis://localhost', db=7, loop=self.loop)

        if not index:
            index, _ = (await redis.hget(self.name + "-es", 'doc', encoding='utf-8')).split("|")
        if not type:
            _, type  = (await redis.hget(self.name + "-es", 'doc', encoding='utf-8')).split("|")

        k = self.name + "_" + index + "_" + type
        k2 = index + "_" + type
        _id = await redis.hget(self.name + "-es", "uniq-id", encoding='utf-8') 
        if isinstance(data, list):
            d = self._buld_many(index, type, data, id=_id)
        else:
            d = self._buld_many(index, type, [data], id=_id)
        # await redis.append(k, d + "\n")
        status = await redis.hget('sess-manager', self.name, encoding='utf-8')
        
        # if status == 'saving':
        #     size = await self.tmp_to_redis(d,bak=True)
        # else:
        size = await self.tmp_to_redis(d)
        await redis.hset(self.name+"-es",'cache', size)
        logging.info("save size: %s" %colored(size, 'blue'))
        # await redis.hset(self.name, 'save', now + 1)
        if size > 1024:
            # if status =='saving':
            #     logging.info(colored('saving ... wait', 'green'))
            #     return
            # else:
            #     await redis.hset('sess-manager', self.name, 'saving')
            q = []
            for i in range(1024):
                one = await redis.lpop(self.name + "-datas")
                h,v = pickle.loads(b64decode(one))
                q.append(h)
                q.append(v)
            # datas = [pickle.loads(b64decode(i)) for i in  _das]
            # q = []
            # for h,v in datas:
            #     q.append(h)
            #     q.append(v)
            await self.save_to_es(q)

        redis.close()

    def set_handler(self, Chains_obj, end_handler=None, handle=None, turn=-1):
        if hasattr(Chains_obj, 'next'):
            _han = []
            if end_handler:
                _han.append(end_handler)
                _han.append(Chains_obj.__name__ + ".end_handler=" + end_handler.__name__)
            if handle:
                _han.append(handle)
                _han.append(Chains_obj.__name__ + ".chains_handler=" + handle.__name__)

            _han.append(Chains_obj.__name__ + ".turn=" + str(turn))

            obj = get_code(Chains_obj, *_han)
            self.add_listener(obj, classname=Chains_obj.__name__)

    def add_listener(self, code, classname="Chains"):
        r = Redis(db=7)
        r.hset(self.name+"-es", 'code', code)
        r.hset(self.name+"-es", 'classname', classname)

    async def get_code(self):
        redis = await aioredis.create_redis(
            'redis://localhost', db=7, loop=self.loop)
        code = await redis.hget(self.name + "-es", "code", encoding='utf-8')
        code_name = await redis.hget(self.name + "-es", "classname", encoding='utf-8')
        redis.close()
        return code, code_name
    
    def __setitem__(self, k,v):
        r = Redis(db=7)
        if not v:
            r.hdel(self.name+"-es", k)
        else:
            r.hset(self.name+"-es", k, v)

    async def clear_index(self):
        name = self.name
        index = self.status()[1].split("|")[0]
        async with Elasticsearch([i for i in self.host.split(",")]) as es:
            ss = self.load_session(name)
            pwd = self['passwd']
            if pwd:
                e = getpass("passwd :")
                if e != pwd:
                    logging.info("error passwd to delete all data in index: %s" %index)
                    return 
            return await es.indices.delete(index)

    def syn_clear_index(self):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.clear_index())
 
