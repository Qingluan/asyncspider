import asyncio
import aioredis
from mroylib.config import Config
import os
import aiofiles
from bs4 import BeautifulSoup as Bs
from base64 import b64decode, b64encode
from functools import partial
import pickle
import time
from redis import Redis
import logging
from concurrent.futures.thread import ThreadPoolExecutor


CONF = Config(file=os.path.expanduser("~/.config/aio.ini"))
encoder = lambda x: b64encode(pickle.dumps(x)).decode()
decoder = lambda x: pickle.loads(b64decode(x))
#aiofiles.threadpool.wrap.register(mock.MagicMock)(
#    lambda *args, **kwargs: threadpool.AsyncBufferedIOBase(*args, **kwargs))

#async def aio_save(filename, data, t='wb'):
#    mock_file = mock.MagicMock()

#    with mock.patch('aiofiles.threadpool.sync_open', return_value=mock_file) as mock_open:
#        async with aiofiles.open(filename, t) as f:
#            await f.write(data)
#        mock_file.write.assert_called_once_with(data)

async def aio_db_save(hand, data,loop ):
    soup = Bs(data, 'lxml')
    redis = await aioredis.create_redis(
     'redis://localhost', db=6, loop=loop)
    m = {}
    selector = hand['selector']
    if selector:
        m['tag'] = []
        for select_id in selector:
            if not select_id.strip():continue
            for s in soup.select(select_id):
                w = s.attrs
                w['xml'] = s.decode()
                m['tag'].append(w)
    else:
        m['html'] = data 
    await redis.set(hand['url'], encoder(m))
    redis.close()
    await redis.wait_closed()

from qlib.io.tracepoint import trace_cls

class RedisListener:

    exe = ThreadPoolExecutor(64)
    
    def __init__(self,db=0, host='localhost', loop=None):
        #if not loop:
        #    loop = asyncio.get_event_loop()
        self.loop = loop    
        self.host = host
        self.redis_db = db
        self.handler = {} 
        self.runtime_gen = self.runtime()

    def regist(self,key,func, **kargs):
        f = partial(func, **kargs)
        self.handler[key.encode()] = f
        #logging.info(key + " : "+ str(f))

    def clear_db(self):
        r = Redis(host=self.host, db=self.redis_db)
        r.flushdb()

    def runtime(self):
        r = Redis(host=self.host, db=self.redis_db)
        while 1:
            keys = r.keys()
            got_key = []
            for k in self.handler:
                if k in keys:
                    got_key.append(k)
                    
            for kk in got_key:        
                fun = self.handler.pop(kk)
                arg = decoder(r.get(kk))
                # logging.info("handle -> " + kk.decode())
                #import pdb; pdb.set_trace()
                fun(arg)
                #self.__class__.exe.submit(fun, arg)
                r.delete(kk)
            yield
    
    def _run_loop(self, sec):
        r = Redis(host=self.host, db=self.redis_db)
        st = time.time()
        while 1:
            keys = r.keys()
            got_key = []
            for k in self.handler:
                if k in keys:
                    got_key.append(k)
                    
            for kk in got_key:        
                fun = self.handler.pop(kk)
                arg = decoder(r.get(kk))
                #logging.info("handle ->" + kk.decode())
                self.__class__.exe.submit(fun, arg)
            if got_key:
                break
            
            et = time.time()
            if et - st > sec:
                break
     
    def run_loop(self, sec):
        self.__class__.exe.submit(self._run_loop, sec)

    def __iter__(self):
        return self.runtime_gen

    def __next__(self):
        return next(self.runtime_gen)

#loop.run_until_complete(go())
