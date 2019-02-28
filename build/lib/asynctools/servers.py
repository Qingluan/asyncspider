import asyncio
import socket
import pickle
import time
import functools
import random
import os, sys
import re
from multiprocessing import Process
import multiprocessing
from threading import Thread
from contextlib import contextmanager
import urllib.parse as up
import async_timeout
import aiosocks
import aiohttp
import json
import time
import random
from termcolor import cprint, colored
from aiosocks.connector import ProxyConnector, ProxyClientRequest
from collections import deque
import argparse
from asynctools.daemon import Daemon
from asynctools.udp import open_remote_endpoint as open_udp_connection
from asynctools.libs import loger
from asynctools.file import aio_db_save, RedisListener, Session
# from qlib.io.tracepoint import trace_cls
from asynctools.res import USER_AGENTS
import traceback


log = loger()

def _r(port=12888):
    a = _AServer(port=port)
    a.run()

def start_local_socket_process(port):
    p = Process(target=_r, kwargs={'port':port})
    p.start()
    log.info("start port: %d" % port)

class ConnectionCache(dict):
    ins = deque()
    timeout = 50
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__class__.ins.append(self)


    def __setitem__(self, k, v):
        super().__setitem__(k, v)
        if 'ctime' not in self:
            self['ctime'] = time.time()
    def __getitem__(self, k):
        ntime = time.time()
        l = len(self.__class__.ins)
        for i in range(l):
            if self == self.__class__.ins[i]:continue
            if ntime -  self.__class__.ins[i]['ctime'] > self.__class__.timeout:
                del self.__class__.ins[i]
        return super().__getitem__(k)



class _AServer:

    def __init__(self, port=12888, error_times=5):
        self.port = port
        self.loop = asyncio.get_event_loop()
        if self.loop.is_closed():
            self.loop = asyncio.new_event_loop()
        self.queue = asyncio.Queue(loop=self.loop)
        self.tcp_handlers = {}
        self.udp_handlers = {}
        self.http_handlers = {}
        self.finish_urls = {}
        self.error_urls = {}

        self.try_error_times = error_times


    async def _tcp(self, ip, port, timeout=7, **kwargs):
        waiter = asyncio.open_connection(ip, port, loop=self.loop)
        try:
            reader, writer = await asyncio.wait_for(waiter, timeout=timeout)
        except asyncio.TimeoutError as e:
            log.error("Timeout : {}:{}".format(ip, port))
            await self.queue.put("timeout :tcp: " + ip)
            return
        except socket.error as e:
            log.error(e)
            await self.queue.put(e)
            return

        id = self._create_id()
        self.tcp_handlers[id] = {'write': writer.write, 'read':reader.read, 'if_close': False, 'close':writer.close}
        return id

    async def _http(self, url, port, timeout=7, **kwargs):
        conn = ProxyConnector()
        if isinstance(url, bytes):
            url = url.decode("utf8", "ignore")
        session =  aiohttp.ClientSession(loop=self.loop, connector=conn, request_class=ProxyClientRequest)
        try:
            proxy = None
            if 'proxy' in kwargs:
                proxy = kwargs['proxy']


            id = self._create_id()
            # log.info('ready session : {}'.format(url))
            self.http_handlers[id] = {
                'read': functools.partial(self._http_read, session, url),
                'close':None,
                'if_close': print,
                'kwargs':{'proxy':proxy},
                'url': url,
                'selector': '',
                'db_to_save':'',
                'session_name': 'default'
            }
            return id
        except asyncio.TimeoutError as e:
            session.close()
            await self.close(id)
        except Exception as e:
            traceback.print_stack()
            session.close()
            log.error(e)
            if id in self.http_handlers:
                await self.close(id)
            return

    async def _http_release(self, response):
        return await response.release()

    async def _http_read(self, session, url,session_name='default', **kwargs):

        m = kwargs.get('method', 'get')
        #selector = kwargs.get("selector","").split("|")

        if 'method' in kwargs:
            kwargs.pop('method')
        if m == 'get':
            async with  session.get(url, **kwargs) as response:
                # log.info('connect : {}'.format(url))
                if not response.status == 200:
                    log.error("[%d] : url : %s" % (response.status, url))
                    await response.release()
                    response.close()
                    await Session.trace(session_name, url, ok=False)
                else:
                    try:
                        text= await response.text()
                        # trance url if ok
                        await Session.trace(session_name, url, ok=True)
                        await response.release()
                        response.close()
                        return text
                    except Exception:
                        response.close()
                        return await response.release()
        elif m =='post':
            async with session.post(url, **kwargs) as response:
                log.info('post : {}'.format(url))
                if not response.status == 200:
                    log.error("Error: %s" % colored(str(response.status), 'red'))
                    await response.release()
                    response.close()
                    await Session.trace(session_name, url, ok=False)
                else:
                    try:
                        text = await response.text()
                        await Session.trace(session_name, url, ok=True)
                        await response.release()
                        response.close()
                        return text
                    except Exception:
                        response.close()
                        return await response.release()





    def _create_id(self):
        id = str(int(time.time())) + str(random.randint(1,10))
        return id

    async def _broadcast(self, msg=None, timeout=7, **kwargs):
        pass

    async def _udp(self, ip, port,msg=None,timeout=7, **kwargs):
        handler = await open_udp_connection(ip, port , loop=self.loop)
        data =None
        try:
            if msg:
                handler.send(msg)
                recive_waiter = handler.recive()
                data, addr = asyncio.wait_for(recive_waiter, timeout=timeout)

        except asyncio.TimeoutError:
            log.error("Timeout udp: {}:{}".format(ip, port))
            await self.queue.put("Timeout :udp: " +ip)
            return
        except socket.error as e:
            log.error(e)
            await self.queue.put(e)
            return

        id = self._create_id()
        self.udp_handlers[id] = {'write': handler.send, 'read':handler.receive, 'if_close': False, 'close': handler.close, 'data':data}
        return id

    def check_write(self,writer, data):
            data += b"{__end__}"
            writer.write(data)

    async def commander(self,reader, writer):
        log.info(f"create tcp : {len(self.tcp_handlers)} http: {len(self.http_handlers)} ")

        data = await reader.read(65534)
        # log.info("recv from " + str(writer.get_extra_info('peername')))
        c = data[:2]
        msg = data[2:]
        if c == b'in':
            tp,ip,port,link_num = msg.split(b',')
            s = self._tcp
            if tp == b'udp':
                s = self._udp
            elif tp == b'http':
                s = self._http

            if link_num == 'D':
                _id = await s(ip, int(port))
                _id2 = await s(ip, int(port))
                id = [_id, _id2]
            else:
                id = [await s(ip, int(port))]

            if len(id) == 0:
                e = await self.queue.get()
                self.check_write(writer, pickle.dumps({'msg':'create failed:' + str(e)}))
                await writer.drain()
            else:
                # log.info("create : " + id)
                # for i in id:
                _id2 = ''
                if len(id) > 1:
                    _id2 = id[1]
                self.check_write(writer, pickle.dumps({'msg':'create ok', 'id':id[0], 'id2':_id2}))
                await writer.drain()


        elif c == b'rd':
            id = msg.decode()
            self.check_write(writer, pickle.dumps({'id': id , 'msg':'read command accept, get data by: {id}'.format(id=id)}))
            await writer.drain()
            await self.read(id)

        elif c == b'wt':
            id, data = msg.split(b',', 1)
            id = id.decode()
            await self.write(id, data)
            self.check_write(writer, pickle.dumps({'id':id, 'msg':'write to ' + id }))
            await writer.drain()

        elif c == b'op':
            id, data = msg.split(b',', 1)
            options = pickle.loads(data)
            selector = []
            db_to_save = ''
            h,tp = self.get_handler(id)
            # log.info("{}".format(options))
            _REGIST_KEY = [
                'selector',
                'db_to_save',
                'es_host',
                'es_index', 
                'es_filter', 
                'es_type',
                'session_name',
                'type',
            ]
            
            for k in _REGIST_KEY:
                if k in options:
                    es_v = options.pop(k)
                    if 'selector' == k:
                        es_v = es_v.split("|")
                        log.info("selector load :" + " ".join(es_v))
                    h[k] = es_v
                    log.info(colored('{} -> {}'.format(k, es_v), 'magenta'))
            
                # log.info(colored("db -> %s" % db_to_save, 'magenta'))
                
            
            
            if h:
                if options:
                    if not 'kwargs' in h:
                        h['kwargs'] = {}
                    h['kwargs'].update(options)
                    options = h['kwargs']
                else:
                    options = h['kwargs']
                self.check_write(writer, pickle.dumps({'id':id, 'msg':'set options ok' , 'options':options}))
                await writer.drain()
            else:
                log.error("Can not found id:{}".format(id))
                self.check_write(writer, pickle.dumps({'id':id, 'msg':'not found id' , 'error':True}))
                await writer.drain()

        elif c == b'da':
            id = msg.decode()
            h,tp = self.get_handler(id)
            if h:
                if 'data' in h and h['data']:
                    if len(h['data']) < 32271 :
                        self.check_write(writer, pickle.dumps({'id': id, 'data': h['data'], 'msg':'ok'}))
                        await writer.drain()
                    else:
                        L = len(h['data'])
                        C = L // 32271

                        for i in range(C):
                            sm = h['data'][i * 32271: (i+1) * 32271]
                            self.check_write(writer, pickle.dumps({'id': id, 'data': sm, 'msg':'ok' , 'wait':i}))
                            log.info(i)
                            await writer.drain()
                        if C * 32271 < L:
                            sm = h['data'][C*32371:]
                            self.check_write(writer, pickle.dumps({'id': id, 'data': sm, 'msg':'ok'}))
                            log.info('last')
                            await writer.drain()

                else:
                    self.check_write(writer, pickle.dumps({'id': id, 'data': None, 'msg':'null'}))
                    await writer.drain()

            else:
                self.check_write(writer, pickle.dumps({'id':id, 'msg':'no id'}))
                await writer.drain()
        elif c == b'cl':
            id = msg.decode()
            await self.close(id)
            self.check_write(writer, pickle.dumps({'id':id, 'msg':'killed'}))
            await writer.drain()

        elif c == b'ls':
            id_t = list(self.tcp_handlers.keys())
            id_u = list(self.udp_handlers.keys())
            id_h = list(self.http_handlers.keys())
            self.check_write(writer, pickle.dumps({'tcp':id_t, 'udp':id_u, 'http':id_h}))
            await writer.drain()

        elif c == b'lo':
            msg = await self.queue.get()
            self.check_write(writer, pickle.dumps({'msg':msg}))
            await writer.drain()
        elif c == b'ki':
            raise KeyboardInterrupt("Exit --")

    async def read(self, id, callback=None, proxy=None):
        hand_bak = None
        with self.deal(id) as hand:
            read = hand['read']
            hand_bak = hand
            kwargs = {}
            if 'kwargs' in hand:
                kwargs = hand['kwargs']
            #log.info("wait read")
            try:
                st = 0
                while 1:
                    if st > 10:
                        log.error("[%s] [%s]" % (colored(hand['url'], 'red'), "try over"))
                        raise asyncio.TimeoutError("socket seemd closed")

                    waiter = read(session_name=hand['session_name'],**kwargs)
                    data = await asyncio.wait_for(waiter, timeout=12)

                    if isinstance(data, str):
                        data = data.encode("utf8")
                    # async wait mtu and all data
                    # only for tcp and udp
                    if isinstance(data, bytes):
                        c = 0
                        #log.info("recv no.%d" % c)
                        while data.endswith(b'[continu@]'):
                            waiter = read()
                            extand_data = await asyncio.wait_for(waiter, timeout=10)
                            c += 1
                            if data.endswith(b'[continu@]'):
                                data = data[:-len('[continu@]')] + extand_data
                            else:
                                data += extand_data

                            #log.info("recv no.%d" % c)

                        log.info("[%s] 200 [%d]" % (colored(hand['url'], 'green'),len(data)))
                    else:
                        log.info("recv a object from remote : {}".format(type(data)))


                    if not data:
                      st += 1
                    # log.info(id + " :got data ")
                    h, t = self.get_handler(id)
                    hand_bak = h
                    if h:
                        h['data'] = data
                        if callback:
                            callback(data)
                        else:
                            # log.info(colored("{}".format(h), 'green') )
                            db_to = h['db_to_save']
                            if 'selector' in h:
                                log.info(colored(h['url'], 'blue') + db_to)
                                log.info(colored(data[:100], "green"))
                                await self.save_local(str(id), h, data)
                        break
                    else:
                        if data:
                            hand['data'] = data
                            db_to = hand['db_to_save']
                            if 'selector' in hand:
                                log.info(colored(hand['url'], 'blue', attrs=['bold']) + " -> %s %d" % (db_to, len(data) ) ) 
                                log.info(colored(data[:100], "green"))
                                await self.save_local(str(id), hand, data)
                            break
                        else:
                            raise Exception("session is loss : "+ hand['url'])
            except asyncio.TimeoutError as e:
                await self.close(id)
                log.error("timeout : %s" % colored(hand['url'],'red') )
                await self.retry_http(str(id), hand, callback, proxy)
                #log.info("close id: " + id)
            except socket.error as e:
                if 'Cannot connect to host' in str(e) and 'ssl' in str(e).lower():
                    log.warn("ssl error , try again")
                    await self.retry_http(str(id), hand_bak, callback ,proxy)
                else:
                    await self.retry_http(str(id), hand, callback, proxy)
            except Exception as e:
                log.error("404: %s" % str(e))
                log.exception(e)
                # traceback.print_stack()
                await self.retry_http(str(id), hand, callback, proxy)

    
    async def retry_http(self,id, hand, callback, proxy):
        url = hand['url']
        if url not in self.error_urls:
            self.error_urls[url] = 1
        else:
            if self.error_urls[url] > self.try_error_times:
                log.error("Over times: %s" % colored(self.error_urls[url], 'red'))
                await self.save_local(id, hand, {"error": "overtime"})
                return
            self.error_urls[url] += 1
        self.http_handlers[id] = hand
        log.info('wider try: ' + colored(hand['url'], 'yellow'))
        await self.read(id, callback=callback, proxy=proxy)

    async def save_local(self,id, hand,data):
        await aio_db_save(id, hand, data, self.loop)


    async def write(self,id ,data):
        with self.deal(id) as hand:
            write = hand['write']
            write(data)

    def run(self):
        coro = asyncio.start_server(self.commander, '127.0.0.1', self.port, loop=self.loop)
        server = self.loop.run_until_complete(coro)
        try:
            print("Run")
            self.loop.run_forever()
        except KeyboardInterrupt:
            pass

        # Close the server
        server.close()
        self.loop.run_until_complete(server.wait_closed())
        self.loop.close()

    async def close(self, id):
        log.info('close id: {id}'.format(id=id))
        h,t = self.get_handler(id)
        if t == 'tcp':
            h['close']()
            del self.tcp_handlers[id]
        elif t == 'udp':
            h['close']()
            del self.udp_handlers[id]
        elif t == 'http':
            del self.http_handlers[id]

    def get_handler(self,id ):
        if isinstance(id, bytes):
            id = id.decode()
        if id in self.udp_handlers:
            return self.udp_handlers[id],'udp'
        elif id in self.tcp_handlers:
            return self.tcp_handlers[id],'tcp'
        elif id in self.http_handlers:
            return self.http_handlers[id],'http'
        else:
            return None,None

    @contextmanager
    def deal(self, id):
        try:
            h,p = self.get_handler(id)
            if not h:
                log.error("not found")
                raise Exception("session id is not found !")
            yield h
        finally:
            if  h and h['if_close']:
                if p == 'udp':
                    del self.udp_handlers[id]
                elif p == 'tcp':
                    del self.tcp_handlers[id]


def test_if_local_socket_open(port=12888):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(('127.0.0.1', port))
        return True
    except Exception:
        return False

#if not test_if_local_socket_open():
#    start_local_socket_process()

class ConnectionCreateError(Exception):
    pass

class MyEventLoopPolicy(asyncio.DefaultEventLoopPolicy):

    def get_event_loop(self):
        """Get the event loop.

        This may be None or an instance of EventLoop.
        """
        loop = super().get_event_loop()
        # Do something with loop ...
        return loop




class ConnectinoWaiter(Thread):
    running_tasks = []
    def __init__(self, fun, *args,double_route=False, judge_fun=None, callback=None, loop=None,policy =None, timeout=38,**kargs):
        super().__init__()
        self.judge_fun = judge_fun
        if not judge_fun:
            self.judge_fun = lambda x: x is not None
        self.callback = callback
        self.args = args
        self.kwargs = kargs
        self.timeout = timeout
        self.fun = fun
        self.loop = loop
        self.policy = policy
        while 1:
            if len(ConnectinoWaiter.running_tasks) > 128:
                break
            time.sleep(1)
        ConnectinoWaiter.running_tasks.append(self)

    def run(self):
        st = time.time()
        res = None
        fun = self.fun
        args = self.args
        kwargs = self.kwargs
        callback = self.callback
        judge_fun = self.judge_fun
        timeout = self.timeout
        if self.loop:
            print("patch loop")
        if self.policy != None:
            print('patch policy')
            asyncio.set_event_loop_policy(self.policy())

        while 1:
            if time.time() - st > timeout:
                break
            res = fun(*args, **kwargs)
            if judge_fun(res):
                # print("finish!")
                break
            time.sleep(0.5)
            # log.info(res)
        if callback:
            if self.loop:
                self.loop.add_callback(functools.partial(callback, res))
            else:
                callback(res)
        ConnectinoWaiter.running_tasks.remove(self)


def load_m_async_num():
    NUM_FILE = os.path.expanduser("~/.config/m-asyncs.num")
    if not os.path.exists(NUM_FILE):
        with open(NUM_FILE, 'w') as fp:
            fp.write('12')
        return [12888 + i for i in range(12)]
    else:

        with open(NUM_FILE) as fp:
            try:
                return [12888 + i for i in range(int(fp.read().strip()))]
            except ValueError:
                return [12888 + i for i in range(12)]


ASYNC_PORTS = load_m_async_num()

class Connection:
    ports = None
    def __init__(self, ip='127.0.0.1', port=80, tp='http', double_route=False, id=None, loop=None, policy=None, ports=ASYNC_PORTS):
        self.loop = loop
        self.ports = ports
        self.__class__.ports = ports
        self.policy = policy
        self.Lport = 12888
        if self.ports:
            self.Lport = random.choice(self.ports)
            # log.info("connect to %d" % self.Lport)
        if self.policy is not None:
            log.info("== prepare policy ==")
        if id:
            self.id = id
        else:
            self.tp = tp
            self.ip = ip
            self.port = int(port)
            self.double_route = "D" if double_route else "S"
            tmp = self._write(b'in' + ",".join([tp, ip, str(port), self.double_route]).encode())
            if 'id' in tmp:
                self.id = tmp['id']
            else:
                log.error(tmp)
                raise ConnectionCreateError(tmp['msg'])



    def _write(self,data, read=True):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            s.connect(("127.0.0.1", self.Lport))
        except socket.error as e:

            log.error(str(e) + " You should check m-async if start ")
            os.popen('m-asyncs start')
            return None
        s.send(data)
        if read:
            D = None
            con = False
            d = b''
            while 1:
                d += s.recv(32280)
                if d.endswith(b"{__end__}"):
                    d = d[:-len('{__end__}')]
                    break

            D = []
            try:
                D = pickle.loads(d)
            except OSError as e:
                log.error(f"pickle error !: {data}")
                return None
            if 'wait' in D:
                Data = D['data']

            while 'wait' in D:
                con = True
                d = b''
                while 1:
                    d += s.recv(32280)
                    if d.endswith(b"{__end__}"):
                        d = d[:-len('{__end__}')]
                        break
                D = pickle.loads(d)
                Data += D['data']
            if not con:
                return pickle.loads(d)
            else:
                D['data'] = Data

                return D

    def get(self, wait=False,callback=None, loop=None):
        if loop:
            self.loop = loop
        if callback:
            return self.read(callback=callback)
        else:
            return self.read()

    def post(self, data, wait=False,callback=None, loop=None, **kwargs):
        if loop:
            self.loop = loop
        self.options(method='post', data=data, **kwargs)
        if callback:
            return self.read(callback=callback)
        else:
            self.read()
            return self.data(wait)

    def options(self, **kwargs):
        """
        @selector if setting will direct select 
        """
        id = self.id
        m = 'op' + id + ","
        msg = self._write(m.encode() + pickle.dumps(kwargs))
        return msg

    def info(self):
        id = self.id
        m = 'rq' + id
        msg = self._write(m.encode())

    def read(self, wait=False, callback=None):
        id = self.id
        m = 'rd' + id
        msg = self._write(m.encode())
        if callback:
            c = ConnectinoWaiter(self.data, callback=callback, loop=self.loop, policy=self.policy)
            c.start()
            return msg

        if not wait:
            return msg
        return self.data(wait=True)

    def log(self):
        return self._write(b'lo')

    def data(self, wait=False):
        id = self.id
        m = 'da' + id
    


        d = self._write(m.encode(), read=True)

        if 'data' in d and d['data']:
            log.debug(d['msg'])
            return d['data']
        else:

            if wait:
                timeout = 7
                if isinstance(wait, int):
                    timeout = int(wait)
                s = time.time()
                while 1:
                    if time.time() - s > timeout:
                        break
                    if d['data']:
                        log.debug(d['msg'])
                        return d['data']
                log.debug(d['msg'])
                return d['data']

    def __del__(self):
        self.close()

    def close(self):
        if self.id:
            return self._write(b'cl' + self.id.encode("utf8"))

    def write(self, data):
        id = self.id
        m = 'wt' + self.id
        m = m.encode()
        if isinstance(data, str):
            data = data.encode('utf8')
            m += b',' + data
        elif isinstance(data, bytes):
            m += b',' + data
        else:
            raise Exception("must str or bytes")

        return self._write(m)

    def ls(self):
        return self._write(b"ls")

    def inject(self, id):
        return Connection(id=id)

    @staticmethod
    def stop():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        for port in  Connection.ports:
            s.connect(("127.0.0.1", port))
            s.send(b'ki')


class HttpXp:
    '''
    @db  db == 'es': save resonpse to es if true
        @es_index : set index
        @es_type: set type
        @es_filter: set filter to re filter
        @es_host:  exm: localhost:9200, xxx:xxx

    '''
    session_name = 'default'
    def __init__(self, url, *selector, agent=True, random_agent=True, proxy=False,
                 db='redis',
                 session=None,
                 socks_proxy='socks5://127.0.0.1:1080', **kargs):
        
        self.options = kargs
        self.options['selector'] = '|'.join(selector)
        if not session:
            session = self.__class__.session_name
        self.options['session_name'] = session
        self.options['db_to_save'] = db
        self.url = url
        self.con = Connection(url)
        if agent:
            if random_agent:
                ag = random.choice(USER_AGENTS)
            else:
                ag = USER_AGENTS[0]
            self.options['headers'] = {"user-agent": ag}
        if proxy:
            self.options['proxy'] = socks_proxy
        
        if self.options.get('type') == 'json' and 'es_filter' in self.options:
            self.options['es_filter'] = json.dumps(self.options['es_filter'])
        
        self.response_type = self.options.get('type', 'raw')
        self.es_filter = self.options.get('es_filter')
        self.db_to_save = self.options.get('db_to_save', 'redis')
        self.session = Session.load_session(session)

    @classmethod
    def get_session(cls, name):
        if not name:
            name = cls.session_name
        return Session.load_session(name)

    @classmethod
    def create_session(cls, name, index='', type=''):
        sess = Session.load_session(name, index=index, type=type)
        cls.session_name = name

    def es_index_type(self, index, type):
        self.session.set_es_index_type(index, type)

    @classmethod
    def switch_session(cls, name):
        cls.session_name = name

    @classmethod
    def destroy_session(cls, name):
        Session.destroy(name)

    @classmethod
    def list_session(cls):
        return Session.list_sessions()

    def __getitem__(self, k):
        return self.session[k]

    def status(self):
        return self.session.status()

    def status_links(self):
        return self.session.status_links()

    def post(self,data, callback=None, runtime=10):
        self.con.post(data=data, **self.options)
        if callback:
            register = RedisListener(db=6)
            id = str(self.con.id)
            register.regist(id, callback )
            register.run_loop(runtime)

    def get(self, callback=None, runtime=120):
        self.con.options(**self.options)
        self.con.get()
        if callback:
            register = RedisListener(db=6)
            id = str(self.con.id)
            register.regist(id, callback )
            register.run_loop(runtime)

    def __repr__(self):
        return "Session_" + self.session.name





class DaemonSocket(Daemon):
    def __init__(self, *args, port=12888,error_times=5, **kwargs):
        super().__init__(*args, **kwargs)
        self.port = int(port)
        self.error_times = error_times

    def run(self):
         serv = _AServer(self.port, error_times=self.error_times)
         log.info("Start local socket server!")
         serv.run()

def start_socket_service():
    port = ''
    error_time = 5
    if len(sys.argv) > 2:
        port = sys.argv[2]
    if len(sys.argv) > 3:
        error_time  = int(sys.argv[3])
    

    if sys.argv[1] == 'start':
        # start_local_socket_process(12889)
        # start_local_socket_process(12890)
        # start_local_socket_process(12891)
        d = DaemonSocket('/tmp/async_sockset.pid' + port, port=port, error_times=error_time)
        d.start()
    elif sys.argv[1] == 'stop':
        print(" clear file")
        os.popen("rm /tmp/async_sockset.pid* ").read()
        print("kill process")
        os.popen("ps aux | grep m-async | grep -v grep | awk '{ print $2 }' | xargs kill -9 ").read()
    elif sys.argv[1] == 'restart':
        d = DaemonSocket('/tmp/async_sockset.pid' + port, port=port)
        d.restart()
    log.info("Start service async socket")

def run_local_async():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--num', default=12, type=int, help='how many server to wait for connect. >= 12')
    parser.add_argument('command', help='start / stop / restart')
    args = parser.parse_args()

    if args.command in ['start',  'restart']:
        if not os.popen("ps aux | grep redis | grep -v 'grep' ").read().strip():
            print(colored("first to install Redis !!\nthen start redis as daemon!!!","red"))
            sys.exit(1)
        start_num = 12888
        with open(os.path.expanduser("~/.config/m-asyncs.num"), 'w') as fp:
            fp.write(str(args.num))
        for i in range(args.num):
            cmd = f"m-async {args.command} {start_num + i}"
            os.popen(cmd)
    elif args.command == 'stop':
        print(" clear file")
        os.popen("rm /tmp/async_sockset.pid* ").read()
        print("kill process")
        os.popen("ps aux | grep m-async | grep -v grep | awk '{ print $2 }' | xargs kill -9 ").read()

    else:
        print('Must in start / stop / restart')
    


async def tcp_echo_client(num, host, loop):
    h, p = host.split(":")
    try:
        st = time.time()
        conner = asyncio.open_connection(h, int(p), loop=loop)
        reader, writer = await asyncio.wait_for(conner, timeout=7)
        et = time.time() -st
        # print('Close the socket')
        writer.close()
        return host,et

    except asyncio.TimeoutError:
        return host,9999
    except socket.error as e:
        # traceback.print_stack()
        return host,9999
    # print('Send: %r' % message)
    # writer.write(message.encode())

    # data = yield from reader.read(100)
    # print('Received: %r' % data.decode())


async def _tcp_test(hosts, loop):

    task = [tcp_echo_client(i, host, loop) for i, host in enumerate(hosts)]
    return await asyncio.gather(*task)


def TcpTests(hosts, loop=None):
    if not loop:
        loop = asyncio.get_event_loop()
    if loop.is_closed():
        loop = asyncio.new_event_loop()
    res = loop.run_until_complete(_tcp_test(hosts, loop))
    loop.close()
    return [i for i in sorted(res, key=lambda x:x[1]) if i[1] < 100]




#def shutdown():
 #   con = Connection.stop()
  #  del con
   # log.info("Bye ~")

#import atexit
#atexit.register(shutdown)
