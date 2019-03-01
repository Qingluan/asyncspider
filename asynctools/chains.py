### this file is to deal with chains requests


# include 
# *. how to deal response 
# *. how to change response and header to new request


# define a simple language to handle response and header
import  re
import  os
import  inspect
import  json
import  time
from bs4 import  BeautifulSoup as Bs
from base64 import  b64decode, b64encode
import importlib.machinery
import  logging
import  tabulate
import inspect
from termcolor import colored, HIGHLIGHTS
from types import FunctionType

log_str = """
import  logging
import  tabulate
from termcolor import colored, HIGHLIGHTS
from types import FunctionType
import inspect

def show_debug(*args, **kargs):
    
    ck = list(HIGHLIGHTS.keys())[1:]
    res = []
    for i,v in enumerate(args):
        c = ck[i % (len(ck)-1)]
        res += [colored(v, on_color=c,attrs=['bold'])]
    w = '|'.join(res) + "\\n"
    s = kargs.items()
    vv = list(map( lambda x: (colored(x[0],'blue'), colored(x[1], 'green')), s))
    w += tabulate.tabulate(vv)
    logging.info(w)

"""

def show_debug(*args, **kargs):
    
    ck = list(HIGHLIGHTS.keys())[1:]
    res = []
    for i,v in enumerate(args):
        c = ck[i % (len(ck)-1)]
        res += [colored(v, on_color=c,attrs=['bold'])]
    w = '|'.join(res) + "\n"
    s = kargs.items()
    vv = list(map( lambda x: (colored(x[0],'blue'), colored(x[1], 'green')), s))
    w += tabulate.tabulate(vv)
    logging.info(w)
    # input("<<any key to go>>")
    # return  w



class Chains:
	turn = 5
	def __init__(self, hand):
		self.hand = hand
		self.turn = self.__class__.turn
		self.order = -1


	def set_headers(self, **kargs):
		self.hand['kwargs']['headers'] = kargs

	def set_url(self,url):
		self.hand['url'] = url

	def next(self, order=None):
		self.order += 1
		args = ['='.join([str(ii) for ii in i]) for i in self.hand.items() if i[0] != 'kwargs' and i[0] != 'read' and i[0] != 'data']
		show_debug(self.order, *args, **self.hand['kwargs'].get('headers'))
		# logging.info(colored(inspect.getsource(self.chains_handler), attrs=['bold']))
		self.chains_handler()

	def set_handle(self, handle):
		self.chains_handler = handle

	def chains_handler(self):
		pass

	# def response(self, resp, command, tp='html'):
	# 	keys,value = command.split("->",1)
	# 	if tp == 'json':
	# 		resp = json.loads(resp)
	# 		key_value = resp
	# 		for k in keys.split(">"):
	# 			key_value = key_value[k.strip()]


	# 	where,where_key = value_rule.findall(value)[0]
	# 	if where == 'url':
	# 		self.hand['url_keys'][where_key] = key_value


def get_code(*O):
	res = b''
	for o in O:
		if isinstance(o, str):
			res += o.encode('utf-8') + b"\n"
			continue
		res += inspect.getsource(o).encode('utf-8') + b"\n"
	res = b64encode(res)
	return res

def import_from_tmp(ff):
	tmp_chains = importlib.machinery.SourceFileLoader('tmp_chains',ff).load_module()
	os.remove(ff)
	return  tmp_chains.Chains

def recv_code(code):
	if isinstance(code, str):
		code = code.encode('utf-8')
	ff = time.time()
	with open('/tmp/' + str(ff), 'wb') as fp:
		fp.write((log_str+"\n").encode('utf-8'))
		fp.write(b64decode(code))
	return  import_from_tmp('/tmp/' + str(ff))



