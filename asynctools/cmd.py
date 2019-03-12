import argparse
import os, sys
from .file import Session
from .chains import show_debug
import  json
from functools import partial
from termcolor import colored
import re

parser = argparse.ArgumentParser(usage="export or import , or start server")
parser.add_argument("-l", "--list", default=False, action='store_true', help="list all session")
parser.add_argument("-d", "--delete", default=False, action='store_true', help="delete this session")
parser.add_argument("-c", "--clear-data", default=False, action='store_true', help="clear this session's data")
parser.add_argument("session", nargs="*", help="show session 's status and links")
parser.add_argument("-e", "--export", default=None, help='export data [redis/es]')
parser.add_argument("-o", "--output",default=None, help='export data output to file')
parser.add_argument("--links", default=False, action='store_true', help="show links status in sesionss") 
parser.add_argument("--chains", default='',  help="filter json res")
parser.add_argument("-k", "--key", default=None, type=str, help="set 'sess_name -k key1 -v val1' key value in sesionss's -es ")
parser.add_argument("-v", "--val", default=None, type=str, help="set 'sess_name -k key1 -v val1'  key value in sesionss's -es ")

def main():
    args = parser.parse_args()

    if args.list:
        for s in Session.list_sessions():
            print(colored("[+]", 'green', attrs=['bold']), colored(s, 'blue'))

        sys.exit(0)

    if args.clear_data and args.session:
        sess = Session.load_session(args.session[0])        
        sess.clear_data()
    
    if args.delete and args.session:
        Session.destroy(args.session[0])
        sys.exit(0)

    if args.export and args.session:
        if args.export == 'es':
            sess = Session.load_session(args.session[0])
            if sess:
                if args.output:
                    with open(args.output, 'w') as fp:
                        f = lambda x: fp.write(json.dumps(x) +"\n")
                        sess.sync_es_range(call=f)
                else:
                    sess.sync_es_range(call=print)

        sys.exit(0)

    if args.key and args.session:
        sess = Session.load_session(args.session[0])
        sess[args.key] = args.val


    if args.session:
        res = []    
        for s in args.session:
            sess = Session.load_session(s)
            if args.links:
                t = sess.status_links()
            else:
                t = sess.all_status()
            if args.chains and 'chains' in t:
                c = t['chains']
                d = {}
                limit = int(re.findall(r'\d+', args.chains)[0])
                for k,v in c.items():
                    v = int(v)
                    if '<' in args.chains:
                        if v < limit:
                            d[k] = v
                    elif '>' in args.chains:
                        if v > limit:
                            d[k] = v
                    else:
                        if v == limit:
                            d[k] = v
                t['chains'] = d

            res.append(t)
        if args.output:
            dir = os.path.dirname(args.output)
            if os.path.exists(dir) and not os.path.exists(args.output):
                with open(args.output, 'w') as fp:
                    for m in res:
                     
                        fp.write(json.dumps(m) + "\n")
        else:
            
            print(json.dumps(res))
        sys.exit(0)
    
