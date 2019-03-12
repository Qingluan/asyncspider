import argparse
import os, sys
from .file import Session
from .chains import show_debug
import  json
from functools import partial
from termcolor import colored

parser = argparse.ArgumentParser(usage="export or import , or start server")
parser.add_argument("-l", "--list", default=False, action='store_true', help="list all session")
parser.add_argument("session", nargs="*", help="show session 's status and links")
parser.add_argument("-e", "--export", default=None, help='export data [redis/es]')
parser.add_argument("-o", "--output",default=None, help='export data output to file')

def main():
    args = parser.parse_args()

    if args.list:
        for s in Session.list_sessions():
            print(colored("[+]", 'green', attrs=['bold']), colored(s, 'blue'))

        sys.exit(0)

    if args.session:
        res = []    
        for s in args.session:
            sess = Session.load_session(s)
            t = sess.all_status()
            res.append(t)
        if args.output:
            dir = os.path.dirname(args.output)
            if os.path.exists(dir) and not os.path.exists(args.output):
                with open(args.output, 'w') as fp:
                    for m in res:
                        fp.write(json.dumps(m) + "\n")
        else:
            print(res)
        sys.exit(0)
    
    if args.export and args.session:
        if args.export == 'es':
            sess = Session.load_session(args.session[0])
            if sess:
                with open(args.output, 'w') as fp:
                    f = lambda x: fp.write(json.dumps(x) +"\n")
                sess.sync_es_range(call=f)
