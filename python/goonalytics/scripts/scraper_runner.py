"""
Deals with bullshit workarounds because twisted and scrapy suck. Literally takes a bunch of args and spawns subprocesses
because you have to kill the whole thing between scrapers
"""
import logging
import argparse
import platform
import os
import shlex

import subprocess

logging.basicConfig()

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

parser = argparse.ArgumentParser()
group = parser.add_mutually_exclusive_group()
parser.add_argument('user', type=str, help='Forums username')
parser.add_argument('password', type=str, help='Forums password')
parser.add_argument('idlist', type=str, help='Forum id(s) to scrape', nargs='+')
args = parser.parse_args()
log.debug("ID list: %s", args.idlist)
# raise ConnectionAbortedError('stopping for debug')
uname = args.user
passwd = args.password

# define strings for subproc commands
system = platform.system()
python_name = 'python' if system == 'Darwin' else 'python3' # the container
scrapefile = os.path.abspath(os.path.join(os.getcwd(), os.pardir)) + "/scraping/bq_scrapers.py"

command_list = list()
for forum_id in args.idlist:
    command_list.append(python_name + " " + scrapefile + " " + uname + " " + passwd + " -t " + forum_id)
    command_list.append(python_name + " " + scrapefile + " " + uname + " " + passwd + " -p " + forum_id)

for command in command_list:
    # execute one at a time because the container is a chuggin lil turd
    args = shlex.split(command)
    proc = subprocess.Popen(args, stdin=subprocess.PIPE)
    proc.wait()