"""
The same bullshit workarounds as scraper_runner but this one gets the info from a simple queue server instead of a file.
Again, the reason this is necessary is because Twisted reactors can't be restarted, and they'd need to be if we wanted to
run thread crawlers followed by post crawlers (and I have zero desire to call their shitty API directly just to make rest calls)
"""

import logging
import argparse
import platform
import os
import shlex

import subprocess


from goonalytics.scraping.bq_scrapers import get_url_from_server

logging.basicConfig()

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

parser = argparse.ArgumentParser()
parser.add_argument('user', type=str, help='Forums username')
parser.add_argument('password', type=str, help='Forums password')
parser.add_argument('server', type=str, help='Server with cli args')
args = parser.parse_args()
# raise ConnectionAbortedError('stopping for debug')
uname = args.user
passwd = args.password
server = args.server

# set up cli crap
system = platform.system()
python_name = 'python' if system == 'Darwin' else 'python3' # the container
scrapefile = os.path.abspath(os.path.join(os.getcwd(), os.pardir)) + "/scraping/bq_scrapers.py"

while True:
    try:
        r = get_url_from_server(server) # actually not a url
        cmd = " ".join([python_name, scrapefile, uname, passwd, r])
        args = shlex.split(cmd)
        proc = subprocess.Popen(args, stdin=subprocess.PIPE)
        proc.wait()
    except ValueError:
        log.info("Server queue exhausted, exiting")
        raise
        # quit(0)

