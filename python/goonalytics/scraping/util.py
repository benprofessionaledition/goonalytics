from urllib.parse import urlparse

import lxml.html as ht

from functools import reduce
from typing import List

from bs4 import UnicodeDammit
from urllib3.util import Url
from scrapy.settings import Settings
from datetime import datetime
from time import strptime
import time

import re
import os

def make_shit_comma_separated_func(x, y):
    """
    func for use in reduce() methods to make a list a comma separated string
    :param x:
    :param y:
    :return:
    """
    return str(x) + ',' + str(y)

def single_url(thread_id, thread_page=1):
    return 'https://forums.somethingawful.com/showthread.php?threadid={}&perpage=40&pagenumber={}'.format(thread_id,thread_page)

def urls_from_comma_sep_str(threads):
    urls = ['https://forums.somethingawful.com/showthread.php?threadid=' + t for t in threads.split(',')]
    return reduce(make_shit_comma_separated_func, urls)

def urls_from_list(threads: List):
    return ['https://forums.somethingawful.com/showthread.php?threadid=' + str(t) for t in threads]

def urls_from_dict(thread_map):
    thread_ids = thread_map.keys()
    urls = list()
    for curr_id in thread_ids:
        curr_page = thread_map[curr_id]
        url = 'https://forums.somethingawful.com/showthread.php?threadid=' + str(
            curr_id) + '&userid=0&perpage=40&pagenumber=' + str(curr_page)
        urls.append(url)
    return reduce(make_shit_comma_separated_func, urls)

def rehtml(content):
    """
    does unicode bullshit
    :param content:
    :return:
    """
    doc = UnicodeDammit(content, is_html=True)
    parser = ht.HTMLParser(encoding=doc.original_encoding)
    root = ht.fromstring(content, parser=parser)
    return root

def extract_forum_id_from_url(url: Url):
    q = urlparse(url).query
    fid = re.search('(?<=forumid=)\\d{2,}', q).group(0)
    return fid


# it's fucking beyond retarded that emojis were added to unicode
emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags=re.UNICODE)


def remove_emojis(content):
    return emoji_pattern.sub(r'', content)


def get_scrapy_settings() -> Settings:
    """
    i don't remember what this does but shit doesn't work without it
    :return:
    """
    settings = Settings()
    os.environ['SCRAPY_SETTINGS_MODULE'] = 'goonalytics.scraping.scrapy_settings'
    settings_module_path = os.environ['SCRAPY_SETTINGS_MODULE']
    settings.setmodule(settings_module_path, priority='project')
    return settings

pid_regex = re.compile(r'(^post)(\d+$)')
quote_id_regex = re.compile(r'(.*showthread\.php\?goto=post&amp;postid\=)(\d+)')
whitespace_regex = re.compile(r'(\s{2,}|\n|\t)')

def re_post_id(value: str):
    return pid_regex.match(value)

def re_quote(value: str):
    return quote_id_regex.match(value)

def clean_text(raw_posts: List[str]) -> List[str]:
    """
    Returns a list of only text posts--ignores stuff like people just posting ^ or emoticons
    :param raw_posts:
    :return:
    """
    textonly = [x for x in raw_posts if re.search('(\w+)', x) is not None]
    return textonly

def clean_dates(raw_dates: List[str]) -> List[datetime]:
    """
    Puts the dates in a format that can be parsed by sql stuff
    :param raw_dates: a list of date text obtained by xpathing the html
    :return: python date objects
    """
    space_removed = [str(x).strip() for x in raw_dates]
    no_blanks = filter(None, space_removed)
    date_objs = [strptime(x, '%b %d, %Y %H:%M') for x in no_blanks]
    return date_objs

current_time_ms = lambda: int(round(time.time() * 1000))
