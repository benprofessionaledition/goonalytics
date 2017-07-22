"""
Basic scraper classes--this entire module can be trashed probably
"""
import logging
import sqlite3 as sql
from datetime import datetime
from time import sleep, strptime
from typing import Iterable

import scrapy
from lxml.etree import XMLSyntaxError
from scrapy import FormRequest
from scrapy import Request
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.http import Response
from scrapy.spiders.init import InitSpider
from scrapy.xlib.pydispatch import dispatcher

from goonalytics.base import Post, Thread
from goonalytics.io.baseio import SQLiteDAO
from goonalytics.io.gcloudio import AvroWriter, PostBigQueryer
from goonalytics.scraping.util import *
from goonalytics.settings import DATABASE_LOCATION, GCLOUD_POST_TABLE

logging.basicConfig()

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

@DeprecationWarning


class ThreadSpider(scrapy.Spider):
    """
    A really basic spider that just gets basic thread info
    """
    name = "threadspider2"

    def __init__(self, username='', password='', dry_run='', ids='', **kwargs):
        super(ThreadSpider, self).__init__(**kwargs)
        idlist = ids.split(',')
        self.dry_run = bool(dry_run)
        self.start_urls = ['http://forums.somethingawful.com/forumdisplay.php?forumid=' + str(id) for id in idlist]

    allowed_domains = {'forums.somethingawful.com'}

    def parse(self, response):
        """
        Parses the first 8ish pages
        """
        print("Extracting...")
        if not self.dry_run:
            for item in self.response_transform(response):
                self.response_load(item)
        for i in range(1, 8):
            url = 'http://forums.somethingawful.com/' + response.xpath('//a[@title="Next page"]/@href').extract()[0]
            print(str(url))
            sleep(0.2)
            print("Iterating in parse: " + str(url))
            yield scrapy.Request(url, callback=self.parse)

    def response_transform(self, response: Response) -> Iterable[Thread]:
        """
        Makes a list of items from the response
        """
        forum_id = self.extract_forum_id_from_url(response.url)
        print(str(forum_id))
        thread_strings = response.xpath('//tbody/tr[@class="thread"]/@id').extract()  # gives 'thread#######'
        thread_authors = response.xpath('//tbody//td[@class="author"]/a/text()').extract()
        titles = response.xpath('//a[@class="thread_title"]/text()').extract()
        views = response.xpath('//td[@class="views"]/text()').extract()
        replies = response.xpath('//td[@class="replies"]/text()').extract()
        # parse everything
        for i in range(0, 40):
            thnum = re.search('(\d{7})', thread_strings[i]).group(0)
            author = thread_authors[i]
            title = titles[i]
            vw = views[i]
            reply = replies[i]
            if views == '-' or reply == '-':  # admin threads, dgaf
                continue
            # print(str([thread_authors,titles,views,replies]))
            item = Thread(int(thnum), title, author, int(vw), int(reply), int(forum_id))
            yield item

    # TODO put this in io and make it an abstract superclass, like "loader"
    @staticmethod
    def response_load(items: Thread) -> None:
        print("Inserting: " + str(items._fields))
        connection = sql.connect(DATABASE_LOCATION)
        c = connection.cursor()
        c.execute(
            "INSERT OR IGNORE INTO threads (thread_id, title, author, views, replies, forum_id) VALUES (?, ?, ?, ?, ?, ?)",
            items._fields)
        connection.commit()
        connection.close()
        return


class PostSpider(InitSpider):
    """
    Gets all the posts from whatever thread
    """

    name = 'postspider'
    allowed_domains = 'forums.somethingawful.com', 'somethingawful.com'
    # start_urls= ['https://forums.somethingawful.com/showthread.php?threadid=3782388']
    login_page = 'https://forums.somethingawful.com/account.php?action=loginform#form'

    def __init__(self, username='', password='', forum_id='', dry_run='', urls='', *args, **kwargs):
        super(PostSpider, self).__init__(*args, **kwargs)
        self.uname = username
        self.password = password
        self.forum_id = int(forum_id)
        self.dry_run = True if dry_run is 'True' else False
        self.start_urls = urls.split(',')  # this is a bullshit hack but the constructor param needs to be a str
        self.loader = AvroWriter("output.avro.tmp", buffer_size=500)
        self.progress = dict()
        # lastly add a shutdown hook
        dispatcher.connect(self.quit, signals.spider_closed)

    def quit(self):
        self.loader.close_and_commit()

    def init_request(self):
        return Request(url=self.login_page, callback=self.login)

    def login(self, response):
        """
        logs into the forums
        :param response:
        :return:
        """
        return FormRequest.from_response(response,
                                         #
                                         #
                                         # !!! TAKE THIS OUT IF YOU DISTRIBUTE THIS !!!
                                         formdata={'username': self.uname, 'password': self.password,
                                                   'checked': 'checked'},  # "checked" is the "use https" checkbox, dgaf
                                         formxpath='//form[@class="login_form"]', callback=self.verify_login)

    def verify_login(self, response: Response) -> None:
        """
        Makes sure the login didn't fuck up
        :param response:
        :return:
        """
        if u'<b>Clicking here makes all your wildest dreams come true.</b>' in response.xpath(
                '//div[@class="mainbodytextsmall"]//b').extract():
            log.info('Login successful')
            return self.initialized()
        else:
            log.error('Login failure')
            log.debug(response.xpath('//div[@class="mainbodytextsmall"]//b').extract())
        return

    def update_state(self, post):
        # self.progress[post.thread_id] = post.thread_page
        pass

    def parse(self, response: Response) -> Request:
        """
        This is an override of a spider method
        :param response:
        :return:
        """
        print("Extracting...")
        items = self.post_transform(response)
        if not self.dry_run:
            for item in items:
                self.post_load(item)
        url_base = 'http://forums.somethingawful.com/'
        url = response.xpath('//a[@title="Next page"]/@href').extract()
        if len(url) > 0:
            url = url_base + url[0]
            log.debug(str(url))
        else:
            log.debug(str(url))
            raise IndexError("No next page for thread!")
        sleep(0.2)
        # log.debug("Iterating in parse: " + str(url))
        yield scrapy.Request(url, callback=self.parse)

    def post_transform(self, response: Response) -> Iterable[Post]:
        """
        xpath's the pluperfect fuck out of the html response to get what we want
        :param response:
        :return:
        """
        thread_id_raw = response.xpath('//div[@id="thread"]/@class').extract()
        thread_id = [re.search('(\d{7})', val).group(0) for val in thread_id_raw]
        post_text = self.posts_from_response(response)
        page = response.xpath('//option[@selected="selected"]/@value').extract()[0]
        if page < 0: page = 1
        post_users = response.xpath('//dl[@class="userinfo"]/dt/text()').extract()
        post_user_ids_raw = response.xpath('//ul[@class="profilelinks"]/li/a/@href').extract()
        # returns something like "user12345", apparently these two statements can be combined but i don't care
        post_user_ids = [re.search('(\d+)', x).group(0) for x in post_user_ids_raw[0::2]]
        post_timestamp_raw = response.xpath('//td[@class="postdate"]/text()').extract()
        post_timestamp = self.clean_dates(post_timestamp_raw)
        post_ids_raw = response.xpath('//div[@id="thread"]//table/@id').extract()
        post_ids = [re.search('(\d+)', x).group(0) for x in post_ids_raw]
        for i in range(0, len(post_text)):
            post = post_text[i]
            user = post_users[i]
            user_id = post_user_ids[i]
            tstamp = post_timestamp[i]
            post_id = post_ids[i]
            item = Post(int(thread_id[0]), post, int(page), user, tstamp, int(user_id), int(post_id))
            yield item

    pid_regex = re.compile(r'(^post)(\d+$)')
    quote_id_regex = re.compile(r'(.*showthread\.php\?goto=post&amp;postid\=)(\d+)')
    whitespace_regex = re.compile(r'(\s{2,}|\n|\t)')

    def is_post_id(self, value: str) -> bool:
        return PostSpider.pid_regex.match(value)

    def is_quote(self, value: str) -> bool:
        return PostSpider.quote_id_regex.match(value)

    def clean_text(self, raw_posts: List[str]) -> List[str]:
        """
        Returns a list of only text posts--ignores stuff like people just posting ^ or emoticons
        :param raw_posts:
        :return:
        """
        textonly = [x for x in raw_posts if re.search('(\w+)', x) is not None]
        return textonly

    @staticmethod
    def posts_from_response(response: Response) -> List[str]:
        """
        Takes the http response and does xpath shit to extract the actual post
        :param response:
        :return:
        """
        posts = response.xpath('//td[@class="postbody"]').extract()
        # that gets a list (length=40) of all the posts, now we exclude quotes
        pfilter = list()
        for p in posts:
            try:
                dom = ht.fromstring(p)  # this way we can xpath on the individual posts to exclude quotes
            except XMLSyntaxError:
                dom = ht.fromstring(p.encode('latin-1', 'ignore'))
            post = dom.xpath('//*/text()[not(ancestor::*[@class="bbc-block"]) and not(ancestor::*[@class="editedby"])]')
            # that returns a list of lists of strings, so we concatenate the entries in the sublists
            post_clean = reduce(lambda s1, s2: s1 + s2, post)
            pfilter.append(post_clean)
        # lastly we replace excess whitespace and newlines with a single space because we don't give a shit
        return [PostSpider.whitespace_regex.sub(' ', p) for p in pfilter]

    @staticmethod
    def _debug_posts(response: Response) -> List[str]:
        posts = response.xpath('//td[@class="postbody"]').extract()
        # that gets a list (length=40) of all the posts, now we exclude quotes
        pfilter = list()
        for i, p in enumerate(posts):
            try:
                dom = ht.fromstring(remove_emojis(p))  # this way we can xpath on the individual posts to exclude quotes
                post = dom.xpath(
                    '//*/text()[not(ancestor::*[@class="bbc-block"]) and not(ancestor::*[@class="editedby"])]')
                # that returns a list of lists of strings, so we concatenate the entries in the sublists
                post_clean = reduce(lambda s1, s2: s1 + s2, post)
                pfilter.append(post_clean)
            except XMLSyntaxError:
                log.debug("Error encountered on post index %d: \n %s", i, p)
                return posts
        # lastly we replace excess whitespace and newlines with a single space because we don't give a shit
        return [PostSpider.whitespace_regex.sub(' ', p) for p in pfilter]

    def clean_dates(self, raw_dates: List[str]) -> List[datetime]:
        """
        Puts the dates in a format that can be parsed by sql stuff
        :param raw_dates: a list of date text obtained by xpathing the html
        :return: python date objects
        """
        space_removed = [str(x).strip() for x in raw_dates]
        no_blanks = filter(None, space_removed)
        date_objs = [strptime(x, '%b %d, %Y %H:%M') for x in no_blanks]
        return date_objs

    def post_load(self, post: Post) -> None:
        """
        Inserts the post into the db
        :param post: a Post object with its fields filled out
        :return: void
        """
        log.info("Inserting: " + str(post['post_id']) + " Page: " + str(post['page']))
        connection = sql.connect(DATABASE_LOCATION)
        c = connection.cursor()
        c.execute(
            "INSERT OR IGNORE INTO posts (thread_id, post_text, thread_page, user_name, post_timestamp, user_id, post_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            post.fields)
        connection.commit()
        connection.close()
        return


"""
Stuff down here is a lot of ad-hoc shit that should be cleaned up --->
"""


def update_bigquery_posts(forum_id: int):
    """
    Restarts the postspider based on what's in gcloud--this method is way incomplete as is gcloud implementation
    :return:
    """
    bq = PostBigQueryer()
    dao = SQLiteDAO()
    threadmap = bq.find_last_updated(forum_id)
    threadlist = dao.thread_ids_for_forum_id(forum_id)
    for t in threadlist:
        if t not in threadmap.keys() or int(threadmap[t]) < 1:
            threadmap[t] = 1
    urls = urls_from_dict(threadmap)
    log.debug("Found %d entries for forumid %d: %s", len(urls), forum_id, str(threadmap))
    run(forum_id, urls)


def bq_update_thread(thread_id: int, forum_id: int = None):
    """
    Updates posts for the thread ID provided
    :param thread_id: the thread ID to update
    :param forum_id: the forum ID the thread resides in. This parameter is necessary because there's no other way to get it
    :return:
    """
    bq = PostBigQueryer()
    last_page = bq.sql_inject(
        "select max(thread_page), forum_id from %s where thread_id=%d" % (GCLOUD_POST_TABLE, thread_id))
    pg = last_page.rows[0][0]
    if pg == 'null':
        pg = 1
    fid = last_page.rows[0][1]
    if fid == 'null':
        if forum_id > 0:
            fid = forum_id
        else:
            raise ValueError("Invalid forum id")
    url = single_url(thread_id, pg)
    run(fid, [url])

