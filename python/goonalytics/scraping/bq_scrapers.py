import logging
import re
from functools import reduce
from time import sleep
from typing import List, Iterable, Dict

import lxml.html as ht
import requests
import scrapy
from lxml.etree import XMLSyntaxError
from scrapy import FormRequest
from scrapy import Request
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.http import Response
from scrapy.spiders.init import InitSpider
from scrapy.xlib.pydispatch import dispatcher

from goonalytics.base import PostAvro, ThreadAvro
from goonalytics.io.gcloudio import AvroWriter, AvroThreadWriter, PostBigQueryer, get_thread_ids_for_forum
from goonalytics.scraping.util import remove_emojis, whitespace_regex, extract_forum_id_from_url, clean_dates, re_quote, \
    re_post_id, get_scrapy_settings, urls_from_dict

logging.basicConfig()

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# this is a huge pain in the ass to define on the fly
excluded_post_ids = set()

FORUM_URL = 'http://forums.somethingawful.com/forumdisplay.php?forumid='

# two methods below here are for getting url info from a server for doing things async
def get_urls(url: str):
    while True:
        yield get_url_from_server(url)

def get_url_from_server(url: str):
    resp = requests.get(url)
    if resp.text is None:
        raise ValueError
    log.debug("Response: {}".format(resp.text))
    return resp.text


class BQThreadSpider(scrapy.Spider):
    """
    A really basic spider that just gets basic thread info
    """
    name = "bq-threadspider"

    def __init__(self, username='', password='', dry_run='', forum_id='', **kwargs):
        super(BQThreadSpider, self).__init__(**kwargs)
        idlist = forum_id.split(',')
        self.dry_run = bool(dry_run)
        self.start_urls = ['http://forums.somethingawful.com/forumdisplay.php?forumid=' + forumid for forumid in idlist]
        excluded = [get_thread_ids_for_forum(int(i)) for i in idlist]
        self.excluded = frozenset(excluded)
        log.debug("Start urls: %s", self.start_urls)
        self.loader = AvroThreadWriter('output.avro.tmp', buffer_size=520)
        dispatcher.connect(self.quit, signals.spider_closed)

    def quit(self):
        self.loader.close_and_commit()

    allowed_domains = {'forums.somethingawful.com'}

    def parse(self, response):
        """
        Parses the first 8ish pages
        """
        log.info("Extracting...")
        if not self.dry_run:
            for item in self.response_transform(response):
                if item.thread_id not in self.excluded:
                    self.loader.submit(item)
        for i in range(1, 8):
            url = 'http://forums.somethingawful.com/' + response.xpath('//a[@title="Next page"]/@href').extract()[0]
            sleep(0.2)
            print("Iterating in parse: " + str(url))
            yield scrapy.Request(url, callback=self.parse)

    thread_author_ids_regex = re.compile(r'(?<=userid=)(\d+)')

    thread_num_id_regex = re.compile('(\d{7})')

    @staticmethod
    def response_transform(response: Response):
        """
        Makes a list of items from the response
        """
        forum_id = extract_forum_id_from_url(response.url)
        thread_strings = response.xpath('//tbody/tr[contains(@class,"thread")]/@id').extract()  # gives 'thread#######'
        thread_authors = response.xpath('//tbody/tr[@id]/td[@class="author"]/a/text()').extract()
        thread_author_ids = BQThreadSpider.get_thread_author_ids(response)
        titles = response.xpath('//a[@class="thread_title"]/text()').extract()

        if not (len(titles) == len(thread_author_ids) and len(thread_author_ids) == len(thread_authors) and len(thread_authors) == len(thread_strings)):
            log.warning("WARNING Extracted components do not match on page %s--titles: \t %d \n author ids: \t %d \n authors: %d \n threadids: \t %d",
                        response.url,
                        len(titles),
                        len(thread_author_ids),
                        len(thread_authors),
                        len(thread_strings))
        # parse everything
        for i in range(0, len(thread_strings)):
            thnum = re.search('(\d{7})', thread_strings[i]).group(0)
            author = thread_authors[i]
            title = titles[i]
            aid = thread_author_ids[i]
            # print(str([thread_authors,titles,views,replies]))
            item = ThreadAvro(int(forum_id), int(thnum), title, author, int(aid), False)
            yield item

    @staticmethod
    def get_thread_author_ids(response: Response):
        xp = response.xpath('//tr[@id]/td[@class="author"]/a').extract()
        return [BQThreadSpider.thread_author_ids_regex.search(t).group(0) for t in xp]


login_page = 'https://forums.somethingawful.com/account.php?action=loginform#form'

class BQPostSpider(InitSpider):
    """
    Gets all the posts from whatever thread
    """

    name = 'bq-postspider'
    allowed_domains = 'forums.somethingawful.com', 'somethingawful.com'
    # start_urls= ['https://forums.somethingawful.com/showthread.php?threadid=347802']

    def __init__(self, username='', password='', forum_id='', dry_run='', urls='', **kwargs):
        super(BQPostSpider, self).__init__(**kwargs)
        self.uname = username
        self.password = password
        log.debug("User name: %s \t Password: %s", username, password)
        self.forum_id = int(forum_id)
        self.dry_run = True if dry_run is 'True' else False
        self.start_urls = urls.split(',')  # this is a bullshit hack but the constructor param needs to be a str
        self.loader = AvroWriter("output.avro.tmp", buffer_size=520)
        self.progress = dict()

        # lastly add a shutdown hook
        dispatcher.connect(self.quit, signals.spider_closed)

    def quit(self):
        self.loader.close_and_commit()

    def init_request(self):
        return Request(url=login_page, callback=self.login)

    def login(self, response):
        """
        logs into the forums
        :param response:
        :return:
        """
        return FormRequest.from_response(response,
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

    def parse(self, response: Response):
        """
        This is an override of a spider method
        :param response:
        :return:
        """
        print("Extracting...")
        items = self.post_transform_avro(response)
        if not self.dry_run:
            for item in items:
                self.loader.submit(item)
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

    def post_transform_avro(self, response: Response) -> Iterable[PostAvro]:
        """
        xpath's the pluperfect fuck out of the html response to get what we want
        :param response:
        :return:
        """
        # there's probably a way to do this with a single giant xpath but fuck it
        thread_id_raw = response.xpath('//div[@id="thread"]/@class').extract()
        thread_id = [re.search('(\d{7})', val).group(0) for val in thread_id_raw]
        post_text = self.posts_from_response(response)
        page = response.xpath('//option[@selected="selected"]/@value').extract()[0]
        page = int(page)
        if page < 0: page = 1
        post_users = response.xpath('//dl[@class="userinfo"]/dt/text()').extract()
        post_user_ids_raw = response.xpath('//ul[@class="profilelinks"]/li/a/@href').extract()
        # returns something like "user12345", apparently these two statements can be combined but i don't care
        post_user_ids = [re.search('(\d+)', x).group(0) for x in post_user_ids_raw[0::2]]
        post_timestamp_raw = response.xpath('//td[@class="postdate"]/text()').extract()
        post_timestamp = clean_dates(post_timestamp_raw)
        post_ids_raw = response.xpath('//div[@id="thread"]//table/@id').extract()
        post_ids = [re.search('(\d+)', x).group(0) for x in post_ids_raw]
        quotes = self.extract_quotemap(response)
        for i in range(0, len(post_text)):
            post = post_text[i]
            user = post_users[i]
            user_id = post_user_ids[i]
            tstamp = post_timestamp[i]
            post_id = int(post_ids[i])
            post_quotes = quotes[post_id]
            item = PostAvro(post_id,
                            int(user_id),
                            user,
                            post,
                            int(page),
                            tstamp,
                            int(thread_id[0]),
                            self.forum_id,
                            post_quotes)
            yield item

    def extract_quotemap(self, response: Response) -> Dict[int, List[int]]:
        # this gives us a list of post ids and quotes, length = # posts + # quotes
        xp = response.xpath('//div[@id="thread"]//table/@id | //div[@class="bbc-block"]').extract()
        """
        filter them into a map: quotes occur after the post ID that they're part of. The list looks like:

        post12345,
        post23456,
        <div class=[bbc-block]> + a bunch of shit + "showthread.php?goto=post&amp;postid=467368110#post467368110",
        post12346

        where the quote is of post 4673... belonging to post id 23456
        note that posts can have more than one quote
        """
        return self.create_quotemap(xp)

    def create_quotemap(self, extract: List[str]) -> Dict[int, List[int]]:
        """
        This method is kinda messy but everything needs to be int because downstream the post_id and quoted ids need
        to be consistent
        :param extract:
        :return:
        """
        output = dict()
        prevkey = extract[0]
        for value in extract:
            pid = re_post_id(value)
            quote_pid = re_quote(value)
            if pid:
                pid = pid.group(2)
                output[int(pid)] = list()
                prevkey = int(pid)
            elif quote_pid:
                qpid = quote_pid.group(2)
                output[prevkey].append(int(qpid))
        return output

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
        return [whitespace_regex.sub(' ', p) for p in pfilter]

    @staticmethod
    def _debug_posts(response: Response) -> List[str]:
        posts = response.xpath('//td[@class="postbody"]').extract()
        # that gets a list (length=40) of all the posts, now we exclude quotes
        pfilter = list()
        for i, p in enumerate(posts):
            try:
                dom = ht.fromstring(
                    remove_emojis(p))  # this way we can xpath on the individual posts to exclude quotes
                post = dom.xpath(
                    '//*/text()[not(ancestor::*[@class="bbc-block"]) and not(ancestor::*[@class="editedby"])]')
                # that returns a list of lists of strings, so we concatenate the entries in the sublists
                post_clean = reduce(lambda s1, s2: s1 + s2, post)
                pfilter.append(post_clean)
            except XMLSyntaxError:
                log.debug("Error encountered on post index %d: \n %s", i, p)
                return posts
        # lastly we replace excess whitespace and newlines with a single space because we don't give a shit
        return [whitespace_regex.sub(' ', p) for p in pfilter]


class ArchiveSpider(BQThreadSpider, InitSpider): # multiple inheritance, this should go well

    # HIGHLY experimental
    name = 'archive-spider'

    allowed_domains = 'forums.somethingawful.com', 'somethingawful.com'
    def __init__(self, username='', password='', forum_id='', year='', **kwargs):
        super().__init__(username=username, password=password, forum_id=forum_id, **kwargs)
        self.uname = username
        self.password = password
        self.forum_id = forum_id
        self.year = year
        self.start_urls = ['http://forums.somethingawful.com/forumdisplay.php?forumid=' + forum_id]

    def init_request(self):
        return Request(url=login_page, callback=self.login)

    def login(self, response):
        """
        logs into the forums
        :param response:
        :return:
        """
        return FormRequest.from_response(response,
                                         formdata={'username': self.uname, 'password': self.password,
                                                   'checked': 'checked'},  # "checked" is the "use https" checkbox, dgaf
                                         formxpath='//form[@class="login_form"]', callback=self.after_login)

    def verify_login(self, response: Response) -> bool:
        """
        Makes sure the login didn't fuck up
        :param response:
        :return:
        """
        if u'<b>Clicking here makes all your wildest dreams come true.</b>' in response.xpath(
                '//div[@class="mainbodytextsmall"]//b').extract():
            log.info('Login successful')
            return True
        else:
            log.error('Login failure')
            log.debug(response.xpath('//div[@class="mainbodytextsmall"]//b').extract())
        return False


    def after_login(self, response):
        if self.verify_login(response):
            return scrapy.Request('http://forums.somethingawful.com/forumdisplay.php?forumid=' + self.forum_id, callback=self.select_archive_year)


    def select_archive_year(self, response):
        return FormRequest.from_response(response,
                                         formdata={'ac_year': self.year},
                                         formxpath='//form[@id="ac_timemachine"]',
                                         callback=self.initialized)


def update_bigquery_threads(spidername, **kwargs):
    run(spidername, **kwargs)


def run(spidername: str, **kwargs):
    settings = get_scrapy_settings()
    cp = CrawlerProcess(settings)
    cp.crawl(spidername, **kwargs)
    cp.start()


def update_bigquery_posts(spidername, forum_id: int, **kwargs):
    """
    Restarts the postspider based on what's in gcloud--this method is way incomplete as is gcloud implementation
    :return:
    """
    bq = PostBigQueryer()
    threadmap = bq.find_last_updated(forum_id)
    threadlist = bq.get_threadlist(forum_id)
    for t in threadlist:
        if t not in threadmap.keys() or int(threadmap[t]) < 1:
            threadmap[t] = 1
    urls = urls_from_dict(threadmap)
    # !!! don't care about excluded IDs--it's like ten times as much I/O as just copying the table without duplicates afterward
    # exclude = set()
    # for thread_id, page in threadmap.items():
    #     exclude.add(get_post_ids_for_thread(thread_id, page))
    # global excluded_post_ids
    # excluded_post_ids = exclude
    log.debug("Found %d entries for forumid %d: %s", len(urls), forum_id, str(threadmap))
    run(spidername, username=kwargs['username'], password=kwargs['password'], forum_id=forum_id, urls=urls)


# def update(forum_ids: List[int], **kwargs):
#     """
#     this is broken because twisted sucks
#     :param forum_ids:
#     :param kwargs:
#     :return:
#     """
#     username = kwargs['username']
#     password = kwargs['password']
#     intlist = [int(x) for x in forum_ids]
#     cp = None
#     for forum_id in intlist:
#         settings = get_scrapy_settings()
#         cp = CrawlerProcess(settings)
#         cp.crawl('bq-threadspider', forum_id=str(forum_id), username=username, password=password,
#                  stop_after_crawl=False)
#         cp.start()
#         cp.join()
#
#         bq = PostBigQueryer()
#         threadmap = bq.find_last_updated(forum_id)
#         threadlist = bq.get_threadlist(forum_id)
#         for t in threadlist:
#             if t not in threadmap.keys() or int(threadmap[t]) < 1:
#                 threadmap[t] = 1
#         urls = urls_from_dict(threadmap)
#         log.debug("Found %d entries for forumid %d: %s", len(urls), forum_id, str(threadmap))
#         cp.crawl('bq-postspider', forum_id=str(forum_id), username=username, password=password, urls=urls,
#                  stop_after_crawl=False)
#     if cp is not None:
#         cp.stop()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-p', '--posts', help='Update posts only', action='store_true')
    group.add_argument('-t', '--threads', help='Update threads only', action='store_true')
    parser.add_argument('-a', '--archive', type=str, help='Scrape archive year provided')
    parser.add_argument('user', type=str, help='Forums username')
    parser.add_argument('password', type=str, help='Forums password')
    parser.add_argument('forumid', type=str, help='Forum id(s) to scrape')
    args = parser.parse_args()
    log.debug("ID: %s", args.forumid)
    # raise ConnectionAbortedError('stopping for debug')
    keyword_args = dict()
    keyword_args['username'] = args.user
    keyword_args['password'] = args.password
    keyword_args['forum_id'] = args.forumid
    archived = False
    if args.archive:
        archived = True
        keyword_args['year'] = args.archive
    # could prob clean this up but fuck it. Posts require some extra steps for de-duping hence the diff commands
    if args.posts:
        if archived:
            log.warning("Archive flag has no effect for post scrape")
        keyword_args['spidername'] = 'bq-postspider'
        update_bigquery_posts(**keyword_args)
    elif args.threads:
        keyword_args['spidername'] = 'archive-spider' if archived else 'bq-threadspider'
        update_bigquery_threads(**keyword_args)
    else:
        log.error("Must select either --posts or --threads to scrape")
