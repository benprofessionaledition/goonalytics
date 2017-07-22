"""
Created on Apr 2, 2016

Dear Ben Plus Six Months,

This is a scraper tailored specifically for the something awful forums. I cannot
for the fucking life of me remember how to run it. It requires twisted, which requires
Python 2.7. It writes to a SQLite database somewhere on the local HDD.

The phases of our project are as follows:

Phase 1: Scrape an absolute fuckton more shit
Phase 2: Use tensorflow to do cool shit whilst we learn tensorflow
Phase 2.5: Use ElasticSearch/Kibana to do some visualizations--this is partially implemented in forums_elastic.py
Phase 3: Try doing some entity extraction garbage, possibly try the tagging thing
either with TF or else a linear SVM from sklearn. Our "production" project will
be hundreds of thousands of labels.
Phase Zero Dark Thirty Alpha Zulu: make this less of a piece of shit, runnable
from the command line
Phase Lima Serpico Mike Mike Six Niner: github it?

@author: blevine
"""

import logging as log
import re
import sqlite3 as sql
from datetime import datetime
from functools import reduce
from time import sleep
from urllib.parse import urlparse

import lxml.html as ht
import scrapy
from scrapy.contrib.spiders.init import InitSpider
from scrapy.crawler import CrawlerProcess
from scrapy.http import Request, FormRequest
from scrapy.utils.project import get_project_settings

DATABASE_LOCATION = '/Users/blevine/saforums/games_crawl.sqlite'


def make_shit_comma_separated_func(x, y):
    return str(x) + ',' + str(y)


class ThreadSpider(scrapy.Spider):
    """
    A really basic spider that just gets basic thread info
    """
    name = "threadspider2"

    def __init__(self, username='', password='', ids='', **kwargs):
        super(ThreadSpider, self).__init__(**kwargs)
        idlist = ids.split(',')
        self.start_urls = ['http://forums.somethingawful.com/forumdisplay.php?forumid=' + str(id) for id in idlist]

    allowed_domains = {'forums.somethingawful.com'}

    def parse(self, response):
        """
        Parses the first 8ish pages
        """
        print("Extracting...")
        for item in self.response_transform(response):
            self.response_load(item)
        for i in range(1, 8):
            url = 'http://forums.somethingawful.com/' + response.xpath('//a[@title="Next page"]/@href').extract()[0]
            print(str(url))
            sleep(0.2)
            print("Iterating in parse: " + str(url))
            yield scrapy.Request(url, callback=self.parse)

    def response_transform(self, response):
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
            item = ThreadItem(int(thnum), title, author, int(vw), int(reply), int(forum_id))
            yield item

    @staticmethod
    def response_load(items):
        print("Inserting: " + str(items.fields))
        connection = sql.connect(DATABASE_LOCATION)
        c = connection.cursor()
        c.execute(
            "INSERT OR IGNORE INTO threads (thread_id, title, author, views, replies, forum_id) VALUES (?, ?, ?, ?, ?, ?)",
            items.fields)
        connection.commit()
        connection.close()
        return

    @staticmethod
    def extract_forum_id_from_url(url):
        q = urlparse(url).query
        fid = re.search('(?<=forumid=)\\d{2,}', q).group(0)
        return fid


class ThreadItem(object):
    def __init__(self, thread_id, title, author, views, replies, forum):
        self.fields = (thread_id, title, author, views, replies, forum)
        return


class PostSpider(InitSpider):
    """
    Gets all the posts from whatever thread
    """

    ROOT_BEER = 3630852  # done
    BLOPS_3 = 3716726
    BLOPS = 3387110  # done
    MW_2 = (3311154, 3227086, 3239216)  # not done, not done, not done
    WAW = 3007345  # done
    COD4 = (2756848, 3093899)  # done, done

    GOATS = 3564911  # done
    BLOPS_2 = (3522296, 3482470)  # not done, done
    MW3 = (3446068, 3461453)  # done, done

    TITANFALL_2 = 3782388  # titanfall

    # blops 2[0], mw_2[1, all],

    name = 'postspider'
    allowed_domains = 'forums.somethingawful.com', 'somethingawful.com'
    # start_urls= ['https://forums.somethingawful.com/showthread.php?threadid=3782388']
    login_page = 'https://forums.somethingawful.com/account.php?action=loginform#form'

    def __init__(self, username='', password='', urls='', *args, **kwargs):
        super(PostSpider, self).__init__(*args, **kwargs)
        self.uname = username
        self.password = password
        self.start_urls = urls.split(',')  # this is a bullshit hack but the constructor param needs to be a str

    @staticmethod
    def urls_from_comma_sep_str(threads):
        urls = ['https://forums.somethingawful.com/showthread.php?threadid=' + t for t in threads.split(',')]
        return reduce(make_shit_comma_separated_func, urls)

    @staticmethod
    def urls_from_dict(thread_map):
        thread_ids = thread_map.keys()
        urls = list()
        for curr_id in thread_ids:
            curr_page = thread_map[curr_id]
            url = 'https://forums.somethingawful.com/showthread.php?threadid=' + str(
                curr_id) + '&userid=0&perpage=40&pagenumber=' + str(curr_page)
            urls.append(url)
        return reduce(make_shit_comma_separated_func, urls)

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
                                         formdata={'username': self.uname, 'password': self.password,
                                                   'checked': 'checked'},  # "checked" is the "use https" checkbox, dgaf
                                         formxpath='//form[@class="login_form"]', callback=self.verify_login)

    def verify_login(self, response):
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

    # def parse(self, response):
    #     self.post_transform(response)

    def parse(self, response):
        """
        This is an override of a spider method
        :param response:
        :return:
        """
        print("Extracting...")
        items = self.post_transform(response)
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

    def post_transform(self, response):
        """
        xpath's the pluperfect fuck out of the html response to get what we want. Will yield 40 posts per page or throw an IndexError,
        I don't remember if there's a reason I did it this way instead of len(post_text) or something
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
        # returns something like "user12345", apparently these two statements can be combined
        post_user_ids = [re.search('(\d+)', x).group(0) for x in post_user_ids_raw[0::2]]
        post_timestamp_raw = response.xpath('//td[@class="postdate"]/text()').extract()
        post_timestamp = self.clean_dates(post_timestamp_raw)
        post_ids_raw = response.xpath('//div[@id="thread"]//table/@id').extract()
        post_ids = [re.search('(\d+)', x).group(0) for x in post_ids_raw]
        for i in range(0, len(post_text)):  # 40 posts per page, will die on the last page
            post = post_text[i]
            user = post_users[i]
            user_id = post_user_ids[i]
            tstamp = post_timestamp[i]
            post_id = post_ids[i]
            item = PostItem(int(thread_id[0]), post, int(page), user, tstamp, int(user_id), int(post_id))
            yield item

    def clean_text(self, raw_posts):
        """
        Returns a list of only text posts--ignores stuff like people just posting ^ or emoticons
        :param raw_posts:
        :return:
        """
        textonly = [x for x in raw_posts if re.search('(\w+)', x) is not None]
        return textonly

    def posts_from_response(self, response):
        """
        Takes the http response and does xpath shit to extract the actual post
        :param response:
        :return:
        """
        posts = response.xpath('//td[@class="postbody"]').extract()
        # that gets a list (length=40) of all the posts, now we exclude quotes
        pfilter = list()
        for p in posts:
            dom = ht.fromstring(p)  # so we can xpath again
            post = dom.xpath('//*/text()[not(ancestor::*[@class="bbc-block"]) and not(ancestor::*[@class="editedby"])]')
            # that returns a list of lists of strings, so we concatenate the entries in the sublists
            post_clean = reduce(lambda s1, s2: s1 + s2, post)
            pfilter.append(post_clean)
        return pfilter

    def clean_dates(self, raw_dates):
        """
        Puts the dates in a format that can be parsed by sql stuff
        :param raw_dates: a list of date text obtained by xpathing the html
        :return: python date objects
        """
        space_removed = [str(x).strip() for x in raw_dates]
        no_blanks = filter(None, space_removed)
        date_objs = [datetime.strptime(x, '%b %d, %Y %H:%M') for x in no_blanks]
        return date_objs

    def post_load(self, post):
        """
        Inserts the post into the db
        :param post: a Post object with its fields filled out
        :return: void
        """
        log.info("Inserting: " + str(post.fields[6]) + " Page: " + str(post.fields[2]))
        connection = sql.connect(DATABASE_LOCATION)
        c = connection.cursor()
        c.execute(
            "INSERT OR IGNORE INTO posts (thread_id, post_text, thread_page, user_name, post_timestamp, user_id, post_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            post.fields)
        connection.commit()
        connection.close()
        return


class PostItem(object):  # is there a reason to use scrapy.Item?
    def __init__(self, thread_id, post_text, page, post_user, post_timestamp, user_id, post_id):
        self.fields = (thread_id, post_text, page, post_user, post_timestamp, user_id, post_id)
        return

