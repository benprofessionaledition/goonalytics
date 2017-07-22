"""
io methods
"""
import csv
import json
import sqlite3 as sql
from abc import abstractmethod, ABCMeta
from datetime import datetime
from functools import reduce
from typing import Iterable, Generic, T
from typing import Tuple

import pandas as pd
from elasticsearch import Elasticsearch

from goonalytics.base import Post
from goonalytics.settings import DATABASE_LOCATION, ELASTIC_LOCATION


class SQLiteDAO(object):
    """
    DAOs are fucking dumb but so am i
    """

    def thread_ids_for_forum_id(self, forum_id):
        """
        Returns a list of thread ids for the given forum id
        :param forum_id: the numerical forum id, e.g. 44 for games
        :return: a comma-separated string of thread ids for the forum given
        """
        forum_id = SQLiteDAO.clean_forum_ids(forum_id)
        frame = self.sql_df("select distinct(thread_id) from threads where forum_id in ({})".format(forum_id))
        l = frame['thread_id'].tolist()

        ignore = self.sql_df("select thread_id from thread_ignore")
        iglist = ignore['thread_id'].tolist()
        return [tid for tid in l if tid not in iglist]

    def thread_ids_as_string(self, thread_ids):
        return reduce(lambda r, s: str(r) + ',' + str(s), thread_ids)

    @staticmethod
    def clean_forum_ids(forum_id) -> str:
        if type(forum_id) == list:
            forum_id_tostr = str(forum_id)
            end = len(forum_id_tostr) - 1
            forum_id = forum_id_tostr[1:end]
        return forum_id

    def thread_ids_and_last_page_scraped_for_forum(self, forum_id):
        """
        returns a map of thread ids mapped to the most recent page scraped for whatever
        forum ID
        :param forum_id: the numerical forum id, e.g. 44 for games
        :return: a map whose keys are thread ids and values are the max page number in the database for those threads
        """
        forum_id = SQLiteDAO.clean_forum_ids(forum_id)
        frame = self.sql_df(
            "select posts.thread_id, max(posts.thread_page) from posts inner join threads using(thread_id) where threads.forum_id in ({}) group by thread_id".format(
                forum_id))
        thread_ids = frame['thread_id'].tolist()
        pagenos = frame['max(posts.thread_page)'].tolist()
        mp = dict()
        for i in range(0, len(thread_ids) - 1):
            mp[thread_ids[i]] = pagenos[i]
        return mp


    def sql_df(self, query):
        """
        A sql injection vulnerability, except I don't care
        :param query:
        :return:
        """
        con = self.create_connection()
        df = pd.read_sql(query, con)
        return df

    def sql_single_value(self, query):
        return self.sql_df(query).iloc[0, 0]

    def sql_list(self, query):
        return list(self.sql_df(query).iloc[:, 0])

    def create_connection(self):
        return sql.connect(DATABASE_LOCATION)

    def posts_from_thread_generator(self, thread_id, chunksize=1000):
        con = self.create_connection()
        query = "select * from posts where thread_id=" + str(thread_id)
        cursor = con.cursor()
        cursor.arraysize = chunksize
        cursor.execute(query)
        return self.fetchsome(cursor, some=chunksize)

    def fetchsome(self, cursor, some=1000):
        fetch = cursor.fetchmany
        while True:
            rows = fetch(some)
            if not rows: break
            for row in rows:
                yield row


def ElasticSearchDAO(object):
    es = Elasticsearch(ELASTIC_LOCATION)

    def index_forum(forum_id, as_ascii=False):
        dao = SQLiteDAO()
        tgen = dao.thread_ids_for_forum_id(forum_id)
        for t in tgen:
            index_thread(forum_id, t, posts_as_ascii=as_ascii)

    def index_thread(forum_id, thread_id, posts_as_ascii=False):
        """
        Indexes the thread on elastic search.
        :param forum_id:
        :param thread_id:
        :param posts_as_ascii: if set to true, will encode post text as ascii to get rid of unprintable unicode shit
        :return:
        """
        dao = SQLiteDAO()
        pgen = dao.posts_from_thread_generator(thread_id)
        i = 0
        inx = str(forum_id) + '-' + str(thread_id)
        if posts_as_ascii: inx += '-ascii'
        for p in pgen:
            text = p[4].encode('ascii', errors='ignore') if posts_as_ascii else p[4]
            post = {
                'post_id': p[0],
                'thread_id': p[1],
                'user_name': p[2],
                'user_id': p[3],
                'text': text,
                'page': p[5],
                'timestamp': datetime.strptime(p[6], '%Y-%m-%d %H:%M:%S')
            }
            es.index(index=inx, doc_type='post', body=post, id=p[0])
            i += 1
            if i % 100 == 0:
                print("Wrote " + str(i) + " posts")

    def thread_to_json(thread_id: int, forum_id: int, filename: str) -> None:
        """
        pulls shit out of sqlite and puts it in a json for each thread
        :param forum_id:
        :param thread_id:
        :param filename:
        :return:
        """
        dao = SQLiteDAO()
        gen = dao.posts_from_thread_generator(thread_id)
        obj = {
            'thread_id': str(thread_id),
            'forum_id': str(forum_id),
            'posts': [{

            }]
        }
        i = 0
        for p in gen:
            post_json = {
                'post_id': p[0],
                'thread_id': p[1],
                'user_name': p[2],
                'user_id': p[3],
                'text': p[4],
                'page': p[5],
                'timestamp': p[6]
            }
            obj['posts'].append(post_json)
            i += 1
            if i % 100 == 0:
                print("Appended " + str(i) + " posts")
        with open(filename, 'w+') as out:
            json.dump(obj, out)
        print("Wrote file to " + filename)


def write_to_csv(posts: Iterable[Tuple], output: str, delimiter=',', trim_newlines=False) -> None:
    """
    writes the posts provided as a unicode csv
    :param posts:
    :param output:
    :param delimiter:
    :return:
    """
    with open(output, 'a') as out:
        cout = csv.writer(out, delimiter=delimiter, encoding='utf-8')
        for i, p in enumerate(posts):
            clean = list(p)
            if trim_newlines:
                clean[4] = p[4].replace('\n', u'')
            cout.writerow(clean)


def write_post_ids(outfile, thread_id):
    d = SQLiteDAO()
    l = d.posts_from_thread_generator(thread_id)
    with open(outfile, 'w') as out:
        for i, p in enumerate(l):
            out.write(str(p[0]) + "\n")
    print("Done")


def get_random_thread_id(has_posts=True):
    d = SQLiteDAO()
    table_name = 'posts' if has_posts else 'threads'
    df = d.sql_df("SELECT thread_id FROM {} ORDER BY RANDOM() LIMIT 1".format(table_name))
    return df.iloc[0, 0]


class Loader(Generic[T], metaclass=ABCMeta):  # python generics are fucking retarded

    @abstractmethod
    def load(self, obj) -> T:
        raise NotImplementedError

    @abstractmethod
    def write(self, obj: T) -> None:
        raise NotImplementedError


class SQLitePostLoader(Loader[Post]):
    def load(self, obj) -> Post:
        pass

    def write(self, obj: Post) -> None:
        pass



if __name__ == '__main__':
    d = SQLiteDAO()
    thread_ids = d.thread_ids_for_forum_id(44)
    thread_ids.append(d.thread_ids_for_forum_id(46))
    output = 'posts-full-pipe-delimited.csv'
    # for i, tid in enumerate(thread_ids):
    #     posts = d.posts_from_thread_generator(tid, chunksize=1000)
    #     write_to_csv(posts, output, delimiter='|', trim_newlines=True)
    #     print("Wrote {} of {} threads".format(i + 1, len(thread_ids)))
