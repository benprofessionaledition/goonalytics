"""
Various IO crap for the sqlite db
"""
import json
from datetime import datetime

import unicodecsv
from elasticsearch import Elasticsearch
from thread_scraper import SQLiteDAO

es = Elasticsearch('http://elastic:changeme@localhost:9200/')


def example():
    # make a retarded json
    doc = {
        'author': 'kimchy',
        'text': 'Elasticsearch: cool. bonsai cool.',
        'timestamp': datetime.now(),
    }
    # this shit all basically just calls the REST API
    res = es.index(index="test-index", doc_type='tweet', id=1, body=doc)
    print(res['created'])

    res = es.get(index="test-index", doc_type='tweet', id=1)
    print(res['_source'])

    es.indices.refresh(index="test-index")

    res = es.search(index="test-index", body={"query": {"match_all": {}}})
    print("Got %d Hits:" % res['hits']['total'])
    for hit in res['hits']['hits']:
        print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])


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


def thread_to_json(thread_id, forum_id, filename):
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
        'forum_id': forum_id,
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


def write_to_csv(posts, output, delimiter=',', trim_newlines=False):
    """
    writes the posts provided as a unicode csv
    :param posts:
    :param output:
    :param delimiter:
    :return:
    """
    with open(output, 'a') as out:
        cout = unicodecsv.writer(out, delimiter=delimiter, encoding='utf-8')
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


if __name__ == '__main__':
    d = SQLiteDAO()
    thread_ids = d.thread_ids_for_forum_id(44)
    thread_ids.append(d.thread_ids_for_forum_id(46))
    output = 'posts-full-pipe-delimited.csv'
    for i, tid in enumerate(thread_ids):
        posts = d.posts_from_thread_generator(tid, chunksize=1000)
        write_to_csv(posts, output, delimiter='|', trim_newlines=True)
        print("Wrote {} of {} threads".format(i + 1, len(thread_ids)))
