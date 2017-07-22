"""
Avro and Google Cloud/BigQuery io methods

3/6/2017 - tried to make this project agnostic and probably killed everything
"""
import json
import logging
import os
import time
from functools import reduce
from queue import Queue
from random import random
from typing import Dict, List, Set, FrozenSet

import avro
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import Table
from google.cloud.bigquery.job import LoadTableFromStorageJob
from google.cloud.bigquery.query import QueryResults
from google.cloud.storage import Blob
from google.cloud.storage import Bucket
from multiprocessing import Process

from goonalytics import settings
from goonalytics.base import PostAvro, User, ThreadAvro
from goonalytics.scraping.util import current_time_ms
from goonalytics.settings import GCLOUD_STORAGE_BUCKET, POST_SCHEMA_LOCATION, GCLOUD_DATASET_NAME, GCLOUD_POST_TABLE, \
    GCLOUD_PROJECT_NAME, THREAD_SCHEMA_LOCATION, GCLOUD_THREAD_TABLE, GCLOUD_CREDENTIAL_FILE

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def post_as_json(post: PostAvro) -> json:
    return {
        "post_id": post.post_id,
        "user_id": post.user_id,
        "user_name": post.user_name,
        "post_text": post.post_text,
        "thread_page": post.thread_page,
        "post_date_timestamp": int(time.mktime(post.post_timestamp) * 1000),
        "quoted_post_ids": post.quoted_posts,
        "thread_id": post.thread_id,
        "forum_id": post.forum_id
    }


def threadinfo_as_json(thread: ThreadAvro) -> json:
    return {
        "forum_id": thread.forum_id,
        "thread_id": thread.thread_id,
        "thread_title": thread.thread_title,
        "author": thread.author,
        "author_id": thread.author_id,
        "ignore": thread.ignore
    }


def get_schema(location: str):
    return avro.schema.Parse(open(location, "r").read())


def get_post_schema():
    return get_schema(POST_SCHEMA_LOCATION)


def get_thread_schema():
    return get_schema(THREAD_SCHEMA_LOCATION)


class CloudStorager(object):
    def __init__(self, credentials=GCLOUD_CREDENTIAL_FILE, bucket=GCLOUD_STORAGE_BUCKET):
        self.client = storage.Client.from_service_account_json(credentials)
        self.bucket = bucket

    def get_cloud_storage_bucket(self) -> Bucket:
        return Bucket(self.client, self.bucket)


class BigQueryer(object):
    def __init__(self, credentials=GCLOUD_CREDENTIAL_FILE, dataset=GCLOUD_DATASET_NAME, table=GCLOUD_POST_TABLE):
        self.client = bigquery.Client.from_service_account_json(credentials)
        self.dataset = dataset
        self.table = table

    def get_bigquery_table(self) -> Table:
        dataset = self.client.dataset(self.dataset)
        return dataset.table(self.table)

    def sql_inject(self, query: str) -> QueryResults:
        logger.debug("Executing synchronous query: %s", query)
        qr = self.client.run_sync_query(query)
        qr.run()
        return qr

class AvroWriter(object):
    """
    this class buffers input since posts are fed one at a time, once it has whatever
    number it writes them all to a file
    """

    def __init__(self, filename: str, buffer_size=40, schema=POST_SCHEMA_LOCATION, tojson: callable = post_as_json,
                 bq=BigQueryer(), cs=CloudStorager(), max_filesize=13000000):
        self.queue = Queue(maxsize=buffer_size)
        self.filename = filename
        self.max_filesize = max_filesize
        self.filename_original = filename
        self.file_partition = 1
        self.schema = get_schema(schema)
        self.tojson = tojson
        self.target_writer = self.initialize_writer()
        self.bq = bq
        self.cs = cs

    def initialize_writer(self) -> DataFileWriter:
        return DataFileWriter(open(self.filename, 'wb+'), DatumWriter(), self.schema)

    def submit(self, item):
        # logger.debug("Queue size: %d", self.queue.qsize())
        if self.queue.full():
            self.write()
        if random() < 0.05:  # log 5% of them, there's a lot
            logger.debug("Enqueuing value: %s", item)
        self.queue.put(item)


    def write(self):
        while not self.queue.empty():
            if os.path.getsize(self.filename) > self.max_filesize:
                logger.warning("output file has exceeded maximum size--uploading")
                current_fname = self.filename
                self.target_writer.flush()
                p = Process(target=self.commit(current_fname))
                p.start()
                self.increment_filename()
            post = self.queue.get()
            pj = self.tojson(post)
            # logger.debug("Writing post: %s", pj)
            self.target_writer.append(pj)

    def increment_filename(self):
        self.file_partition += 1
        self.filename = self.filename_original + '-part' + str(self.file_partition)
        self.target_writer.close()
        self.target_writer = self.initialize_writer()

    def close_and_commit(self):
        self.write()
        self.target_writer.close()
        self.commit(self.filename)
        return

    def commit(self, filename):
        try:
            txfr_blob(filename, bq=self.bq, cs=self.cs)
        except RuntimeError:
            logger.critical("Critical error transferring binary object: {} Creating new file...".format(filename))
            self.increment_filename()



class AvroThreadWriter(AvroWriter):
    """
    made for threads
    """

    def __init__(self, filename: str, buffer_size=40):
        super().__init__(filename, buffer_size, schema=THREAD_SCHEMA_LOCATION, tojson=threadinfo_as_json,
                         bq=ThreadBigQueryer())


class PostBigQueryer(BigQueryer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def find_last_updated(self, forum_id: int = 44) -> Dict[int, int]:
        query = "SELECT thread_id, max(thread_page) FROM " + fully_qualified_tablename(
            GCLOUD_POST_TABLE) + " where forum_id=%d group by thread_id" % int(forum_id)
        qr = self.client.run_sync_query(query)
        output = dict()
        logger.info("Executing synchronous query on Google Cloud...")
        qr.run()
        for row in qr.rows:
            output[row[0]] = row[1]
        return output

    def get_threadlist(self, forum_id: int) -> List[int]:
        dataset = self.sql_inject("select thread_id from " + fully_qualified_tablename(
            GCLOUD_THREAD_TABLE) + " where forum_id=%s and [ignore]=false group by thread_id" % forum_id)
        out = list()
        for row in dataset.rows:
            out.append(row[0])
        return out


class ThreadBigQueryer(PostBigQueryer):
    def __init__(self):
        super().__init__(table=GCLOUD_THREAD_TABLE)


def evaluate_file(fname: str):
    logger.info("Opening file %s", fname)
    reader = DataFileReader(open(fname, "rb"), DatumReader())
    logger.info("Counting lines...")
    i = 0
    for val in reader:
        i += 1
        if i % 1000 == 0:
            logger.debug("Read %d lines", i)
    logger.info("Found %d lines in file", i)



def txfr_blob(filename: str, bq: BigQueryer = PostBigQueryer(),
              cs: CloudStorager = CloudStorager()):
    """
    uploads the blob to bigquery. This would probably be better as a shell script
    :param cs:
    :param bq:
    :param bucket:
    :param filename:
    :return:
    """
    tm = current_time_ms()  # pain in the ass to get nanotime in python apparently
    objname = 'api-update-blob-{}'.format(tm)
    blob = Blob(objname, cs.get_cloud_storage_bucket())
    logger.info("Uploading file (this will take a long time)... ")
    blob.upload_from_filename(filename)
    # change this to change table
    table = bq.get_bigquery_table()
    uri = 'gs://' + cs.bucket + "/" + objname
    logger.info("Loading file to BQ...")
    # insert into tmp table
    # tmptable = bq.client.dataset('forums').table(objname)
    job = LoadTableFromStorageJob('api-job-{}'.format(tm), table, [uri], client=bq.client)
    job.write_disposition = 'WRITE_APPEND'
    job.source_format = 'AVRO'
    job.begin()
    wait_for_job(job)
    logger.info("Cleaning up...")
    blob.delete(cs.client)


def load_data_from_file(source_file_name):
    bq = PostBigQueryer()
    table = bq.get_bigquery_table()

    # Reload the table to get the schema(?)
    table.reload()

    with open(source_file_name, 'rb') as source_file:
        job = table.upload_from_file(
            source_file,
            source_format='AVRO',
            write_disposition='WRITE_APPEND',
            create_disposition='CREATE_NEVER')

    wait_for_job(job)

    print('Loaded {} rows'.format(
        job.output_rows))


def wait_for_job(job):
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)


def get_posts_for_user(user_id: int) -> str:
    query = ''' select post_text from %s where user_id=%d ''' % (fully_qualified_tablename(GCLOUD_POST_TABLE), user_id)
    bq = PostBigQueryer()
    qres = bq.sql_inject(query)
    from functools import reduce
    posts = [row[0] for row in qres.rows]
    return reduce(lambda x, y: x + ' ' + y, posts)


def random_user(post_count_min: int = 0) -> User:
    """
    :param post_count_min: minimum post count for the user
    :return:
    """
    query = ''' select user_id, user_name, min(rand()) as rand from %s group by user_id, user_name having count(user_id) > %d order by rand limit 1''' % (
    GCLOUD_POST_TABLE, post_count_min)
    bq = PostBigQueryer()
    res = bq.sql_inject(query).rows[0]
    return User(user_id=res[0], user_name=res[1])


def get_thread_posts(thread_id: int) -> Dict[int, str]:
    """
    Gets all posts from the thread and returns them as a dict grouped by user ID
    :param thread_id:
    :return:
    """
    query = ''' select user_id, post_text from %s where thread_id = %d''' % (
    fully_qualified_tablename(GCLOUD_POST_TABLE), thread_id)
    bq = PostBigQueryer()
    res = bq.sql_inject(query)
    output = dict()
    for row in res.rows:
        user_id = row[0]
        post_text = row[1]
        if user_id not in output.keys():
            output[user_id] = list()
        output[user_id].append(post_text)
    # reduce the values
    output_reduced = dict()
    for k in output.keys():
        output_reduced[k] = reduce(lambda x, y: x + ' ' + y, output[k])
    return output_reduced


def random_thread_id(post_count_min: int = 0) -> int:
    """
    Returns a random thread with the minimum post count provided
    :param post_count_min:
    :return:
    """
    query = ''' select thread_id, min(rand()) as rand from %s group by thread_id having count(post_id) > %d order by rand limit 1 ''' % (
    fully_qualified_tablename(GCLOUD_POST_TABLE), post_count_min)
    qres = PostBigQueryer().sql_inject(query)
    return qres.rows[0][0]


# deprecated - there's no point in uniquing out post IDs beforehand because it takes more I/O than just copying the table
def get_post_ids_for_thread(thread_id: int, thread_page: int) -> FrozenSet[int]:
    tbl = fully_qualified_tablename(GCLOUD_POST_TABLE)
    qr = PostBigQueryer().sql_inject(
        "select post_id from " + tbl + " where thread_id=%d and thread_page=%d group by post_id" % (
        thread_id, thread_page))
    out = set()
    for row in qr.rows:
        out.add(row[0])
    return frozenset(out)


def get_thread_ids_for_forum(forum_id: int) -> FrozenSet[int]:
    qr = PostBigQueryer().sql_inject(
        "select thread_id from " + fully_qualified_tablename(GCLOUD_THREAD_TABLE) + " where forum_id=%d group by thread_id" % forum_id)
    out = set()
    for row in qr.rows:
        out.add(row[0])
    return frozenset(out)


def fully_qualified_tablename(table: str) -> str:
    return "[" + GCLOUD_PROJECT_NAME + ":" + GCLOUD_DATASET_NAME + "." + table + "]"
