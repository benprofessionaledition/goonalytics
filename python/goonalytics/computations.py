"""
various neato computations on posts. A lot of this was abandoned/postponed in favor of ipynb, web dev shit and porting
all this to google cloud
"""
from functools import reduce
from typing import List, NamedTuple, Dict

from sklearn.feature_extraction.text import TfidfVectorizer

from goonalytics.base import PostAvro, User
from goonalytics.io.gcloudio import PostBigQueryer, random_thread_id, get_thread_posts
from goonalytics.settings import GCLOUD_POST_TABLE


def noisiest_user(posts: List[PostAvro]) -> User:
    """
    returns the poster with the lowest post to quote ratio
    :param posts:
    :return:
    """
    pass


def most_oblivious_user(posts: List[PostAvro], min_post_count=10) -> User:
    """
    the user with the lowest quote to post ratio
    :param min_post_count: minimum posts required before including a user
    :param posts:
    :return:
    """
    pass


def most_quoted_user(posts: List[PostAvro]) -> User:
    pass


def most_cusses(posts: List[PostAvro]) -> User:
    pass


def least_cusses(posts: List[PostAvro]) -> User:
    pass


def do_LDA(posts: List[List[str]], number_of_topics: int=10):
    """
    Latent Dirichlet Allocation.
    :param posts:
    :return:
    """
    pass


def find_similar(posts: List[List[str]]):
    pass


def try_tfidf():
    tposts = get_thread_posts(random_thread_id(post_count_min=4000))
    clf = TfidfVectorizer(input='content', stop_words='english', analyzer='word', norm='l2')
    clf.fit(tposts.values())



