"""
Base classes
"""

from datetime import datetime
from typing import NamedTuple, Tuple

# these could be made PostItems and then scrapy could do concurrent shit, but the bottleneck is Comcast anyway
Thread = NamedTuple('Thread', [('thread_id', int),
                               ('title', str),
                               ('author', str),
                               ('views', int),  # TODO don't give a shit about this
                               ('replies', int),  # TODO see above
                               ('forum', int),
                               ('retrieved_date', datetime)])

Post = NamedTuple('Post', [('post_id', int),
                           ('thread_id', int),
                           ('user_id', int),
                           ('user_name', str),
                           ('post_text', str),
                           ('thread_page', int),
                           ('post_timestamp', datetime),
                           ('retrieved_date', datetime)])

PostAvro = NamedTuple('PostAvro', [('post_id', int),
                                   ('user_id', int),
                                   ('user_name', str),
                                   ('post_text', str),
                                   ('thread_page', int),
                                   ('post_timestamp', datetime),
                                   ('thread_id', int),
                                   ('forum_id', int),
                                   ('quoted_posts', Tuple[int])])

ThreadAvro = NamedTuple('ThreadAvro', [('forum_id', int),
                                       ('thread_id', int),
                                       ('thread_title', str),
                                       ('author', str),
                                       ('author_id', int),
                                       ('ignore', bool)])

User = NamedTuple('User', [('user_id', int), ('user_name', str)])
