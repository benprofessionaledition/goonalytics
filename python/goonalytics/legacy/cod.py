'''
Created on Apr 18, 2016

@author: blevine
'''

# thread ids

import logging
from logging import INFO

import numpy as np
import pandas as pd
from forumsml import create_connection, load_post_id_map
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pandas.core.frame import DataFrame

logging.basicConfig()

log = logging.getLogger(__name__)
log.setLevel(INFO)

BLOPS3 = (3716726,)
ROOT_BEER = (3630852,)
GOATS = (3564911,)
BLOPS2 = (3522296, 3482470)
MW3 = (3446068, 3461453)
BLOPS = (3387110,)
MW2 = (3311154, 3227086, 3239216)
WAW = (3007345,)
COD4 = (2756848, 3093899)
TITANFALL_2 = 3779817

# blops 2[0], mw_2[1, all],

# thread map
THREADS = {'cod4': COD4, 'waw': WAW, 'mw2': MW2, 'blops': BLOPS, 'mw3': MW3, 'blops2': BLOPS2, 'goats': GOATS,
           'rootbeer': ROOT_BEER, 'blops3': BLOPS3}
# release dates
RELEASE_DATES = {'cod4': ''' '2007-11-05' ''',
                 'waw': ''' '2008-11-11' ''',
                 'mw2': ''' '2009-11-10' ''',
                 'blops': ''' '2010-11-09' ''',
                 'mw3': ''' '2011-11-08' ''',
                 'blops2': ''' '2012-11-12' ''',
                 'goats': ''' '2013-11-05' ''',
                 'rootbeer': ''' '2014-11-04' ''',
                 'blops3': ''' '2015-11-06' '''}


def count_posts(game):
    ids = THREADS.get(game)
    con = create_connection()
    count = 0
    for val in ids:
        query = "select count(*) from posts where thread_id=%d" % val;
        cursor = con.cursor()
        c = cursor.execute(query).fetchall()
        count += c[0][0]  # so ugly

    return count


def write_polarities(t_id, filename, write_header=False):
    ids = THREADS[t_id]
    pmap = load_post_id_map(ids)
    if write_header:
        option = 'w+'
    else:
        option = 'a'
    with(open(filename, option)) as outf:
        sia = SentimentIntensityAnalyzer()
        if write_header:
            outf.write('thread,post_id,neg,neu,pos,compound\n')
        i = 0
        size = len(pmap)
        for post_id in pmap.keys():
            ptext = pmap[post_id]
            if ptext is not None:
                pols = sia.polarity_scores(pmap[post_id])
                outf.write(t_id + ',' + str(post_id) + ',' + str(pols['neg']) + ',' + str(pols['neu']) + ',' + str(
                    pols['pos']) + ',' + str(pols['compound']) + '\n')
                i += 1
                log.info('Wrote polarities for post %d of %d', i, size)
            else:
                log.info('Skipping post %d', post_id)
    return


def load_polarities():
    with (open('post_polarities.csv')) as f:
        df = pd.read_csv(f)
    return df


def avg_game_polarity(game, column, df=None):
    if (df is None):
        df = load_polarities()
    vals = df[df.thread == game][column].values
    avg = np.mean(vals)
    return avg


def unique_posters(tkey):
    thread = THREADS[tkey]
    if len(thread) == 1:
        df = sql_df("select count(distinct user_id) from posts where thread_id == " + str(thread[0]))
    else:
        df = sql_df("select count (distinct user_id) from posts where thread_id in " + str(thread))
    return df.iloc[0][0]


def macys_posts(tkey):
    thread = THREADS[tkey]
    return val(varargs_df("select count(*) from posts where user_id==114997 and ", thread))


def post_count(tkey):
    thread = THREADS[tkey]
    return val(varargs_df("select count(*) from posts where ", thread))


def val(df):
    return df.iloc[0][0]


def varargs_df(string, threads):
    if len(threads) == 1:
        df = sql_df(string + 'thread_id == ' + str(threads[0]))
    else:
        df = sql_df(string + 'thread_id in ' + str(threads))
    return df


def create_frame(functions):
    '''
    Creates a dataframe with statistics for the threads provided
    '''
    threadkeys = THREADS.keys()
    df = DataFrame(columns=['cod4', 'waw', 'mw2', 'blops', 'mw3', 'blops2', 'goats', 'rootbeer', 'blops3'],
                   index=[f.__name__ for f in functions])
    for e, function in enumerate(functions):
        for tkey in threadkeys:
            val = function(tkey)
            df.ix[e, tkey] = val
    return df


if __name__ == '__main__':
    f = create_frame((unique_posters, post_count, macys_posts))
    print(str(f))
    out = open('macys_count2.csv', 'w+')
    f.to_csv(out)
