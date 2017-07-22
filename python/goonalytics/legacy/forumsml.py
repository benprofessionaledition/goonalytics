'''
Created on Apr 8, 2016

@author: blevine
'''
import logging
import random
import re
from functools import reduce
from logging import DEBUG
from random import shuffle

from nltk.sentiment.util import mark_negation
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from numpy import array, mean
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB

from goonalytics.io import SQLiteDAO

'''
Notes:

Post sentiments should be pulled from like a week after the game came out

Entries that are one word (~10 chars) should be trimmed out

VADER sentiment polarities are going to be overwhelmingly neutral. Negative 
scores need to be weighted more heavily. A lot of the time, even positive stuff 
is still negative
'''
logging.basicConfig(level=DEBUG)
log = logging.getLogger(__name__)

RANDOM_SEED = 1

FACTORIO = 3629545
FACTORIO_RELEASE_DATE = '''date('2016-02-25','+14 day')'''  # steam release
ROCKET_LEAGUE = 3718089
ROCKET_LEAGUE_RELEASE_DATE = '''(date '2015-07-07', '+14 day')'''
GOOD_THREADS = (FACTORIO, ROCKET_LEAGUE)
HALO_5 = 3742406
HALO_5_RELEASE_DATE = '''(date '2015-10-27', '+14 day')'''
STARBOUND = 3697749
STARBOUND_RELEASE_DATE = '2013-12-03'  # apparently it's really old
BAD_THREADS = (HALO_5, STARBOUND)

# cod threads
ROOT_BEER = 3630852
BLOPS_3 = 3716726
BLOPS = 3387110
MW_2 = (3311154, 3311154, 3227086, 3239216)
WAW = 3007345
COD4 = (2756848, 3093899)

GOATS = 3564911
BLOPS_2 = (3522296, 3482470)
MW3 = (3446068, 3461453)

dao = SQLiteDAO()


def load_posts(thread_id, after_date=''' '2001-01-01' '''):
    con = dao.create_connection()
    cursor = con.cursor()
    cursor.execute(
        'select post_text from posts where thread_id={0} and post_timestamp > {1}'.format(str(thread_id), after_date))
    return [res[0] for res in cursor.fetchall() if res is not None]


def load_cleaned(id_set, min_words=4):
    return clean_raw_posts(load_all(id_set), min_word_limit=min_words)


def load_post_id_map(id_set):
    pmap = dict()
    for id in id_set:
        con = dao.create_connection()
        cursor = con.cursor()
        cursor.execute('select post_id, post_text from posts where thread_id=' + str(id))
        for res in cursor.fetchall():
            cln = clean_post(res[1])
            pmap[res[0]] = cln
    log.info('Retrieved ' + str(len(pmap)) + ' posts')
    return pmap


def load_all(id_set):
    combined = list()
    for t_id in id_set:
        combined += load_posts(t_id)
    return combined


def clean_raw_posts(post_list, use_sentences=True, min_word_limit=4):
    '''
    Return a cleaned list of posts. The parameter is expected to be a 
    raw list of posts from the database. This method then removes URLs and 
    posts less than the minimum word limit specified. If as_sentences is set 
    to False, this method returns a list of lists of words. 
    '''
    # create a list of lists of words
    words = as_words(post_list)
    # clean the links out of the list of lists etc
    cleaned_vals = list()
    for wordarray in words:
        cleanedarray = [w for w in wordarray if re.search('http', w) is None]
        if len(cleanedarray) >= min_word_limit:
            cleaned_vals.append(cleanedarray)
    if use_sentences:
        cleaned_vals = as_sentences(cleaned_vals)
    return cleaned_vals


def clean_post(post, min_word_limit=4):
    words = post.split()
    cleanedwords = [w for w in words if re.search('http', w) is None]
    if len(cleanedwords) >= min_word_limit:
        return reduce(lambda p, q: p + ' ' + q, cleanedwords)
    return None


def mean_polarity_values(polarities):
    output = dict()
    for k in polarities[0].keys():
        output[k] = mean([pol[k] for pol in polarities])
    return output


def as_words(posts):
    return [x.split() for x in posts]


def as_sentences(posts):
    return [reduce(lambda p, q: p + ' ' + q, x) for x in posts]


def rank_polarity(post_list, sentiment='pos'):
    '''
    Returns a dict of the entries in post_list, with the value of the sentiment specified
    mapped to the index in the list
    '''
    sia = SentimentIntensityAnalyzer()
    d = dict()
    for e, p in enumerate(post_list):
        curr_polarity = sia.polarity_scores(p)
        d[e] = curr_polarity[sentiment]
    return d


def mean_compound_polarity(post_list, sia=SentimentIntensityAnalyzer()):
    polarities = [sia.polarity_scores(p) for p in post_list]
    compounds = [pol['compound'] for pol in polarities]
    return mean(array(compounds))


def feature_set(post_list):
    """
    Expects a list of cleaned posts in sentence format and returns a featureset
    calculated by marking negation then doing a count vectorization and tf-idf
    transform
    """
    # mark negation
    # count vectorizer
    # tf-idf
    # isn't fucked up -> isn't fucked_NEG up_NEG
    marked = [mark_negation(p) for p in post_list]
    tv = TfidfVectorizer(min_df=1)
    marked_words = flatten(as_words(marked))
    return tv.fit_transform(marked_words)


def flatten(post_list):
    return reduce(lambda s, r: s + r, post_list)


def max_sentiment(post_list, sentiment='pos', sia=SentimentIntensityAnalyzer()):
    '''
    Returns a tuple containing the post with the maximum value of 
    the sentiment specified, and the sentiment score
    '''
    ret = tuple()
    for p in post_list:
        curr_polarity = sia.polarity_scores(p)
        if len(ret) < 1 or curr_polarity[sentiment] > ret[1]:
            ret = (p, curr_polarity[sentiment])
    return ret


def pull_thread_dataset(thread_id, release_date, min_char_limit=20):
    '''
    Returns a cleaned dataset. Currently, this entails grabbing all posts 
    (quotes removed) occurring two weeks or more after the game's release, 
    removing URLs, and removing posts containing less than a minimum character 
    limit
    '''
    pass


def rnd_seed():
    return RANDOM_SEED


if __name__ == '__main__':
    log.info("Loading data sets...")
    good = load_cleaned(GOOD_THREADS)
    bad = load_cleaned(BAD_THREADS)
    log.info("Pulled %d good posts and %d bad posts", len(good), len(bad))
    # create test and train sets by shuffling crap
    log.info("Creating testing and training sets...")
    random.seed(1)
    shuffle(good)
    shuffle(bad)
    good_bnd = len(good) // 2
    bad_bnd = len(bad) // 2
    good_train = good[:good_bnd]
    good_test = good[good_bnd:]
    bad_train = bad[:bad_bnd]
    bad_test = bad[bad_bnd:]
    log.info("Calculating good training set features...")
    g_tr_feat = feature_set(good_train)
    log.info("Calculating good testing set features...")
    g_tst_feat = feature_set(good_test)
    log.info("Calculating bad training set features...")
    b_tr_feat = feature_set(bad_train)
    log.info("Calculating bad testing set features...")
    b_tst_feat = feature_set(bad_test)
    # train the classifier
    log.info("Training classifier...")
    clf = MultinomialNB()
    target = [0, 1]
    clf.fit([g_tr_feat, b_tr_feat], target)
    log.info("Predicting test sets...")
    g_pred = clf.predict(g_tst_feat)
    log.info("Predicted value %d for good test set", g_pred)
    b_pred = clf.predict(b_tst_feat)
    log.info("Predicted value %d for bad test set", b_pred)
