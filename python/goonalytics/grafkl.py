"""
graphs and shit
"""
import json
import logging
from typing import Dict, Set

import matplotlib.pyplot as plt
import networkx as nx
from networkx.readwrite import json_graph as nxjs

from goonalytics.io.gcloudio import PostBigQueryer
from goonalytics.settings import GCLOUD_POST_TABLE as tbl

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_post_quote_dict(thread_id: int) -> (Dict[int, Set[int]], Dict[int, str]):
    """
    some confusing shit that somehow gets data for a graph
    :param thread_id:
    :return: a tuple whose 0-index is a dict suitable for use with create_graph and whose 1-index is a username dict suitable
    for use with graph_to_node_link
    """
    # three maps: post id -> quoted post ids, uids -> usernames, post id -> user id
    # this could be simplified with a more complicated query
    pid_qpid_map = dict()
    uid_uname_map = dict()
    postid_uid_map = dict()
    bq = PostBigQueryer()
    # TODO fix this injection thing before making public
    qr = bq.sql_inject('''select post_id, user_id, quoted_post_ids, user_name from %s where thread_id=%d''' % ('forums.' + tbl, thread_id))
    # that query returns repeated rows for each different quoted pid which affects the iteration
    for row in qr.rows:
        post_id = row[0]
        user_id = row[1]
        quote = row[2] if row[2] is not None else 'null'
        user_name = row[3]
        uid_uname_map[user_id] = user_name
        if post_id not in pid_qpid_map.keys():
            pid_qpid_map[post_id] = set()
        pid_qpid_map[post_id].add(quote)
        postid_uid_map[post_id] = user_id
    # next transform the post_ids to the user ids
    pid_qpid_map_intermediate = dict()
    for key in pid_qpid_map.keys():
        user = postid_uid_map[key]
        # next we get the username for each quote
        quote_users = set()
        for val in pid_qpid_map[key]:
            try:
                quote_users.add(postid_uid_map[val])
            except KeyError:
                continue
        pid_qpid_map_intermediate[user] = quote_users
    return pid_qpid_map_intermediate, uid_uname_map


def create_graph(user_dict: Dict[int, Set[int]]) -> nx.Graph:
    """
    Creates a networkx.Graph object out of the dict provided
    :param user_dict: a dictionary whose keys are the user id of the vertex and whose values are the set of user ids that that
    user will be connected to
    :return: an nx.Graph instance
    """
    g = nx.Graph();
    # add all vertices
    for u in user_dict.keys():
        g.add_node(u)
    for u in user_dict.keys():
        for u2 in user_dict[u]:
            g.add_edge(u, u2)
    return g


def graph_to_node_link(g: nx.Graph, user_name_dict: Dict[int, str]=None, min_degree: int=0) -> dict:
    # logger.debug("Graph nodes: %s", g.nodes())
    # logger.debug("Graph edges: %s", g.edges())
    # add usernames before taking stuff out
    """
    transforms the graph to node link for d3js and optionally removes nodes having fewer than a certain number of edges
    :param g: an instance of networkx.Graph
    :param user_name_dict: a mapping of user ids to user names
    :param min_degree: the minimum number of edges for a node to be included
    :return: a dict/json sufficient for use with d3js force layouts etc
    """
    if user_name_dict:
        nx.relabel_nodes(g, user_name_dict, copy=False)
    if min_degree > 0:
        outdeg = g.degree()
        to_remove = [n for n in outdeg if outdeg[n] < min_degree]
        g.remove_nodes_from(to_remove)
    data = nxjs.node_link_data(g)
    return data


def graph_to_adjacency(g: nx.Graph) -> str:
    data = nxjs.adjacency_data(g)
    return json.dumps(data)


if __name__ == '__main__':
    mp, un = get_post_quote_dict(3763968)
    print(mp)
    print(mp[112394])
    g = create_graph(mp)
    nx.draw(g, labels=un)
    plt.show()
