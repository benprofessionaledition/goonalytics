"""
This file is part of the flask+d3 Hello World project.
"""
import json

import flask
from flask import request, jsonify, render_template

import goonalytics.grafkl as graphs
from goonalytics.io.gcloudio import random_thread_id
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

app = flask.Flask(__name__, static_folder="templates")


@app.route("/")
def index():
    """
    When you request the root path, you'll get the index.html template.

    """
    return flask.render_template("index.html")

@app.route("/thread")
def get_graph_data():

    """
    returns json of a network graph for the specified thread
    :param thread_id:
    :return:
    """
    thread_id = request.args.get("thread_id", random_thread_id(post_count_min=200), type=int)
    min_edges = request.args.get("min_edges", 1, type=int)
    pqdict, userdict = graphs.get_post_quote_dict(thread_id)
    G = graphs.create_graph(pqdict)
    s = graphs.graph_to_node_link(G, userdict, min_degree=min_edges)
    return json.dumps(s)


@app.route("/showgraph")
def showgraph():
    min_edges = request.args.get("min_edges", 1, type=int)
    thread_id = request.args.get("thread_id", random_thread_id(post_count_min=200), type=int)
    return render_template("threadgraph.html", threadid=thread_id, minedges=min_edges)


if __name__ == "__main__":
    import os

    port = 8888

    # Open a web browser pointing at the app.
    os.system("open http://localhost:{0}".format(port))

    # Set up the development server on port 8000.
    app.debug = True
    app.run(port=port)
