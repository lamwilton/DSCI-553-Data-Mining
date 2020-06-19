from pyspark import SparkContext, SparkConf
import sys
import time
from itertools import combinations
import os
from collections import defaultdict

import networkx as nx
import matplotlib.pyplot as plt


def corated_helper(user_reviews_dict, a, b):
    """
    Check if user pair has more than 7 corated businesses
    :param user_reviews_dict: Tuple of user reviews
    :param a: User A's number
    :param b: User B's number
    :return: True if corated users >= 7
    """
    if len(user_reviews_dict[a].intersection(user_reviews_dict[b])) >= filter_threshold:
        return True
    return False


def graph_construct():
    """
    Constructing graph from candidate pairs as usual
    Do not use dictionaries here. It give different results

    Reviews count = 38648
    Business count (before filtering) = 9947
    Users count (before filtering) = 3374
    Users count (after filtering, vertices) = 222
    Edges count = 498 (x2 for reverse)
    :return: Users (Vertices) and Candidate pairs (Edges)
    """
    lines = sc.textFile(input_file_path).distinct()
    header = lines.first()
    reviews_long = lines.filter(lambda line: len(line) != 0) \
        .filter(lambda line: line != header) \
        .map(lambda x: (str(x.split(",")[0]), str(x.split(",")[1]))) \
        .persist()

    reviews = reviews_long.map(lambda x: (x[0], x[1]))\
        .persist()
    reviews_long.unpersist()

    baskets_pre = reviews.groupByKey()
    # Convert value list to set
    baskets = baskets_pre.map(lambda x: (x[0], set(x[1].data))) \
        .filter(lambda x: len(x[1]) >= filter_threshold)  # filter qualified users more than or equal 7

    # Output users as a dictionary, index = user number
    # eg {1: {513, 515, 4, 517, 519, 2055...}, {2: {6160, 3104, 556, ...}, ...}
    user_reviews_dict = baskets.sortByKey().collectAsMap()

    # Generate all pairs of users, dont know why the hell cartesian method aint work here
    users = baskets.map(lambda x: x[0])\
        .sortBy(lambda x: x)\
        .collect()
    all_pairs = sc.parallelize(list(combinations(users, 2)))

    # Filter only corated businesses >= 7
    # eg [(0, 10), (0, 14), (0, 16), (0, 20), (0, 28), (0, 30), (0, 32), ...]
    candidate_pairs_pre = all_pairs.filter(lambda x: corated_helper(user_reviews_dict, x[0], x[1])).persist()
    candidate_pairs_pre_2 = candidate_pairs_pre.map(lambda x: (x[1], x[0]))
    candidate_pairs = candidate_pairs_pre.union(candidate_pairs_pre_2).collect()
    candidate_users = candidate_pairs_pre.flatMap(lambda x: [x[0], x[1]]).distinct().collect()
    return candidate_users, candidate_pairs


def convert_short():
    users_inv = tuple(candidate_users)
    users_dict = defaultdict(int)
    for i in range(len(users_inv)):
        users_dict[users_inv[i]] = i
    pairs_short = list(map(lambda x: (users_dict[x[0]], users_dict[x[1]]), candidate_pairs))
    return users_dict, users_inv, pairs_short


if __name__ == '__main__':

    # ========================================== Initializing ==========================================
    time1 = time.time()
    conf = SparkConf()
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.master", "local[*]")
    conf.set("spark.app.name", "task2")
    conf.set("spark.driver.maxResultSize", "4g")

    sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")
    filter_threshold = int(sys.argv[1])
    input_file_path = sys.argv[2]
    community_output_file_path = sys.argv[3]

    # ========================================== Graph Construction ==========================================
    candidate_users, candidate_pairs = graph_construct()
    users_dict, users_inv, pairs_short = convert_short()

    totaltime = time.time() - time1
    print("Duration Graph Construction: " + str(totaltime))


    # Test plot graph
    graph = defaultdict(list)
    for item in pairs_short:
        graph[item[0]].append(item[1])

    plt.rcParams["figure.figsize"] = (50, 40)
    nxgraph = nx.DiGraph(graph)
    nx.draw(nxgraph, with_labels=True)
    plt.savefig("what.png")
    plt.show()

    totaltime = time.time() - time1
    print("Duration: " + str(totaltime))
