from pyspark import SparkContext, SparkConf
import sys
import time
import json
from collections import defaultdict
from itertools import combinations
import os
from graphframes import *
from pyspark.sql import SparkSession, Row
from pyspark.sql import Row


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
    Reviews count = 38648
    Business count (before filtering) = 9947
    Users count (before filtering) = 3374
    Users count (after filtering, vertices) = 222
    Edges count = 498
    :return: Users (Vertices) and Candidate pairs (Edges)
    """
    lines = sc.textFile(input_file_path).distinct()
    header = lines.first()
    reviews_long = lines.filter(lambda line: len(line) != 0) \
        .filter(lambda line: line != header) \
        .map(lambda x: (str(x.split(",")[0]), str(x.split(",")[1]))) \
        .persist()

    # Get lists of unique businesses and users as inverse dictionary from integer code to ID
    businesses_inv = tuple(reviews_long.map(lambda x: x[1]).distinct().collect())
    users_inv = tuple(reviews_long.map(lambda x: x[0]).distinct().collect())

    # Make dictionaries to convert long IDs to integer code
    businesses_dict = defaultdict(int)
    for i in range(len(businesses_inv)):
        businesses_dict[businesses_inv[i]] = i
    users_dict = defaultdict(int)
    for i in range(len(users_inv)):
        users_dict[users_inv[i]] = i

    reviews = reviews_long.map(lambda x: (users_dict[x[0]], businesses_dict[x[1]]))\
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
    candidate_pairs_pre_2 = candidate_pairs_pre.map(lambda x:(x[1], x[0]))
    candidate_pairs = candidate_pairs_pre.union(candidate_pairs_pre_2).collect()
    candidate_users = candidate_pairs_pre.flatMap(lambda x: [x[0], x[1]]).distinct().collect()
    return candidate_users, candidate_pairs, users_dict, users_inv


if __name__ == '__main__':

    # ========================================== Initializing ==========================================
    time1 = time.time()
    os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11  pyspark-shell")
    conf = SparkConf()
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.master", "local[*]")
    conf.set("spark.app.name", "task1")
    conf.set("spark.driver.maxResultSize", "4g")

    sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")
    ss = SparkSession(sc)
    filter_threshold = int(sys.argv[1])
    input_file_path = sys.argv[2]
    community_output_file_path = sys.argv[3]

    # ========================================== Graph Construction ==========================================
    candidate_users, candidate_pairs, users_dict, users_inv = graph_construct()
    row = Row("id")
    vertices = sc.parallelize(candidate_users).map(row).toDF()
    edges = sc.parallelize(candidate_pairs).toDF(["src", "dst"])
    graph = GraphFrame(vertices, edges)

    totaltime = time.time() - time1
    print("Duration Graph Construction: " + str(totaltime))

    # ========================================== LPA ==========================================
    # Do LPA and return as dataframe
    # eg (id, label) = (26, 1), (29, 0), ...
    result = graph.labelPropagation(maxIter=5)

    # Convert dataframe back to RDD, map with label as key and group by key. Also translate back to long userids
    # Have to coalesce or else it gets stuck
    pre_result = result.rdd.coalesce(2)\
        .map(lambda x: (users_inv[x[1]], users_inv[x[0]]))\
        .groupByKey()

    # Drop keys and Sort lexigraphical order
    final_result = pre_result.map(lambda x: sorted(x[1].data))\
        .sortBy(lambda x: (len(x), x[0]))\
        .collect()

    # ======================================= Write results =======================================
    with open(community_output_file_path, "w") as file:
        for item in final_result:
            line = str(item).strip("[]")
            file.write(line)
            file.write("\n")

    totaltime = time.time() - time1
    print("Duration: " + str(totaltime))
