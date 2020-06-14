from pyspark import SparkContext, SparkConf
import sys
import time
import json
import math
from collections import defaultdict
import operator


def initialize():
    """
    Read file and make dictionaries to shorten the long user and business IDs
    :return: reviews, businesses_inv, users_inv, businesses_dict, users_dict
    eg for reviews [(20513, 2236, 5.0), (24264, 7332, 4.0), (16861, 9483, 5.0), ...]

    Number of businesses = 10253
    Number of users = 26184
    """
    # Get reviews
    # eg ('VTbkwu0nGwtD6xiIdtD00Q', 'fjMXGgOr3aCxnN48kovZ_Q', 5.0)
    reviews_long = lines.filter(lambda line: len(line) != 0) \
        .map(lambda line: (json.loads(line))) \
        .map(lambda x: (x['user_id'], x['business_id'], x['stars'])) \
        .filter(lambda x: x[0] is not None and x[1] is not None and x[2] is not None and x[0] != "" and x[1] != "" and x[2] != "") \
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

    # Get a shorter version of the reviews_long crap
    # eg [(20513, 2236, 5.0), (24264, 7332, 4.0), (16861, 9483, 5.0), ...]
    reviews = reviews_long.map(lambda x: (users_dict[x[0]], businesses_dict[x[1]], x[2])) \
        .persist()
    reviews_long.unpersist()
    # Take Cartesian product with itself and generate business pairs, also the length of intersection between two sets
    # eg (('WEeMwRLhgCyO1b4kikVcuQ', 'YQ--LJ7pvjiDSqNv0TuKTQ'), 3)
    #pairs = businesses.cartesian(businesses) \
    #    .map(lambda x: ((x[0][0], x[1][0]), len(x[0][1].intersection(x[1][1]))))

    # Filter only those with 3 or above corated users, also I dont want the same business
    #result = pairs.filter(lambda x: x[0][0] != x[0][1] and x[1] >= 3).collect()
    return reviews, businesses_inv, users_inv, businesses_dict, users_dict


if __name__ == '__main__':

    # ========================================== Initializing ==========================================
    time1 = time.time()
    conf = SparkConf()
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.master", "local[*]")
    conf.set("spark.app.name", "task3")
    conf.set("spark.driver.maxResultSize", "4g")
    sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("ERROR")
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    case = sys.argv[3]

    # ============================ Read file and Initialize ==========================
    lines = sc.textFile(input_file).distinct()
    reviews, businesses_inv, users_inv, businesses_dict, users_dict = initialize()
    totaltime = time.time() - time1
    print("Duration Initialize: " + str(totaltime))

    # ========================================== Ending ==========================================
    totaltime = time.time() - time1
    print("Duration: " + str(totaltime))
    sc.stop()