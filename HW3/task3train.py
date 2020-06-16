from pyspark import SparkContext, SparkConf
import sys
import time
import json
import math
from collections import defaultdict


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
    return reviews, businesses_inv, users_inv, businesses_dict, users_dict


def corated_helper(business_reviews_tuple, a, b):
    """
    Check if business pair has more than 3 corated users
    :param business_reviews_tuple: Tuple of business reviews
    :param a: Business A's number
    :param b: Business B's number
    :return: True if corated users >= 3
    """
    if len(set(business_reviews_tuple[a].keys()).intersection(set(business_reviews_tuple[b].keys()))) >= 3:
        return True
    return False


def item_based():
    """
    Case 1 item based. Get candidate business pairs with more than 3 corated users
    :return: Candidate pairs and business_reviews_tuple
    candidate_pairs eg [(0, 7336), (0, 9492), (0, 5908), (0, 5152), (0, 6622)]
    business_reviews_tuple eg ({24267: 1.0, 5670: 3.0, 15085: 2.0, 7731: 3.0, 300: 3.0, ...}, {...})
    """
    # Group the reviews by business
    business_reviews = reviews.map(lambda x: (x[1], (x[0], x[2])))\
        .groupByKey()\
        .map(lambda x: (x[0], dict(x[1].data)))\
        .persist()

    # Output as a tuple of dict, index = business number
    # eg ({24267: 1.0, 5670: 3.0, 15085: 2.0, 7731: 3.0, 300: 3.0, ...}, {...})
    business_reviews_tuple = tuple(business_reviews.sortByKey().map(lambda x: x[1]).collect())

    # Generate all pairs of businesses
    businesses = business_reviews.map(lambda x: x[0])
    all_pairs = businesses.cartesian(businesses)\
        .filter(lambda x: x[0] < x[1])

    # Remove those who has less than 3 corated users
    candidate_pairs = all_pairs.filter(lambda x: corated_helper(business_reviews_tuple, x[0], x[1]))
    # print("Number of candidate pairs: " + str(candidate_pairs.count()))
    return candidate_pairs, business_reviews_tuple


def pearson_helper(data, a, b):
    """
    Pearson for item based
    :param data: Tuple of business reviews or user reviews
    :param a: User/Business A's number
    :param b: User/Business B's number
    :param avg_a: User/Business A's average rating
    :param avg_b: User/Business B's average rating
    :return: Pearson correlation value
    """
    # Find corated items
    corate_set = set(data[a].keys()).intersection(set(data[b].keys()))

    # Get the normalized vectors of a and b
    vec_a_pre = [data[a].get(item) for item in corate_set]
    vec_b_pre = [data[b].get(item) for item in corate_set]
    avg_a = sum(vec_a_pre) / len(vec_a_pre)
    avg_b = sum(vec_b_pre) / len(vec_b_pre)
    vec_a = list(map(lambda x: x - avg_a, vec_a_pre))
    vec_b = list(map(lambda x: x - avg_b, vec_b_pre))

    numerator = sum([x * y for x, y in zip(vec_a, vec_b)])
    denominator = math.sqrt(sum([x ** 2 for x in vec_a])) * math.sqrt(sum([x ** 2 for x in vec_b]))
    if denominator == 0:
        return 0
    else:
        result = numerator / denominator
    return result


def format_output(final_result):
    """
    Format output file
    :param final_result: List of tuples
    :return: List of dictionaries
    eg [{'b1': businessid1, 'b2': businessid2, 'sim': 0.07693}, {'b1': businessid1, 'b2': businessid2, 'sim': 0.052632}, ...]
    """
    result = []
    for item in final_result:
        result.append({'b1': item[0], 'b2': item[1], 'sim': item[2]})
    return result


if __name__ == '__main__':

    # ========================================== Initializing ==========================================
    time1 = time.time()
    conf = SparkConf()
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.master", "local[2]")  # Change to local[*] on vocareum
    conf.set("spark.app.name", "task3")
    conf.set("spark.driver.maxResultSize", "4g")
    sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("ERROR")
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    cf_type = sys.argv[3]
    business_avg_file = "business_avg.json"
    user_avg_file = "user_avg.json"

    # ============================ Read file and Initialize ==========================
    lines = sc.textFile(input_file).distinct()
    reviews, businesses_inv, users_inv, businesses_dict, users_dict = initialize()
    totaltime = time.time() - time1
    print("Duration Initialize: " + str(totaltime))

    # ============================ Item/business based ==========================
    if cf_type == "item_based":
        candidate_pairs, business_reviews_tuple = item_based()
        final_pairs_pre = candidate_pairs.map(lambda x: (x[0], x[1], pearson_helper(data=business_reviews_tuple, a=x[0], b=x[1])))\
            .filter(lambda x: x[2] > 0)

        # Get the business ID back
        final_result = final_pairs_pre.map(lambda x: (businesses_inv[x[0]], businesses_inv[x[1]], x[2])).collect()
        print("Number of pairs final: " + str(len(final_result)))

        totaltime = time.time() - time1
        print("Duration Item Based: " + str(totaltime))

    # ======================================= Write results =======================================
    final_result_write = format_output(final_result)
    with open(output_file, "w") as file:
        for line in final_result_write:
            file.write(json.dumps(line))
            file.write("\n")

    # ========================================== Ending ==========================================
    totaltime = time.time() - time1
    print("Duration: " + str(totaltime))
    sc.stop()