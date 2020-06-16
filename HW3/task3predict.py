from pyspark import SparkContext, SparkConf
import sys
import time
import json
from collections import defaultdict


def initialize():
    """
    * Copied from task3train.py because I need the same thing

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


def initialize_item_based():
    """
    Item based case, read test and model files and process them, also process reviews to 2d dictionary
    :return: model_dict and test_pairs (rdd)
    model_dict eg {0: [(5152, 0.3227486121839514), (7364, 1.0), ...], 7336: [(8764, 0.17270852553681604), (8064, 0.3400721382955765), ...], ...}
    reviews_dict eg {0: {8761: 5.0, 56: 3.0, ...}, 24276: {4: 5.0, 5173: 2.0, 9630: 1.0, 2285: 5.0, 7897: 5.0, 6750: 4.0, 9497: 1.0}, ...}
    test_pairs (userid, businessid) eg (13465, 8849), (21851, 958), (15944, 3831), (9517, 5264), (12273, 5911), ...
    """
    # Read test file
    # eg (13465, 8849), (21851, 958), (15944, 3831), (9517, 5264), (12273, 5911), ...
    test_pairs = test_lines.filter(lambda line: len(line) != 0) \
        .map(lambda s: (json.loads(s)['user_id'], json.loads(s)['business_id'])) \
        .filter(lambda x: x[0] is not None and x[1] is not None and x[0] != "" and x[1] != "") \
        .map(lambda x: (users_dict[x[0]], businesses_dict[x[1]]))

    # Read Model file
    model_pairs = model_lines.filter(lambda line: len(line) != 0) \
        .map(lambda line: (json.loads(line))) \
        .map(lambda x: (x['b1'], x['b2'], x['sim'])) \
        .filter(lambda x: x[0] is not None and x[1] is not None and x[2] is not None and x[0] != "" and x[1] != "" and x[2] != "") \
        .map(lambda x: (businesses_dict[x[0]], (businesses_dict[x[1]], x[2])))

    # Switch b1 and b2
    model_pairs_dup = model_pairs.map(lambda x: (x[1][0], (x[0], x[1][1])))

    # Concatanate the pairs so b1/b2 and b2/b1, group by key and Output model file as dictionary
    # eg {0: {5152: 0.3227486121839514, 7364: 1.0}, ...}, 7336: {8764: 0.17270852553681604, 8064: 0.3400721382955765, ...}, ...}
    model_dict = model_pairs.union(model_pairs_dup)\
        .groupByKey()\
        .map(lambda x: (x[0], dict(x[1].data)))\
        .collectAsMap()

    # Convert reviews rdd to a 2d dictionary (user then business)
    # eg {0: {8761: 5.0, 56: 3.0, ...}, 24276: {4: 5.0, 5173: 2.0, 9630: 1.0, 2285: 5.0, 7897: 5.0, 6750: 4.0, 9497: 1.0}, ...}
    reviews_dict = reviews.map(lambda x: (x[0], (x[1], x[2])))\
        .groupByKey()\
        .map(lambda x: (x[0], dict(x[1].data)))\
        .collectAsMap()

    # Filter out businesses/users not in model
    users = set(reviews_dict.keys())
    businesses = set(model_dict.keys())
    test_pairs = test_pairs.filter(lambda x: x[0] in users and x[1] in businesses)
    return model_dict, reviews_dict, test_pairs


def prediction_item_based(user, business):
    """
    Item-based prediction formula
    :param user: Testing user
    :param business: Testing business
    :return: Predicted rating
    """
    K = 5  # Number of nearest neighbours to use
    sim_business = model_dict[business]  # Get all similar businesses from model of business of interest
    user_reviews = reviews_dict[user]  # Get reviews of user of interest
    neighbours = set(sim_business.keys()).intersection(set(user_reviews.keys()))  # Get similar businesses which is rated by that user of interest

    # Get a list of ratings and weights sorted by weights
    # eg [(0.692833855070933, 5.0), (0.6305538695530374, 5.0), (0.5706052915642571, 2.0), ...]
    ratings_list = sorted([(sim_business[i], user_reviews[i]) for i in neighbours], key=lambda x: -x[0])
    ratings_sublist = ratings_list[0:K]
    numerator = sum([x * y for x, y in ratings_sublist])
    denominator = sum([x for x, y in ratings_sublist])
    if denominator == 0:
        return 0
    else:
        result = numerator / denominator
    return result


if __name__ == '__main__':

    # ========================================== Initializing ==========================================
    time1 = time.time()
    conf = SparkConf()
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.master", "local[*]")
    conf.set("spark.app.name", "task1")
    conf.set("spark.driver.maxResultSize", "4g")
    sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("ERROR")
    input_file = sys.argv[1]
    test_file = sys.argv[2]
    model_file = sys.argv[3]
    output_file = sys.argv[4]
    cf_type = sys.argv[5]

    # ============================ Read train file and Initialize ==========================
    lines = sc.textFile(input_file).distinct()
    reviews, businesses_inv, users_inv, businesses_dict, users_dict = initialize()
    totaltime = time.time() - time1
    print("Duration Initialize: " + str(totaltime))

    # ========================== Read test and model files ==========================
    test_lines = sc.textFile(test_file).distinct()
    model_lines = sc.textFile(model_file).distinct()
    if cf_type == "item_based":
        model_dict, reviews_dict, test_pairs = initialize_item_based()
    totaltime = time.time() - time1
    print("Duration read test/model: " + str(totaltime))

    # ========================== Do the prediction ==========================
    # Item based eg ((13465, 8849), 4.096186807416058), ((21851, 958), 3.097635508434237)
    if cf_type == "item_based":
        result = test_pairs.map(lambda x: ((x[0], x[1]), prediction_item_based(x[0], x[1]))).collect()
    totaltime = time.time() - time1
    print("Duration predict: " + str(totaltime))

    # ============================ Write results ==========================
    with open(output_file, "w") as file:
        for item in result:
            entry = {"user_id": item[0][0], "business_id": item[0][1], "stars": item[1]}
            file.write(json.dumps(entry))
            file.write("\n")

    totaltime = time.time() - time1
    print("Duration: " + str(totaltime))