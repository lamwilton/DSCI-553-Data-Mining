from pyspark import SparkContext, SparkConf
import sys
import time
import json
from collections import defaultdict
import random


def char_table():
    """
    Read file and convert business id to numbers
    :return: characteristic table(rows = users, each row is set of businesses), list of businesses (from number to string)
    eg [(userid1, {12, 2, 4, 23, 42, 25}), ...] and [businessid1, businessid2, businessid3, ...]
    """
    def tablehelper(x):
        """
        Converting business id to numbers
        :param x:
        :return:
        """
        result = set()
        for business in x[1]:
            result.add(businessinv1.value[business])
        return tuple((x[0], result))

    # Parse file
    lines1 = lines.filter(lambda line: len(line) != 0) \
        .map(lambda s: (json.loads(s)['user_id'], json.loads(s)['business_id'])) \
        .filter(lambda x: x[0] is not None and x[1] is not None and x[0] != "" and x[1] != "")

    # Group by users
    users = lines1.groupByKey()
    users1 = users.map(lambda x: (x[0], set(x[1].data)))

    # Get list of businesses
    businesses = lines.filter(lambda line: len(line) != 0) \
        .map(lambda s: json.loads(s)['business_id']) \
        .filter(lambda x: x is not None and x != "") \
        .distinct()
    businesslist = tuple(businesses.collect())

    # Inverse index for businesses
    businessinv = defaultdict(int)
    for i in range(len(businesslist)):
        businessinv[businesslist[i]] = i
    businessinv1 = sc.broadcast(businessinv)

    # Generate characteristic table (rows = users, each row is set of businesses)
    table = users1.map(lambda x: tablehelper(x))
    table1 = table.values().collect()  # Remove user ids

    # Ending
    businessinv1.destroy()
    totaltime = time.time() - time1
    print("Duration table: " + str(totaltime))
    return table1, businesslist


def minhash(table, a, b, num_business):
    """
    Minhash method
    :param table: Characteristic table with row = users
    :param a:
    :param b:
    :return: Minhash of one hash function
    eg [749, 724, 194, 11, 103, 115, 216, 955, 192, 322, 105, 704, 32, ...]
    """
    table = table.value
    m = len(table)
    p = 479001599
    result = [(m + 10) for _ in range(num_business)]
    for row in range(len(table)):
        hashvalue = ((a * row + b) % p) % m
        for business_id in table[row]:
            if hashvalue < result[business_id]:
                result[business_id] = hashvalue
    return result


def hash_func_generate(num_func):
    """
    Generate hash functions a and b
    :return: list of a and b pairs
    eg [[983, 294], [1777, 208], [557, 236], ...]
    """
    result = []
    primes = random.sample(range(1000, sys.maxsize), num_func)
    b = random.sample(range(1000, sys.maxsize), num_func)
    for i in range(0, num_func):
        result.append([primes[i], b[i]])
    return result


def lsh_signature(minhashes):
    """
    LSH with band size of 1 rows (r = 1)
    :param minhashes: 2d list of minhashes
    :return: Set of Candidate pairs
    eg {(241, 235), (3242 ,2352), ...}
    """
    result = set()
    num_business = len(minhashes[0])
    for i in range(num_business):
        for j in range(i + 1, num_business):
            if minhashes[0][i] == minhashes[0][j]:
                result.add((i, j))
    return result


def business_table():
    """
    Generate business table for Jaccard (Rows = business)
    :return: Dictionary of businesses
    eg [('businessid1', {'userid1', ...}), ...]
    """
    busi_table = lines.filter(lambda line: len(line) != 0) \
        .map(lambda s: (json.loads(s)['business_id'], json.loads(s)['user_id'])) \
        .filter(lambda x: x[0] is not None and x[1] is not None and x[0] != "" and x[1] != "") \
        .groupByKey() \
        .map(lambda x: (x[0], set(x[1].data))) \
        .collect()
    busi_dict = dict(busi_table)
    return busi_dict


def jaccard(pair):
    """
    Find jaccard of a pair of business
    :param pair: business num (int)
    :return: tuple of business_id and their Jaccard similarity
    eg (businessid1, businessid2, 0.05)
    """
    # Find back the business ids
    business_a = businesslist[pair[0]]
    business_b = businesslist[pair[1]]

    # Look up busi dict for the sets
    a = busi_dict[business_a]
    b = busi_dict[business_b]
    union = len(a.union(b))
    if union == 0:
        similarity = 0
    else:
        similarity = len(a.intersection(b)) / union
    return tuple((business_a, business_b, similarity))


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
    conf.set("spark.master", "local[*]")
    conf.set("spark.app.name", "task1")
    sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("ERROR")
    input_file = sys.argv[1]
    output_file = sys.argv[2]

    # ============================ Read file and convert business id to numbers ==========================
    lines = sc.textFile(input_file).distinct().persist()
    table, businesslist = char_table()
    num_business = len(businesslist)

    # ===================================== Doing the Minhashing =========================================
    table1 = sc.broadcast(table)
    hash_ab = hash_func_generate(num_func=40)
    hash_ab_rdd = sc.parallelize(hash_ab)
    result_minhash = hash_ab_rdd.map(lambda x: minhash(table1, x[0], x[1], num_business)).collect()
    table1.destroy()
    totaltime = time.time() - time1
    print("Duration minhash: " + str(totaltime))

    # =========================================== Doing the LSH ===========================================
    R = 1   # Number of rows in a band
    lsh_input = []
    for i in range(0, len(result_minhash), R):
        lsh_input.append(result_minhash[i:i+R])
    lsh_input1 = sc.parallelize(lsh_input)
    lsh_part = lsh_input1.map(lsh_signature).collect()
    result_lsh = set()
    for item in lsh_part:
        result_lsh = result_lsh.union(item)
    totaltime = time.time() - time1
    print("Duration LSH: " + str(totaltime))

    # ================ Making business table (inverse char table, each row = business) =====================
    busi_dict = business_table()

    # ======================================= Jaccard and final results ====================================
    # eg [(businessid1, businessid2, 0.05263), (businessid4, businessid5, 0.07609), ...]
    result_lsh_rdd = sc.parallelize(list(result_lsh))
    final_result = result_lsh_rdd.map(lambda x: jaccard(x)) \
        .filter(lambda x: x[2] >= 0.05) \
        .collect()
    print(" True positives (Need 47548): " + str(len(final_result)))

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