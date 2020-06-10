from pyspark import SparkContext, SparkConf
import sys
import time
import json
from collections import defaultdict


def char_table(input_file):
    """
    Read file and convert business id to numbers
    :param input_file:
    :return: characteristic table(rows = users, each row is set of businesses), list of businesses (from number to string)
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
    num_business = len(businessinv)

    # Generate characteristic table (rows = users, each row is set of businesses)
    table = users1.map(lambda x: tablehelper(x))
    table1 = table.values().collect()  # Remove user ids

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
    """
    table = table.value
    m = len(table)
    result = [(m + 10) for _ in range(num_business)]
    for row in range(len(table)):
        hashvalue = (a * row + b) % m
        for business_id in table[row]:
            if hashvalue < result[business_id]:
                result[business_id] = hashvalue
    return result

def hash_func_generate(num_func):
    """
    Generate hash functions a and b
    :return:
    """
    primes = [2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97,101,103,107,109,113,127,131,137,139,149,151,157,163,167,173,179,181,191,193,197,199,211,223,227,229,233,239,241,251,257,263,269,271,277,281,283,293,307,311,313,317,331,337,347,349,353,359,367,373,379,383,389,397,401,409,419,421,431,433,439,443,449,457,461,463,467,479,487,491,499,503,509,521,523,541,547,557,563,569,571,577,587,593,599,601,607,613,617,619,631,641,643,647,653,659,661,673,677,683,691,701,709,719,727,733,739,743,751,757,761,769,773,787,797,809,811,821,823,827,829,839,853,857,859,863,877,881,883,887,907,911,919,929,937,941,947,953,967,971,977,983,991,997,1009,1013,1019,1021,1031,1033,1039,1049,1051,1061,1063,1069,1087,1091,1093,1097,1103,1109,1117,1123,1129,1151,1153,1163,1171,1181,1187,1193,1201,1213,1217,1223]
    result = []
    for i in range(0, num_func):
        result.append([primes[i], i])
    return result


def signature(minhashes):
    """
    LSH with band size of 4 rows (r = 4)
    :param minhashes: 2d list of minhashes
    :return: Set of Candidate pairs
    """
    result = set()
    num_business = len(minhashes[0])
    for i in range(num_business):
        for j in range(i + 1, num_business):
            if minhashes[0][i] == minhashes[0][j] and minhashes[1][i] == minhashes[1][j] and minhashes[2][i] == minhashes[2][j] and minhashes[3][i] == minhashes[3][j]:
                result.add((i, j))
    return result


def business_table():
    """
    Generate business table for Jaccard (Rows = business)
    :return: Dictionary of businesses
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


if __name__ == '__main__':
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

    # Read file and convert business id to numbers
    lines = sc.textFile(input_file).distinct().persist()
    table, businesslist = char_table(input_file)
    num_business = len(businesslist)

    # Doing the Minhashing
    table1 = sc.broadcast(table)
    hash_ab = hash_func_generate(num_func=200)
    hash_ab_rdd = sc.parallelize(hash_ab)
    result_minhash = hash_ab_rdd.map(lambda x: minhash(table1, x[0], x[1], num_business)).collect()
    table1.destroy()
    totaltime = time.time() - time1
    print("Duration minhash: " + str(totaltime))

    # Doing the LSH
    # TODO: Optimize LSH
    R = 4   # Number of rows in a band
    lsh_input = []
    for i in range(0, len(result_minhash), R):
        lsh_input.append(result_minhash[i:i+R])
    lsh_input1 = sc.parallelize(lsh_input)
    boo = lsh_input1.map(signature).collect()
    result_lsh = set()
    for item in boo:
        result_lsh = result_lsh.union(item)
    totaltime = time.time() - time1
    print("Duration LSH: " + str(totaltime))

    busi_dict = business_table()

    final_result = list(map(jaccard, result_lsh))

    # Ending
    totaltime = time.time() - time1
    print("Duration: " + str(totaltime))
    sc.stop()