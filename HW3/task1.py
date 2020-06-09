from pyspark import SparkContext, SparkConf
import sys
import time
import json
from collections import defaultdict


def char_table(input_file):
    """
    Read file and convert business id to numbers
    :param input_file:
    :return:
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
        return result

    # Parse file
    lines = sc.textFile(input_file).distinct().persist()
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

    # Generate characteristic table. Columns = businesses, rows = users. Also removes user_ids
    table = users1.map(lambda x: tablehelper(x))
    table1 = table.collect()

    businessinv1.destroy()
    totaltime = time.time() - time1
    print("Duration table: " + str(totaltime))
    return table1, num_business


def minhash(table, a, b, num_business):
    """
    Minhash method
    :param table:
    :param a:
    :param b:
    :return:
    """
    m = len(table)
    result = [(m + 10) for _ in range(num_business)]
    for row in range(len(table)):
        hashvalue = (a * row + b) % m
        for business_id in table[row]:
            if hashvalue < result[business_id]:
                result[business_id] = hashvalue
    return result


def signature(minhashes):
    """
    LSH
    :param minhashes: Minhash of one hash function
    :return:
    """
    result = set()
    for i in range(len(minhashes)):
        for j in range(i + 1, len(minhashes)):
            if minhashes[i] == minhashes[j]:
                result.add((i, j))
    return result


def jaccard(pair):
    a = table1.value[pair[0]]
    b = table1.value[pair[1]]
    union = len(a.union(b))
    if union == 0:
        similarity = 0
    else:
        similarity = len(a.intersection(b)) / union
    return tuple((pair[0], pair[1], similarity))

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
    table, num_business = char_table(input_file)

    # Doing the Minhashing
    hash_ab = [[1, 1], [2, 2], [3, 3], [5, 4], [7, 5], [11, 6], [13, 7], [17, 8], [19, 9], [23, 10]]
    result_minhash = []
    for item in hash_ab:
        result_minhash.append(minhash(table, a=item[0], b=item[1], num_business=num_business))

    # Doing the LSH
    minhashes = sc.parallelize(result_minhash)
    boo = minhashes.map(signature).collect()
    result_lsh = set()
    for item in boo:
        result_lsh = result_lsh.union(item)


    totaltime = time.time() - time1
    print("Duration LSH: " + str(totaltime))

    # Jaccard
    table1 = sc.broadcast(table)
    lsh = sc.parallelize(result_lsh)
    jaccard_result = lsh.map(jaccard).collect()

    # Ending
    totaltime = time.time() - time1
    print("Duration: " + str(totaltime))
    sc.stop()