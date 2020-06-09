from pyspark import SparkContext, SparkConf
import sys
import time
import json
from collections import defaultdict


def char_table(input_file):
    """
    Read file and generate charactierstic table
    :param input_file:
    :return:
    """
    def tablehelper(x):
        """
        Start with zero vector. Change to 1 if that user rated that business in that position
        :param x:
        :return:
        """
        result = [0 for _ in range(len(businessinv1.value))]
        for business in x[1]:
            result[businessinv1.value[business]] = 1
        return tuple(result)

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

    # Generate characteristic table. Columns = businesses, rows = users. Also removes user_ids
    table = users1.map(lambda x: tablehelper(x))
    table1 = table.collect()

    totaltime = time.time() - time1
    print("Duration table: " + str(totaltime))
    return table1


def minhash(table, a, b):
    """
    Minhash method
    :param table:
    :param a:
    :param b:
    :return:
    """
    m = len(table)
    result = [(m + 10) for _ in range(len(table[0]))]
    for row in range(len(table)):
        hashvalue = (a * row + b) % m
        for column in range(len(table[0])):
            if table[row][column] == 1 and hashvalue < result[column]:
                result[column] = hashvalue
    return result


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
    table = char_table(input_file)

    print(minhash(table, a=1, b=1))

    # Ending
    totaltime = time.time() - time1
    print("Duration: " + str(totaltime))