from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import sys
import time
from datetime import datetime
import json
import binascii


def convert_str(s):
    """
    Convert string to int, also do the hashing
    :param s: String
    :return: Int
    """
    p = 479001599
    m = 1024  # 10 bits
    if s == "":
        return 2387462387782346
    num = int(binascii.hexlify(s.encode('utf8')), 16)
    hash_num = (982735982389 * num + 293879238759283) % p % m
    return hash_num


def count_zeros(num):
    """
    Count number of trailing zeros of int as binary
    :param num:
    :return: number of zeros
    """
    result = 0
    if num == 0:
        return 0
    while True:
        if num & 1 == 0:
            result += 1
            num = num >> 1
        else:
            return result


def reduce_helper(x, y):
    """
    Find max length of trailing zeros for reduce
    :param x:
    :param y:
    :return:
    """
    x_zeros = count_zeros(x)
    y_zeros = count_zeros(y)
    max_zeros = max(x_zeros, y_zeros)
    result = 2 ** max_zeros
    return result


def rdd_helper(rdd):
    """
    Compute result for each rdd and write to file
    :param rdd:
    :return:
    """
    truth = rdd.distinct().count()
    estimate = rdd.reduce(lambda x, y: reduce_helper(x, y))
    time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    result = time + "," + str(truth) + "," + str(estimate)

    with open(output_file_name, "a+") as file:
        file.write(str(result))
        file.write("\n")
    return


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
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 5)
    port_num = int(sys.argv[1])
    output_file_name = sys.argv[2]

    # ========================================== Main ==========================================
    lines = ssc.socketTextStream("localhost", port_num)
    cities_stream = lines.window(30, 10)\
        .filter(lambda line: len(line) != 0) \
        .map(lambda line: (json.loads(line))) \
        .map(lambda x: (x['city'])) \
        .filter(lambda x: x is not None)\
        .map(lambda x: convert_str(x))

    cities_stream.foreachRDD(lambda rdd: rdd_helper(rdd))

    ssc.start()
    ssc.awaitTermination()
