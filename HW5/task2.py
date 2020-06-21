from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import sys
import time
from datetime import datetime
import json
import binascii
import random


def hash_func_generate(num_func):
    """
    Generate hash functions a and b
    :return: list of a and b pairs
    eg [[983, 294], [1777, 208], [557, 236], ...]
    """
    result = []
    a = random.sample(range(1000000, sys.maxsize), num_func)
    b = random.sample(range(1000000, sys.maxsize), num_func)
    for i in range(0, num_func):
        result.append([a[i], b[i]])
    return result


def convert_str(s):
    """
    Convert string to int, also do the hashing using multiple hashes
    :param s: String
    :return: list of tuples, key = number of hash function
    eg [(0, 574), (1, 457), (2, 547), (3, 456), (4, 567), (5, 4567), (6, 868), (7, 565), (8, 564), (9, 457), (10, 456), (11, 754)]
    """
    p = 479001599
    m = 1024  # 10 bits
    if s == "":
        return 2387462387782346
    num = int(binascii.hexlify(s.encode('utf8')), 16)
    ab_pairs = hash_func_generate(num_func=12)
    result = []
    for i in range(len(ab_pairs)):
        hash_result = ((ab_pairs[i][0] * num + ab_pairs[i][1]) % p) % m
        result.append(tuple((i, hash_result)))
    return result


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
    Find max length of trailing zeros out of all hashes for reduce
    :param x:
    :param y:
    :return: Length of trailing zero ** 2
    """
    x_zeros = count_zeros(x)
    y_zeros = count_zeros(y)
    max_zeros = max(x_zeros, y_zeros)
    result = 2 ** max_zeros
    return result


def rdd_helper(rdd):
    """
    Compute result for each rdd and write to file
    :param rdd: Inputing rdd of 12 hashes of each city
    :return:
    """
    truth = rdd.distinct().count()

    # Estimate the number of unique elements using multiple hash functions
    # Reduce according to the number of hash function, then drop key and sort by values
    # eg [512, 256, 512, 256, 64, 512, 64, 256, 64, 32, 512, 512]
    estimate = rdd.flatMap(lambda x: convert_str(x))\
        .reduceByKey(lambda x, y: reduce_helper(x, y))\
        .values()\
        .collect()

    # Take average of middle group only as the final estimate
    estimate_final = sum(sorted(estimate)[4:8]) / 4
    print(sorted(estimate)[4:8])
    time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    result = time + "," + str(truth) + "," + str(estimate)
    print(result)
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

    # Write header for output file
    with open(output_file_name, "w") as file:
        file.write("Time,Ground Truth,Estimation")
        file.write("\n")

    # ========================================== Main ==========================================
    lines = ssc.socketTextStream("localhost", port_num)
    cities_stream = lines.window(30, 10)\
        .filter(lambda line: len(line) != 0) \
        .map(lambda line: (json.loads(line))) \
        .map(lambda x: (x['city'])) \
        .filter(lambda x: x is not None)

    cities_stream.foreachRDD(lambda rdd: rdd_helper(rdd))

    ssc.start()
    ssc.awaitTermination()
