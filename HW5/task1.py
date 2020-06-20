from pyspark import SparkContext, SparkConf
import sys
import time
import json
import binascii
import random
from itertools import combinations
from collections import defaultdict


def convert_str(s: str):
    """
    Convert string to int
    :param s: String
    :return: Int
    """
    return int(binascii.hexlify(s.encode('utf8)), 16)')))


def hash_func_generate(num_func):
    """
    Generate hash functions a and b
    :return: list of a and b pairs
    eg [[983, 294], [1777, 208], [557, 236], ...]
    """
    result = []
    primes = random.sample(range(1000000, sys.maxsize), num_func)
    b = random.sample(range(1000000, sys.maxsize), num_func)
    for i in range(0, num_func):
        result.append([primes[i], b[i]])
    return result


def training():
    lines = sc.textFile(first_json_path)
    cities = lines.filter(lambda line: len(line) != 0) \
        .map(lambda line: (json.loads(line))) \
        .map(lambda x: (x['city'])) \
        .filter(lambda x: x is not None and x != "")\
        .map(convert_str) \
        .persist()



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
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 10)
    first_json_path = sys.argv[1]
    second_json_path = sys.argv[2]
    output_file_path = sys.argv[3]
