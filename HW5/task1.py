from pyspark import SparkContext, SparkConf
import sys
import time
import json
import binascii
import random


def convert_str(s: str):
    """
    Convert string to int
    :param s: String
    :return: Int
    """
    if s == "":
        return 2387462387782346
    return int(binascii.hexlify(s.encode('utf8')), 16)


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


def hashing(city_int):
    """
    Hashing the city as integer
    :param city_int:
    :return: List of result hashes
    """
    result = []
    p = 479001599
    for pair in ab_pairs:
        hash_result = ((pair[0] * city_int + pair[1]) % p) % m
        result.append(hash_result)
    return result


def training():
    """
    Number of unique cities = 860
    :return: Filter bit array
    """
    lines = sc.textFile(first_json_path)
    cities = lines.filter(lambda line: len(line) != 0) \
        .map(lambda line: (json.loads(line))) \
        .map(lambda x: (x['city'])) \
        .filter(lambda x: x is not None)\
        .distinct()\
        .map(convert_str) \
        .persist()
    hash_result = cities.flatMap(lambda x: hashing(x))\
        .distinct()\
        .collect()
    bit_array = []
    for i in range(0, m):
        if i in hash_result:
            bit_array.append(1)
        else:
            bit_array.append(0)
    return bit_array


def check_hash(hashes):
    for i in hashes:
        if bit_array[i] == 0:
            return 0
    return 1


def predicting():
    result = []
    with open(second_json_path, "r") as file:
        for line in file:
            city = json.loads(line)['city']
            city_int = convert_str(city)
            hashes = hashing(city_int)
            result.append(check_hash(hashes))
    return result


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
    first_json_path = sys.argv[1]
    second_json_path = sys.argv[2]
    output_file_path = sys.argv[3]
    m = 8000  # Number of buckets

    # ========================================== Main ==========================================
    ab_pairs = hash_func_generate(num_func=6)
    bit_array = training()

    totaltime = time.time() - time1
    print("Duration training : " + str(totaltime))
    result = predicting()

    # ========================================== Write results ==========================================

    with open(output_file_path, "w") as file:
        for item in result:
            file.write(str(item) + " ")

    totaltime = time.time() - time1
    print("Duration: " + str(totaltime))
    sc.stop()
