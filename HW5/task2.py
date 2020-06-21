from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import sys
import time
from datetime import datetime
import json
import binascii


def convert_str(s: str):
    """
    Convert string to int
    :param s: String
    :return: Int
    """
    if s == "":
        return 2387462387782346
    return int(binascii.hexlify(s.encode('utf8')), 16)


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
    x_zeros = count_zeros(x)
    y_zeros = count_zeros(y)
    max_zeros = max(x_zeros, y_zeros)
    result = 2 ** max_zeros
    return result


def rdd_helper(rdd):
    truth = rdd.distinct().count()
    estimate = rdd.reduce(lambda x, y: reduce_helper(x, y))
    time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    result = time + "," + str(truth) + "," + str(estimate)
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
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 5)
    port_num = int(sys.argv[1])
    output_file_name = sys.argv[2]

    lines = ssc.socketTextStream("localhost", port_num)
    cities_stream = lines.window(30, 10)\
        .filter(lambda line: len(line) != 0) \
        .map(lambda line: (json.loads(line))) \
        .map(lambda x: (x['city'])) \
        .filter(lambda x: x is not None)\
        .map(lambda x: convert_str(x))
    result = cities_stream.foreachRDD(lambda rdd: print(rdd_helper(rdd)))
    #result = cities_stream.reduce(lambda x, y: reduce_helper(x, y)).pprint()
    ssc.start()
    ssc.awaitTermination()