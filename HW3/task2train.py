from pyspark import SparkContext, SparkConf
import sys
import time
import json
from collections import defaultdict
import random


def reading_file():
    """
    Read file and do word counting
    :return: Word counts
    eg ('UO--KDTb5NEwp3SCdX5CMQ', [('nice', 8), ('good', 7), ...])
    """
    # Read stopwords file
    with open(stopwords_file) as file:
        stopwords = file.read().splitlines()

    # Parse file, remove puntuations stopwords
    # eg reviews = [('zK7sltLeRRioqYwgLiWUIA', "second time i've first time whatever burger side"), ...]
    reviews = lines.filter(lambda line: len(line) != 0) \
        .map(lambda s: (json.loads(s)['business_id'], json.loads(s)['text'])) \
        .filter(lambda x: x[0] is not None and x[1] is not None and x[0] != "" and x[1] != "") \
        .mapValues(lambda line: line.translate({ord(i): None for i in '([,.!?:;])&\"0123456789'}).lower()) \
        .mapValues(lambda line: " ".join([word for word in line.split() if word not in stopwords]))

    # Concat according to each business
    reviews_concat = reviews.reduceByKey(lambda a, b: str(a) + " " + str(b))

    # Word count, using business id and word as composite key eg (('djAWtGq2IxKaxMIMw-P58A', 'due'), 10)
    # Also I dont want any words appearing only once
    counts = reviews.flatMapValues(lambda line: line.split(" ")) \
        .map(lambda x: (x, 1)) \
        .filter(lambda x: x[0][1] != "") \
        .reduceByKey(lambda a, b: a + b) \
        .filter(lambda x: x[1] > 1)

    # Change key/value, group by key
    # eg ('UO--KDTb5NEwp3SCdX5CMQ', [('nice', 8), ('good', 7), ...])
    counts1 = counts.map(lambda x: (x[0][0], (x[0][1], x[1])))\
        .groupByKey()\
        .map(lambda x: (x[0], x[1].data))\
        .collect()
    return counts1


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
    output_file = sys.argv[2]
    stopwords_file = sys.argv[3]

    # ============================ Read file ==========================
    lines = sc.textFile(input_file).distinct()
    ooo = reading_file()

    # ========================================== Ending ==========================================
    totaltime = time.time() - time1
    print("Duration: " + str(totaltime))
    sc.stop()