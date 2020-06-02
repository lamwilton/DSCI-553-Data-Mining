from pyspark import SparkContext
import json
import sys
import time
from collections import Counter


def case_1(input_file):
    # Read csv, tokenize and remove header
    lines = sc.textFile(input_file) \
        .map(lambda x: (x.split(",")[0], x.split(",")[1])) \
        .filter(lambda x: x[0] != "user_id")
    baskets = lines.groupByKey()
    # Convert value list to set
    baskets1 = baskets.map(lambda x: (x[0], set(x[1].data)))
    return baskets1


def test(iterator):
    result = []
    for sub_list in iterator:
        result.append(sub_list[0])
    return [len(result)]


if __name__ == '__main__':
    time1 = time.time()
    sc = SparkContext(master="local[*]", appName="task1")
    sc.setLogLevel("ERROR")
    case_number = int(sys.argv[1])
    support = int(sys.argv[2])
    input_file = sys.argv[3]
    output_file = sys.argv[4]
    baskets = case_1(input_file)

    baskets1 = baskets.mapPartitions(test)
    print(baskets1.getNumPartitions())
    print(baskets1.collect())

