from pyspark import SparkContext
import json
import sys
import time
from collections import Counter
import itertools


def case_1(input_file):
    # Read csv, tokenize and remove header
    lines = sc.textFile(input_file) \
        .map(lambda x: (x.split(",")[0], x.split(",")[1])) \
        .filter(lambda x: x[0] != "user_id")
    baskets = lines.groupByKey()
    # Convert value list to set
    baskets1 = baskets.map(lambda x: (x[0], set(x[1].data)))
    return baskets1


def a_priori(iterator):
    # Copy the subset of baskets so I can reloop it many many times. Iterator only allows traversing once!
    baskets = [i[1] for i in iterator]
    cnt = Counter()
    # Count frequent singletons using python counter
    for sub_list in baskets:
        for item in sub_list:
            cnt[item] += 1
    # Filter out the infrequent elements (pruning)
    l_1 = set([frozenset([item]) for item in cnt if cnt[item] >= support_part])
    print("L1 number of elements: " + str(len(l_1)))

    # Following pseudocode of apriori, with k more than 1
    k = 2
    c_2 = set([x.union(y) for x in l_1 for y in l_1 if x != y and len(x.union(y)) == k])
    print("Candidate k item sets: " + str(c_2))
    cnt = Counter()
    for sub_list in baskets:
        # Generate subsets of size k from each basket, then find their set intersection with candidate itemsets c_k
        subsets = set(map(frozenset, itertools.combinations(sub_list, k)))
        intersection = c_2.intersection(subsets)
        # Counting as above
        for item in intersection:
            cnt[item] += 1
    print("Length of counter: " + str(len(cnt)))
    # Filter out the infrequent elements (pruning)
    l_2 = set([item for item in cnt if cnt[item] >= support_part])
    print("Length after pruning infrequent:" + str(len(l_2)))

    return l_2


if __name__ == '__main__':
    time1 = time.time()
    sc = SparkContext(master="local[*]", appName="task1")
    sc.setLogLevel("ERROR")
    case_number = int(sys.argv[1])
    support = int(sys.argv[2])
    input_file = sys.argv[3]
    output_file = sys.argv[4]
    baskets = case_1(input_file)
    num_part = baskets.getNumPartitions()
    support_part = support // num_part

    baskets1 = baskets.mapPartitions(a_priori)
    print(baskets1.glom().collect())

