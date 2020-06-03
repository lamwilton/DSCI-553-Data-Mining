from pyspark import SparkContext
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


def case_2(input_file):
    # Read csv, tokenize and remove header
    # Must only be one partition for case 2 for small2.csv, or else will take forever
    lines = sc.textFile(input_file, minPartitions=1) \
        .map(lambda x: (x.split(",")[1], x.split(",")[0])) \
        .filter(lambda x: x[0] != "business_id")
    baskets = lines.groupByKey()
    # Convert value list to set
    baskets1 = baskets.map(lambda x: (x[0], set(x[1].data)))
    return baskets1


def a_priori(iterator):
    # Copy the subset of baskets so I can reloop it many many times. Iterator only allows traversing once!
    baskets = [i[1] for i in iterator]
    l, c = [], []
    l.append(set())  # L_0, C_0, C_1 does not exist
    c.append(set())
    c.append(set())

    cnt = Counter()
    # Count frequent singletons using python counter
    for sub_list in baskets:
        for item in sub_list:
            cnt[item] += 1

    # Filter out the infrequent elements (pruning)
    l.append(set([frozenset([item]) for item in cnt if cnt[item] >= support_part]))
    print()
    #print("L1 number of elements: " + str(len(l[1])))

    # Following pseudocode of apriori on Wikipedia, with k more than 1
    k = 2
    while True:
        #print("k = " + str(k))
        c.append(set([x.union(y) for x in l[k - 1] for y in l[k - 1] if x != y and len(x.union(y)) == k]))
        #print("Number of Candidate k item sets: " + str(len(c[k])))
        cnt = Counter()
        for sub_list in baskets:
            # Generate subsets of size k from each basket, then find their set intersection with candidate itemsets c_k
            subsets = set(map(frozenset, itertools.combinations(sub_list, k)))
            intersection = c[k].intersection(subsets)
            # Counting as above
            for item in intersection:
                cnt[item] += 1
        #print("Length of counter: " + str(len(cnt)))

        # Filter out the infrequent elements (pruning)
        l.append(set([item for item in cnt if cnt[item] >= support_part]))
        #print("Length after pruning infrequent: " + str(len(l[k])))
        if len(l[k]) == 0:
            break
        k += 1

    # Collect all frequent itemsets and union for all k's
    result = set()
    for i in range(1, k):
        result = result.union(l[i])

    # Output final results in the form of (F, 1)
    final_result = [(item, 1) for item in result]
    return final_result


def phase2counting(baskets):
    baskets1 = baskets.values()
    baskets2 = baskets1.flatMap(countinghelper)
    return baskets2


def countinghelper(a_basket):
    subsets = set()
    # Generate all combinations from the basket
    for k in range(1, max_itemsets_size + 1):
        subsets = subsets.union(set(map(frozenset, itertools.combinations(a_basket, k))))
    #print(subsets)

    # Intersect it with the candidate itemsets
    intersection = subsets.intersection(itemsets)

    # Output as keyvalue pair
    result = [(item, 1) for item in intersection]
    return result


if __name__ == '__main__':
    time1 = time.time()
    sc = SparkContext(master="local[*]", appName="task1")
    sc.setLogLevel("ERROR")
    case_number = int(sys.argv[1])
    support = int(sys.argv[2])
    input_file = sys.argv[3]
    output_file = sys.argv[4]

    # Phase 1 Map
    if case_number == 1:
        baskets = case_1(input_file)
    else:
        baskets = case_2(input_file)
    num_part = baskets.getNumPartitions()
    support_part = support // num_part

    aprioriresult = baskets.mapPartitions(a_priori)

    # Phase 1 Reduce: Just union the result from all partitions
    itemsets = aprioriresult.groupByKey().keys()
    max_itemsets_size = itemsets.map(lambda x: len(x)).max()  # Get max length of candidate itemset so no need to generate subsets more than this
    itemsets_output = itemsets.map(lambda x: tuple(sorted(x))).collect()
    itemsets = itemsets.collect()
    print()
    print("Candidates: " + str(itemsets_output))

    # Phase 2 Map
    freq_itemsets = phase2counting(baskets)

    # Phase 2 reduce
    freq_itemsets1 = freq_itemsets.reduceByKey(lambda x, y: x + y)
    freq_itemsets2 = freq_itemsets1.filter(lambda x: x[1] >= support)  # Prune nonfrequent
    freq_itemsets3 = freq_itemsets2.keys().map(lambda x: tuple(sorted(x)))  # Get key only, sort and convert to tuple
    final_result = freq_itemsets3.collect()
    print()
    print("Frequent Itemsets: " + str(final_result))

    # Ending
    totaltime = time.time() - time1
    print("Duration : " + str(totaltime))

