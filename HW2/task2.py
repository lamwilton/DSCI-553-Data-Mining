from pyspark import SparkContext
from pyspark import StorageLevel
import sys
import time
from collections import Counter


def case_1(input_file):
    # Read csv, tokenize and remove header
    # Remove the u in front of string
    lines = sc.textFile(input_file).distinct() \
        .filter(lambda line: len(line) != 0) \
        .map(lambda x: (str(x.split(",")[0]), str(x.split(",")[1]))) \
        .filter(lambda x: x[0] != "user_id")
    baskets = lines.groupByKey()
    # Convert value list to set
    baskets1 = baskets.map(lambda x: (x[0], set(x[1].data))) \
        .filter(lambda x: len(x[1]) > case_number)  # TASK2
    return baskets1


def a_priori(iterator):
    # Copy the subset of baskets so I can reloop it many many times. Iterator only allows traversing once!
    baskets = [i[1] for i in iterator]
    l = []
    l.append(set())  # L_0, C_0, C_1 does not exist

    cnt = Counter()
    # Count frequent singletons using python counter
    for sub_list in baskets:
        for item in sub_list:
            cnt[item] += 1

    # Filter out the infrequent elements (pruning)
    support_part = float(support) * len(baskets) / baskets_count
    l.append(set([frozenset([item]) for item in cnt if cnt[item] >= support_part]))
    print("L1 number of elements: " + str(len(l[1])))

    # Following pseudocode of apriori on Wikipedia, with k more than 1
    k = 2
    while True:
        print("k = " + str(k))
        c = list(set([x.union(y) for x in l[k - 1] for y in l[k - 1] if x != y and len(x.union(y)) == k]))
        print("Number of Candidate k item sets: " + str(len(c)))
        cnt = Counter()
        xcount = 1
        for sub_list in baskets:
            # TODO: Fix this bottle neck.
            # Filter from the list of candidates, take if the item is in the subset of the particular basket
            items = list(filter(lambda x: x.issubset(sub_list), c))
            for item in items:
                cnt[item] += 1
            xcount += 1
            if xcount % 50 == 0:
                print(xcount)
                print(time.time() - time1)
        print("Length of counter: " + str(len(cnt)))

        # Filter out the infrequent elements (pruning)
        l.append(set([item for item in cnt if cnt[item] >= support_part]))
        print("Length after pruning infrequent: " + str(len(l[k])))
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
    intersection = []
    for item in itemsets:
        if item.issubset(a_basket):
            intersection.append(item)

    # Output as keyvalue pair
    result = [(item, 1) for item in intersection]
    return result


def format_output(input_list, maxsize):
    """
    Format the result correctly
    :param input_list:
    :param maxsize: Max number of items (k) in itemsets
    :return: result as list of strings sorted
    """
    result = [[] for _ in range(maxsize)]
    for item in input_list:
        result[len(item) - 1].append(item)
    result = list(map(sorted, result))
    result = list(map(str, result))

    result[0] = result[0].replace("\',)", "\')").replace("\'), ", "\'),").strip("[]")
    for i in range(1, maxsize):
        result[i] = result[i].replace("\'), ", "\'),").strip("[]")
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
    baskets = case_1(input_file)  # TASK2
    baskets = baskets.partitionBy(4)
    baskets_count = baskets.count()  # Total basket count

    num_part = baskets.getNumPartitions()

    aprioriresult = baskets.mapPartitions(a_priori).persist(StorageLevel.MEMORY_AND_DISK)  # Save Memory

    # Phase 1 Reduce: Just union the result from all partitions
    itemsets = aprioriresult.groupByKey().keys().persist()
    max_itemsets_size = itemsets.map(lambda x: len(x)).max()  # Get max length of candidate itemset so no need to generate subsets more than this
    itemsets_output = itemsets.map(lambda x: tuple(sorted(x))).collect()
    itemsets = itemsets.collect()
    print("Candidates: " + str(itemsets_output))

    # Phase 2 Map
    freq_itemsets = phase2counting(baskets)

    # Phase 2 reduce
    freq_itemsets1 = freq_itemsets.reduceByKey(lambda x, y: x + y)
    freq_itemsets2 = freq_itemsets1.filter(lambda x: x[1] >= support)  # Prune nonfrequent
    freq_itemsets3 = freq_itemsets2.keys().map(lambda x: tuple(sorted(x)))  # Get key only, sort and convert to tuple
    max_freq_itemsets = freq_itemsets3.map(lambda x: len(x)).max()
    final_result = freq_itemsets3.collect()
    print("Frequent Itemsets: " + str(final_result))


    # Write results
    final_candidates = format_output(itemsets_output, max_itemsets_size)
    final_freq = format_output(final_result, max_freq_itemsets)

    with open(output_file, "w") as file:
        file.write("Candidates:")
        file.write("\n")
        for line in final_candidates:
            file.write(line)
            file.write("\n")
            file.write("\n")
        file.write("Frequent Itemsets:")
        file.write("\n")
        for line in final_freq:
            file.write(line)
            file.write("\n")
            file.write("\n")

    # Ending
    totaltime = time.time() - time1
    print("Duration : " + str(totaltime))

