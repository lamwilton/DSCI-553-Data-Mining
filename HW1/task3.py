from pyspark import SparkContext
import json
import sys
import time


if __name__ == '__main__':
    time1 = time.time()
    sc = SparkContext(master="local[*]", appName="task3")
    sc.setLogLevel("ERROR")
    input_file = sys.argv[1]
    output = sys.argv[2]
    part_type = sys.argv[3]
    part = int(sys.argv[4])
    n = int(sys.argv[5])

    review_lines = sc.textFile(input_file)

    review_lines2 = review_lines.map(lambda x: (json.loads(x)['business_id'], 1))

    # After mapping, partition is lost because key is changed, so should partition again to increase speed
    if part_type == "customized":
        review_lines2 = review_lines2.partitionBy(part)

    # Count and filter for more than n reviews
    review_lines4 = review_lines2.reduceByKey(lambda x, y: x + y) \
        .filter(lambda x: x[1] > n)

    answer = dict()
    answer['n_partitions'] = review_lines2.getNumPartitions()
    answer['n_items'] = review_lines2.mapPartitions(lambda iter: [sum(1 for _ in iter)]).collect()
    num_reviews = review_lines4.collect()
    answer['result'] = [[str(elem[0]), elem[1]] for elem in num_reviews]

    # Write answers
    file = open(output, "w")
    file.write(json.dumps(answer))
    file.close()

    print("Final answers:")
    print(json.dumps(answer))
    print(time.time() - time1)
    exit()