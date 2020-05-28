from pyspark import SparkContext
import json
import sys


def default(review_lines, n):
    num_reviews = review_lines.map(lambda x: (json.loads(x)['business_id'], 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda x: -x[1])
    parts = num_reviews.getNumPartitions()
    items = num_reviews.mapPartitions(lambda iter: [sum(1 for _ in iter)]).collect()
    num_reviews = num_reviews.collect()
    result = [[str(elem[0]), elem[1]] for elem in num_reviews[0:n]]
    return result, parts, items


def custom(review_lines, n, part):
    num_reviews = review_lines.map(lambda x: (json.loads(x)['business_id'], 1)) \
        .repartition(part) \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda x: -x[1]).take(n)
    parts = num_reviews.getNumPartitions()
    items = num_reviews.mapPartitions(lambda iter: [sum(1 for _ in iter)]).collect()
    num_reviews = num_reviews.collect()
    result = [[str(elem[0]), elem[1]] for elem in num_reviews]
    return result, parts, items


if __name__ == '__main__':
    sc = SparkContext(master="local[*]", appName="task3")
    sc.setLogLevel("ERROR")
    review_lines = sc.textFile(sys.argv[1])
    part = sys.argv[4]
    review_lines = review_lines.repartition(part)
    n = int(sys.argv[5])
    answer = dict()
    answer['result'], answer['n_partitions'], answer['n_items'] = default(review_lines, n, part)
    print("Final answer:")
    print(json.dumps(answer))