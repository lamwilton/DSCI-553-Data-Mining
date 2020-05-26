from pyspark import SparkContext
import json
import sys


def no_spark():
    """
    No spark version
    :return:
    """
    review = dict()
    business = dict()
    # Read review and create list of dictionary
    with open(sys.argv[1], encoding='utf-8') as review_file:
        for jsonobj in review_file:
            review_line = json.loads(jsonobj)
            # For each row, check if entry already exists. If it is, append new score to the list of scores of a business
            if review.get(review_line['business_id']) is None:
                review.update({review_line['business_id']: [review_line['stars']]})
            else:
                current_stars = review.get(review_line['business_id'])
                current_stars.append(review_line['stars'])
                review.update({review_line['business_id']: current_stars})

    # Compute average score for each business
    review_avg = dict(map(lambda x: (x[0], sum(x[1]) / len(x[1])), review.items()))

    # Read business.json and split categories into list of categories, then make a new dictionary
    with open(sys.argv[2], encoding='utf-8') as bus_file:
        for jsonobj in bus_file:
            bus_line = json.loads(jsonobj)
            if bus_line['categories'] is not None:
                bus_line['categories'] = bus_line['categories'].split(", ")
            business.update({bus_line['business_id']: bus_line['categories']})

    # Merge the two dictionaries
    merged = list(map(lambda x: (business.get(x[0]), x[1]), review_avg.items()))

    # For each entry, iterate each category and update the average scores
    result = dict()
    for entry in merged:
        if entry[0] is not None:
            for cat in entry[0]:
                if result.get(cat) is None:
                    result.update({cat: [entry[1]]})
                else:
                    current = result.get(cat)
                    current.append(entry[1])
                    result.update({cat: current})

    # Final results
    n = int(sys.argv[5])
    result_avg = list(map(lambda x: (x[0], sum(x[1]) / len(x[1])), result.items()))
    result_avg = sorted(result_avg, key=lambda x: (-x[1], x[0]))
    answer = json.dumps({"result": sorted([[str(elem[0]), elem[1]] for elem in result_avg[0:n]])})
    print("No spark: " + answer)
    return answer


def spark_average(rdd):
    """
    Method to compute average of values by key of an rdd of key value pairs
    Too damn annoying to repeat this over and over
    x and y are tuples (key, value)
    :param rdd: Input rdd
    :return: Average
    """
    return rdd.map(lambda x: (x[0], (x[1], 1))) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .mapValues(lambda x: x[0] / float(x[1]))


def use_spark():
    """
    Use spark version
    :return:
    """
    sc = SparkContext(master="local[6]", appName="task2")
    sc.setLogLevel("WARN")
    review_lines = sc.textFile(sys.argv[1])

    # Read review file, Compute average score for each business
    review_avg = spark_average(review_lines.map(lambda s: (json.loads(s)['business_id'], json.loads(s)['stars'])))

    # Read business file, get categories of each business
    business_lines = sc.textFile(sys.argv[2])
    business_cat = business_lines.filter(lambda s: json.loads(s)['categories'] is not None) \
        .map(lambda s: (json.loads(s)['business_id'], json.loads(s)['categories'].split(", ")))

    cat_scores = review_avg.join(business_cat).map(lambda x: (x[1][1], x[1][0]))  # (key=cat list, value=score)
    # Split the key value pairs according to the cat list
    # Reference: https://stackoverflow.com/questions/55562636/spark-how-to-split-key-value-list-into-key-value-pairs
    cat_scores = cat_scores.flatMap(lambda x: [(value, x[1]) for value in x[0]])  # (key=cat, value=score)
    cat_scores = spark_average(cat_scores).collect()

    cat_scores = sorted(cat_scores, key=lambda x: (-x[1], x[0]))
    n = int(sys.argv[5])
    answer = json.dumps({"result": [[str(elem[0]), elem[1]] for elem in cat_scores[0:n]]})
    print("With spark: " + answer)
    return answer


if __name__ == '__main__':
    """
    Task 2
    """
    if sys.argv[4] == "no_spark":
        answer = no_spark()
    else:
        answer = use_spark()

    # Write answers to file
    file = open(sys.argv[3], "w")
    file.write(answer)
    file.close()

    print("Final answers:")
    print(answer)
    exit()

