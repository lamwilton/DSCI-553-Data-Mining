from pyspark import SparkContext
import json
import sys


if __name__ == '__main__':
    sc = SparkContext(master="local[*]", appName="task2")
    sc.setLogLevel("WARN")
    business_lines = sc.textFile("business.json")
    business_list = business_lines.map(lambda s: (json.loads(s)['business_id'], json.loads(s)['state'])) \
        .filter(lambda x: x[1] == 'NV')

    review_lines = sc.textFile("review.json")
    result = review_lines.map(lambda s: (json.loads(s)['business_id'], json.loads(s)['user_id']))
    result1 = result.join(business_list) \
        .map(lambda x: (x[0], x[1][0])) \
        .collect()

    with open("output_file.csv", "w") as file:
        file.write("user_id,business_id")
        file.write("\n")
        for line in result1:
            file.write(str(line).strip("()").replace("\'", "").replace(", ", ","))
            file.write("\n")
