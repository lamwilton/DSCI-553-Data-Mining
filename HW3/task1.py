from pyspark import SparkContext, SparkConf
import sys
import time
import json
from collections import defaultdict


def initial_steps(input_file):
    """
    Read file and generate charactierstic table
    :param input_file:
    :return:
    """
    def tablehelper(x):
        result = []
        for business in businesslist1.value:
            if business in x[1]:
                result.append(1)
            else:
                result.append(0)
        return tuple((x[0], tuple(result)))

    lines = sc.textFile(input_file).distinct().persist()
    lines1 = lines.filter(lambda line: len(line) != 0) \
        .map(lambda s: (json.loads(s)['user_id'], json.loads(s)['business_id'])) \
        .filter(lambda x: x[0] is not None and x[1] is not None and x[0] != "" and x[1] != "")
    users = lines1.groupByKey()
    users1 = users.map(lambda x: (x[0], set(x[1].data)))
    businesses = lines.filter(lambda line: len(line) != 0) \
        .map(lambda s: json.loads(s)['business_id']) \
        .filter(lambda x: x is not None and x != "") \
        .distinct()
    businesslist = tuple(businesses.collect())
    businesslist1 = sc.broadcast(businesslist)

    # Inverse index for businesses
    businessinv = defaultdict(int)
    for i in range(len(businesslist)):
        businessinv[businesslist[i]] = i

    userlist = users.keys().collect()

    table = users1.map(lambda x: tablehelper(x))
    table1 = table.collect()
    totaltime = time.time() - time1
    print("Duration: " + str(totaltime))
    print()


if __name__ == '__main__':
    time1 = time.time()
    conf = SparkConf()
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.master", "local[*]")
    conf.set("spark.app.name", "task1")
    sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("ERROR")
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    initial_steps(input_file)


    # Ending
    totaltime = time.time() - time1
    print("Duration: " + str(totaltime))