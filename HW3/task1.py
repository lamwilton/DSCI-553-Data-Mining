from pyspark import SparkContext, SparkConf
import sys
import time
import json


def initial_steps(input_file):
    """
    Read file and generate charactierstic table
    :param input_file:
    :return:
    """
    def tablehelper(x):
        result = []
        for user in userlist1.value:
            if user in x[1]:
                result.append(1)
            else:
                result.append(0)
        return tuple((x[0], tuple(result)))

    lines = sc.textFile(input_file).distinct().persist()
    lines1 = lines.filter(lambda line: len(line) != 0) \
        .map(lambda s: (json.loads(s)['business_id'], (json.loads(s)['user_id'], json.loads(s)['stars']))) \
        .filter(lambda x: x[0] is not None and x[1] is not None and x[0] != "" and x[1] != "")
    businesses = lines1.groupByKey()
    businesses1 = businesses.map(lambda x: (x[0], dict(x[1].data)))
    users = lines.filter(lambda line: len(line) != 0) \
        .map(lambda s: (json.loads(s)['user_id'])) \
        .filter(lambda x: x[0] is not None and x[1] is not None and x[0] != "" and x[1] != "") \
        .distinct()
    userlist = tuple(users.collect())
    userlist1 = sc.broadcast(userlist)
    table = businesses1.map(lambda x: tablehelper(x))
    table1 = table.collect()

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