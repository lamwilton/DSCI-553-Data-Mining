from pyspark import SparkContext, SparkConf
import json
import sys


def part_a(lines):
    total_reviews = lines.count()
    print("The total number of reviews is " + str(total_reviews))
    return total_reviews


def part_b(lines, y):
    year_filter = lines.filter(lambda s: y in json.loads(s)['date'])
    year_count = year_filter.count()
    print("The number of reviews in " + y + " is " + str(year_count))
    return year_count


def part_c(lines):
    users = lines.map(lambda s: (json.loads(s)['user_id'], 1))
    users_group = users.groupByKey()
    users_count = users_group.count()
    print("The number of unique users is : " + str(users_count))
    return users_count


if __name__ == '__main__':
    conf = SparkConf().setAppName("task1").setMaster("local[6]")
    sc = SparkContext(conf=conf)
    lines = sc.textFile(sys.argv[1])
    answer = dict()
    answer['A'] = part_a(lines)
    y = str(sys.argv[4])
    answer['B'] = part_b(lines, y=y)
    answer['C'] = part_c(lines)
    print("Final answers:")
    print(answer)
    exit()