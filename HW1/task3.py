from pyspark import SparkContext
from pyspark import SQLContext
import json
import sys

if __name__ == '__main__':
    sc = SparkContext(master="local[*]", appName="task3")
    sc.setLogLevel("WARN")
    review_lines = sc.textFile(sys.argv[1])
