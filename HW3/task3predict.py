from pyspark import SparkContext, SparkConf
import sys
import time
import json
import math


if __name__ == '__main__':

    # ========================================== Initializing ==========================================
    time1 = time.time()
    conf = SparkConf()
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.master", "local[*]")
    conf.set("spark.app.name", "task1")
    conf.set("spark.driver.maxResultSize", "4g")
    sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("ERROR")
    input_file = sys.argv[1]
    test_file = sys.argv[2]
    model_file = sys.argv[3]
    output_file = sys.argv[4]
    case = sys.argv[5]

    # ============================ Read file and Initialize ==========================
    train_lines = sc.textFile(input_file).distinct()
    test_lines = sc.textFile(test_file).distinct()