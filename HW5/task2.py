from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import sys
import time
from datetime import datetime
import json
import binascii
import math


def hash_func_generate(num_func):
    """
    Changed random a and b to deterministic numbers (40 each), because results are not consistent enough

    Generate hash functions a and b
    :return: list of a and b pairs
    eg [[983, 294], [1777, 208], [557, 236], ...]
    """
    result = []
    a = [2804719937407207392, 534994712587407555, 1601909024677598044, 2089490284880646279, 8414088687051772023, 3644236982176003868, 466687142266060766, 4672373026195998284, 3764267651550914304, 6533137187227616905, 4951831455943176399, 4672111685205947033, 7376139691444783240, 3463676941055429533, 6253034855854182492, 9018898581601740230, 7599784503649697838, 5111266208362289839, 173317686022300016, 9152903329779437301, 1379314340011820716, 1221797632932456767, 1403084241832846237, 7626424395439277508, 4926548870419737199, 5398409104409432359, 5356622762787712675, 8239362391813211761, 1864230895160475954, 3397262191295676582, 6254363775058371, 1526220602689429395, 1204669664190514764, 644074909920937317, 8039626307304172430, 1898099221580113242, 2206020038482243946, 6253186566693881898, 4568431704251661209, 2638816304279928943]

    b = [5725800687130655672, 4203308029349429530, 4152582977204863414, 8555727663436542088, 78405877465159341, 3528192278960117267, 8445100161981955964, 3463033447047251189, 5596846033166394413, 5179524996342163489, 3775015537456167604, 645643607844972752, 3070766084859089582, 3192526463038951724, 2493761628734736590, 3857160150556736379, 1964339335460775439, 6140715221175129114, 416415872079097186, 4836197209032996625, 1037112848981039238, 4891542093360440671, 7247618140793086033, 6705978494322432284, 4304242550928511474, 3059461750904296009, 8728352405985964029, 5523265721431783379, 3116366252289719943, 2181171545728392844, 8237352502378141560, 8774881536305656744, 5506519824345470923, 8926420760314345034, 8186260785229177904, 7340267265213638946, 3698124102192423578, 8638665317802841360, 1023624725347704865, 6574969859210812490]

    for i in range(0, num_func):
        result.append([a[i], b[i]])
    return result


def convert_str(s):
    """
    Convert string to int, also do the hashing using multiple hashes
    :param s: String
    :return: list of tuples, key = number of hash function
    eg [(0, 574), (1, 457), (2, 547), (3, 456), (4, 567), (5, 4567), (6, 868), (7, 565), (8, 564), (9, 457), (10, 456), (11, 754)]
    """
    p = 479001599
    m = 1024  # 10 bits
    if s == "":
        return 2387462387782346
    num = int(binascii.hexlify(s.encode('utf8')), 16)
    result = []
    for i in range(len(ab_pairs)):
        hash_result = ((ab_pairs[i][0] * num + ab_pairs[i][1]) % p) % m
        result.append(tuple((i, hash_result)))
    return result


def count_zeros(num):
    """
    Count number of trailing zeros of int as binary
    :param num:
    :return: number of zeros
    """
    result = 0
    if num == 0:
        return 0
    while True:
        if num & 1 == 0:
            result += 1
            num = num >> 1
        else:
            return result


def reduce_helper(x, y):
    """
    Find max length of trailing zeros out of all hashes for reduce
    :param x:
    :param y:
    :return: Length of trailing zero ** 2
    """
    x_zeros = count_zeros(x)
    y_zeros = count_zeros(y)
    max_zeros = max(x_zeros, y_zeros)
    result = 2 ** max_zeros
    return result


def rdd_helper(rdd):
    """
    Compute result for each rdd and write to file
    :param rdd: Inputing rdd of 12 hashes of each city
    :return:
    """
    truth = rdd.distinct().count()

    # Estimate the number of unique elements using multiple hash functions
    # Reduce according to the number of hash function, then drop key and sort by values
    # eg [512, 256, 512, 256, 64, 512, 64, 256, 64, 32, 512, 512]
    estimate = rdd.flatMap(lambda x: convert_str(x)) \
        .reduceByKey(lambda x, y: reduce_helper(x, y)) \
        .values() \
        .collect()

    # Take average of log2(estimate), then take power of 2
    # Acceptable range is (0.5gt, 1.5gt)
    estimate_final = 2 ** (sum(list(map(lambda x: math.log2(x), estimate))) / len(estimate))

    time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    result = time + "," + str(truth) + "," + str(estimate_final)

    print("Estimates: " + str(estimate))
    print("Ratio: " + str(estimate_final / truth))
    print(result)

    with open(output_file_name, "a+") as file:
        file.write(str(result))
        file.write("\n")
    return


if __name__ == '__main__':
    # ========================================== Initializing ==========================================
    time1 = time.time()
    conf = SparkConf()
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.master", "local[*]")
    conf.set("spark.app.name", "task2")
    conf.set("spark.driver.maxResultSize", "4g")

    sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 5)
    port_num = int(sys.argv[1])
    output_file_name = sys.argv[2]
    NUM_HASH = 40

    # Write header for output file
    with open(output_file_name, "w") as file:
        file.write("Time,Ground Truth,Estimation")
        file.write("\n")

    # ========================================== Main ==========================================
    lines = ssc.socketTextStream("localhost", port_num)
    cities_stream = lines.window(30, 10) \
        .filter(lambda line: len(line) != 0) \
        .map(lambda line: (json.loads(line))) \
        .map(lambda x: (x['city'])) \
        .filter(lambda x: x is not None)

    # Initialize hash functions
    ab_pairs = hash_func_generate(num_func=NUM_HASH)
    cities_stream.foreachRDD(lambda rdd: rdd_helper(rdd))

    ssc.start()
    ssc.awaitTermination()
