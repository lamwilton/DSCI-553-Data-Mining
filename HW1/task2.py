from pyspark import SparkContext
import json
import sys

def no_spark():
    review = []
    business = []
    with open(sys.argv[1], encoding='utf-8') as review_file:
        for jsonobj in review_file:
            review_line = json.loads(jsonobj)
            review.append(review_line)
    with open(sys.argv[2], encoding='utf-8') as bus_file:
        for jsonobj in bus_file:
            bus_line = json.loads(jsonobj)
            business.append(bus_line)
    print()


if __name__ == '__main__':
    """
    
    """
    no_spark()