from statistics import mean
from pyspark import SparkContext
from collections import defaultdict
import json
import sys


def no_spark():
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
    review_avg = dict(map(lambda x: (x[0], mean(x[1])), review.items()))

    # Read business.json and split categories into list of categories, then make a new dictionary
    with open(sys.argv[2], encoding='utf-8') as bus_file:
        for jsonobj in bus_file:
            bus_line = json.loads(jsonobj)
            if bus_line['categories'] is not None:
                bus_line['categories'] = bus_line['categories'].split(", ")
            business.update({bus_line['business_id']: bus_line['categories']})

    print()


if __name__ == '__main__':
    """
    
    """
    no_spark()