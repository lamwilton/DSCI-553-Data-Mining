import task1
import random
import sys
from collections import defaultdict
import itertools


def minhash(table, a, b, num_business):
    """
    Minhash method
    :param table: Characteristic table with row = users
    :param a:
    :param b:
    :return: Minhash of one hash function
    """
    #table = table.value
    m = len(table)
    result = [(m + 10) for _ in range(num_business)]
    for row in range(len(table)):
        hashvalue = (a * row + b) % m
        for business_id in table[row]:
            if hashvalue < result[business_id]:
                result[business_id] = hashvalue
    return result


def jaccard(a, b):
    """
    Find jaccard of a pair of business
    :param pair: business num (int)
    :return: tuple of business_id and their Jaccard similarity
    """
    # Find back the business ids
    #business_a = businesslist[pair[0]]
    #business_b = businesslist[pair[1]]

    # Look up busi dict for the sets
    #a = busi_dict[business_a]
    #b = busi_dict[business_b]
    union = len(a.union(b))
    if union == 0:
        similarity = 0
    else:
        similarity = len(a.intersection(b)) / union
    return tuple((a, b, similarity))


def hash_func_generate(num_func):
    """
    Generate hash functions a and b
    :return: list of a and b pairs
    eg [[983, 294], [1777, 208], [557, 236], ...]
    """
    result = []
    a = random.sample(range(1000, sys.maxsize), num_func)
    b = random.sample(range(1000, sys.maxsize), num_func)
    for i in range(0, num_func):
        result.append([a[i], b[i]])
    return result


def lsh_signature(minhashes):
    """
    * Improved to O(n) runtime using defaultdict

    LSH with band size of 1 rows (r = 1)
    :param minhashes: 2d list of minhashes
    :return: Set of Candidate pairs
    eg {(241, 235), (3242 ,2352), ...}
    """
    result = set()
    minhash = minhashes[0]  # convert to 1d
    tally = defaultdict(list)
    for i, item in enumerate(minhash):
        tally[item].append(i)
    for item in tally.values():
        if len(item) > 1:
            result = result.union(set(itertools.combinations(item, 2)))
    return result


if __name__ == '__main__':
    print("Testing minhash======================================================")
    print(str(minhash([{0,3},{2},{1},{0,2,3},{2}], a=1, b=1, num_business=4)) + " Expected [1, 3, 0, 1]")
    print(str(minhash([{0,3},{2},{1},{0,2,3},{2}], a=3, b=1, num_business=4)) + " Expected [0, 2, 0, 0]")
    print(str(minhash([{0},{0,1},{0,1},{1},{0,1},{0},{1}], a=1, b=1, num_business=2)) + " Expected [1, 0]")
    print(str(minhash([{0},{0,1},{0,1},{1},{0,1},{0},{1}], a=1, b=4, num_business=2)) + " Expected [1, 0]")
    print()

    print("Testing hash function generator======================================================")
    print(hash_func_generate(num_func=40))
    print()

    print("Testing signature======================================================")
    print(task1.lsh_signature([[1, 0, 2, 4, 5, 3, 2, 1, 1], [2, 0, 2, 6, 1, 5, 4, 8, 2]]))
    print(task1.lsh_signature([[1, 0, 2, 4, 4, 3, 2, 1, 1], [2, 0, 2, 6, 6, 5, 4, 8, 2]]))
    print()

    print("Testing new signature======================================================")
    print(lsh_signature([[1, 0, 2, 4, 5, 3, 2, 1, 1]]))
    print()

    print("Testing Jaccard======================================================")
    print(jaccard({1, 2}, {1, 2, 4}))
    print()

