import task1

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


if __name__ == '__main__':
    print("Testing minhash======================================================")
    print(str(minhash([{0,3},{2},{1},{0,2,3},{2}], a=1, b=1, num_business=4)) + " Expected [1, 3, 0, 1]")
    print(str(minhash([{0,3},{2},{1},{0,2,3},{2}], a=3, b=1, num_business=4)) + " Expected [0, 2, 0, 0]")
    print(str(minhash([{0},{0,1},{0,1},{1},{0,1},{0},{1}], a=1, b=1, num_business=2)) + " Expected [1, 0]")
    print(str(minhash([{0},{0,1},{0,1},{1},{0,1},{0},{1}], a=1, b=4, num_business=2)) + " Expected [1, 0]")

    print("Testing signature======================================================")
    print(task1.lsh_signature([[1, 0, 2, 4, 5, 3, 2, 1, 1], [2, 0, 2, 6, 1, 5, 4, 8, 2]]))
    print(task1.lsh_signature([[1, 0, 2, 4, 4, 3, 2, 1, 1], [2, 0, 2, 6, 6, 5, 4, 8, 2]]))