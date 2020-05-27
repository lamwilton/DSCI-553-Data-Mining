from pyspark import SparkContext
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


def part_c_d(lines, m):
    # Part C
    users = lines.map(lambda s: (json.loads(s)['user_id'], 1))
    users_group = users.groupByKey()
    users_count = users_group.count()

    # Part D
    top_users = users.reduceByKey(lambda a, b: a + b)
    top_users_col = top_users.collect()
    top_users_col = sorted(top_users_col, key=lambda x: -x[1])

    top_m = top_users_col[0:m]
    top_m_list = [[str(elem[0]), elem[1]] for elem in top_m]  # Convert to list format from tuple
    print("The number of unique users is : " + str(users_count))
    print("Top " + str(m) + " users: " + str(top_m_list[0:10]))
    return users_count, top_m_list


def part_e(lines, n):
    stopwords_file = sys.argv[3]
    with open(stopwords_file) as file:
        stopwords = file.read().splitlines()

    # process each input line by first loading, then remove punctuation/to lower case, finally remove stopwords
    reviews = lines.map(lambda line: json.loads(line)['text'])\
        .map(lambda line: line.translate({ord(i): None for i in '([,.!?:;])'}).lower())\
        .map(lambda line: " ".join([word for word in line.split() if word not in stopwords]))

    # Regular spark Word count https://spark.apache.org/examples.html
    counts = reviews.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    count_result = counts.collect()

    # Remove the empty string entry for edge case
    for i in range(len(count_result) - 1):
        if count_result[i][0] == "":
            del count_result[i]
            break

    count_result = sorted(count_result, key=lambda x: -x[1])
    top_n = count_result[0:n]
    top_n_list = [str(elem[0]) for elem in top_n]  # Convert to list format from tuple
    print("Top " + str(n) + "list is : " + str(top_n_list))
    return top_n_list


if __name__ == '__main__':
    """
    Run command: spark-submit task1.py $ASNLIB/public_data/review.json output.txt $ASNLIB/public_data/stopwords 2011 10 10 
    sys.argv: 1 = input_file 2 = output_file, 3 = stopwords, 4 = y, 5 = m, 6 = n
    """
    sc = SparkContext(master="local[6]", appName="task1")
    sc.setLogLevel("WARN")
    lines = sc.textFile(sys.argv[1])
    answer = dict()
    answer['A'] = part_a(lines)
    y = str(sys.argv[4])
    answer['B'] = part_b(lines, y=y)
    answer['C'], answer['D'] = part_c_d(lines, m=int(sys.argv[5]))
    answer['E'] = part_e(lines, n=int(sys.argv[6]))

    # Write answers to file
    file = open(sys.argv[2], "w")
    file.write(json.dumps(answer))
    file.close()

    print("Final answers:")
    print(json.dumps(answer))
    exit()
