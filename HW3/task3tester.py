import task3train

# Page 19 of the slides
print("Testing Pearson ======================================================")
data = [{1: 4, 3: 5, 4: 5}, {1: 4, 2: 2, 3: 1}, {1: 3, 3: 2, 4: 4}, {1: 4, 2: 4}, {1: 2, 2: 1, 3: 3, 4: 5}]
print(task3train.pearson_helper(data, 0, 4, 14/3, 10/3))

print("Testing average reader ======================================================")
task3train.reading_average("business_avg.json", "user_avg.json")