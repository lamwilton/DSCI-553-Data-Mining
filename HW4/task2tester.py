from collections import defaultdict
import networkx as nx
import matplotlib.pyplot as plt
import task2
from pyspark import SparkContext, SparkConf

print("Testing task 2.1 helper ====================================")

pairs_short = [(1, 2), (1, 3), (2, 3), (2, 4), (4, 5), (4, 6), (4, 7), (5, 6), (6, 7)]
graph_adj = defaultdict(list)
for item in pairs_short:
    graph_adj[item[0]].append(item[1])
    graph_adj[item[1]].append(item[0])  # Rev edges.
result = task2.bfs_tree(graph_adj, 5)
result.girvan_newman()

print(task2.betweenness_helper(graph_adj, 5))


print("Testing task 2.2 modularity ====================================")
print(task2.modularity(graph_adj, [{1,2,3,4,5,6,7}], 7))
print(task2.modularity(graph_adj, [{1,2,3},{4,5,6,7}], 7))
print(task2.modularity(graph_adj, [{1,3},{2},{4,5,6,7}], 7))

print("Testing task 2.1 betweenness ====================================")
conf = SparkConf()
conf.set("spark.driver.memory", "4g")
conf.set("spark.executor.memory", "4g")
conf.set("spark.master", "local[*]")
conf.set("spark.app.name", "task2")
conf.set("spark.driver.maxResultSize", "4g")
sc = SparkContext.getOrCreate(conf)
sc.setLogLevel("WARN")

nodes_rdd = sc.parallelize([1,2,3,4,5,6,7])
betweeness = nodes_rdd.flatMap(lambda x: task2.betweenness_helper(graph_adj, x))
sum_betweenness = betweeness.reduceByKey(lambda x, y: x + y)
final_result = sum_betweenness.map(lambda x: ((x[0][0], x[0][1]), x[1] / 2)) \
    .map(lambda x: (tuple(sorted([x[0][0], x[0][1]])), x[1])) \
    .sortBy(lambda x: (-x[1], x[0][0])) \
    .collect()
print(final_result)