from collections import defaultdict
import networkx as nx
import matplotlib.pyplot as plt
import task2
from pyspark import SparkContext, SparkConf
import copy
import time

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
print(task2.modularity_calc(graph_adj, graph_adj, [{1,2,3,4,5,6,7}], 9))
print(task2.modularity_calc(graph_adj, graph_adj, [{1,2,3},{4,5,6,7}], 9))
print(task2.modularity_calc(graph_adj, graph_adj, [{1,3},{2},{4,5,6,7}], 9))

conf = SparkConf()
conf.set("spark.driver.memory", "4g")
conf.set("spark.executor.memory", "4g")
conf.set("spark.master", "local[*]")
conf.set("spark.app.name", "task2")
conf.set("spark.driver.maxResultSize", "4g")
sc = SparkContext.getOrCreate(conf)
sc.setLogLevel("WARN")

print("Testing task 2.2 modularity 2 ====================================")

def find_communities(graph_adj):
    """
    Task 2.2
    :param graph_adj: Adj dict
    :return: best communities
    """
    MAX_ITER = 60
    TIME_LIMIT = 200
    num_iter = 0
    communities_list = []
    modularity_list = []
    graph_adj_orig = copy.deepcopy(graph_adj)
    while num_iter < MAX_ITER:
        # Recompute betweenness together with the communities
        nodes_rdd = sc.parallelize([1,2,3,4,5,6,7]).coalesce(1)

        between_comm = nodes_rdd.map(lambda x: task2.betweenness_helper_2(graph_adj, x)).persist()
        betweeness = between_comm.flatMap(lambda x: x[0])
        sum_betweenness = betweeness.reduceByKey(lambda x, y: x + y)

        # If no edges left, or almost out of time, break loop
        if sum_betweenness.isEmpty():
            break

        # Find which edges has max betweenness, so they can be removed
        max_betweenness = sum_betweenness.sortBy(lambda x: -x[1]).first()[1]
        edges_removing = sum_betweenness.filter(lambda x: x[1] == max_betweenness)\
            .keys()\
            .collect()

        communities = between_comm.map(lambda x: frozenset(x[1])).distinct().collect()
        communities_list.append(communities)
        modularity = task2.modularity_calc(graph_adj_orig, graph_adj, communities_list[num_iter], m=len(pairs_short))
        modularity_list.append(modularity)
        print("Num iter:" + str(num_iter) + " Modularity: " + str(modularity))
        print("Num iter:" + str(num_iter) + " Modularity: " + str(modularity_list[num_iter]) + " # communities: " + str(len(communities_list[num_iter])))

        num_iter += 1

        # Remove the edges with highest betweenness. Do nothing if not found
        for edge in edges_removing:
            try:
                graph_adj[edge[0]].remove(edge[1])
                graph_adj[edge[1]].remove(edge[0])
            except ValueError:
                pass
    # Output best communities
    best_iter = modularity_list.index(max(modularity_list))
    result = communities_list[best_iter]
    print("Best results:")
    print("Num iter:" + str(best_iter) + " Modularity: " + str(modularity_list[best_iter]) + " # communities: " + str(
        len(communities_list[best_iter])))

    return result


result = find_communities(graph_adj)

print("Testing task 2.1 betweenness ====================================")
nodes_rdd = sc.parallelize([1,2,3,4,5,6,7]).coalesce(1)
betweeness = nodes_rdd.flatMap(lambda x: task2.betweenness_helper(graph_adj, x))
sum_betweenness = betweeness.reduceByKey(lambda x, y: x + y)
final_result = sum_betweenness.map(lambda x: ((x[0][0], x[0][1]), x[1] / 2)) \
    .map(lambda x: (tuple(sorted([x[0][0], x[0][1]])), x[1])) \
    .sortBy(lambda x: (-x[1], x[0][0])) \
    .collect()
print(final_result)