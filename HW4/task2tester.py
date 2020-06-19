from collections import defaultdict
import networkx as nx
import matplotlib.pyplot as plt
import task2

pairs_short = [(1, 2), (1, 3), (2, 3), (2, 4), (4, 5), (4, 6), (4, 7), (5, 6), (6, 7)]
graph_adj = defaultdict(list)
for item in pairs_short:
    graph_adj[item[0]].append(item[1])
    graph_adj[item[1]].append(item[0])  # Rev edges.
tree, level, paths = task2.bfs_tree(graph_adj, 5)
task2.plot_graph(tree)
