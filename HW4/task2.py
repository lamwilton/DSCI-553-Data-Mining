from pyspark import SparkContext, SparkConf
import sys
import time
from itertools import combinations
import os
from collections import defaultdict, deque

import networkx as nx
import matplotlib.pyplot as plt


def corated_helper(user_reviews_dict, a, b):
    """
    Check if user pair has more than 7 corated businesses
    :param user_reviews_dict: Tuple of user reviews
    :param a: User A's number
    :param b: User B's number
    :return: True if corated users >= 7
    """
    if len(user_reviews_dict[a].intersection(user_reviews_dict[b])) >= filter_threshold:
        return True
    return False


def graph_construct():
    """
    Constructing graph from candidate pairs as usual
    Do not use dictionaries here. It give different results

    Reviews count = 38648
    Business count (before filtering) = 9947
    Users count (before filtering) = 3374
    Users count (after filtering, vertices) = 222
    Edges count = 498 (x2 for reverse)
    :return: Users (Vertices) and Candidate pairs (Edges)
    """
    lines = sc.textFile(input_file_path).distinct()
    header = lines.first()
    reviews_long = lines.filter(lambda line: len(line) != 0) \
        .filter(lambda line: line != header) \
        .map(lambda x: (str(x.split(",")[0]), str(x.split(",")[1]))) \
        .persist()

    reviews = reviews_long.map(lambda x: (x[0], x[1]))\
        .persist()
    reviews_long.unpersist()

    baskets_pre = reviews.groupByKey()
    # Convert value list to set
    baskets = baskets_pre.map(lambda x: (x[0], set(x[1].data))) \
        .filter(lambda x: len(x[1]) >= filter_threshold)  # filter qualified users more than or equal 7

    # Output users as a dictionary, index = user number
    # eg {1: {513, 515, 4, 517, 519, 2055...}, {2: {6160, 3104, 556, ...}, ...}
    user_reviews_dict = baskets.sortByKey().collectAsMap()

    # Generate all pairs of users, dont know why the hell cartesian method aint work here
    users = baskets.map(lambda x: x[0])\
        .sortBy(lambda x: x)\
        .collect()
    all_pairs = sc.parallelize(list(combinations(users, 2)))

    # Filter only corated businesses >= 7
    # eg [(0, 10), (0, 14), (0, 16), (0, 20), (0, 28), (0, 30), (0, 32), ...]
    candidate_pairs_pre = all_pairs.filter(lambda x: corated_helper(user_reviews_dict, x[0], x[1])).persist()
    candidate_pairs_pre_2 = candidate_pairs_pre.map(lambda x: (x[1], x[0]))
    candidate_pairs = candidate_pairs_pre.union(candidate_pairs_pre_2).collect()
    candidate_users = candidate_pairs_pre.flatMap(lambda x: [x[0], x[1]]).distinct().collect()
    return candidate_users, candidate_pairs


def convert_short():
    """
    Hash user ids pairs to integers
    :return: dictionaries and hashed pairs
    """
    users_inv = tuple(candidate_users)
    users_dict = defaultdict(int)
    for i in range(len(users_inv)):
        users_dict[users_inv[i]] = i
    pairs_short = list(map(lambda x: (users_dict[x[0]], users_dict[x[1]]), candidate_pairs))
    return users_dict, users_inv, pairs_short


class Tree:
    """
    Class for BFS tree with required information
    Number of nodes in main tree: 190
    """
    def __init__(self):
        self.tree = defaultdict(dict)  # Adjacency dict, Use list for unweighted graph, {5: {4: 0, 6: 0}, 4: {2: 0, 7: 0}, 6: {7: 0}, 2: {1: 0, 3: 0}})
        self.level = defaultdict(int)  # Track what the level of the nodes are, {5: 0, 4: 1, 6: 1, 2: 2, 7: 2, 1: 3, 3: 3})
        self.levelset = defaultdict(set)  # Save set of nodes according to levels (inverse levels), {0: {5}, 1: {4, 6}, 2: {2, 7}, 3: {1, 3}})
        self.paths = defaultdict(int)  # Number of shortest paths, {5: 1, 4: 1, 6: 1, 2: 1, 7: 2, 1: 1, 3: 1})
        self.parents = defaultdict(set)  # Keep track of parents of each node, {4: {5}, 6: {5}, 2: {4}, 7: {4, 6}, 1: {2}, 3: {2}})
        self.credits = defaultdict(float)  # For storing credits of each node, {5: 1, 4: 1, 6: 1, 2: 1, 7: 1, 1: 1, 3: 1})

    def girvan_newman(self):
        """
        Girvan newman
        eg defaultdict(<class 'dict'>, {5: {4: 4.5, 6: 1.5}, 4: {2: 3.0, 7: 0.5}, 6: {7: 0.5}, 2: {1: 1.0, 3: 1.0}})
        :return:
        """
        for i_level in reversed(range(1, len(self.levelset))):
            for node in self.levelset[i_level]:
                # Add children edges credits
                if node in self.tree:
                    for edge in self.tree[node]:
                        self.credits[node] += self.tree[node][edge]
                # Distribute credits to parents
                for parent in self.parents[node]:
                    if self.paths[node] == 0:
                        self.tree[parent][node] = 0
                    else:
                        self.tree[parent][node] = self.credits[node] * self.paths[parent] / self.paths[node]
        return


def bfs_tree(graph, start_node):
    """
    Do the BFS tree
    :param graph: Adj dict
    :param start_node: Starting node
    :return: Result as a new tree object
    """
    visited = defaultdict(float)  # Track if node is visited
    dist = defaultdict(lambda: sys.maxsize)  # Length of shortest paths
    result = Tree()

    queue = deque()
    queue.append(start_node)
    result.level[start_node] = 0
    result.levelset[0].add(start_node)
    visited[start_node] = 1.0
    dist[start_node] = 0
    result.paths[start_node] = 1
    while queue:  # While queue is not empty
        s = queue.popleft()
        for t in graph[s]:
            if not visited[t]:
                queue.append(t)
                result.tree[s][t] = 0  # Add edge to the tree with weight 0
                result.level[t] = result.level[s] + 1
                result.levelset[result.level[t]].add(t)
                visited[t] = 1.0
                result.parents[t].add(s)  # Keep track of the parents

            else:
                # If visited, add edge only if they are at lower levels
                if result.level[t] > result.level[s]:
                    result.tree[s][t] = 0
                    result.parents[t].add(s)

            # Keeping track of the number of shortest paths
            if dist[t] > dist[s] + 1:
                dist[t] = dist[s] + 1
                result.paths[t] = result.paths[s]

            # Add shortest paths if found new ones
            elif dist[t] == dist[s] + 1:
                result.paths[t] += result.paths[s]

    # Record the list of nodes, each node starts with 1 credit
    result.credits = visited
    return result


def betweenness_helper(graph_adj, x):
    """
    Helper to parallelize girvan newman
    :param graph_adj: Adj dict of graph
    :param x: starting node
    :return: edges and weights as tuples
    eg [((4, 5), 4.5), ((5, 6), 1.5), ((2, 4), 3.0), ((4, 7), 0.5), ((6, 7), 0.5), ((1, 2), 1.0), ((2, 3), 1.0)]
    """
    tree_obj = bfs_tree(graph_adj, x)
    tree_obj.girvan_newman()
    result = []
    for x, subdict in tree_obj.tree.items():
        for y, weight in subdict.items():
            result.append((tuple(sorted([x, y])), weight))
    return result


def plot_graph(graph):
    """
    Graph visualization (Remove when submit)
    :param graph: Adj list
    :return:
    """
    # plt.rcParams["figure.figsize"] = (40, 30)
    nxgraph = nx.DiGraph(graph)
    nx.draw_planar(nxgraph, with_labels=True)
    plt.savefig("graphviz.png")
    plt.show()


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
    sc.setLogLevel("WARN")
    filter_threshold = int(sys.argv[1])
    input_file_path = sys.argv[2]
    betweenness_output_file_path = sys.argv[3]
    community_output_file_path = sys.argv[4]

    # ========================================== Graph Construction ==========================================
    candidate_users, candidate_pairs = graph_construct()
    users_dict, users_inv, pairs_short = convert_short()

    # Convert to adjacency list
    graph_adj = defaultdict(list)
    for item in pairs_short:
        graph_adj[item[0]].append(item[1])

    totaltime = time.time() - time1
    print("Duration Graph Construction: " + str(totaltime))

    # ========================================== BFS ==========================================
    nodes_rdd = sc.parallelize(users_dict.values())
    betweeness = nodes_rdd.flatMap(lambda x: betweenness_helper(graph_adj, x))
    sum_betweenness = betweeness.reduceByKey(lambda x, y: x + y)
    final_result = sum_betweenness.map(lambda x: ((users_inv[x[0][0]], users_inv[x[0][1]]), x[1]))\
        .map(lambda x: (tuple(sorted([x[0][0], x[0][1]])), x[1]))\
        .sortBy(lambda x: (-x[1], x[0][0]))\
        .collect()

    # ======================================= Write results =======================================
    with open(betweenness_output_file_path, "w") as file:
        for item in final_result:
            line = str(item)[1:-1]
            file.write(line)
            file.write("\n")

    totaltime = time.time() - time1
    print("Duration: " + str(totaltime))
