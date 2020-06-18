# Python3 Program to print BFS traversal
# from a given source vertex. BFS(int s)
# traverses vertices reachable from s.
from collections import defaultdict
import networkx as nx
import matplotlib.pyplot as plt


# This class represents a directed graph
# using adjacency list representation
class Graph:

    # Constructor
    def __init__(self):

        # default dictionary to store graph
        self.graph = defaultdict(list)

    # function to add an edge to graph
    def addEdge(self, u, v):
        self.graph[u].append(v)
        self.graph[v].append(u)
        # Function to print a BFS of graph

    def BFS(self, s):
        tree = defaultdict(dict)  # Use list for unweighted graph
        levels = defaultdict(int)

        # Mark all the vertices as not visited
        visited = defaultdict(bool)

        # Create a queue for BFS
        queue = []

        # Mark the source node as
        # visited and enqueue it
        queue.append(s)
        levels[s] = 0
        visited[s] = True
        while queue:

            # Dequeue a vertex from
            # queue and print it
            s = queue.pop(0)

            print(s, end=" ")

            # Get all adjacent vertices of the
            # dequeued vertex s. If a adjacent
            # has not been visited, then mark it
            # visited and enqueue it
            for i in self.graph[s]:
                if visited[i] == False:
                    queue.append(i)
                    tree[s][i] = 0
                    levels[i] = levels[s] + 1
                    visited[i] = True
                else:
                    # If visited, add edge only if they are at lower levels
                    if levels[i] > levels[s]:
                        tree[s][i] = 0
        return tree, levels


# Driver code

# Create a graph given in
# the above diagram
g = Graph()
g.addEdge(1, 2)
g.addEdge(1, 3)
g.addEdge(2, 3)
g.addEdge(2, 4)
g.addEdge(4, 5)
g.addEdge(4, 6)
g.addEdge(4, 7)
g.addEdge(5, 6)
g.addEdge(6, 7)

print("Following is Breadth First Traversal"
      " (starting from vertex 2)")
tree5, levels5 = g.BFS(5)
nxgraph = nx.Graph(tree5)
nx.draw(nxgraph, with_labels=True)
plt.show()
print()