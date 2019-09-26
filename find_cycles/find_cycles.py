# define graph & cycles
cycles = []
# taken from the pairs of transactions in bitfinance (bfnx) that had at least
# 10,000 transactions over the 3 months from June 16th, 2019 to September 20th, 2019.
# this graph consists of 51 nodes (currencies) and 98 edges (transaction pairs).
graph = [(35, 20),
 (1, 24),
 (15, 20),
 (25, 20),
 (7, 20),
 (24, 30),
 (43, 24),
 (5, 20),
 (8, 18),
 (34, 20),
 (22, 20),
 (24, 20),
 (8, 20),
 (8, 38),
 (0, 20),
 (48, 24),
 (28, 26),
 (9, 24),
 (44, 8),
 (49, 8),
 (16, 20),
 (22, 24),
 (10, 20),
 (19, 20),
 (36, 24),
 (35, 24),
 (47, 20),
 (24, 18),
 (31, 24),
 (11, 24),
 (28, 24),
 (23, 8),
 (37, 18),
 (33, 20),
 (32, 20),
 (18, 20),
 (28, 38),
 (23, 20),
 (8, 30),
 (39, 24),
 (8, 26),
 (45, 18),
 (5, 8),
 (24, 38),
 (17, 20),
 (7, 24),
 (28, 30),
 (2, 18),
 (33, 24),
 (11, 8),
 (12, 20),
 (3, 24),
 (2, 20),
 (42, 20),
 (44, 20),
 (41, 20),
 (3, 20),
 (43, 8),
 (12, 24),
 (46, 20),
 (8, 24),
 (29, 20),
 (14, 24),
 (0, 24),
 (13, 20),
 (47, 18),
 (4, 20),
 (28, 18),
 (34, 24),
 (27, 24),
 (6, 20),
 (5, 26),
 (0, 8),
 (48, 8),
 (9, 20),
 (12, 18),
 (49, 20),
 (32, 18),
 (36, 8),
 (17, 24),
 (5, 24),
 (49, 24),
 (40, 24),
 (21, 20),
 (24, 26),
 (41, 24),
 (25, 8),
 (45, 20),
 (50, 20),
 (28, 8),
 (0, 26),
 (16, 8),
 (28, 20),
 (39, 20),
 (45, 24),
 (10, 24),
 (27, 20),
 (6, 24)]

## functions
def findNewCycles(path):
    start_node = path[0]
    next_node= None
    sub = []

    #visit each edge and each node of each edge
    for edge in graph:
        node1, node2 = edge
        if start_node in edge:
                if node1 == start_node:
                    next_node = node2
                else:
                    next_node = node1
                if not visited(next_node, path):
                        # neighbor node not on path yet
                        sub = [next_node]
                        sub.extend(path)
                        # explore extended path
                        findNewCycles(sub);
                elif len(path) > 2  and next_node == path[-1]:
                        # cycle found
                        p = rotate_to_smallest(path);
                        inv = invert(p)
                        if isNew(p) and isNew(inv):
                            cycles.append(p)

def invert(path):
    return rotate_to_smallest(path[::-1])

#  rotate cycle path such that it begins with the smallest node
def rotate_to_smallest(path):
    n = path.index(min(path))
    return path[n:]+path[:n]

def isNew(path):
    return not path in cycles

def visited(node, path):
    return node in path

def main():
    output_log = open('cycles_10k.log', 'w')
    output_log.close()
    
    for idx, edge in enumerate(graph):
        output_log = open('cycles_10k.log', 'a')
        output_log.write('processing node {}/{}\n'.format(idx + 1, len(graph)))
        output_log.close()

        print('processing node {}/{}\n'.format(idx + 1, len(graph)))
        for node in edge:
            findNewCycles([node])
    output = open('cycles_10k.txt', 'w')
    for cy in cycles:
        path = [str(node) for node in cy]
        s = ",".join(path)
        output.write(s + '\n')
        print(s + '\n')
    output.close()

main()
