# define graph
cycles = []
# define graph
# forex
# the ids are defined in forex.keys
graph = [[1, 2], [1, 3], [1, 4], [1, 5], [1, 6],
         
         [2, 3], [2, 4],
         
         [3, 4],
         
         [5, 2], [5, 3], [5, 4], [5, 6],
         
         [6, 2], [6, 3], [6, 4], [6, 8], [6, 9],
         [6, 11], [6, 12], [6, 13], [6, 14], [6, 15],
         [6, 16], [6, 17], [6, 18], [6, 19],
         
         [7, 1], [7, 2], [7, 3], [7, 4], [7, 5], [7, 6], [7, 8], [7, 9],
         [7, 10], [7, 11], [7, 12], [7, 13], [7, 14], [7, 15],
         
         [10, 1], [10, 2], [10, 3], [10, 4], [10, 5], [10, 6],
         
         [16, 4], [19, 4]], [20, 6], [21, 6]


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


if __name__==__main__:
    main()
