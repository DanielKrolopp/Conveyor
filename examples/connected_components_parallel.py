from Pdp import PdpPipeline
from Pdp import PdpStages
import math
import time


class ConnectedComponentsParallel:

    def __init__(self, processors):
        self.partition = {}
        self.union_find = []
        self.count = 0
        self.processors = processors

        def grow_spanning_tree(current_component, neighbors, usable_nodes, added_nodes):
            queue = []
            for neighbor in neighbors:
                if neighbor not in added_nodes:
                    current_component.append(neighbor)
                    added_nodes[neighbor] = 1
                if neighbor in usable_nodes and usable_nodes[neighbor] == 1:
                    usable_nodes[neighbor] = 0
                    queue.append(neighbor)
            for node in queue:
                grow_spanning_tree(current_component, self.partition[node], usable_nodes, added_nodes)

        def create_spanning_tree(arg):
            if arg.get("vertex") != -1:
                self.partition[arg.get("vertex")] = arg.get("neighbors")
                return [-1]
            else:
                usable_nodes = {}
                for node in self.partition.keys():
                    usable_nodes[node] = 1
                components = []
                total_nodes = len(self.partition)
                count = 0
                while count < total_nodes:
                    added_nodes = {}
                    current_component = []
                    current_node = self.partition.popitem()
                    added_nodes[current_node[0]] = 1
                    usable_nodes[current_node[0]] = 0
                    current_component.append(current_node[0])
                    grow_spanning_tree(current_component, current_node[1], usable_nodes, added_nodes)
                    components.append(current_component)
                    count += len(current_component)
                return components

        def merge_connected_component(arg):
            if arg[0] == -1:
                return [-1]
            for component in arg:
                root = component[0]
                for node in component:
                    while node >= len(self.union_find):
                        self.union_find.append(len(self.union_find))
                    self.union_find[node] = self.union_find[root]
            return [-1]

        def create_input():
            graph = []
            primes = [7, 11, 13, 17, 19, 23, 29, 31]
            for i in range(30000):
                graph.append({"vertex": i, "neighbors": []})
            for i in range(30000):
                count = 0
                prime = 0
                for k in primes:
                    if i % k == 0:
                        count += 1
                        prime = k
                        if count > 1:
                            break
                if count != 1:
                    continue
                for j in range(i + 1, 30000):
                    count = 0
                    for k in primes:
                        if j % k == 0:
                            count += 1
                            if count > 1:
                                break
                    if (count == 1) and (j % prime == 0):
                        graph[i].get("neighbors").append(j)
                        graph[j].get("neighbors").append(i)
            for i in range(self.processors):
                graph.append({"vertex": -1, "neighbors": []})
            return graph

        start = time.monotonic()
        graph = create_input()
        print(time.monotonic() - start)
        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpStages.PdpBalancingFork(self.processors))
        pl.add(PdpStages.PdpProcessor(create_spanning_tree))
        pl.add(PdpStages.PdpJoin(self.processors))
        pl.add(PdpStages.PdpProcessor(merge_connected_component))
        pl.run(graph)



#ConnectedComponentsParallel(2)
ConnectedComponentsParallel(4)
#ConnectedComponentsParallel(8)
#ConnectedComponentsParallel(16)


