from Pdp import PdpPipeline
from Pdp import PdpStages
import math


class ConnectedComponent:

    def __init__(self):
        self.partition = []
        self.union_find = []
        self.count = 0
        self.processors = 8

        def grow_spanning_tree(current_component, neighbors, usable_nodes):
            for neighbor in neighbors:
                # TODO: Still should add to component even if not allowed to traverse
                if current_component.count(neighbor) == 0:
                    current_component.append(neighbor)
                if usable_nodes.count(neighbor) > 0:
                    for node in self.partition:
                        if node.get("vertex") == neighbor:
                            self.partition.remove(node)
                            usable_nodes.remove(neighbor)
                            grow_spanning_tree(current_component, node.get("neighbors"), usable_nodes)
                            break

        def create_spanning_tree(arg):
            if arg.get("vertex") != -1:
                self.partition.append(arg)
                return [-1]
            else:
                usable_nodes = []
                for node in self.partition:
                    usable_nodes.append(node.get("vertex"))
                components = []
                total_nodes = len(self.partition)
                count = 0
                while count < total_nodes:
                    current_component = []
                    current_node = self.partition.pop()
                    usable_nodes.remove(current_node.get("vertex"))
                    current_component.append(current_node.get("vertex"))
                    grow_spanning_tree(current_component, current_node.get("neighbors"), usable_nodes)
                    components.append(current_component)
                    count += len(current_component)
                return components

        def merge_connected_component(arg):
            if arg[0] == -1:
                return
            for component in arg:
                root = component[0]
                for node in component:
                    while node >= len(self.union_find):
                        self.union_find.append(len(self.union_find))
                    self.union_find[node] = self.union_find[root]
            self.count += 1
            if self.count == self.processors:
                components = []
                for i in range(len(self.union_find)):
                    components.append([])
                for i in range(len(self.union_find)):
                    components[self.union_find[i]].append(i)
                for component in components:
                    if component and len(component) > 1:
                        print(str(component))

        def create_input():
            graph = []
            primes = [7, 11, 13, 17, 19, 23, 29, 31]
            for i in range(30000):
                graph.append({"vertex": i, "neighbors": []})
            for i in range(10000):
                count = 0
                for k in primes:
                    if i % k == 0:
                        count += 1
                        if count > 1:
                            break
                if count != 1:
                    continue
                for j in range(i + 1, 10000):
                    count = 0
                    for k in primes:
                        if j % k == 0:
                            count += 1
                            if count > 1:
                                break
                    if count == 1:
                        graph[i].get("neighbors").append(j)
                        graph[j].get("neighbors").append(i)
            for i in range(8):
                graph.append({"vertex": -1, "neighbors": []})
            return graph

        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpStages.PdpBalancingFork(self.processors))
        pl.add(PdpStages.PdpProcessor(create_spanning_tree))
        pl.add(PdpStages.PdpJoin(self.processors))
        pl.add(PdpStages.PdpProcessor(merge_connected_component))

        graph = create_input()
        pl.run(graph)


ConnectedComponent()


