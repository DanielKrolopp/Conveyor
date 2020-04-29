from Pdp import PdpPipeline
from Pdp import PdpStages
import math


class ConnectedComponent:

    def __init__(self):
        self.partition = []
        self.union_find = []

        def grow_spanning_tree(current_component, neighbors, usable_nodes):
            for neighbor in neighbors:
                if usable_nodes.count(neighbor) > 0:
                    for node in self.partition:
                        if node.get("vertex") == neighbor:
                            self.partition.remove(node)
                            usable_nodes.remove(neighbor)
                            current_component.append(neighbor)
                            grow_spanning_tree(current_component, node.get("neighbors"), usable_nodes)
                            break

        def create_spanning_tree(arg):
            if arg.get("vertex") != -1:
                self.partition.append(arg)
            else:
                usable_nodes = []
                for node in self.partition:
                    usable_nodes.append(node.get("vertex"))
                components = []
                current_component = []
                total_nodes = len(self.partition)
                count = 0
                while count < total_nodes:
                    current_node = self.partition.pop()
                    usable_nodes.remove(current_node.get("vertex"))
                    current_component.append(current_node.get("vertex"))
                    grow_spanning_tree(current_component, current_node.get("neighbors"), usable_nodes)
                    components.append(current_component)
                    count += len(current_component)
                components.append([])
                return components

        def merge_connected_component(arg):
            if arg:
                for component in arg:
                    root = component[0]
                    for node in component:
                        while node >= len(self.union_find):
                            self.union_find.append(len(self.union_find))
                        self.union_find[node] = self.union_find[root]
            else:
                components = []
                for i in range(len(self.union_find)):
                    components.append([])
                for i in range(len(self.union_find)):
                    components[self.union_find[i]].append(i)
                for component in components:
                    if component:
                        print(str(component))

        def create_input():
            graph = []
            for i in range(1000):
                graph.append({"vertex": i, "neighbors": []})
                for j in range(1000):
                    if math.pow((i * j * (i + j)), 2) % 13 == 3:
                        graph[len(graph) - 1].get("neighbors").append(j)
            for i in range(8):
                graph.append({"vertex": -1, "neighbors": []})
            return graph

        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpStages.PdpBalancingFork(8))
        pl.add(PdpStages.PdpProcessor(create_spanning_tree))
        pl.add(PdpStages.PdpJoin(8))
        pl.add(PdpStages.PdpProcessor(merge_connected_component))

        graph = create_input()
        pl.run(graph)


ConnectedComponent()


