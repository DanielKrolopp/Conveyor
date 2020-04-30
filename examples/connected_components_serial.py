import time


class ConnectedComponentsSerial:

    def __init__(self):
        self.partition = {}

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
            for node in arg:
                self.partition[node.get("vertex")] = node.get("neighbors")
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

        def create_input():
            graph = []
            for i in range(10000):
                graph.append({"vertex": i, "neighbors": []})
            for i in range(10000):
                for j in range(i + 1, 10000):
                    if (i + j) % 10 == 0:
                        graph[i].get("neighbors").append(j)
                        graph[j].get("neighbors").append(i)
            return graph

        graph = create_input()
        start = time.monotonic()
        create_spanning_tree(graph)
        print(time.monotonic() - start)


ConnectedComponentsSerial()
