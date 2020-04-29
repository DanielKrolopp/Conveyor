
class ConnectedComponentsSerial:

    def __init__(self):
        self.partition = []

        def grow_spanning_tree(current_component, neighbors, usable_nodes):
            for neighbor in neighbors:
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
            self.partition = arg
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
            print(components)
            return components

        def create_input():
            graph = []
            primes = [7, 11, 13, 17, 19, 23, 29, 31]
            for i in range(20000):
                graph.append({"vertex": i, "neighbors": []})
            for i in range(20000):
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
                for j in range(i + 1, 20000):
                    count = 0
                    for k in primes:
                        if j % k == 0:
                            count += 1
                            if count > 1:
                                break
                    if (count == 1) and (j % prime == 0):
                        graph[i].get("neighbors").append(j)
                        graph[j].get("neighbors").append(i)
            return graph

        graph = create_input()
        create_spanning_tree(graph)


ConnectedComponentsSerial()
