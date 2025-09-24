import matplotlib.pyplot as plt
import networkx as nx
import os

# Create output directory if it doesn't exist
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)


class Node:
    def __init__(self, name):
        self.name = name
        self.prev = None
        self.next = None


class DoublyLinkedList:
    def __init__(self):
        self.head = None
        self.tail = None
        self.hash_map = {}

    def append(self, name):
        new_node = Node(name)
        if self.head is None:
            self.head = self.tail = new_node
        else:
            self.tail.next = new_node
            new_node.prev = self.tail
            self.tail = new_node
        self.hash_map[name] = new_node

    def visualize(self, step):
        G = nx.DiGraph()
        labels = {}
        node = self.head
        while node:
            G.add_node(node.name)
            labels[node.name] = node.name
            if node.prev:
                G.add_edge(node.name, node.prev.name, color="blue")
            if node.next:
                G.add_edge(node.name, node.next.name, color="red")
            node = node.next

        edge_colors = [G[u][v]["color"] for u, v in G.edges()]
        pos = nx.spring_layout(G)
        plt.figure(figsize=(10, 6))
        nx.draw(
            G,
            pos,
            with_labels=True,
            labels=labels,
            node_color="lightgreen",
            edge_color=edge_colors,
            node_size=2000,
            font_size=12,
        )
        plt.title(f"Step {step}: Doubly Linked List and Hash Map")
        plt.text(
            -1.5,
            -1.2,
            f"Hash Map: {list(self.hash_map.keys())}",
            fontsize=12,
            bbox=dict(facecolor="white", alpha=0.5),
        )
        plt.savefig(f"{output_dir}/dll_hashmap_step{step}.png")
        plt.close()


# Simulate steps
dll = DoublyLinkedList()

# Step 1: Add Alice
dll.append("Alice")
dll.visualize(step=1)

# Step 2: Add Bob
dll.append("Bob")
dll.visualize(step=2)

# Step 3: Add Charlie
dll.append("Charlie")
dll.visualize(step=3)

# Step 4: Add Diane
dll.append("Diane")
dll.visualize(step=4)
