class Node:

    def __init__(self, key):
        self.key = key
        self.left = None
        self.right = None


class BinarySearchTree:
    """
    A simple Binary Search Tree (BST) implementation.

    Methods:
        insert(key): Add a new key into the BST.
        search(key): Find whether a key exists in the BST.
        delete(key): Remove a key from the BST, if present.
        inorder(): Generator yielding keys in ascending order.
        preorder(): Generator yielding keys in root-left-right order.
        postorder(): Generator yielding keys in left-right-root order.
    """

    def __init__(self):
        """Initialize an empty BST."""
        self.root = None

    def insert(self, key):
        """
        Insert 'key' into the BST.

        If tree is empty, new node becomes root.
        Otherwise, traverse left/right based on comparisons.
        """
        if self.root is None:
            self.root = Node(key)
            return

        current = self.root
        while True:
            if key < current.key:
                # go left
                if current.left is None:
                    current.left = Node(key)
                    return
                current = current.left
            else:
                # go right for key >= current.key
                if current.right is None:
                    current.right = Node(key)
                    return
                current = current.right

    def search(self, key):
        """
        Search for 'key'.

        Returns:
            Node if found, else None.
        """
        current = self.root
        while current:
            if key == current.key:
                return current
            elif key < current.key:
                current = current.left
            else:
                current = current.right
        return None

    def delete(self, key):
        """
        Delete 'key' from the BST if it exists.

        Handles three cases:
          1. Node is a leaf → remove it directly.
          2. Node has one child → replace with the child.
          3. Node has two children → replace with in-order successor.
        """

        def _delete_rec(node, key):
            if node is None:
                return node  # key not found

            if key < node.key:
                node.left = _delete_rec(node.left, key)
            elif key > node.key:
                node.right = _delete_rec(node.right, key)
            else:
                # Found node to delete
                if node.left is None:
                    return node.right
                if node.right is None:
                    return node.left
                # Two children: find in-order successor (smallest in right subtree)
                succ = node.right
                while succ.left:
                    succ = succ.left
                node.key = succ.key  # copy successor's key
                node.right = _delete_rec(node.right, succ.key)
            return node

        self.root = _delete_rec(self.root, key)

    def inorder(self):
        """
        In-order traversal: Left → Root → Right.

        Yields keys in ascending order.
        """

        def _inorder(node):
            if node:
                yield from _inorder(node.left)
                yield node.key
                yield from _inorder(node.right)

        yield from _inorder(self.root)

    def preorder(self):
        """
        Pre-order traversal: Root → Left → Right.
        """

        def _preorder(node):
            if node:
                yield node.key
                yield from _preorder(node.left)
                yield from _preorder(node.right)

        yield from _preorder(self.root)

    def postorder(self):
        """
        Post-order traversal: Left → Right → Root.
        """

        def _postorder(node):
            if node:
                yield from _postorder(node.left)
                yield from _postorder(node.right)
                yield node.key

        yield from _postorder(self.root)


if __name__ == "__main__":
    bst = BinarySearchTree()

    # Insert values
    for value in [50, 30, 70, 20, 40, 60, 80]:
        bst.insert(value)

    # Search for a key
    node = bst.search(40)
    print("Found:", node.key if node else "Not found")  # Found: 40

    # Traverse the tree
    print("In-order:", list(bst.inorder()))  # [20, 30, 40, 50, 60, 70, 80]
    print("Pre-order:", list(bst.preorder()))  # [50, 30, 20, 40, 70, 60, 80]
    print("Post-order:", list(bst.postorder()))  # [20, 40, 30, 60, 80, 70, 50]

    # Delete a node (e.g., 70) and traverse again
    bst.delete(70)
    print("After deleting 70, in-order:", list(bst.inorder()))
