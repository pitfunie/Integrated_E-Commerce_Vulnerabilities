import heapq


class Node:
    """
    Node for a doubly linked list.
    Each node holds a value and pointers to previous and next nodes.
    Used to build the merged result as a doubly linked list.
    """

    def __init__(self, val):
        self.val = val
        self.prev = None
        self.next = None


class MinHeapItem:
    """
    Wrapper for heap elements.
    Stores the value, the index of the originating list, and the index within that list.
    Implements __lt__ for heapq to compare based on value.

    This wrapper class isn't just packaging data—it’s enabling custom comparison logic for the priority queue.
    Here’s why it's critical:

    ✅ Tracks origin: It remembers which list each value came from (list_idx) and where it was inside that list
    (elem_idx).
    🧠 Facilitates ordering: The __lt__ method tells Python’s heapq how to prioritize elements—specifically,
    compare by the actual value (val), not by object identity.
    🔁 Drives iteration: It allows the heap to know which element to pull next and where to look for the subsequent
    value in the same list.
    """

    def __init__(self, val, list_idx, elem_idx):
        self.val = val
        self.list_idx = list_idx  # Which input list this value came from
        self.elem_idx = elem_idx  # Index in that input list

    def __lt__(self, other):
        return self.val < other.val


def k_way_merge(lists):
    """
    Merges k sorted lists into a single sorted doubly linked list using a min-heap.

    Args:
        lists (List[List[int]]): List of k sorted lists.

    Returns:
        Node: Head of the resulting doubly linked list (MRU).
    """
    heap = []
    result_head, result_tail = None, None

    # Initialize heap with the first element from each list
    for i, lst in enumerate(lists):
        if lst:
            heapq.heappush(heap, MinHeapItem(lst[0], i, 0))

    # Extract the smallest item and add the next item from the same list to the heap
    while heap:
        smallest = heapq.heappop(heap)
        val, i, j = smallest.val, smallest.list_idx, smallest.elem_idx

        # Build the doubly linked list as we go
        new_node = Node(val)
        if not result_head:
            result_head = result_tail = new_node  # First node
        else:
            result_tail.next = new_node
            new_node.prev = result_tail
            result_tail = new_node

        # If there is a next element in the same list, push it to the heap
        if j + 1 < len(lists[i]):
            heapq.heappush(heap, MinHeapItem(lists[i][j + 1], i, j + 1))

    return result_head  # MRU (head), result_tail is LRU (tail)


# Example usage
lists = [[1, 4, 5], [1, 3, 4], [2, 6]]
mru_head = k_way_merge(lists)

# Traverse from MRU (head) to LRU (tail) and print values
curr = mru_head
while curr:
    print(curr.val, end=" → ")
    curr = curr.next
# Output: 1 → 1 → 2 → 3 → 4 → 4 → 5 → 6 →
