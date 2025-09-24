import heapq


class Node:
    def __init__(self, val):
        self.val = val
        self.prev = None
        self.next = None


def merge_k_lists(lists):
    min_heap = []
    for i, lst in enumerate(lists):
        if lst:
            heapq.heappush(min_heap, (lst[0], i, 0))

    head = tail = None

    while min_heap:
        val, list_idx, elem_idx = heapq.heappop(min_heap)
        new_node = Node(val)

        if not head:
            head = tail = new_node
        else:
            tail.next = new_node
            new_node.prev = tail
            tail = new_node

        if elem_idx + 1 < len(lists[list_idx]):
            next_val = lists[list_idx][elem_idx + 1]
            heapq.heappush(min_heap, (next_val, list_idx, elem_idx + 1))

    return head  # Head of the DLL, could represent MRU
