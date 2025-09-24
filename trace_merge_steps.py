import heapq

lists = [[1, 4, 5], [1, 3, 4], [2, 6]]
heap = []
dll = []  # log of values in doubly linked list (in order)

# Step 1: Initialize heap
for i in range(len(lists)):
    val = lists[i][0]
    heapq.heappush(heap, (val, i, 0))

print("Initial Heap:", heap)

step = 1
while heap:
    print(f"\nStep {step}:")
    val, list_idx, elem_idx = heapq.heappop(heap)
    print(f"  Extracted: {val} from list[{list_idx}][{elem_idx}]")

    dll.append(val)
    print("  DLL after append:", dll)

    # Push next element from same list
    if elem_idx + 1 < len(lists[list_idx]):
        next_val = lists[list_idx][elem_idx + 1]
        heapq.heappush(heap, (next_val, list_idx, elem_idx + 1))
        print(f"  Inserted next: {next_val} from list[{list_idx}][{elem_idx + 1}]")

    print("  Heap now:", heap)
    step += 1
