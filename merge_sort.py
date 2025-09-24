def merge_sort(arr, key=lambda x: x):
    """
    Sorts an array using the merge sort algorithm.

    Args:
        arr (list): The list of elements to sort.
        key (function): Function to extract comparison key from each element.

    Returns:
        list: A new sorted list.
    """
    if len(arr) <= 1:
        return arr
    mid = len(arr) // 2
    left = merge_sort(arr[:mid], key)
    right = merge_sort(arr[mid:], key)
    return merge(left, right, key)


def merge(left, right, key):
    """
    Merges two sorted lists into a single sorted list using a key function.
    """
    result = []
    while left and right:
        result.append(left.pop(0) if key(left[0]) < key(right[0]) else right.pop(0))
    return result + left + right


# Example usage with numbers
print(merge_sort([1, 3, 5, 7, 9, 2, 4, 6, 8, 0]))

# Example usage with objects (dictionaries)
documents = [
    {"id": 3, "text": "apple"},
    {"id": 1, "text": "banana"},
    {"id": 2, "text": "cherry"},
]
# Sort by 'id'
print(merge_sort(documents, key=lambda x: x["id"]))
# Sort by 'text'
print(merge_sort(documents, key=lambda x: x["text"]))
