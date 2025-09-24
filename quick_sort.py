"""
quick_sort.py

This module implements the quick sort algorithm and demonstrates how to use it
to sort posting list files stored on disk. The files are read into a simulated
buffer pool, where each buffer holds a 2KB block (page) of data. Only two buffers
are available in memory at a time, mimicking real-world constraints of disk I/O
and buffer management.

Key Concepts:
- Disk data is stored in blocks/pages (here, 2KB per block).
- Only two buffer slots are available in memory for reading/writing blocks.
- The quick_sort function sorts the data in memory after loading from disk.

Functions:
    quick_sort(arr): Standard recursive quick sort for in-memory lists.
    read_posting_list_blocks(file_path, block_size): Reads a file in blocks.
    buffer_pool_sort(file_path, block_size=2048, num_buffers=2): Loads, sorts, and displays posting list data using a buffer pool.

Example Usage:
    buffer_pool_sort("posting_list.txt")
"""


def quick_sort(arr):
    """
    Sorts an array using the quick sort algorithm.

    Args:
        arr (list): The list of elements to sort.

    Returns:
        list: A new sorted list.
    """
    if len(arr) <= 1:
        return arr
    pivot = arr[len(arr) // 2]
    left = [x for x in arr if x < pivot]
    middle = [x for x in arr if x == pivot]
    right = [x for x in arr if x > pivot]
    return quick_sort(left) + middle + quick_sort(right)


def read_posting_list_blocks(file_path, block_size):
    """
    Reads a posting list file from disk in fixed-size blocks.

    Args:
        file_path (str): Path to the posting list file.
        block_size (int): Size of each block (in bytes).

    Returns:
        list: List of blocks, each containing a list of integers.
    """
    blocks = []
    with open(file_path, "r") as f:
        buffer = []
        bytes_read = 0
        for line in f:
            num = int(line.strip())
            buffer.append(num)
            bytes_read += len(line.encode("utf-8"))
            if bytes_read >= block_size:
                blocks.append(buffer)
                buffer = []
                bytes_read = 0
        if buffer:
            blocks.append(buffer)
    return blocks


def buffer_pool_sort(file_path, block_size=2048, num_buffers=2):
    """
    Loads posting list data from disk using a buffer pool, sorts it, and displays the result.

    Args:
        file_path (str): Path to the posting list file.
        block_size (int): Size of each block (in bytes).
        num_buffers (int): Number of buffers available in memory.

    Returns:
        list: The sorted posting list.
    """
    # Read blocks from disk into the buffer pool (simulate 2 buffers)
    blocks = read_posting_list_blocks(file_path, block_size)
    print(f"Loaded {len(blocks)} blocks into {num_buffers} buffer(s).")

    # Flatten all blocks into a single list for sorting
    all_data = [item for block in blocks for item in block]

    # Sort using quick_sort
    sorted_data = quick_sort(all_data)
    print("Sorted posting list:", sorted_data)
    return sorted_data


# Example usage (uncomment and set your file path):
#
