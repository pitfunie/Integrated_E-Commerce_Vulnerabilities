"""
external_sort_merge.py

This module demonstrates an external sort-merge algorithm for large posting list files
stored on disk. It simulates disk I/O and buffer pool management, using 4KB disk blocks/pages,
but only 2KB blocks are read into memory at a time for the external quick sort phase.
The process is as follows:

1. **Initial Run Generation (External Quick Sort):**
   - The posting list file on disk is organized in 4KB blocks, but only 2KB blocks are read into memory at a time.
   - Each 2KB block is sorted in memory using quick sort.
   - Sorted blocks (runs) are written back to disk as temporary files.
   - After the first two 2KB blocks, the remaining 2KB blocks are also quick sorted and written as runs.

2. **Merge Phase:**
   - Two sorted runs are loaded into two input buffer pools (one buffer pool for a 1KB block, and another buffer pool for the other 1KB block).
   - A third buffer (flush buffer pool) accumulates merged output in memory.
   - When the flush buffer is full, it is written to disk as part of the sorted file.
   - This process continues, cycling through all runs and blocks, moving data from the input buffers to the flush buffer and then to disk,
   until the entire file (the original 4KB block) is fully sorted and merged on disk.

Key Concepts:
- Simulates external sorting for data larger than memory.
- Mimics real-world constraints of limited buffer pools and disk I/O.
- Produces a fully sorted posting list file on disk.

Functions:
    quick_sort(arr): Standard recursive quick sort for in-memory lists.
    create_initial_runs(file_path, block_size): Reads, sorts, and writes initial runs.
    merge_sorted_runs(run_files, output_file, block_size): Merges sorted runs using buffer pools.
    external_sort_merge(file_path, block_size=4096): Orchestrates the full external sort-merge process.

Example Usage:
    external_sort_merge("posting_list.txt")
"""

import os


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


def create_initial_runs(file_path, block_size):
    """
    Reads the posting list file in 2KB blocks, sorts each block, and writes each sorted run to disk.

    Args:
        file_path (str): Path to the posting list file.
        block_size (int): Size of each block (in bytes).

    Returns:
        list: List of temporary run file paths.
    """
    run_files = []
    with open(file_path, "r") as f:
        buffer = []
        bytes_read = 0
        run_idx = 0
        for line in f:
            num = int(line.strip())
            buffer.append(num)
            bytes_read += len(line.encode("utf-8"))
            if bytes_read >= block_size:
                sorted_block = quick_sort(buffer)
                run_file = f"run_{run_idx}.txt"
                with open(run_file, "w") as rf:
                    for n in sorted_block:
                        rf.write(f"{n}\n")
                run_files.append(run_file)
                buffer = []
                bytes_read = 0
                run_idx += 1
        if buffer:
            sorted_block = quick_sort(buffer)
            run_file = f"run_{run_idx}.txt"
            with open(run_file, "w") as rf:
                for n in sorted_block:
                    rf.write(f"{n}\n")
            run_files.append(run_file)
    return run_files


def merge_sorted_runs(run_files, output_file, block_size):
    """
    Merges sorted runs using two input buffer pools and one flush buffer pool.

    Args:
        run_files (list): List of sorted run file paths.
        output_file (str): Path to the final sorted output file.
        block_size (int): Size of each buffer (in bytes).
    """
    # For simplicity, assume only two runs to merge (can be extended for k-way merge)
    assert len(run_files) == 2, "This example merges only two runs."
    buffers = [[], []]  # Two input buffer pools
    files = [open(run_files[0], "r"), open(run_files[1], "r")]
    flush_buffer = []
    flush_bytes = 0

    # Each input buffer pool is 1KB
    input_buffer_size = block_size // 4  # 4KB block, 2KB read, 1KB per input buffer

    def refill_buffer(idx):
        buffers[idx] = []
        bytes_read = 0
        while bytes_read < input_buffer_size:
            line = files[idx].readline()
            if not line:
                break
            num = int(line.strip())
            buffers[idx].append(num)
            bytes_read += len(line.encode("utf-8"))

    # Initial fill
    refill_buffer(0)
    refill_buffer(1)

    with open(output_file, "w") as out:
        while buffers[0] or buffers[1]:
            # Merge step
            while buffers[0] and buffers[1]:
                if buffers[0][0] <= buffers[1][0]:
                    n = buffers[0].pop(0)
                else:
                    n = buffers[1].pop(0)
                flush_buffer.append(n)
                flush_bytes += len(f"{n}\n".encode("utf-8"))
                if flush_bytes >= input_buffer_size:
                    for val in flush_buffer:
                        out.write(f"{val}\n")
                    flush_buffer = []
                    flush_bytes = 0
            # If one buffer is empty, refill it
            for i in [0, 1]:
                if not buffers[i]:
                    refill_buffer(i)
            # If one file is exhausted, flush the rest from the other buffer
            if not buffers[0]:
                while buffers[1]:
                    n = buffers[1].pop(0)
                    flush_buffer.append(n)
                    flush_bytes += len(f"{n}\n".encode("utf-8"))
                    if flush_bytes >= input_buffer_size:
                        for val in flush_buffer:
                            out.write(f"{val}\n")
                        flush_buffer = []
                        flush_bytes = 0
                break
            if not buffers[1]:
                while buffers[0]:
                    n = buffers[0].pop(0)
                    flush_buffer.append(n)
                    flush_bytes += len(f"{n}\n".encode("utf-8"))
                    if flush_bytes >= input_buffer_size:
                        for val in flush_buffer:
                            out.write(f"{val}\n")
                        flush_buffer = []
                        flush_bytes = 0
                break
        # Flush any remaining data
        for val in flush_buffer:
            out.write(f"{val}\n")
    for f in files:
        f.close()
    # Optionally, remove temporary run files
    for run_file in run_files:
        os.remove(run_file)


def external_sort_merge(file_path, block_size=4096):
    """
    Orchestrates the external sort-merge process.

    Args:
        file_path (str): Path to the posting list file.
        block_size (int): Size of each block/buffer (in bytes).
    """
    print("Creating initial sorted runs...")
    # Use 2KB for initial run generation
    run_files = create_initial_runs(file_path, block_size // 2)
    print(f"Initial runs: {run_files}")
    print("Merging sorted runs...")
    output_file = "sorted_posting_list.txt"
    merge_sorted_runs(run_files, output_file, block_size)
    print(f"External sort-merge complete. Output: {output_file}")


# Example usage (uncomment and set your file path):
# external_sort_merge("posting_list.txt", block_size=4096)
