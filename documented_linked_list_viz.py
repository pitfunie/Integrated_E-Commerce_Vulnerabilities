"""
Doubly Linked List Visualization Tool

This module creates visual representations of doubly linked lists and demonstrates
different node removal scenarios. It generates PNG images showing the structure
of the list before and after various operations.

Dependencies:
    - matplotlib: For creating plots and saving images
    - seaborn: For styling the plots with a clean appearance

Author: [Your Name]
Date: [Current Date]
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches
import seaborn as sns

# Set the visual style for all plots to have a clean white grid background
sns.set_style("whitegrid")


def draw_linked_list(nodes, title, filename):
    """
    Draw a visual representation of a doubly linked list.
    
    This function creates a horizontal visualization where each node is represented
    as a rectangle with bidirectional arrows showing the connections between nodes.
    
    Args:
        nodes (list): List of strings representing the data in each node
        title (str): Title to display above the visualization
        filename (str): Path where the generated image will be saved
    
    Returns:
        None: Saves the visualization as a PNG file
    
    Visual Elements:
        - Each node is drawn as a light blue rectangle
        - Node data is displayed as text in the center of each rectangle
        - Bidirectional arrows show the forward and backward links
        - Forward arrows point right (connecting to next node)
        - Backward arrows point left (connecting to previous node)
    """
    # Create a new figure with specified dimensions (12 inches wide, 2 inches tall)
    fig, ax = plt.subplots(figsize=(12, 2))
    
    # Set the coordinate system boundaries
    # X-axis: 0 to (number of nodes * 2) to provide spacing between nodes
    # Y-axis: 0 to 2 to center the nodes vertically
    ax.set_xlim(0, len(nodes) * 2)
    ax.set_ylim(0, 2)
    
    # Hide the axes (no tick marks, labels, or borders)
    ax.axis('off')

    # Draw each node and its connections
    for i, node in enumerate(nodes):
        # Calculate the x-position for this node (spaced 2 units apart)
        x = i * 2
        
        # Create a rectangle to represent the node
        # Position: (x, 0.5) with width=1.5, height=1
        # Style: black border, light blue fill
        rect = patches.Rectangle(
            (x, 0.5),           # Bottom-left corner coordinates
            1.5,                # Width of rectangle
            1,                  # Height of rectangle
            linewidth=1,        # Border thickness
            edgecolor='black',  # Border color
            facecolor='lightblue'  # Fill color
        )
        ax.add_patch(rect)
        
        # Add the node's data as text in the center of the rectangle
        ax.text(
            x + 0.75,          # X-coordinate (center of rectangle)
            1,                 # Y-coordinate (center of rectangle)
            node,              # Text to display
            ha='center',       # Horizontal alignment
            va='center',       # Vertical alignment
            fontsize=12        # Font size
        )

        # Draw arrows connecting this node to the previous node (skip first node)
        if i > 0:
            # Forward arrow: points from previous node to current node (top level)
            ax.annotate(
                '',                    # No text label
                xy=(x, 1),            # Arrow head position (current node)
                xytext=(x - 0.5, 1),  # Arrow tail position (previous node)
                arrowprops=dict(
                    arrowstyle='<-',   # Arrow style (pointing left, but drawn right-to-left)
                    lw=1.5            # Line width
                )
            )
            
            # Backward arrow: points from current node to previous node (bottom level)
            ax.annotate(
                '',                      # No text label
                xy=(x - 0.5, 0.75),     # Arrow head position (previous node)
                xytext=(x, 0.75),       # Arrow tail position (current node)
                arrowprops=dict(
                    arrowstyle='<-',     # Arrow style (pointing left)
                    lw=1.5              # Line width
                )
            )

    # Set the title for this visualization
    ax.set_title(title, fontsize=14)
    
    # Optimize the layout to prevent clipping
    plt.tight_layout()
    
    # Save the figure as a PNG file
    plt.savefig(filename)
    
    # Close the figure to free memory
    plt.close()


# Define the data for different linked list scenarios
# Each list represents the nodes that remain after a specific operation

# Original list with all nodes present
nodes_initial = ['Head', 'A', 'B', 'C', 'Tail']

# List after removing the head node (first node)
# The 'Head' node is removed, 'A' becomes the new first node
nodes_remove_head = ['A', 'B', 'C', 'Tail']

# List after removing the tail node (last node)
# The 'Tail' node is removed, 'C' becomes the new last node
nodes_remove_tail = ['Head', 'A', 'B', 'C']

# List after removing a middle node (node 'B')
# Node 'B' is removed, 'A' now connects directly to 'C'
nodes_remove_middle = ['Head', 'A', 'C', 'Tail']

# Generate visualizations for each scenario
# Each call creates a separate PNG file showing the list state

print("Generating linked list visualizations...")

# Create visualization of the initial state
draw_linked_list(
    nodes_initial, 
    'Initial Doubly Linked List', 
    '/mnt/data/linked_list_initial.png'
)
print("✓ Created initial list visualization")

# Create visualization after head removal
draw_linked_list(
    nodes_remove_head, 
    'After Removing Head Node', 
    '/mnt/data/linked_list_remove_head.png'
)
print("✓ Created head removal visualization")

# Create visualization after tail removal
draw_linked_list(
    nodes_remove_tail, 
    'After Removing Tail Node', 
    '/mnt/data/linked_list_remove_tail.png'
)
print("✓ Created tail removal visualization")

# Create visualization after middle node removal
draw_linked_list(
    nodes_remove_middle, 
    'After Removing Middle Node (B)', 
    '/mnt/data/linked_list_remove_middle.png'
)
print("✓ Created middle node removal visualization")

print("\nAll visualizations have been generated successfully!")
print("Files saved to /mnt/data/ directory:")
print("- linked_list_initial.png")
print("- linked_list_remove_head.png") 
print("- linked_list_remove_tail.png")
print("- linked_list_remove_middle.png")