import matplotlib.pyplot as plt
import matplotlib.patches as patches

def draw_custom_linked_list(nodes, title=None, filename=None, **style):
    # Layout parameters (easily tweakable)
    node_w = style.get('node_width', 2.0)
    node_h = style.get('node_height', 1.2) 
    gap = style.get('gap', 0.8)
    margin = style.get('margin', 0.6)
    y = style.get('y_position', 0.4)
    
    # Styling parameters
    node_color = style.get('node_color', '#e6f3ff')
    border_color = style.get('border_color', '#2c3e50')
    border_width = style.get('border_width', 2.0)
    font_size = style.get('font_size', 14)
    
    # Arrow styling
    arrow_width = style.get('arrow_width', 2.0)
    arrow_style = style.get('arrow_style', '-|>')
    curve_amount = style.get('curve_amount', 0.15)
    
    # Calculate canvas size
    n = len(nodes)
    width = 2 * margin + n * node_w + (n - 1) * gap
    height = 3.0
    
    fig, ax = plt.subplots(figsize=(width, height))
    ax.set_xlim(0, width)
    ax.set_ylim(0, 2.5)
    ax.axis('off')
    
    # Draw nodes
    x = margin
    positions = []
    for label in nodes:
        rect = patches.Rectangle((x, y), node_w, node_h,
                               linewidth=border_width, 
                               edgecolor=border_color, 
                               facecolor=node_color)
        ax.add_patch(rect)
        ax.text(x + node_w/2, y + node_h/2, label,
                ha='center', va='center', fontsize=font_size, weight='bold')
        positions.append((x, y))
        x += node_w + gap
    
    # Draw curved arrows
    for i in range(1, n):
        prev_x, prev_y = positions[i-1]
        curr_x, curr_y = positions[i]
        
        # Arrow anchor points with better spacing
        prev_right = (prev_x + node_w, prev_y + node_h * 0.75)
        curr_left = (curr_x, curr_y + node_h * 0.75)
        prev_right_back = (prev_x + node_w, prev_y + node_h * 0.25)
        curr_left_back = (curr_x, curr_y + node_h * 0.25)
        
        # Custom arrow properties
        arrow_props = {
            'arrowstyle': arrow_style,
            'lw': arrow_width,
            'shrinkA': 8,
            'shrinkB': 8,
            'color': '#34495e'
        }
        
        # Forward arrow (curved up)
        ax.annotate('', xy=curr_left, xytext=prev_right,
                   arrowprops={**arrow_props, 'connectionstyle': f'arc3,rad={curve_amount}'})
        
        # Backward arrow (curved down)
        ax.annotate('', xy=prev_right_back, xytext=curr_left_back,
                   arrowprops={**arrow_props, 'connectionstyle': f'arc3,rad=-{curve_amount}'})
    
    if title:
        ax.set_title(title, fontsize=16, weight='bold', pad=15)
    
    fig.tight_layout()
    if filename:
        fig.savefig(filename, dpi=200, bbox_inches='tight')
    plt.close(fig)

# Demo with custom styling
states = {
    "Modern Doubly Linked List": ['Start', 'Node1', 'Node2', 'End'],
    "Compact Version": ['A', 'B', 'C', 'D', 'E']
}

# Style 1: Modern look
modern_style = {
    'node_width': 2.2,
    'node_height': 1.0,
    'gap': 1.0,
    'node_color': '#3498db',
    'border_color': '#2c3e50',
    'border_width': 2.5,
    'font_size': 12,
    'arrow_width': 2.2,
    'curve_amount': 0.2
}

# Style 2: Compact look  
compact_style = {
    'node_width': 1.5,
    'node_height': 0.8,
    'gap': 0.4,
    'node_color': '#e74c3c',
    'border_color': '#c0392b',
    'font_size': 10,
    'arrow_width': 1.5,
    'curve_amount': 0.1
}

draw_custom_linked_list(states["Modern Doubly Linked List"], 
                       title="Modern Style", 
                       filename="modern_list.png", 
                       **modern_style)

draw_custom_linked_list(states["Compact Version"], 
                       title="Compact Style", 
                       filename="compact_list.png", 
                       **compact_style)