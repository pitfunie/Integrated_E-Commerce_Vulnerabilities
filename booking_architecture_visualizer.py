#!/usr/bin/env python3
"""
Booking Service Architecture Visualizer

This script creates a visual representation of a depth-level architecture
for a booking service system, showing the layered approach with functional
and non-functional requirements.

Architecture Components:
- Application Layer: Stateless request handling
- Cache Layer: Frequently accessed data storage  
- Database Layer: Persistent storage with sharding and replication

Usage:
    python3 booking_architecture_visualizer.py
    
    Or make executable and run directly:
    chmod +x booking_architecture_visualizer.py
    ./booking_architecture_visualizer.py

Dependencies:
    pip install matplotlib seaborn

Output:
    Creates booking_service_architecture.png in the current directory
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
import os
import sys
from pathlib import Path


def setup_plot_style():
    """
    Configure the visual style for the architecture diagram.
    
    Uses seaborn's whitegrid style for clean, professional appearance.
    """
    sns.set_style("whitegrid")
    plt.rcParams['font.size'] = 10
    plt.rcParams['font.family'] = 'sans-serif'


def create_architecture_layers():
    """
    Define the architecture layers with their descriptions.
    
    Returns:
        dict: Layer definitions with position and description
        
    Each layer represents a different level of the system architecture:
    - Layer 1: Application (top level, user-facing)
    - Layer 2: Cache (middle level, performance optimization)  
    - Layer 3: Database (bottom level, data persistence)
    """
    return {
        "Application Layer (Stateless)": {
            "position": 3,
            "description": "Handles requests, stateless logic",
            "color": "lightblue"
        },
        "Cache Layer": {
            "position": 2, 
            "description": "Stores frequently accessed data",
            "color": "lightgreen"
        },
        "Database Layer": {
            "position": 1,
            "description": "Persistent storage with sharding and replication", 
            "color": "lightcoral"
        }
    }


def draw_layer_boxes(ax, layers):
    """
    Draw the architecture layer boxes on the plot.
    
    Args:
        ax: Matplotlib axis object
        layers (dict): Layer definitions from create_architecture_layers()
        
    Each layer is drawn as a rounded rectangle with:
    - Layer name in bold
    - Description text below
    - Color-coded background
    """
    for layer_name, layer_info in layers.items():
        y_pos = layer_info["position"]
        description = layer_info["description"]
        color = layer_info["color"]
        
        # Draw rounded rectangle for layer
        layer_box = mpatches.FancyBboxPatch(
            (1, y_pos), 8, 0.8,  # (x, y, width, height)
            boxstyle="round,pad=0.1",
            edgecolor='black',
            facecolor=color,
            linewidth=2
        )
        ax.add_patch(layer_box)
        
        # Add layer name (bold)
        ax.text(1.2, y_pos + 0.5, layer_name, 
                fontsize=12, weight='bold', 
                verticalalignment='center')
        
        # Add layer description
        ax.text(1.2, y_pos + 0.15, description, 
                fontsize=10, style='italic',
                verticalalignment='center')


def add_requirements_text(ax):
    """
    Add functional and non-functional requirements to the diagram.
    
    Args:
        ax: Matplotlib axis object
        
    Displays two types of requirements:
    - Functional: What the system does (features)
    - Non-functional: How the system performs (quality attributes)
    """
    # Functional Requirements
    functional_text = (
        "Functional Requirements:\n"
        "• Ticket booking\n"
        "• Availability check\n" 
        "• Payment processing\n"
        "• User authentication\n"
        "• Seat selection"
    )
    ax.text(10.5, 3.5, functional_text,
            fontsize=10, verticalalignment='top',
            bbox=dict(boxstyle="round,pad=0.3", facecolor='lightyellow'))
    
    # Non-Functional Requirements  
    nonfunctional_text = (
        "Non-Functional Requirements:\n"
        "• Scalability (1000+ concurrent users)\n"
        "• Fault tolerance (99.9% uptime)\n"
        "• Performance (<200ms response)\n"
        "• Security (PCI compliance)\n"
        "• Availability (24/7 operation)"
    )
    ax.text(10.5, 2.2, nonfunctional_text,
            fontsize=10, verticalalignment='top',
            bbox=dict(boxstyle="round,pad=0.3", facecolor='lightcyan'))


def add_database_strategies(ax):
    """
    Add database strategy information to the diagram.
    
    Args:
        ax: Matplotlib axis object
        
    Shows key database design patterns:
    - Sharding: Horizontal partitioning for scalability
    - Replication: Data redundancy for availability
    - Read/write separation: Performance optimization
    """
    db_strategies_text = (
        "Database Strategies:\n"
        "• Sharding: by region/event type\n"
        "• Replication: master-slave setup\n"
        "• Read/write separation\n"
        "• Connection pooling\n"
        "• Query optimization"
    )
    ax.text(10.5, 0.9, db_strategies_text,
            fontsize=10, verticalalignment='top',
            bbox=dict(boxstyle="round,pad=0.3", facecolor='lightpink'))


def draw_flow_arrows(ax):
    """
    Draw arrows showing data flow between layers.
    
    Args:
        ax: Matplotlib axis object
        
    Arrows indicate the typical request flow:
    Application → Cache → Database (and back up)
    """
    arrow_style = dict(arrowstyle="->", color='darkblue', lw=3)
    
    # Application to Cache
    ax.annotate("", xy=(5, 2.8), xytext=(5, 3.2), arrowprops=arrow_style)
    ax.text(5.2, 3.0, "Request", fontsize=8, color='darkblue')
    
    # Cache to Database  
    ax.annotate("", xy=(5, 1.8), xytext=(5, 2.2), arrowprops=arrow_style)
    ax.text(5.2, 2.0, "Cache Miss", fontsize=8, color='darkblue')
    
    # Return arrows (lighter color)
    return_arrow_style = dict(arrowstyle="->", color='gray', lw=2, linestyle='dashed')
    ax.annotate("", xy=(4.5, 3.2), xytext=(4.5, 2.8), arrowprops=return_arrow_style)
    ax.annotate("", xy=(4.5, 2.2), xytext=(4.5, 1.8), arrowprops=return_arrow_style)


def create_architecture_diagram():
    """
    Main function to create the complete architecture diagram.
    
    Returns:
        str: Path to the saved diagram file
        
    Creates a comprehensive visualization showing:
    - Layered architecture structure
    - System requirements
    - Database strategies  
    - Data flow patterns
    """
    # Setup
    setup_plot_style()
    
    # Create figure with appropriate size
    fig, ax = plt.subplots(figsize=(16, 10))
    
    # Get layer definitions
    layers = create_architecture_layers()
    
    # Draw all components
    draw_layer_boxes(ax, layers)
    add_requirements_text(ax)
    add_database_strategies(ax)
    draw_flow_arrows(ax)
    
    # Configure plot appearance
    ax.set_title("Depth-Level Architecture of Booking Service", 
                fontsize=16, weight='bold', pad=20)
    ax.set_xlim(0, 18)
    ax.set_ylim(0, 5)
    ax.axis('off')  # Hide axes for cleaner look
    
    # Add timestamp and metadata
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ax.text(0.5, 0.2, f"Generated: {timestamp}", 
            fontsize=8, style='italic', color='gray')
    
    # Save the diagram
    output_path = Path.cwd() / "booking_service_architecture.png"
    plt.savefig(output_path, 
                bbox_inches='tight', 
                dpi=300,  # High resolution
                facecolor='white')
    plt.close()
    
    return str(output_path)


def check_dependencies():
    """
    Check if required dependencies are installed.
    
    Returns:
        bool: True if all dependencies are available
        
    Raises:
        ImportError: If required packages are missing
    """
    try:
        import matplotlib
        import seaborn
        return True
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("Install with: pip install matplotlib seaborn")
        return False


def main():
    """
    Main execution function.
    
    Handles the complete workflow:
    1. Check dependencies
    2. Create architecture diagram  
    3. Save and report results
    4. Handle any errors gracefully
    """
    print("🏗️  Booking Service Architecture Visualizer")
    print("=" * 50)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    try:
        # Create the diagram
        print("📊 Creating architecture diagram...")
        output_path = create_architecture_diagram()
        
        # Success message
        print(f"✅ Architecture diagram saved successfully!")
        print(f"📁 Location: {output_path}")
        print(f"📏 File size: {os.path.getsize(output_path):,} bytes")
        
        # Additional info
        print("\n📋 Diagram includes:")
        print("   • 3-layer architecture visualization")
        print("   • Functional and non-functional requirements")
        print("   • Database strategy details")
        print("   • Data flow arrows")
        
    except Exception as e:
        print(f"❌ Error creating diagram: {e}")
        sys.exit(1)


if __name__ == "__main__":
    """
    Entry point when script is run directly.
    
    This allows the script to be executed as:
    - python3 booking_architecture_visualizer.py
    - ./booking_architecture_visualizer.py (if made executable)
    """
    main()