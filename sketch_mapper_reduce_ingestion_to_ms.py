import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import os

# Ensure output directory exists
output_dir = "/mnt/data"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "architecture_diagram.png")

plt.figure(figsize=(14, 10))
plt.axis("off")

# Define box positions and labels
boxes = {
    "Logs": (0.1, 0.8),
    "Mapper": (0.3, 0.8),
    "Shuffle": (0.5, 0.8),
    "Reducer": (0.7, 0.8),
    "Inverted Index": (0.9, 0.8),
    "TTL Filter": (0.1, 0.6),
    "Cache (TTL+LRU)": (0.1, 0.4),
    "Query Deduplication": (0.1, 0.2),
    "Posting List": (0.7, 0.6),
    "S3 Data Lake": (0.7, 0.4),
    "Glue Crawler": (0.7, 0.3),
    "Glue Catalog": (0.7, 0.2),
    "SageMaker Training": (0.7, 0.1),
    "Delivery Diagnosis": (0.9, 0.1),
}

# Draw boxes
for label, (x, y) in boxes.items():
    plt.gca().add_patch(
        mpatches.FancyBboxPatch(
            (x, y),
            0.15,
            0.08,
            boxstyle="round,pad=0.02",
            edgecolor="black",
            facecolor="lightblue",
        )
    )
    plt.text(x + 0.075, y + 0.04, label, ha="center", va="center", fontsize=9)

# Define arrows
arrows = [
    ("Logs", "Mapper"),
    ("Mapper", "Shuffle"),
    ("Shuffle", "Reducer"),
    ("Reducer", "Inverted Index"),
    ("Logs", "TTL Filter"),
    ("TTL Filter", "Cache (TTL+LRU)"),
    ("Cache (TTL+LRU)", "Query Deduplication"),
    ("Reducer", "Posting List"),
    ("Posting List", "S3 Data Lake"),
    ("S3 Data Lake", "Glue Crawler"),
    ("Glue Crawler", "Glue Catalog"),
    ("Glue Catalog", "SageMaker Training"),
    ("SageMaker Training", "Delivery Diagnosis"),
]

# Draw arrows
for start, end in arrows:
    x1, y1 = boxes[start]
    x2, y2 = boxes[end]
    plt.arrow(
        x1 + 0.15,
        y1 + 0.04,
        x2 - x1 - 0.15,
        y2 - y1,
        length_includes_head=True,
        head_width=0.02,
        head_length=0.02,
        fc="gray",
        ec="gray",
    )

plt.savefig(output_path)
print(f"Architecture diagram saved to {output_path}")
