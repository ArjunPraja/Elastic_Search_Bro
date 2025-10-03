from Elasticconfig_file_1 import es
from rich.console import Console
from rich.table import Table

console = Console()


# Important Note: just read the line no 19 ton 22 and direct run the code.
# No Need To Remember The Code This is the Only for view Purpose and to understand how to use the cat API in Elasticsearch using Python.
# Just run the code and see the output in a tabular format.








# Some Theory 
# The Cat API in Elasticsearch provides a simple and human-readable way to access various information about the cluster, indices, nodes, and more. It is designed for quick and easy access to important metrics and status information.
# The Cat API is often used for monitoring and debugging purposes, as it provides a concise overview of the cluster's state.

















# ---------------------------------------
# 1. Display all indices
# ---------------------------------------
try:
    indices = es.cat.indices(format="json")  # Get all indices as JSON

    table = Table(title="Elasticsearch Indices")
    table.add_column("Health")
    table.add_column("Status")
    table.add_column("Index")
    table.add_column("Docs Count")
    table.add_column("Store Size")

    for idx in indices:
        table.add_row(
            idx.get("health", ""),
            idx.get("status", ""),
            idx.get("index", ""),
            str(idx.get("docs.count", "")),
            idx.get("store.size", "")
        )

    console.print(table)

except Exception as e:
    console.print(f"[red]Error fetching indices: {e}[/red]")

# ---------------------------------------
# 2. Display all nodes
# ---------------------------------------
try:
    nodes = es.cat.nodes(format="json")  # Get all nodes as JSON

    table = Table(title="Elasticsearch Nodes")
    table.add_column("Name")
    table.add_column("IP")
    table.add_column("Roles")
    table.add_column("CPU%")
    table.add_column("Heap%")
    table.add_column("Disk%")

    for node in nodes:
        table.add_row(
            node.get("name", ""),
            node.get("ip", ""),
            node.get("node.role", ""),
            str(node.get("cpu", "")),
            str(node.get("heap.percent", "")),
            str(node.get("disk.percent", ""))
        )

    console.print(table)

except Exception as e:
    console.print(f"[red]Error fetching nodes: {e}[/red]")

# ---------------------------------------
# 3. Display templates
# ---------------------------------------
try:
    templates = es.cat.templates(format="json", s="order:desc,index_patterns")  # Sorted templates

    table = Table(title="Elasticsearch Templates")
    table.add_column("Name")
    table.add_column("Index Patterns")
    table.add_column("Order")
    table.add_column("Version")

    for tmpl in templates:
        table.add_row(
            tmpl.get("name", ""),
            str(tmpl.get("index_patterns", "")),
            str(tmpl.get("order", "")),
            str(tmpl.get("version", ""))
        )

    console.print(table)

except Exception as e:
    console.print(f"[red]Error fetching templates: {e}[/red]")

# ---------------------------------------
# 4. Display cluster health
# ---------------------------------------
try:
    health = es.cat.health(format="json")[0]  # Get cluster health

    table = Table(title="Cluster Health")
    table.add_column("Cluster")
    table.add_column("Status")
    table.add_column("Node Total")
    table.add_column("Shards")
    table.add_column("Active Shards%")

    table.add_row(
        health.get("cluster", ""),
        health.get("status", ""),
        health.get("node.total", ""),
        health.get("active_shards", ""),
        health.get("active_shards_percent", "")
    )

    console.print(table)

except Exception as e:
    console.print(f"[red]Error fetching cluster health: {e}[/red]")

# ---------------------------------------
# 5. Display total document count
# ---------------------------------------
try:
    count = es.cat.count(format="json")[0]

    table = Table(title="Total Documents in Cluster")
    table.add_column("Timestamp")
    table.add_column("Document Count")

    table.add_row(
        count.get("timestamp", ""),
        str(count.get("count", ""))
    )

    console.print(table)

except Exception as e:
    console.print(f"[red]Error fetching document count: {e}[/red]")
