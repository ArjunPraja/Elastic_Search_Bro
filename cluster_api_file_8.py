from Elasticconfig_file_1 import es
from rich.console import Console
from rich.table import Table

console = Console()


# Important Notes:
# This script demonstrates how to interact with various Elasticsearch cluster APIs using the Python Elasticsearch client.
# It retrieves and displays information about nodes, cluster health, state, stats, settings, node stats, and hot threads.


# Just Read The Things no need to remember anythigns or write anything





# =======================================
# 1. Get Node Info (_nodes/_local)
# =======================================
try:
    node_info = es.nodes.info(node_id="_local")  # Get info about local node

    console.print("[bold green]Node Info (_nodes/_local)[/bold green]")
    for node_id, info in node_info["nodes"].items():
        table = Table(title=f"Node: {info.get('name', '')}")
        table.add_column("Attribute")
        table.add_column("Value")

        table.add_row("Node ID", node_id)
        table.add_row("Name", info.get("name", ""))
        table.add_row("IP", info.get("ip", ""))
        table.add_row("Host", info.get("host", ""))
        table.add_row("Version", info.get("version", ""))
        table.add_row("Roles", str(info.get("roles", [])))
        table.add_row("Transport Address", info.get("transport_address", ""))
        console.print(table)

    console.print("[blue]Benefit:[/blue] Helps identify node configuration, roles, IPs, and other attributes for cluster management.\n")

except Exception as e:
    console.print(f"[red]Error fetching node info: {e}[/red]")

# =======================================
# 2. Cluster Health (_cluster/health)
# =======================================
try:
    health = es.cluster.health()

    table = Table(title="Cluster Health")
    table.add_column("Attribute")
    table.add_column("Value")

    for key in ["cluster_name", "status", "number_of_nodes", "number_of_data_nodes",
                "active_primary_shards", "active_shards", "relocating_shards",
                "initializing_shards", "unassigned_shards", "active_shards_percent_as_number"]:
        table.add_row(key, str(health.get(key, "")))

    console.print(table)
    console.print("[blue]Benefit:[/blue] Shows cluster health status (green/yellow/red), shard allocation, and active nodes for monitoring.\n")

except Exception as e:
    console.print(f"[red]Error fetching cluster health: {e}[/red]")

# =======================================
# 3. Cluster State (_cluster/state)
# =======================================
try:
    state = es.cluster.state()

    table = Table(title="Cluster State Summary")
    table.add_column("Attribute")
    table.add_column("Value")

    table.add_row("Cluster Name", state.get("cluster_name", ""))
    table.add_row("Cluster UUID", state.get("cluster_uuid", ""))
    table.add_row("Master Node", state.get("master_node", ""))
    table.add_row("Version", str(state.get("version", "")))
    table.add_row("Number of Blocks", str(len(state.get("blocks", {}))))
    table.add_row("Number of Nodes", str(len(state.get("nodes", {}))))
    console.print(table)

    console.print("[blue]Benefit:[/blue] Gives full cluster state including master, nodes, metadata, and routing table. Useful for debugging and monitoring cluster structure.\n")

except Exception as e:
    console.print(f"[red]Error fetching cluster state: {e}[/red]")

# =======================================
# 4. Cluster Stats (_cluster/stats)
# =======================================
try:
    stats = es.cluster.stats()

    table = Table(title="Cluster Stats Summary")
    table.add_column("Attribute")
    table.add_column("Value")

    table.add_row("Cluster Name", stats.get("cluster_name", ""))
    table.add_row("Cluster UUID", stats.get("cluster_uuid", ""))
    table.add_row("Status", stats.get("status", ""))
    table.add_row("Number of Indices", str(stats.get("indices", {}).get("count", "")))
    table.add_row("Number of Shards", str(stats.get("indices", {}).get("shards", {}).get("total", "")))
    table.add_row("Store Size (Bytes)", str(stats.get("indices", {}).get("store", {}).get("size_in_bytes", "")))
    table.add_row("Total Nodes", str(stats.get("nodes", {}).get("count", {}).get("total", "")))
    table.add_row("Data Nodes", str(stats.get("nodes", {}).get("count", {}).get("data", "")))

    console.print(table)
    console.print("[blue]Benefit:[/blue] Provides detailed stats about indices, shards, storage, and nodes for performance monitoring.\n")

except Exception as e:
    console.print(f"[red]Error fetching cluster stats: {e}[/red]")

# =======================================
# 5. Cluster Update Settings (_cluster/settings)
# =======================================
try:
    settings = es.cluster.get_settings()

    table = Table(title="Cluster Settings")
    table.add_column("Setting Type")
    table.add_column("Settings JSON")

    table.add_row("Persistent", str(settings.get("persistent", {})))
    table.add_row("Transient", str(settings.get("transient", {})))
    console.print(table)

    console.print("[blue]Benefit:[/blue] Shows persistent and transient cluster settings. Useful for configuration management.\n")

except Exception as e:
    console.print(f"[red]Error fetching cluster settings: {e}[/red]")

# =======================================
# 6. Node Stats (_nodes/stats)
# =======================================
try:
    node_stats = es.nodes.stats()

    table = Table(title="Node Stats Summary")
    table.add_column("Node Name")
    table.add_column("Roles")
    table.add_column("CPU%")
    table.add_column("Heap%")
    table.add_column("Disk Used (Bytes)")

    for node_id, info in node_stats.get("nodes", {}).items():
        table.add_row(
            info.get("name", ""),
            str(info.get("roles", [])),
            str(info.get("process", {}).get("cpu", {}).get("percent", "")),
            str(info.get("jvm", {}).get("mem", {}).get("heap_used_percent", "")),
            str(info.get("fs", {}).get("total", {}).get("available_in_bytes", ""))
        )

    console.print(table)
    console.print("[blue]Benefit:[/blue] Provides node-level metrics including CPU, heap usage, disk, and roles for monitoring.\n")

except Exception as e:
    console.print(f"[red]Error fetching node stats: {e}[/red]")

# =======================================
# 7. Nodes Hot Threads (_nodes/hot_threads)
# =======================================
try:
    hot_threads = es.nodes.hot_threads()

    console.print("[bold green]Nodes Hot Threads[/bold green]")
    console.print(hot_threads)
    console.print("[blue]Benefit:[/blue] Helps identify threads consuming the most CPU on each node, useful for performance troubleshooting.\n")

except Exception as e:
    console.print(f"[red]Error fetching hot threads: {e}[/red]")
