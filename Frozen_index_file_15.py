from Elasticconfig_file_1 import es 
from rich.console import Console

console = Console()
 
index_name = "my_logs"

# -----------------------
# Freeze Index
# -----------------------
try:
    freeze_res = es.transport.perform_request("POST", f"/{index_name}/_freeze")
    console.print("[green]Index frozen successfully[/green]")
    console.print_json(data=freeze_res.body)
except Exception as e:
    console.print(f"[red]Error freezing index:[/red] {e}")

# -----------------------
# Search Frozen Index
# -----------------------
query = {"query": {"match_all": {}}}
try:
    search_res = es.options(ignore_status=404).search(index=index_name, body=query, ignore_throttled=False)
    console.print("[green]Search on frozen index completed[/green]")
    console.print_json(data=search_res.body)
except Exception as e:
    console.print(f"[red]Error searching frozen index:[/red] {e}")

# -----------------------
# Unfreeze Index
# -----------------------
try:
    unfreeze_res = es.transport.perform_request("POST", f"/{index_name}/_unfreeze")
    console.print("[green]Index unfrozen successfully[/green]")
    console.print_json(data=unfreeze_res.body)
except Exception as e:
    console.print(f"[red]Error unfreezing index:[/red] {e}")
