from Elasticconfig_file_1 import es
from rich import print
from rich.console import Console

if __name__ == "__main__":
    console = Console()

    # ---------------------------------------
    # Step 1: Create an index with custom settings
    # ---------------------------------------
    try:
        es.indices.create(
            index="products",
            body={
                "settings": {
                    "number_of_shards": 3,    # Each index will have 3 primary shards
                    "number_of_replicas": 1   # Each shard will have 1 replica
                }
            },
            ignore=400  # If the index already exists, ignore the error
        )
        print("[green]Index 'products' created successfully![/green]")
    except Exception as e:
        print(f"[red]Failed to create index: {e}[/red]")

    # ---------------------------------------
    # Step 2: Retrieve all indices
    # ---------------------------------------
    try:
        all_indices = es.indices.get(index="*")
        console.print("[bold yellow]All indices in the cluster:[/bold yellow]")
        console.print_json(data=all_indices.body)
    except Exception as e:
        print(f"[red]Failed to fetch all indices: {e}[/red]")

    # ---------------------------------------
    # Step 3: Retrieve a specific index
    # ---------------------------------------
    try:
        product_index = es.indices.get(index="products")
        console.print("[bold yellow]Details of 'products' index:[/bold yellow]")
        console.print_json(data=product_index.body)
    except Exception as e:
        print(f"[red]Failed to fetch 'products' index: {e}[/red]")

    # ---------------------------------------
    # Step 4: Get index settings
    # ---------------------------------------
    try:
        settings = es.indices.get_settings(index="products")
        console.print("[bold yellow]Settings for 'products' index:[/bold yellow]")
        console.print_json(data=settings.body)
    except Exception as e:
        print(f"[red]Failed to fetch index settings: {e}[/red]")

    # ---------------------------------------
    # Step 5: Get index statistics
    # ---------------------------------------
    try:
        stats = es.indices.stats(index="products")
        console.print("[bold yellow]Statistics for 'products' index:[/bold yellow]")
        console.print_json(data=stats.body)
    except Exception as e:
        print(f"[red]Failed to fetch index stats: {e}[/red]")

    # ---------------------------------------
    # Step 6: Flush the index (persist in-memory data)
    # ---------------------------------------
    try:
        flush_response = es.indices.flush(index="products")
        console.print("[bold yellow]'products' index flushed successfully:[/bold yellow]")
        console.print_json(data=flush_response.body)
    except Exception as e:
        print(f"[red]Failed to flush index: {e}[/red]")

    # ---------------------------------------
    # Step 7: Delete the index
    # ---------------------------------------
    try:
        es.options(ignore_status=[400, 404]).indices.delete(index="products")
        print("[green]Index 'products' deleted successfully![/green]")
    except Exception as e:
        print(f"[red]Failed to delete index: {e}[/red]")
