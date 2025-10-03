from Elasticconfig_file_1 import es
from rich.console import Console

console = Console()

if __name__ == "__main__":
    # ---------------- Create multiple product docs ----------------
    products = [
        {"name": "iPhone 14", "category": "mobile", "price": 70000, "brand": "Apple"},
        {"name": "Samsung Galaxy S23", "category": "mobile", "price": 65000, "brand": "Samsung"},
        {"name": "OnePlus 11", "category": "mobile", "price": 55000, "brand": "OnePlus"},
        {"name": "MacBook Pro", "category": "laptop", "price": 150000, "brand": "Apple"},
        {"name": "Dell XPS 15", "category": "laptop", "price": 120000, "brand": "Dell"},
        {"name": "Sony WH-1000XM5", "category": "headphone", "price": 30000, "brand": "Sony"},
    ]


    #  Here We are inserting the docuemtns in the Two different indices products and books
    #  So We Can SEarchj In The All The indeceas and sopecific indices also 

    # Insert documents one by one
    for i, product in enumerate(products, start=1):
        res = es.index(index="products", id=i, document=product)
        console.print(f"[green]Inserted doc {i}:[/green] {res.body['result']}")
    console.print("[bold cyan]All product documents inserted successfully![/bold cyan]")

    
    
    # Insert documents one by one

    for i, product in enumerate(products, start=1):
        res = es.index(index="books", id=i, document=product)
        console.print(f"[green]Inserted doc {i}:[/green] {res.body['result']}")

    console.print("[bold cyan]All product documents inserted successfully![/bold cyan]")
