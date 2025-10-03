from Elasticconfig_file_1 import es
from rich.console import Console
from rich.pretty import pprint

console = Console()


# import Notes :- Mapping is the process of defining how a document, and the fields it contains, are stored and indexed.
# its  similar to defining a schema in database.












# -----------------------------
# 1. Create Index with Mapping
# -----------------------------
index_name = "schools_example"

# Define mapping (schema) for the index
mapping = {
    "mappings": {
        "properties": {
            "name": {"type": "text"},             # full-text search
            "description": {"type": "text"},
            "street": {"type": "text"},
            "city": {"type": "keyword"},          # exact match
            "state": {"type": "keyword"},
            "zip": {"type": "keyword"},
            "location": {"type": "geo_point"},    # geo search
            "fees": {"type": "integer"},
            "tags": {"type": "keyword"},
            "rating": {"type": "float"}
        }
    }
}

# Create the index (ignore if it already exists)
es.options(ignore_status=400).indices.create(index=index_name, body=mapping)
console.print(f"[green]Index '{index_name}' created with mapping.[/green]")

# -----------------------------
# 2. Insert Sample Documents
# -----------------------------
docs = [
    {
        "name": "Central School",
        "description": "CBSE Affiliation",
        "street": "Nagan",
        "city": "Paprola",
        "state": "HP",
        "zip": "176115",
        "location": {"lat": 31.8955385, "lon": 76.8380405},
        "fees": 2200,
        "tags": ["Senior Secondary", "beautiful campus"],
        "rating": 3.3
    },
    {
        "name": "City Best School",
        "description": "ICSE",
        "street": "West End",
        "city": "Meerut",
        "state": "UP",
        "zip": "250002",
        "location": {"lat": 28.9926174, "lon": 77.692485},
        "fees": 3500,
        "tags": ["fully computerized"],
        "rating": 4.5
    }
]

# Index documents into Elasticsearch
for i, doc in enumerate(docs, start=1):
    es.index(index=index_name, id=i, body=doc)

console.print(f"[blue]{len(docs)} sample documents inserted.[/blue]")

# -----------------------------
# 3. Run Match All Query
# -----------------------------
match_all_query = {"query": {"match_all": {}}}
res = es.search(index=index_name, body=match_all_query)

console.print("\n[bold yellow]--- Match All Query Results ---[/bold yellow]")
for hit in res['hits']['hits']:
    pprint(hit["_source"])

# -----------------------------
# 4. Retrieve Index Mapping
# -----------------------------
console.print(f"\n[bold cyan]--- Mapping for Index '{index_name}' ---[/bold cyan]")
mapping_res = es.indices.get_mapping(index=index_name)
console.print_json(data=mapping_res.body)
