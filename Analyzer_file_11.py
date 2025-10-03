from Elasticconfig_file_1 import es
from rich.console import Console

console = Console()



# Run Code And Understand How Elasticsearch Anylyzer Works

# Practical Flow:- 

        # Input text → Analyzer

        # Analyzer → Tokenizer → splits into tokens

        # Tokens → Token Filters (lowercase, stopwords, stemming, etc.)

        # Indexed tokens stored in Elasticsearch → ready for search




# Flow Of Anlayzer

                                        # +------------------------+
                                        # |  Input Document         |
                                        # |------------------------|
                                        # | title: "Today's Weather |
                                        # | is Beautiful"           |
                                        # | description: "..."      |
                                        # | author: Alice           |
                                        # | publish_year: 2020      |
                                        # +-----------+------------+
                                                    # |
                                                    # v
                                        # +------------------------+
                                        # |    Analyzer             |
                                        # |------------------------|
                                        # | 1. Char Filters         |
                                        # # |    (remove unwanted)    |
                                        # | 2. Tokenizer            |
                                        # |    -> ["Today's",       |
                                        # |        "Weather", "is", |
                                        # |        "Beautiful"]     |
                                        # | 3. Token Filters        |
                                        # |    - lowercase          |
                                        # |    - stopwords removal  |
                                        # |    - max_token_length=5 |
                                        # |  Output Tokens:         |
                                        # |  ["today", "s", "weath",|
                                        # |   "er", "beaut", "iful"]|
                                        # +-----------+------------+
                                                    # |
                                                    # v
                                        # +------------------------+
                                        # |  Inverted Index         |
                                        # |------------------------|
                                        # | Token -> Document IDs    |
                                        # | "today" -> [1]           |
                                        # | "s" -> [1]               |
                                        # | "weath" -> [1]           |
                                        # | "er" -> [1]              |
                                        # | "beaut" -> [1]            |
                                        # | "iful" -> [1]            |
                                        # +-----------+------------+
                                        #             # |
                                        #             v
                                        # +------------------------+
                                        # |   Search Query Input    |
                                        # |------------------------|
                                        # | User types: "beautiful  |
                                        # | weather"               |
                                        # +-----------+------------+
                                        #             |
                                        #             v
                                        # +------------------------+
                                        # |    Analyzer (Query)     |
                                        # |------------------------|
                                        # | Same analyzer applied:  |
                                        # | Tokens -> ["beaut",     |
                                        # | "iful", "weath", "er"] |
                                        # +-----------+------------+
                                        #             |
                                        #             v
                                        # +------------------------+
                                        # |    Matching Engine      |
                                        # |------------------------|
                                        # | Matches query tokens    |
                                        # | with inverted index     |
                                        # | Scores & ranks results  |
                                        # +-----------+------------+
                                        #             |
                                        #             v
                                        # +------------------------+
                                        # |   Output Documents      |
                                        # |------------------------|
                                        # | Document ID: 1          |
                                        # | title: "Today's Weather |
                                        # | is Beautiful"           |
                                        # | description: "..."      |
                                        # | author: Alice           |
                                        # | publish_year: 2020      |
                                        # +------------------------+






















# -----------------------------
# 1. Create an index with custom analyzer
# -----------------------------
index_name = "analysis_example"

custom_mapping = {
    "settings": {
        "analysis": {
            "analyzer": {
                "my_english_analyzer": {
                    "type": "standard",
                    "max_token_length": 5,
                    "stopwords": "_english_"
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "title": {"type": "text", "analyzer": "my_english_analyzer"},
            "description": {"type": "text", "analyzer": "my_english_analyzer"},
            "author": {"type": "keyword"},
            "publish_year": {"type": "integer"}
        }
    }
}

# Create index (ignore if already exists)
es.indices.create(index=index_name, body=custom_mapping, ignore=400)
console.print(f"[bold green]Index '{index_name}' created with custom analyzer.[/bold green]")

# -----------------------------
# 2. Insert sample documents
# -----------------------------
docs = [
    {
        "title": "Today's Weather is Beautiful",
        "description": "A detailed analysis of weather patterns over the past decade.",
        "author": "Alice",
        "publish_year": 2020
    },
    {
        "title": "Beautiful Landscapes of Switzerland",
        "description": "Exploring the mountains and lakes of Switzerland.",
        "author": "Bob",
        "publish_year": 2018
    },
    {
        "title": "Understanding Climate Change",
        "description": "Impacts of global warming and environmental changes.",
        "author": "Charlie",
        "publish_year": 2021
    },
    {
        "title": "Python Programming Basics",
        "description": "Learn Python programming for beginners with practical examples.",
        "author": "David",
        "publish_year": 2019
    },
    {
        "title": "Advanced Machine Learning",
        "description": "In-depth exploration of ML algorithms and data science techniques.",
        "author": "Eve",
        "publish_year": 2022
    }
]

# Index the documents
for i, doc in enumerate(docs, start=1):
    es.index(index=index_name, id=i, body=doc)

console.print(f"[bold green]{len(docs)} sample documents inserted.[/bold green]")

# -----------------------------
# 3. Analyze a sample text
# -----------------------------
analyze_text = "Today's weather is beautiful"
analysis_result = es.indices.analyze(
    index=index_name,
    body={"analyzer": "my_english_analyzer", "text": analyze_text}
)

console.print("\n[bold blue]--- Analyzed Tokens ---[/bold blue]")
for token_info in analysis_result['tokens']:
    console.print(token_info)

# -----------------------------
# 4. Search using Match All query
# -----------------------------
match_all_query = {"query": {"match_all": {}}}
res = es.search(index=index_name, body=match_all_query)

console.print("\n[bold blue]--- Match All Query Results ---[/bold blue]")
for hit in res['hits']['hits']:
    console.print(hit["_source"])

# -----------------------------
# 5. Real-time interactive search
# -----------------------------
console.print(f"\n[bold green]Real-time Search on Index '{index_name}'[/bold green]")

while True:
    # Take input from user
    user_input = console.input("\n[bold yellow]Enter search text (or 'exit' to quit): [/bold yellow]")
    
    if user_input.lower() == "exit":
        console.print("[bold red]Exiting...[/bold red]")
        break

    # Elasticsearch query (multi_match for title and description)
    query = {
        "query": {
            "multi_match": {
                "query": user_input,
                "fields": ["title", "description"]
            }
        }
    }

    # Execute search
    res = es.search(index=index_name, body=query)

    # Print results
    hits = res['hits']['hits']
    if not hits:
        console.print("[bold red]No results found.[/bold red]")
    else:
        console.print(f"[bold blue]Found {len(hits)} results:[/bold blue]")
        for hit in hits:
            console.print(hit["_source"])
