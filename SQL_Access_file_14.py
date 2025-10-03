from Elasticconfig_file_1 import es
from rich.console import Console
from rich.table import Table
 
console = Console()



# Important Note :- Basically here we CAn Able to Use The Sql Like Query langauge 













# -------------------------------
# 1. Bulk insert documents
# -------------------------------
docs = [
    {"_id": "CBSE", "name": "GleanDale", "Address": "JR. Court Lane", "start_date": "2011-06-02", "student_count": 561},
    {"_id": "ICSE", "name": "Top-Notch", "Address": "Gachibowli Main Road", "start_date": "1989-05-26", "student_count": 482},
    {"_id": "State Board", "name": "Sunshine", "Address": "Main Street", "start_date": "1965-06-01", "student_count": 604},
]

actions = []
for doc in docs:
    actions.append({"index": {"_index": "schoollist", "_id": doc["_id"]}})
    actions.append({k: v for k, v in doc.items() if k != "_id"})

res = es.bulk(operations=actions, refresh=True)
console.print_json(data=res.body)
console.print("[green]Bulk insert completed[/green]\n\n")

# -------------------------------
# 2. Perform SQL query
# -------------------------------
sql_query = {
    "query": "SELECT name, Address, start_date, student_count FROM schoollist WHERE start_date < '1990-01-01'"
}

res_sql = es.sql.query(body=sql_query, format="json")  # format=json returns dict

# -------------------------------
# 3. Display results in table
# -------------------------------
columns = [col["name"] for col in res_sql["columns"]]
rows = res_sql["rows"]

table = Table(title="School List (start_date < 1990-01-01)")
for col in columns:
    table.add_column(col, justify="center")

for row in rows:
    table.add_row(*[str(item) for item in row])

console.print(table)
