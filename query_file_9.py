from Elasticconfig_file_1 import es
from rich.console import Console

console = Console()

# -----------------------------
# Helper function to display results as JSON
# -----------------------------
def display_json(title, hits):
    console.print(f"\n--- {title} ---")
    for hit in hits:
        console.print_json(data=hit["_source"])

# -----------------------------
# 1. Create Index and Insert Sample Data
# -----------------------------
schools_index = "schools"
if not es.indices.exists(index=schools_index):
    es.indices.create(index=schools_index)
    # Sample school data
    sample_schools = [
        {
            "_index": schools_index,
            "_id": "1",
            "_source": {
                "name": "Central School",
                "description": "CBSE Affiliation",
                "street": "Nagan",
                "city": "paprola",
                "state": "HP",
                "zip": "176115",
                "location": [31.8955385, 76.8380405],
                "fees": 2200,
                "tags": ["Senior Secondary", "beautiful campus"],
                "rating": "3.3"
            }
        },
        {
            "_index": schools_index,
            "_id": "2",
            "_source": {
                "name": "City Best School",
                "description": "ICSE",
                "street": "West End",
                "city": "Meerut",
                "state": "UP",
                "zip": "250002",
                "location": [28.9926174, 77.692485],
                "fees": 3500,
                "tags": ["fully computerized"],
                "rating": "4.5"
            }
        }
    ]
    for doc in sample_schools:
        es.index(index=doc["_index"], id=doc["_id"], body=doc["_source"])
    es.indices.refresh(index=schools_index)

# -----------------------------
# 2. Match All Query
# -----------------------------
match_all_query = {"query": {"match_all": {}}}
res = es.search(index=schools_index, body=match_all_query)
display_json("Match All Query", res['hits']['hits'])

# -----------------------------
# 3. Match Query
# -----------------------------
match_query = {"query": {"match": {"rating": "4.5"}}}
res = es.search(index=schools_index, body=match_query)
display_json("Match Query (rating=4.5)", res['hits']['hits'])

# -----------------------------
# 4. Multi-Match Query
# -----------------------------
multi_match_query = {"query": {"multi_match": {"query": "paprola", "fields": ["city", "state"]}}}
res = es.search(index=schools_index, body=multi_match_query)
display_json("Multi-Match Query (city/state='paprola')", res['hits']['hits'])

# -----------------------------
# 5. Query String Query
# -----------------------------
query_string_query = {"query": {"query_string": {"query": "beautiful"}}}
res = es.search(index=schools_index, body=query_string_query)
display_json("Query String Query (query='beautiful')", res['hits']['hits'])

# -----------------------------
# 6. Term Query
# -----------------------------
term_query = {"query": {"term": {"zip": "176115"}}}
res = es.search(index=schools_index, body=term_query)
display_json("Term Query (zip=176115)", res['hits']['hits'])

# -----------------------------
# 7. Range Query
# -----------------------------
range_query = {"query": {"range": {"rating": {"gte": 3.5}}}}
res = es.search(index=schools_index, body=range_query)
display_json("Range Query (rating >= 3.5)", res['hits']['hits'])

# -----------------------------
# 8. Exists Query
# -----------------------------
exists_query = {"query": {"exists": {"field": "rating"}}}
res = es.search(index=schools_index, body=exists_query)
display_json("Exists Query (field 'rating')", res['hits']['hits'])

# -----------------------------
# 9. Bool (Compound) Query
# -----------------------------
bool_query = {
    "query": {
        "bool": {
            "must": {"term": {"state": "UP"}},
            "filter": {"term": {"fees": "2200"}},
            "minimum_should_match": 1,
            "boost": 1.0
        }
    }
}
res = es.search(index=schools_index, body=bool_query)
display_json("Bool Query (state=UP, fees=2200)", res['hits']['hits'])

# -----------------------------
# 10. Geo Queries
# -----------------------------
geo_index = "geo_example"
if not es.indices.exists(index=geo_index):
    geo_mapping = {"mappings": {"properties": {"location": {"type": "geo_shape"}}}}
    es.indices.create(index=geo_index, body=geo_mapping)

geo_doc = {
    "name": "Chapter One, London, UK",
    "location": {"type": "point", "coordinates": [11.660544, 57.800286]}
}
es.index(index=geo_index, body=geo_doc, refresh=True)

geo_distance_query = {
    "query": {
        "geo_distance": {
            "distance": "100km",
            "location": {"lat": 57.8, "lon": 11.66}
        }
    }
}
res = es.search(index=geo_index, body=geo_distance_query)
display_json("Geo Distance Query (within 100km)", res['hits']['hits'])
