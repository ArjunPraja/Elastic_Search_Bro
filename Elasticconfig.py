# from elasticsearch import Elasticsearch

# es = Elasticsearch(
#     "https://localhost:9200",
#     basic_auth=("elastic", "lKkPPbV*QMSHU-cFce3A"),
#     verify_certs=False  # just for local testing
# )

# print("Connected?", es.ping())






from elasticsearch import Elasticsearch

es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "lKkPPbV*QMSHU-cFce3A"),
    ca_certs=r"C:\Users\ArjunPrajapati\Downloads\elasticsearch-9.1.4-windows-x86_64\elasticsearch-9.1.4\config\certs\http_ca.crt"
)

print("Connected?", es.ping())
