# from elasticsearch import Elasticsearch


# ---------------------------This is the not secure one because here it can not varify the cetificate that can identify that the request comming is secure or not 


# es = Elasticsearch(
#     "https://localhost:9200",
    # basic_auth=("elastic", "lKkPPbV*QMSHU-cFce3A"),        
#     verify_certs=False  # just for local testing
# )

# print("Connected?", es.ping())




#----------------------------- Here We Are  checking  the ca_certs autorithy that can check the requests is commit are the secure or not 

from elasticsearch import Elasticsearch

es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "lKkPPbV*QMSHU-cFce3A"),
    ca_certs=r"C:\Users\ArjunPrajapati\Downloads\elasticsearch-9.1.4-windows-x86_64\elasticsearch-9.1.4\config\certs\http_ca.crt"
)

print("Connected?", es.ping())
