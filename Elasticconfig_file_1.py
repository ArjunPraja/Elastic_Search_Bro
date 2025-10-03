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
from rich import print     # rich is the library that can print the json data in the formate way so output  that look sexy 
from rich.pretty import Pretty # it is used to print the data in the pretty formate
from rich.console import Console   # it is used to print the data in the console formate
console=Console()   

es = Elasticsearch(     # here we are using the ca_certs to verify the certificate authority that can identify the request is secure or not
    "https://localhost:9200",
    basic_auth=("elastic", "lKkPPbV*QMSHU-cFce3A"),
    ca_certs=r"C:\Users\ArjunPrajapati\Downloads\elasticsearch-9.1.4-windows-x86_64\elasticsearch-9.1.4\config\certs\http_ca.crt"
)


print("Elastic Search Connected Successfully")

client_info = es.info()    # it will give the all the information about the elasticsearch server
 
console.print_json(data=client_info.body) # it will print the json data in the pretty formate

print("Connected?", es.ping())     # it will return true if the connection is successful otherwise it will return false







if __name__ == "__main__":
        















    clusturr_info = es.cluster.health()   # it will give the all the information about the cluster      
    console.print_json(data=clusturr_info.body)  # it will print the json data in the pretty formate
    print("Cluster Health", clusturr_info.body["status"])   # it will print the status of the cluster like green yellow red








    nodes=es.nodes.info()   # it will give the all the information about the nodes
    console.print_json(data=nodes.body)    


    indeces=es.indices.get(index="*")   # it will give the all the information about the indices
    console.print_json(data=indeces.body)





