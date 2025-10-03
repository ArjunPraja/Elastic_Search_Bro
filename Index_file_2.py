from elasticsearch import Elasticsearch
from Elasticconfig_file_1 import es
from rich import print      
from rich.pretty import Pretty
from rich.console import Console







if __name__=="__main__":        
        
    
    console=Console()

    # ---------------------------------------Create a Index in Elastic Search With Specific Serring If Want The you Can Remove Body That Can create a index with default setting which is the single shared and single replicas-------------------------------------------------

    # es.indices.create(
    #     index="product",   # Name of the index
    #     body={              # Settings go inside body
    #         "settings": {
    #             "number_of_shards": 3,    # Primary shards
    #             "number_of_replicas": 1   # Replica shards
    #         }
    #     },
    #     ignore=400  # Ignore error if index already exists
    # )
    # print("Index Created Successfully")














    # ---------------------------------------Get All the Indices in the Elastic Search-------------------------------------------------
    indeces=es.indices.get(index="*")   # it will give the all the information about the indices
    console.print_json(data=indeces.body)

    # ---------------------------------------Get Specific Indices in the Elastic Search-------------------------------------------------
    indeces=es.indices.get(index="products")   # it will give the all the information about the indices
    console.print_json(data=indeces.body)





    # ---------------------------------------Delete a Index in Elastic Search-------------------------------------------------
    es.options(ignore_status=[400, 404]).indices.delete(index="product")
    # it will delete the index named product if exist otherwise it will ignore the error
    print("Index Deleted Successfully")







    