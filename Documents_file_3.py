from Elasticconfig_file_1 import es 
from rich import print
from rich.console import Console
console=Console()



if __name__ == "__main__":

    print("This is the Docs Api That we can Use To perform Crud Operations in Documents in Elasticsearch")

    # first We are Creating The Index If Not Exists
    if not es.indices.exists(index="products"):
        es.indices.create(index="products",body={
            "settings":{
                "number_of_shards":3,
                "number_of_replicas":1}
        })
    else:
        print("index Already Exists")




    # Create Document 
    es.options(ignore_status=400).index(index="products", document={"title": "First Document", "content": "This is the content of the first document."})
    print("Document Created Successfully")




    # Get All Documetns And If You Dont Crete A Index At The Time Of Creating The  Documet then it can assign The Defauly document id 
    try:
        response = es.search(
            index="products",
            body={
                "query": {
                    "match_all": {}   # Matches all documents
                }
            },
            size=1000   # How many docs you want to fetch (default = 10)
        )

        console.print_json(data=response.body)

    except Exception as e:
        print(f"Error performing search: {e}")    






   





    # Update Docuements 
    es.options(ignore_status=404).update(index="products",id="6",doc={"doc":{"title":"Updated Document"}})
    print("Document Updated Successfully")






     #read Document
    docs=es.options(ignore_status=404).get(index="products",id="5")
    console.print_json(data=docs.body)
    print("Document Read Successfully")





    # Delete Document
    es.options(ignore_status=404).delete(index="products",id="6")
    print("Document Deleted Successfully")


    

    

     #read Document
    docs=es.options(ignore_status=404).get(index="products",id="5")
    console.print_json(data=docs.body)
    print("Document Read Successfully")











    