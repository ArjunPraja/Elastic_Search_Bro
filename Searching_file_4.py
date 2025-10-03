from Elasticconfig_file_1 import es 
from rich import print
from rich.console import Console
console=Console()




if __name__ == "__main__":
    print("This is the File Where We CAn Search the documetns in different ways ")

    #  Before Searching we need to create add the documents in the index
    # check if index is extists then no need to create else crearte
    #  Then insert the documents in the index 
    # for that i have create the products.py file where i have added the multiple documents in the index 
    #  so first run the products.py file then come back to this file to search the documents 






    # Get Document  By id 
    # Use get → when you want by ID only (fastest).
    # Use search → when you want to combine with other queries.

    # 1 ways Direct 
    try:
        res1=es.get(index="products",id="1")
        console.print_json(data=res1.body)
    except:
      print('An exception occurred')

    # 2 Through Search Query
    try:
        res2=es.search(index="products",body={"query":{"match":{"_id":"1"}}})
        console.print_json(data=res2.body)
    except:
      print('An exception occurred')








    # Search Documents By Match Query 

    # in All The Indeces
    try:
       res3=es.options(ignore_status=404).search(
        index="_all",
        body={
            "query":{
                "match":{
                    "category": "mobile"
                    }
                }
            }
       )

       console.print_json(data=res3.body)
         
    except Exception as e:
        print(f"Error performing search: {e}")







    #  Search in the Specific Index
    try:
      
      res4=es.options(ignore_status=404).search(
        index=["books", "products"],
        body={
           "query":{
              "match":{
                "name": "OnePlus 11"
              }
        }
        }
      )
      
      console.print_json(data=res4.body)
      console.print("This is the output of the res4 ")
    except:
      print('An exception occurred')
















    #    Search with q (Query String) :- Allows quick search without writing the full DSL.
    res = es.search(index="products", q="name:OnePlus 11")
    console.print_json(data=res.body)
    console.print("This is the output of the res   Q String ")





    # search with the . lenient:- True parameter to avoid errors due to data type mismatches.
    try:
       res=es.search(index="products",q="price:low",lenient=True)
       console.print_json(data=res.body)
       print("This is the output of the res lenient True ")
    except Exception as e:
        print(f"Error performing search: {e}")  








    # search with the fields :- Specify which fields to search in. 
    # With Sorting and Source Filtering
    # with timeout that can procide the time limite to complete the search operations 
    # with Terminates :- True parameter to stop searching after finding a certain number of documents to reduce load on the cluster.
    #  with from and size :- Pagination of results.

    res5=es.search(index=["books","products"],body={
       
          "_source":["name","price"],          # source filtering
    
          "query":{                             # match query
             "match":{
                 "category": "mobile"
             }
          }, 
          "sort": [{"price": "desc"}]            # sorting
          },
        from_=0,                         # pagination
        size=5,                          # pagination
        timeout="2s",                     # timeout
        terminate_after=10                 # terminate after 10 documents

       
    )
    console.print_json(data=res5.body)
    print("This is the output of the res5 fields ") 