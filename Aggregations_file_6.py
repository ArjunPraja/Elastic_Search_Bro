from Elasticconfig_file_1 import es 
from rich import print
from rich.console import Console
console=Console()




if __name__ == "__main__":
 
    print("This is the File Where We CAn do the Aggregations on the documents in different ways ")



    
    #This is the Only for checking purpose that documents are alreadt exitst or not in books indesx
    
    # res=es.search(index="books"
    #     ,body={
    #     "query":{
    #         "match_all":{}
    #     }
    # })
    # console.print_json(data=res.body)




    #  Metrics Aggregations: its used to perform mathematical calculations on your data.
    #  Example: Calculate the average price of products in a specific category.


    # Avg max min sum count stats extended_stats and meta property for prociding information about the aggregations 
    res1=es.options(ignore_status=404).search(index="products",body={
      "size":0,
        "aggs":{
            "average_price":{
                "avg":{
                    "field":"price"
                }
            },
            "min_price":{
                "avg":{
                    "field":"price"
                }
            },
            "max_price":{
                "avg":{
                    "field":"price"
                }
            },
            "total_price":{
                "avg":{
                    "field":"price"
                }
            },
            
            "price_Stats":{
                "extended_stats":{
                    "field":"price"
                },
                "meta": {
                "description": "This aggregation calculates total price of all products",
                "unit": "USD"
            }
            }
            
        }
    })
    console.print_json(data=res1.body)





