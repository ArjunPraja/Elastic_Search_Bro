from Elasticconfig_file_1 import es
from rich import print
from rich.console import Console





# Ingest is Elasticsearch’s way to transform documents before they are actually stored (indexed).
# Instead of inserting raw data directly, you can pass it through an Ingest Pipeline.
# A pipeline is like a data preprocessing workflow.




console = Console()

if __name__ == "__main__":
    pipeline_id = "lowarcase_pipeline1"
    index_name = "test_index"

    try:
        # ====================================================
        # 1. Create the pipeline
        # ====================================================
        res = es.ingest.put_pipeline(
                id='lowarcase_pipeline1',
                description="A pipeline to lowercase the field 'message'",
                processors=[
                    {
                        "lowercase": {"field": "message"}
                    }
                ],
                on_failure=[
                    {
                        "set": {
                            "field": "messages",
                            "value": "Habibi This is the Failure"
                        }
                    }
                ]
            )

        print(res)
        print(f"Pipeline '{pipeline_id}' created ✅")

        # ====================================================
        # 2. Get pipeline details
        # ====================================================
        res = es.ingest.get_pipeline(id=pipeline_id)
        console.print_json(data=res.body)
        print(f"Pipeline '{pipeline_id}' fetched ✅")

        # ====================================================
        # 3. Simulate pipeline with WRONG field (to trigger on_failure)    
        # ====================================================
        # simulation is the method that can be used to test before applying the pipeline  on the actual index
        res = es.ingest.simulate(
            id=pipeline_id,
            docs=[
                {"_source": {"messag": "THIS IS A TEST MESSAGE"}}  # Wrong field (missing 'e')
            ]
        )
        console.print_json(data=res.body)
        print("Pipeline simulated ✅ (failure handler triggered if field missing)")




        # ====================================================
        # 4. Insert multiple docs with Bulk API and pipeline
        # ====================================================
        actions = [
            {"index": {"_index": index_name, "_id": "1", "pipeline": pipeline_id}},
            {"message": "HELLO WORLD"},  # valid → lowercase

            {"index": {"_index": index_name, "_id": "2", "pipeline": pipeline_id}},
            {"messag": "ELASTICSEARCH IS POWERFUL"},  # typo → failure handler adds "messages"

            {"index": {"_index": index_name, "_id": "3", "pipeline": pipeline_id}},
            {"message": "INGEST PIPELINES ARE USEFUL"},  # valid → lowercase
        ]

        res = es.bulk(operations=actions, refresh=True)
        console.print_json(data=res.body)
        print("Bulk insert completed ✅ with pipeline applied")





        # ====================================================
        # 5. Fetch all docs to verify pipeline worked
        # ====================================================
        res = es.search(index=index_name, query={"match_all": {}})
        console.print_json(data=res.body)
        print(f"Indexed documents fetched ✅ from '{index_name}'")

    except Exception as e:
        print(f"[bold red]❌ Error occurred: {e}[/bold red]")
