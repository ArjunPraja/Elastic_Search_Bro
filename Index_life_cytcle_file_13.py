from Elasticconfig_file_1 import es
from rich.console import Console
import json
from elasticsearch import exceptions

console = Console()

index_name = "my_index"
policy_name = "my_policy"

# -------------------------
# 1. Create / Update ILM Policy
# -------------------------
policy_body = {
    "policy": {
        "phases": {
            "hot": {
                "min_age": "0ms",
                "actions": {
                    "rollover": {"max_size": "50gb", "max_age": "30d"}
                }
            },
            "warm": {
                "min_age": "30d",
                "actions": {"forcemerge": {"max_num_segments": 1}}
            },
            "delete": {
                "min_age": "90d",
                "actions": {"delete": {}}
            }
        }
    }
}

try:
    response = es.ilm.put_lifecycle(name=policy_name, body=policy_body)
    console.print_json(json.dumps(response))
except exceptions.ElasticsearchException as e:
    console.print(f"[red]Error creating/updating policy:[/red] {e}")

# -------------------------
# 2. Assign Policy to Index (or Update Index Settings)
# -------------------------
try:
    if not es.indices.exists(index=index_name):
        resp = es.indices.create(
            index=index_name,
            body={
                "settings": {
                    "index.lifecycle.name": policy_name,
                    "index.lifecycle.rollover_alias": "my_index_alias"
                },
                "aliases": {"my_index_alias": {}}
            }
        )
        console.print_json(json.dumps(resp))
    else:
        resp = es.indices.put_settings(
            index=index_name,
            body={"index.lifecycle.name": policy_name}
        )
        console.print_json(json.dumps(resp))
except exceptions.ElasticsearchException as e:
    console.print(f"[red]Error assigning policy:[/red] {e}")

# -------------------------
# 3. Remove Policy from Index
# -------------------------
try:
    remove_response = es.ilm.remove_policy(index=index_name)
    console.print_json(json.dumps(remove_response))
except exceptions.ElasticsearchException as e:
    console.print(f"[red]Error removing policy:[/red] {e}")

# -------------------------
# 4. Move Index to a Lifecycle Step
# -------------------------
try:
    move_response = es.ilm.move_to_step(
        index=index_name,
        body={
            "current_step": {"phase": "hot", "action": "rollover", "name": "check-rollover-ready"},
            "next_step": {"phase": "hot", "action": "rollover", "name": "rollover"}
        }
    )
    console.print_json(json.dumps(move_response))
except exceptions.ElasticsearchException as e:
    console.print(f"[red]Error moving index to step:[/red] {e}")

# -------------------------
# 5. Retry Policy if Failed
# -------------------------
try:
    retry_response = es.ilm.retry(index=index_name)
    console.print_json(json.dumps(retry_response))
except exceptions.ElasticsearchException as e:
    console.print(f"[red]Error retrying ILM:[/red] {e}")

# -------------------------
# 6. Explain Lifecycle for Index
# -------------------------
try:
    explanation = es.ilm.explain(index=index_name)
    console.print_json(json.dumps(explanation))
except exceptions.ElasticsearchException as e:
    console.print(f"[red]Error explaining ILM:[/red] {e}")

# -------------------------
# 7. Start ILM
# -------------------------
try:
    start_response = es.ilm.start()
    console.print_json(json.dumps(start_response))
except exceptions.ElasticsearchException as e:
    console.print(f"[red]Error starting ILM:[/red] {e}")

# -------------------------
# 8. Stop ILM
# -------------------------
try:
    stop_response = es.ilm.stop()
    console.print_json(json.dumps(stop_response))
except exceptions.ElasticsearchException as e:
    console.print(f"[red]Error stopping ILM:[/red] {e}")

# -------------------------
# 9. Get ILM Status
# -------------------------
try:
    status = es.ilm.get_status()
    console.print_json(json.dumps(status))
except exceptions.ElasticsearchException as e:
    console.print(f"[red]Error fetching ILM status:[/red] {e}")

# -------------------------
# 10. Delete ILM Policy
# -------------------------
try:
    delete_response = es.ilm.delete_lifecycle(name=policy_name)
    console.print_json(json.dumps(delete_response))
except exceptions.ElasticsearchException as e:
    console.print(f"[red]Error deleting ILM policy:[/red] {e}")
