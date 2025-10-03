from Elasticconfig_file_1 import es
from rich.console import Console
from elasticsearch.exceptions import ApiError

console = Console()
INDEX_NAME = "my_index"
ALIAS_NAME = "my_index_alias"
POLICY_NAME = "my_policy"

# -------------------------
# 1. Create / Update ILM Policy
# -------------------------
def create_or_update_policy():
    policy_body = {
        "policy": {
            "phases": {
                "hot": {
                    "min_age": "0ms",
                    "actions": {"rollover": {"max_size": "50gb", "max_age": "30d"}}
                },
                "delete": {
                    "min_age": "90d",
                    "actions": {"delete": {}}
                }
            }
        }
    }
    try:
        response = es.ilm.put_lifecycle(name=POLICY_NAME, body=policy_body)
        console.print_json(data=response.body)
        console.print(f"[green]ILM policy '{POLICY_NAME}' created/updated successfully ✅[/green]")
    except ApiError as e:
        console.print(f"[red]Error creating/updating policy:[/red] {e}")

# -------------------------
# 2. Assign Policy to Index
# -------------------------
def assign_policy_to_index():
    try:
        if not es.indices.exists(index=INDEX_NAME):
            resp = es.indices.create(
                index=INDEX_NAME,
                body={
                    "settings": {
                        "index.lifecycle.name": POLICY_NAME,
                        "index.lifecycle.rollover_alias": ALIAS_NAME
                    },
                    "aliases": {ALIAS_NAME: {}}
                }
            )
            console.print_json(data=resp.body)
            console.print(f"[green]Index '{INDEX_NAME}' created and ILM policy assigned ✅[/green]")
        else:
            resp = es.indices.put_settings(
                index=INDEX_NAME,
                body={"index.lifecycle.name": POLICY_NAME}
            )
            console.print_json(data=resp.body)
            console.print(f"[green]ILM policy assigned to existing index '{INDEX_NAME}' ✅[/green]")
    except ApiError as e:
        console.print(f"[red]Error assigning policy to index:[/red] {e}")

# -------------------------
# 3. Remove Policy from Index
# -------------------------
def remove_policy():
    try:
        resp = es.ilm.remove_policy(index=INDEX_NAME)
        console.print_json(data=resp.body)
        console.print(f"[green]ILM policy removed from '{INDEX_NAME}' ✅[/green]")
    except ApiError as e:
        console.print(f"[red]Error removing policy:[/red] {e}")

# -------------------------
# 4. Move Index to a Lifecycle Step
# -------------------------
def move_index_to_step():
    try:
        resp = es.ilm.move_to_step(
            index=INDEX_NAME,
            body={
                "current_step": {"phase": "hot", "action": "rollover", "name": "check-rollover-ready"},
                "next_step": {"phase": "hot", "action": "rollover", "name": "rollover"}
            }
        )
        console.print_json(data=resp.body)
        console.print(f"[green]Index '{INDEX_NAME}' moved to next ILM step ✅[/green]")
    except ApiError as e:
        console.print(f"[red]Error moving index to step:[/red] {e}")

# -------------------------
# 5. Retry Policy if Failed
# -------------------------
def retry_ilm():
    try:
        resp = es.ilm.retry(index=INDEX_NAME)
        console.print_json(data=resp.body)
        console.print(f"[green]ILM retry triggered for '{INDEX_NAME}' ✅[/green]")
    except ApiError as e:
        console.print(f"[red]Error retrying ILM:[/red] {e}")

# -------------------------
# 6. Explain Lifecycle for Index
# -------------------------
def explain_lifecycle():
    try:
        resp = es.ilm.explain_lifecycle(index=INDEX_NAME)
        console.print_json(data=resp.body)
        console.print(f"[green]Lifecycle explanation fetched for '{INDEX_NAME}' ✅[/green]")
    except ApiError as e:
        console.print(f"[red]Error explaining ILM:[/red] {e}")

# -------------------------
# 7. Start ILM
# -------------------------
def start_ilm():
    try:
        resp = es.ilm.start()
        console.print_json(data=resp.body)
        console.print("[green]ILM started ✅[/green]")
    except ApiError as e:
        console.print(f"[red]Error starting ILM:[/red] {e}")

# -------------------------
# 8. Stop ILM
# -------------------------
def stop_ilm():
    try:
        resp = es.ilm.stop()
        console.print_json(data=resp.body)
        console.print("[green]ILM stopped ✅[/green]")
    except ApiError as e:
        console.print(f"[red]Error stopping ILM:[/red] {e}")

# -------------------------
# 9. Get ILM Status
# -------------------------
def get_ilm_status():
    try:
        resp = es.ilm.get_status()
        console.print_json(data=resp.body)
        console.print("[green]ILM status fetched ✅[/green]")
    except ApiError as e:
        console.print(f"[red]Error fetching ILM status:[/red] {e}")

# -------------------------
# 10. Delete ILM Policy
# -------------------------
def delete_policy():
    try:
        resp = es.ilm.delete_lifecycle(name=POLICY_NAME)
        console.print_json(data=resp.body)
        console.print(f"[green]ILM policy '{POLICY_NAME}' deleted ✅[/green]")
    except ApiError as e:
        console.print(f"[red]Error deleting ILM policy:[/red] {e}")


# =========================
# MAIN EXECUTION
# =========================
if __name__ == "__main__":
    print("\n\n\n\n\nFunction Name: Create_or_update_Policy_Output")
    create_or_update_policy()

    print("\n\n\n\n\nFunction Name: Assign_Policy_to_Index_Output")
    assign_policy_to_index()

    print("\n\n\n\n\nFunction Name: Explain_Lifecycle_Output")
    explain_lifecycle()

    print("\n\n\n\n\nFunction Name: Start_ILM_Output")
    start_ilm()

    print("\n\n\n\n\nFunction Name: Get_ILM_Status_Output")
    get_ilm_status()

    print("\n\n\n\n\nFunction Name: Retry_ILM_Output")
    retry_ilm()

    print("\n\n\n\n\nFunction Name: Move_Index_to_Step_Output")
    move_index_to_step()

    print("\n\n\n\n\nFunction Name: Stop_ILM_Output")
    stop_ilm()

    print("\n\n\n\n\nFunction Name: Remove_Policy_Output")
    remove_policy()

    print("\n\n\n\n\nFunction Name: Delete_Policy_Output")
    delete_policy()
