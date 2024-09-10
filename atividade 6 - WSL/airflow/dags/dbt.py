import os
import json
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

HOME = os.environ["HOME"] # retrieve the location of your home folder
dbt_path = os.path.join(HOME, "/mnt/c/Users/teu20/Documents/Poli/Repo/eEDB011-20243/atividade 6 - WSL/dbt_project/bancos_airflow") # path to your dbt project
manifest_path = os.path.join(dbt_path, "target/manifest.json") # path to manifest.json

with open(manifest_path) as f: # Open manifest.json
  manifest = json.load(f) # Load its contents into a Python Dictionary
  nodes = manifest["nodes"] # Extract just the nodes

# Build an Airflow DAG
with DAG(
  dag_id="dbt_example", # The name that shows up in the UI
  start_date=pendulum.today(), # Start date of the DAG
  catchup=False,
) as dag:

  # Create a dict of Operators
  dbt_tasks = dict()
  for node_id, node_info in nodes.items():
      dbt_tasks[node_id] = BashOperator(
          task_id=".".join(
              [
                  node_info["resource_type"],
                  node_info["package_name"],
                  node_info["name"],
              ]
          ),
          bash_command=f"cd {dbt_path}" # Go to the path containing your dbt project
          + ' && eval "\$(pyenv init -)"' # Load Pyenv
          + ' && eval "\$(pyenv virtualenv-init -)"' # Load Pyenv Virtualenv
          + " && pyenv activate dbt_env" # Activate the dbt virtual environment
          + f" && dbt run --models {node_info['name']}", # run the model!
      )

  # Define relationships between Operators
  # for node_id, node_info in nodes.items():
  #     upstream_nodes = node_info["depends_on"]["nodes"]
  #     if upstream_nodes:
  #         for upstream_node in upstream_nodes:
  #             dbt_tasks[upstream_node] >> dbt_tasks[node_id]

if __name__ == "__main__":
  dag.cli()