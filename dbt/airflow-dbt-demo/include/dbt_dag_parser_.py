from airflow.operators import BashOperator
from airflow.utils.task_group import TaskGroup
import json
import logging
import os


class DbtDagParser:
    """
    A utility class that parses out a dbt project and creates the respective task groups
    Args:
        dag: The Airflow DAG
        dbt_global_cli_flags: Any global flags for the dbt CLI
        dbt_project_dir: The directory containing the dbt_project.yml
        dbt_profiles_dir: The directory containing the profiles.yml
        dbt_target: The dbt target profile (e.g. dev, prod)
        dbt_tag: Limit dbt models to this tag if specified.
        dbt_run_group_name: Optional override for the task group name.
        dbt_test_group_name: Optional override for the task group name.
    """

    def __init__(self,
                 dag=None,
                 dbt_global_cli_flags=None,
                 dbt_project_dir=None,
                 dbt_profiles_dir=None,
                 dbt_target=None,
                 dbt_tag=None,
                 dbt_run_group_name='dbt_run',
                 dbt_test_group_name='dbt_test'
                 ):

        self.dag = dag
        self.dbt_global_cli_flags = dbt_global_cli_flags
        self.dbt_project_dir = dbt_project_dir
        self.dbt_profiles_dir = dbt_profiles_dir
        self.dbt_target = dbt_target
        self.dbt_tag = dbt_tag

        self.dbt_run_group = TaskGroup(dbt_run_group_name)
        self.dbt_test_group = TaskGroup(dbt_test_group_name)

        # Parse the manifest and populate the two task groups
        self.make_dbt_task_groups()

    def load_dbt_manifest(self):
        manifest_path = os.path.join(self.dbt_project_dir, 'target/manifest.json')
        with open(manifest_path) as f:
            content = json.load(f)

        return content['nodes']

    def make_dbt_task(self, node_name, dbt_verb):
        """
        Takes the manifest JSON content and returns a BashOperator task
        to run a dbt command.

        Args:
            node_name: The name of the node
            dbt_verb: 'run' or 'test'

        Returns: A BashOperator task that runs the respective dbt command

        """

        model_name = node_name.split('.')[-1]
        if dbt_verb == 'test':
            node_name = node_name.replace('model', 'test')  # Just a cosmetic renaming of the task
            task_group = self.dbt_test_group
        else:
            task_group = self.dbt_run_group

        command = f'\
            dbt {self.dbt_global_cli_flags} {dbt_verb} --target\
                {self.dbt_target} --models {model_name} --profiles-dir\
                {self.dbt_profiles_dir} --project-dir {self.dbt_project_dir}'

        dbt_task = BashOperator(
            task_id=node_name,
            task_group=task_group,
            bash_command=command,
            dag=self.dag,
        )
        # Keeping the log output, it's convenient to see when testing the python code outside of Airflow
        logging.info('Created task: %s', node_name)
        return dbt_task

    def make_dbt_task_groups(self)-> None:
        """
        Parse out a JSON file and populates the task groups with dbt tasks
        """
        manifest_json = self.load_dbt_manifest()
        dbt_tasks = {}

        # Create the tasks for each model
        for node_name in manifest_json.keys():
            if node_name.split('.')[0] == 'model':
                tags = manifest_json[node_name]['tags']
                # Only use nodes with the right tag, if tag is specified
                if not self.dbt_tag or (self.dbt_tag and self.dbt_tag in tags):
                    # Make the run nodes
                    dbt_tasks[node_name] = self.make_dbt_task(node_name, 'run')

                    # Make the test nodes
                    node_test = node_name.replace('model', 'test')
                    dbt_tasks[node_test] = self.make_dbt_task(node_name, 'test')

                    for upstream_node in manifest_json[node_name]['depends_on']['nodes']:
                        upstream_node_type = upstream_node.split('.')[0]
                        if upstream_node_type == 'model':
                            dbt_tasks[upstream_node] >> dbt_tasks[node_name]

    def get_dbt_run_group(self):
        """
        Getter method to retrieve the previously constructed dbt tasks.

        Returns: An Airflow task group with dbt run nodes.
        """
        return self.dbt_run_group

    def get_dbt_test_group(self):
        """
        Getter method to retrieve the previously constructed dbt tasks.

        Returns: An Airflow task group with dbt test nodes.
        """
        return self.dbt_test_group