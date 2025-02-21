# @package bstglobal.orchestration.datapipeline.airflow
from typing import Dict, Iterator
from datetime import datetime, timedelta
import inflection
import requests
from requests import Response

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import BaseOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow import settings
from airflow.models import Connection

from bstglobal.orchestration.datapipeline.airflow.dagfactory import DagFactory
from bstglobal.orchestration.core.models import SourceDatabaseItem, DataWarehouseTarget_WorkflowStep, MachineLearning_WorkflowStep
from bstglobal.orchestration.datapipeline.airflow.operators import (CopyFromDataLakeToAzureSQLOperator, CheckForStagingBatchBranchOperator, 
    StartStagingBatchOperator, StartDWBatchOperator, EndStagingBatchOperator, EndDWBatchOperator, CheckForDWBatchBranchOperator,
    CheckCurrentDWBatchTypeBranchOperator, ExecuteDWWorkflowStepOperator, TriggerPipelineOperator, CheckNextDWBatchTypeBranchOperator)
from bstglobal.orchestration.core.models import BSTClient
from bstglobal.orchestration.datapipeline.airflow.exceptions import PipelineBuildException
from bstglobal.orchestration.datapipeline.airflow.operators import EndMLPipelineOperator, ExecutePreMLStoredProcedureStepOperator, ExecutePostMLStoredProcedureStepOperator, StartMLPipelineOperator
from bstglobal.orchestration.datapipeline.airflow.operators import EndMLPipelineStepOperator, StartMLPipelineStepOperator

class DataLakeToStagingPipeline:
    """
    Defines the tasks and order of task execution for all tasks that make up the "Data Lake to Staging" data pipeline.
    This data pipeline copies data from the data lake into the staging tables inside of the data warehouse and then calls a downstream
    pipeline asynchronously to load/update the data warehouse with data from the staging tables.
    @param: client: The BST client
    @param: pipeline_id: A unique identifier used to identify the data pipeline
    @param: trigger_dw_pipeline_id: A unique identifier used to identify the downstream data pipeline to load the data warehouse.
    @param: source_items: A list of `SourceDatabaseItem` (source tables) whose data needs to be copied from the data lake to the
    data warehouse initially and as changes occur.
    @param: pipeline_tags: Labels to associate with the data pipeline to distinguish the data pipeline from other data pipelines.  
    Tags are useful in the airflow UI to quickly find a data pipeline(s) by filtering by labels assigned to them.
    """
    
    CHECK_FOR_STAGING_BATCH_TASK_ID = 'check_for_staging_batch'
    START_STAGING_BATCH_TASK_ID = 'start_staging_batch'
    END_STAGING_BATCH_TASK_ID = 'end_staging_batch'
    STAGING_COMPLETE_TASK_ID = 'staging_complete'
    END_COPY_TASK_ID = 'end_copy'
    START_PIPELINE_TASK_ID = 'start_pipeline'
    END_PIPELINE_TASK_ID = 'end_pipeline'
    TASK_ID_PREFIX = 'copy_'
    COPY_FILES_TASKGROUP_NAME = 'copy_files_to_staging'
    TRIGGER_LOAD_DW_TASK_ID = 'trigger_load_dw_pipeline'
    
    def __init__( self, client: BSTClient, pipeline_id: str, trigger_dw_pipeline_id: str, source_items: Iterator[SourceDatabaseItem], 
                pipeline_tags: Iterator[str] = None):
        self.client = client
        self.pipeline_id = pipeline_id
        self.source_items = source_items
        self.pipeline_tags = pipeline_tags
        self.trigger_dw_pipeline_id = trigger_dw_pipeline_id
        # The default args that are appropriate for this particular pipeline.
        self.default_args = {
            'owner':'airflow',
            'depends_on_past': False,
            'start_date': datetime(2022,1,1),
            'email_on_failure': False,
            'email_on_success': False,
            'retries': 2,
            'retry_delay': timedelta(seconds=30),
            'retry_exponential_backoff': True,
            'max_active_tis_per_dag': 1  #the maximum number of times that the same task can run concurrently across all DAG Runs
        }
 
    def build(self) -> DAG:
        """
        Builds a data pipeline for a client using a pre-defined template that retrieves data changes stored in the 
        data lake and runs all the processes needed to apply those changes to the client's data warehouse.
        """
        # use the dag factory to build an empty dag, then add tasks to the dag
        dag = DagFactory.build_dag(dag_id=self.pipeline_id, client_code=self.client.client_code, 
                default_args=self.default_args, tags=self.pipeline_tags)

        try:

            # An anchor for identifying the start of the pipeline that downstream tasks can refer to.
            start_pipeline = DummyOperator(
                task_id = self.START_PIPELINE_TASK_ID,
                dag=dag
            )

            # Determine if there is work to do for this workflow or not. Branch according to the results received. 
            check_for_staging_batch = CheckForStagingBatchBranchOperator(
                task_id = self.CHECK_FOR_STAGING_BATCH_TASK_ID, 
                client = self.client,
                true_task_id = self.START_STAGING_BATCH_TASK_ID,
                false_task_id = self.STAGING_COMPLETE_TASK_ID,
                dag=dag
            )

            staging_complete = DummyOperator(
                task_id = self.STAGING_COMPLETE_TASK_ID,
                trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                dag=dag
            )

            # Log the start of the batch
            start_staging_batch = StartStagingBatchOperator(
                task_id = self.START_STAGING_BATCH_TASK_ID, 
                client = self.client,
                dag=dag
            )

            # Provides a downstream task (anchor) for all copy tasks.
            end_copy = DummyOperator(
                task_id = self.END_COPY_TASK_ID,
                dag=dag
            )

            # Define the execution order of the tasks defined thus far:
            # If there is work to do, then start the staging batch otherwise staging is complete.
            start_pipeline >> check_for_staging_batch >> [start_staging_batch, staging_complete]

            # Place all copy tasks in a task group as a logical unit.  They all run in parallel, with no 
            # dependencies between each other.
            # note: task groups can have their own default_args that override dag, such as default_args={'pool':'file_copy_pool'}
            copy_files_taskgroup = TaskGroup(group_id=self.COPY_FILES_TASKGROUP_NAME, prefix_group_id=False, dag=dag)

            # Each source item (e.g. source table) needs a copy task.  Airflow works best with static workflows.
            for source_item in self.source_items:

                copy_file = CopyFromDataLakeToAzureSQLOperator(
                        task_id = f"{self.TASK_ID_PREFIX}{source_item.Name}",
                        source_item = source_item,
                        client=self.client,
                        task_group = copy_files_taskgroup,
                        dag=dag
                    )

                # Each copy task occurs after the start of the batch and then
                # is followed by the end_copy anchor task.
                start_staging_batch >> copy_file >> end_copy
            

            # Once all the copy tasks are complete, the 'DataLake to Staging' process
            # can be reported as completed.
            end_staging_batch = EndStagingBatchOperator(
                task_id = self.END_STAGING_BATCH_TASK_ID, 
                client = self.client,
                dag=dag
            )

            # trigger the downstream pipeline to load the data warehouse
            trigger_load_dw_pipeline = TriggerPipelineOperator(
                task_id = self.TRIGGER_LOAD_DW_TASK_ID,
                trigger_dag_id= self.trigger_dw_pipeline_id,
                wait_for_completion = False,
                dag=dag
            )

            # An anchor task for the end that any upstream tasks can reference.
            end_pipeline = DummyOperator(
                task_id = self.END_PIPELINE_TASK_ID,
                # trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                dag=dag
            )

            # Define the downstream tasks after the copy is complete.
            end_copy >> end_staging_batch >> staging_complete 
            # After staging is complete, call the downstream pipeline, then end.
            staging_complete >> trigger_load_dw_pipeline >> end_pipeline

        except Exception as ex:
            raise PipelineBuildException(f"An issue occurred when attempting to construct a 'DataLakeToStagingPipeline' pipeline for {self.client.client_code}") from ex

        return dag


class TriggerLoadDataWarehousePipeline:
    """
    Defines the tasks and order of task execution for all tasks that make up the data pipeline that determines which of the two
    data pipelines to call to load the data warehouse based on the batch type to be processed: Snapshot or Change batch. Only one
    of the pipelines can execute at a given time so the chosen data pipeline is executed synchronously to ensure it completes before
    another data pipeline that loads the DW can be triggered.
    @param: client: The BST client
    @param: pipeline_id: A unique identifier used to identify the data pipeline
    @param: dw_snapshot_pipeline_id: A unique identifier used to identify the downstream data pipeline to load the data warehouse
    if the batch type is 'Snapshot'.
    @param: dw_changes_pipeline_id: A unique identifier used to identify the downstream data pipeline to load the data warehouse
    if the batch type is 'Changes'.
    @param: pipeline_tags: Labels to associate with the data pipeline to distinguish the data pipeline from other data pipelines.  
    Tags are useful in the airflow UI to quickly find a data pipeline(s) by filtering by labels assigned to them.
    """

    START_PIPELINE_TASK_ID = 'start_pipeline'
    END_PIPELINE_TASK_ID = 'end_pipeline'
    CHECK_NEXT_LOAD_DW_BATCH_TYPE_TASK_ID = 'check_next_load_dw_batch_type'
    TRIGGER_LOAD_DW_SNAPSHOT_TASK_ID = 'trigger_load_dw_snapshot_pipeline'
    TRIGGER_LOAD_DW_CHANGES_TASK_ID = 'trigger_load_dw_changes_pipeline'
    NO_LOAD_DW_BATCH_TO_PROCESS_TASK_ID = 'no_load_dw_batch_to_process'
    
    def __init__( self, client: BSTClient, pipeline_id: str, dw_snapshot_pipeline_id: str, dw_changes_pipeline_id: str, 
                pipeline_tags: Iterator[str] = None
                ):
        self.client = client
        self.pipeline_id = pipeline_id
        self.pipeline_tags = pipeline_tags
        self.dw_snapshot_pipeline_id = dw_snapshot_pipeline_id
        self.dw_changes_pipeline_id = dw_changes_pipeline_id
        # The default args that are appropriate for this particular pipeline.
        self.default_args = {
            'owner':'airflow',
            'depends_on_past': False,
            'start_date': datetime(2022,1,1),
            'email_on_failure': False,
            'email_on_success': False,
            'retries': 2,
            'retry_delay': timedelta(seconds=30),
            'retry_exponential_backoff': True,
            'max_active_tis_per_dag': 1  #the maximum number of times that the same task can run concurrently across all DAG Runs
        }

    def build(self) -> DAG:
        """
        Builds a data pipeline for a client using a pre-defined template that retrieves data changes stored in the 
        data lake and runs all the processes needed to apply those changes to the client's data warehouse.
        """
        # use the dag factory to build an empty dag, then add tasks to the dag
        dag = DagFactory.build_dag(dag_id=self.pipeline_id, client_code=self.client.client_code, 
                default_args=self.default_args, tags=self.pipeline_tags)

        try:

            # An anchor for identifying the start of the pipeline that downstream tasks can refer to.
            start_pipeline = DummyOperator(
                task_id = self.START_PIPELINE_TASK_ID,
                dag=dag
            )
    
            # branches based on the batch type
            check_next_load_dw_batch_type = CheckNextDWBatchTypeBranchOperator(
                task_id = self.CHECK_NEXT_LOAD_DW_BATCH_TYPE_TASK_ID,
                client = self.client,
                snapshot_task_id=self.TRIGGER_LOAD_DW_SNAPSHOT_TASK_ID,
                changes_task_id = self.TRIGGER_LOAD_DW_CHANGES_TASK_ID, 
                none_task_id=self.NO_LOAD_DW_BATCH_TO_PROCESS_TASK_ID ,
                dag=dag
            )

            trigger_load_dw_snapshot_pipeline = TriggerPipelineOperator(
                task_id = self.TRIGGER_LOAD_DW_SNAPSHOT_TASK_ID,
                trigger_dag_id= self.dw_snapshot_pipeline_id,
                wait_for_completion = True,
                dag=dag
            )

            # The pipeline needs to be triggered, and this pipeline needs to wait for completion
            # to ensure that no other DW Load pipelines are triggered and therefore run at the same time.
            trigger_load_dw_changes_pipeline = TriggerPipelineOperator(
                task_id = self.TRIGGER_LOAD_DW_CHANGES_TASK_ID,
                trigger_dag_id= self.dw_changes_pipeline_id,
                wait_for_completion = True,  
                dag=dag
            )

            no_load_dw_batch_to_process = DummyOperator(
                task_id = self.NO_LOAD_DW_BATCH_TO_PROCESS_TASK_ID ,
                dag=dag
            )

            # An anchor task for the end that any upstream tasks can reference.
            end_pipeline = DummyOperator(
                task_id = self.END_PIPELINE_TASK_ID,
                trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                dag=dag
            )

            start_pipeline >> check_next_load_dw_batch_type
            check_next_load_dw_batch_type >> [trigger_load_dw_snapshot_pipeline, trigger_load_dw_changes_pipeline, no_load_dw_batch_to_process]
            trigger_load_dw_snapshot_pipeline  >> end_pipeline
            trigger_load_dw_changes_pipeline  >> end_pipeline
            no_load_dw_batch_to_process >> end_pipeline

        except Exception as ex:
            raise PipelineBuildException(f"An issue occurred when attempting to construct a 'DataLakeToStagingPipeline' pipeline for {self.client.client_code}") from ex

        return dag


class LoadDataWarehousePipeline:
    """
    Defines the tasks and order of task execution for all tasks that make up the "Load Data Warehouse" data pipeline.
    This data pipeline loads/updates the data warehouse with data from the staging tables.
    @param: client: The BST client
    @param: pipeline_id: A unique identifier used to identify the data pipeline
    @param: load_dw_metadata: Metadata that defines the tasks to be orchestrated in order to accomplish
    the Load DW workflow.
    @param: pipeline_tags: Labels to associate with the data pipeline to distinguish the data pipeline from other data pipelines.  
    Tags are useful in the airflow UI to quickly find a data pipeline(s) by filtering by labels assigned to them.
    """
    
    START_PIPELINE_TASK_ID = 'start_pipeline'
    END_PIPELINE_TASK_ID = 'end_pipeline'
    CHECK_FOR_DW_BATCH_TASK_ID = 'check_for_dw_batch'
    NO_DW_WORK_TO_DO_TASK_ID = 'no_dw_work_to_do'
    START_DW_BATCH_TASK_ID = 'start_dw_batch'
    CHECK_DW_BATCH_TYPE_TASK_ID = 'check_dw_batch_type'
    SNAPSHOT_BATCH_TASK_ID = 'snapshot_batch'
    CHANGES_BATCH_TASK_ID = 'changes_batch'
    END_SNAPSHOT_BATCH_TASK_ID = 'end_snapshot_batch'
    END_CHANGES_BATCH_TASK_ID = 'end_changes_batch'
    END_DW_BATCH_TASK_ID = 'end_dw_batch'
    
    
    def __init__( self, client: BSTClient, pipeline_id: str, load_dw_metadata: Iterator[DataWarehouseTarget_WorkflowStep], 
                pipeline_tags: Iterator[str] = None):
        self.client = client
        self.pipeline_id = pipeline_id
        if len(load_dw_metadata) == 0:
            raise Exception(ValueError("Data Warehouse Target Data is required to build the pipeline. \
                Verify DataWarehouseTarget_WorkflowStep table is loaded."))
        self.load_dw_metadata = load_dw_metadata
        self.pipeline_tags = pipeline_tags
        # The default args that are appropriate for this particular pipeline.
        self.default_args = {
            'owner':'airflow',
            'depends_on_past': False,
            'start_date': datetime(2022,1,1),
            'email_on_failure': False,
            'email_on_success': False,
            'retries': 2,
            'retry_delay': timedelta(seconds=30),
            'retry_exponential_backoff': True,
            'max_active_tis_per_dag': 1  #the maximum number of times that the same task can run concurrently across all DAG Runs
        }

    def create_groups_of_target_collections(self, batch_type:str) -> Dict[str,Iterator]:
        """ Iterates through the rows of metadata representing data warehouse load steps and dependencies and creates
        a dictionary containing each load group, where a group represents workflow steps that need to occur in sequence.
        The group name is the key and the value is a collection of target collections by load order.  The target collection
        itself contains target sequences, which is workflow steps per target.
        @param: batch_type: The batch type of the load steps to build. Any metadata not of that type will be skipped.
        """
        # First, iterate through the rows of metadata and build collections that facilitate creating tasks and their dependencies. 
        target_sequence = []  # A collection of workflow steps required to load a single target.
        load_order_targets = [] # A collection of target_sequences for targets having the same load order.
        load_sequence = [] # A collection of load order targets belonging to the same group.
        groups = {} # Contains each workflow group (key) and it's load_sequence (value).
        # the build is specific to a batch type, so skip any metadata not belonging to the batch_type specified.
        metadata = [row for row in self.load_dw_metadata if row.BatchType == batch_type]
        for index, row in enumerate(metadata):
            if index > 0:
                if metadata[index-1].TargetName != row.TargetName:
                    load_order_targets.append(target_sequence)
                    target_sequence = []
                if (metadata[index-1].TargetLoadOrder != row.TargetLoadOrder or
                        metadata[index-1].GroupName != row.GroupName):
                    load_sequence.append(load_order_targets)
                    load_order_targets = []
                if metadata[index-1].GroupName not in groups.keys():
                    groups[metadata[index-1].GroupName] = load_sequence
                if  metadata[index-1].GroupName != row.GroupName:
                    load_sequence = []
            target_sequence.append(row)
        # finish processing the last row
        load_order_targets.append(target_sequence)
        load_sequence.append(load_order_targets)
        if row.GroupName not in groups.keys():
            groups[row.GroupName] = load_sequence
        return groups

    def build_load_dw_tasks(self, dag:DAG, batch_type:str, predecessor_task:BaseOperator, successor_task:BaseOperator):
        """ Iterates through the rows of metadata representing data warehouse load steps and dependencies and creates
        the airflow TaskGroups, Tasks, and dependencies between tasks and adds them to the `dag`.
        @param: dag: The dag in which to add the airflow objects created.
        @param: batch_type: The batch type of the load steps to build. Any metadata not of that type will be skipped.
        @param: predecessor_task: The task to set as the task immediately upstream of the load steps.
        @param: successor_task: The task to set as the task immediately downstream of the load steps.
        """
        groups = self.create_groups_of_target_collections(batch_type=batch_type)

        # Now that we have the targets and their sequence of steps separated by load order and assigned to a
        # group, we can build the TaskGroups and Operators and set the dependencies.
        task_groups = []
        # A load_sequence contains collections of targets, divided up by load order.
        for key, load_sequence in groups.items():  
            # each group needs a taskgroup
            task_group_name = f"{key}_{batch_type}".replace(' ', '_')
            task_group = TaskGroup(group_id=task_group_name, dag=dag)
            task_groups.append(task_group)
            previous_load_order_task = None # reset for each group
            # For the targets having the same load order:
            for load_order_targets in load_sequence:
                # Create a task to serve as a downstream task that begins when all targets complete.
                target_order = load_order_targets[0][0].TargetLoadOrder
                target_load_order_task = DummyOperator(
                    task_id = f"{target_order}_priority_end",
                    task_group = task_group,
                    dag = dag
                )
                # Save head and tail tasks of each target's load steps, to use for dependency hookup between tasks.
                head_tasks = []
                tail_tasks = []
                # for each target, which may have a sequence of steps:
                for target_sequence in load_order_targets:  
                    previous_task = None
                    # for each workflow step in the target sequence, create a task and hook them together in sequence
                    for step_index, workflow_step in enumerate(target_sequence):
                        task_name = f"{inflection.underscore(workflow_step.WorkflowStepName.replace(' ', ''))}__{workflow_step.TargetTableName}"
                        workflow_task = ExecuteDWWorkflowStepOperator(
                                task_id = task_name,
                                client=self.client,
                                target_workflow_step=workflow_step,
                                task_group = task_group,
                                dag = dag
                            )
                        if previous_task:
                            previous_task >> workflow_task
                        previous_task = workflow_task
                        if step_index == 0:
                            head_tasks.append(workflow_task)
                    # The last step in the sequence for the task is associated with the load order task
                    workflow_task >> target_load_order_task 
                    tail_tasks.append(workflow_task)
                # If the targets cannot run in parallel, then create a dependency between targets such that the tail task
                # of the prior target depends on the head task of the following target.
                if not workflow_step.CanRunInParallel:
                    for index, head_task in enumerate(head_tasks[1:]):
                        head_task << tail_tasks[index]
                # If there are multiple load orders in the group, then the prior load order task needs to be a dependency to the 
                # first step in the next target's sequence.        
                if previous_load_order_task:
                    for task in head_tasks:
                        previous_load_order_task >> task
                previous_load_order_task = target_load_order_task  
        # Now make the task groups dependent.
        for index,task_group in enumerate(task_groups):
            if index == 0:
                predecessor_task >> task_group
            else:
                task_groups[index-1] >> task_group
        task_group >> successor_task

    def build(self):
        raise NotImplementedError("Each subclass of 'LoadDataWarehousePipeline' must implement its own build method.")

 
class LoadDataWarehouseSnapshotPipeline(LoadDataWarehousePipeline):
    """
    Defines the tasks and order of task execution for all tasks that make up the "Load Data Warehouse" data pipeline for batches of
    type 'Snapshot'.  This data pipeline loads/updates the data warehouse with data from the staging tables.
    @param: client: The BST client
    @param: pipeline_id: A unique identifier used to identify the data pipeline
    @param: load_dw_metadata: Metadata that defines the tasks to be orchestrated in order to accomplish
    the Load DW workflow.
    @param: pipeline_tags: Labels to associate with the data pipeline to distinguish the data pipeline from other data pipelines.
    Tags are useful in the airflow UI to quickly find a data pipeline(s) by filtering by labels assigned to them.
    """
    
    def __init__( self, client: BSTClient, pipeline_id: str, load_dw_metadata: Iterator[DataWarehouseTarget_WorkflowStep],
                pipeline_tags: Iterator[str] = None):
        super().__init__(client=client, pipeline_id=pipeline_id, load_dw_metadata = load_dw_metadata, pipeline_tags=pipeline_tags)
        self.load_dw_metadata = [i for i in load_dw_metadata if i.BatchType == 'Snapshot']

    def build(self) -> DAG:
        """
        Builds a data pipeline for a client using a pre-defined template that retrieves data changes stored in the
        data lake and runs all the processes needed to apply those changes to the client's data warehouse.
        """
        # use the dag factory to build an empty dag, then add tasks to the dag
        dag = DagFactory.build_dag(dag_id=self.pipeline_id, client_code=self.client.client_code,
                default_args=self.default_args, tags=self.pipeline_tags)

        try:

            # An anchor for identifying the start of the pipeline that downstream tasks can refer to.
            start_pipeline = DummyOperator(
                task_id = self.START_PIPELINE_TASK_ID,
                dag=dag
            )

            # An anchor task for the end that any upstream tasks can reference.
            end_pipeline = DummyOperator(
                task_id = self.END_PIPELINE_TASK_ID,
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,  
                dag=dag
            )

            # Determine if there is work to do for DW workflow or not. Branch according to the results received.
            check_for_dw_batch = CheckForDWBatchBranchOperator(
                task_id = self.CHECK_FOR_DW_BATCH_TASK_ID, 
                client = self.client,
                true_task_id = self.CHECK_DW_BATCH_TYPE_TASK_ID,
                false_task_id = self.END_PIPELINE_TASK_ID,
                dag=dag
            )

            # branches based on the batch type
            check_batch_type = CheckCurrentDWBatchTypeBranchOperator(
                task_id = self.CHECK_DW_BATCH_TYPE_TASK_ID,
                client = self.client,
                snapshot_task_id=self.SNAPSHOT_BATCH_TASK_ID,
                changes_task_id = self.END_PIPELINE_TASK_ID, 
                dag=dag
            )

            # Log the start of the DW batch
            start_dw_batch = StartDWBatchOperator( 
                task_id = self.START_DW_BATCH_TASK_ID, 
                client = self.client,
                dag=dag
            )

            # Target of branch operator for snapshot batch
            snapshot_batch = DummyOperator(
                task_id = self.SNAPSHOT_BATCH_TASK_ID,
                dag=dag
            )

            # Once all the snapshot/changes tasks are complete, the 'Load DW' process
            # can be reported as completed.
            end_dw_batch = EndDWBatchOperator(   
                task_id = self.END_DW_BATCH_TASK_ID, 
                client = self.client,
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                dag=dag
            )

            start_pipeline >> check_for_dw_batch >> [check_batch_type, end_pipeline]
            check_batch_type >> [snapshot_batch, end_pipeline]
            snapshot_batch >> start_dw_batch
            end_dw_batch >> end_pipeline

        except Exception as ex:
            raise PipelineBuildException(f"An issue occurred when attempting to construct a 'LoadDataWarehouseSnapshotPipeline' pipeline for {self.client.client_code}") from ex

        try:

            if len(self.load_dw_metadata) > 0:
                self.build_load_dw_tasks(dag=dag, batch_type='Snapshot', predecessor_task=start_dw_batch, 
                            successor_task=end_dw_batch)
            else:
                raise PipelineBuildException(f"An issue occurred when constructing the 'LoadDataWarehouseSnapshotPipeline' pipeline; metadata not found for client {self.client.client_code}")
        
        except Exception as ex:
            raise PipelineBuildException(f"An issue occurred when constructing the 'LoadDataWarehouseSnapshotPipeline' pipeline based on the metadata for client {self.client.client_code}") from ex
        
        return dag


class LoadDataWarehouseChangesPipeline(LoadDataWarehousePipeline):
    """
    Defines the tasks and order of task execution for all tasks that make up the "Load Data Warehouse" data pipeline for batches of 
    type 'Changes'.  This data pipeline loads/updates the data warehouse with data from the staging tables.
    @param: client: The BST client
    @param: pipeline_id: A unique identifier used to identify the data pipeline
    @param: load_dw_metadata: Metadata that defines the tasks to be orchestrated in order to accomplish
    the Load DW workflow.
    @param: pipeline_tags: Labels to associate with the data pipeline to distinguish the data pipeline from other data pipelines.  
    Tags are useful in the airflow UI to quickly find a data pipeline(s) by filtering by labels assigned to them.
    """
    
    def __init__( self, client: BSTClient, pipeline_id: str, load_dw_metadata: Iterator[DataWarehouseTarget_WorkflowStep], 
                pipeline_tags: Iterator[str] = None):
                super().__init__(client=client, pipeline_id=pipeline_id, load_dw_metadata = load_dw_metadata, pipeline_tags=pipeline_tags)
                self.load_dw_metadata = [i for i in load_dw_metadata if i.BatchType == 'Changes']

    def build(self) -> DAG:
        """
        Builds a data pipeline for a client using a pre-defined template that retrieves data changes stored in the 
        data lake and runs all the processes needed to apply those changes to the client's data warehouse.
        """
        # use the dag factory to build an empty dag, then add tasks to the dag
        dag = DagFactory.build_dag(dag_id=self.pipeline_id, client_code=self.client.client_code, 
                default_args=self.default_args, tags=self.pipeline_tags)

        try:

            # An anchor for identifying the start of the pipeline that downstream tasks can refer to.
            start_pipeline = DummyOperator(
                task_id = self.START_PIPELINE_TASK_ID,
                dag=dag
                )

            # An anchor task for the end that any upstream tasks can reference.
            end_pipeline = DummyOperator(
                task_id = self.END_PIPELINE_TASK_ID,
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,  
                dag=dag
            )

            # Determine if there is work to do for DW workflow or not. Branch according to the results received. 
            check_for_dw_batch = CheckForDWBatchBranchOperator(
                task_id = self.CHECK_FOR_DW_BATCH_TASK_ID, 
                client = self.client,
                true_task_id = self.CHECK_DW_BATCH_TYPE_TASK_ID,
                false_task_id = self.END_PIPELINE_TASK_ID,
                dag=dag
            )

            # Log the start of the DW batch
            start_dw_batch = StartDWBatchOperator( 
                task_id = self.START_DW_BATCH_TASK_ID, 
                client = self.client,
                dag=dag
            )

            # branches based on the current batch type
            check_batch_type = CheckCurrentDWBatchTypeBranchOperator(
                task_id = self.CHECK_DW_BATCH_TYPE_TASK_ID,
                client = self.client,
                snapshot_task_id=self.END_PIPELINE_TASK_ID,
                changes_task_id = self.CHANGES_BATCH_TASK_ID, 
                dag=dag
            )

            # Target of branch operator for changes batch
            changes_batch = DummyOperator(
                task_id = self.CHANGES_BATCH_TASK_ID,
                dag=dag
            )

            # Once all the snapshot/changes tasks are complete, the 'Load DW' process
            # can be reported as completed.
            end_dw_batch = EndDWBatchOperator(   
                task_id = self.END_DW_BATCH_TASK_ID, 
                client = self.client,
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                dag=dag
            )

            start_pipeline >> check_for_dw_batch >> [check_batch_type, end_pipeline]
            check_batch_type >> [end_pipeline, changes_batch]
            changes_batch >> start_dw_batch
            end_dw_batch >> end_pipeline

        except Exception as ex:
            raise PipelineBuildException(f"An issue occurred when attempting to construct a 'LoadDataWarehouseChangesPipeline' pipeline for {self.client.client_code}") from ex

        try:
            if len(self.load_dw_metadata) > 0:   
                self.build_load_dw_tasks(dag=dag, batch_type='Changes', predecessor_task=start_dw_batch, 
                            successor_task=end_dw_batch)
            else:
                raise PipelineBuildException(f"An issue occurred when attempting to construct a 'LoadDataWarehouseChangesPipeline' pipeline for {self.client.client_code}")
        
        except Exception as ex:
            raise PipelineBuildException(f"An issue occurred when constructing the 'LoadDataWarehouseChangesPipeline' pipeline based on the metadata for client {self.client.client_code}") from ex
        
        return dag
       
class LoadMachineLearningPipeline:
    """
    Defines the tasks and order of task execution for all tasks that make up the "MAchine Learning" data pipeline.
    This data pipeline transforms data from the data warehouse tables into the machine learning tables,
    executes the machine learing models on Databricks, then transforms the results to be used in BST Insight reports.

    @param: client: The BST client
    @param: pipeline_id: A unique identifier used to identify the data pipeline
    @param: load_dw_metadata: Metadata that defines the tasks to be orchestrated in order to accomplish
    the Load DW workflow.
    @param: pipeline_tags: Labels to associate with the data pipeline to distinguish the data pipeline from other data pipelines.

    Tags are useful in the airflow UI to quickly find a data pipeline(s) by filtering by labels assigned to them.
    """
    
    START_PIPELINE_TASK_ID = 'Start_Pipeline'
    END_PIPELINE_TASK_ID = 'End_Pipeline'
    DATABRICKS_STEP_TYPE = 'Databricks'
    STOREDPROCEDURE_STEP_TYPE = 'Stored Procedure'
    DAG_TASK_ID_PREFIX = 'MachineLearning'

   
    def __init__( self, client: BSTClient, pipeline_name:str, pipeline_id: int, 
                ml_metadata_preprocess: Iterator[MachineLearning_WorkflowStep],
                ml_metadata_modelexecution: Iterator[MachineLearning_WorkflowStep],
                ml_metadata_postprocess: Iterator[MachineLearning_WorkflowStep],
                pipeline_tags: Iterator[str] = None):

        self.client = client
        self.pipeline_name = pipeline_name
        self.pipeline_id = pipeline_id

        if len(ml_metadata_preprocess) == 0 and len(ml_metadata_modelexecution) == 0 and len(ml_metadata_postprocess) ==0:
            raise Exception(ValueError("ML Pipeline tasks are needed to build the pipeline. \
                Verify MachineLearningOrchestration.Pipeline_Step table is loaded."))

        self.ml_metadata_preprocess = ml_metadata_preprocess
        self.ml_metadata_modelexecution = ml_metadata_modelexecution
        self.ml_metadata_postprocess = ml_metadata_postprocess

        self.pipeline_tags = pipeline_tags

        # The default args that are appropriate for this particular pipeline.
        self.default_args = {
            'owner':'airflow',
            'depends_on_past': False,
            'start_date': datetime(2022,1,1),
            'email_on_failure': False,
            'email_on_success': False,
            'retries': 2,
            'retry_delay': timedelta(seconds=30),
            'retry_exponential_backoff': True,
            'provide_context': True,
            'max_active_tis_per_dag': 1  #Please use this metric to set the maximum number of runs this task can run concurrently across all DAG Runs
        }

    def build(self) -> DAG:
        """
        Populate and process Machine Learning pipeline using a pre-defined template
        and runs all the processes needed to apply those changes to the client's data warehouse.
        """
        # use the dag factory to build an empty dag, then add tasks to the dag
        dag_id = f"{self.DAG_TASK_ID_PREFIX}_{self.pipeline_name}_{self.client.client_code}"
        dag = DagFactory.build_dag(dag_id=dag_id, client_code=self.client.client_code,
                default_args=self.default_args, tags=self.pipeline_tags)

        try:

            # Pipeline starts
            start_pipeline = StartMLPipelineOperator(
                            task_id = self.START_PIPELINE_TASK_ID,
                            client=self.client,
                            pipeline_id = self.pipeline_id,
                            dag = dag
                        )

            # Upstream tasks end 
            end_pipeline = EndMLPipelineOperator(
                            task_id = self.END_PIPELINE_TASK_ID,
                            client=self.client,
                            dag = dag
                        )

            previous_task = start_pipeline

            # Pre-processing execution tasks
            for step in self.ml_metadata_preprocess:
                if step.ExecutableType == self.STOREDPROCEDURE_STEP_TYPE:
                    workflow_task = ExecutePreMLStoredProcedureStepOperator(
                            task_id = step.ExecutableName,
                            client=self.client,
                            target_workflow_step=step,
                            dag = dag
                        )

                    previous_task >> workflow_task 
                    previous_task = workflow_task


            # Databricks execution and processing
            for step in self.ml_metadata_modelexecution:
                if step.ExecutableType == self.DATABRICKS_STEP_TYPE:

                    # Execute an Airflow Databricks task
                    task_id = f"StartPipelineStep_{step.ExecutableName}"
                    workflow_task = StartMLPipelineStepOperator(
                        task_id = task_id,
                        client=self.client,
                        target_workflow_step=step,
                        dag = dag
                    )

                    previous_task >> workflow_task 
                    previous_task = workflow_task

                    # Set mount path
                    mount_path = f"dbfs:/mnt/{self.client.client_code}-001"
                    # <pipeline_id> <pipeline_run_id> <model_type> <project_count> <project_list> <execute_remote> <extract_source>
                    # <publish_results> <mount_path> <odbc_connection_string> <jdbc_connection_string>
                    # TODO Get and send the Pipeline Run ID / Saving old parameter key for fix in future sprint                  
                    # python_params = [str(self.pipeline_id),0,"all","999999","all","True","False","False",mount_path, self.client.datawarehouse_connection_string, self.client.jdbc_connection_string]
                    max_project_count = 999999
                    python_params = [str(self.pipeline_id),0,"all",str(max_project_count),"all","True","True","True",self.client.datawarehouse_connection_string, self.client.jdbc_connection_string, mount_path, ""]

                    # Set connection
                    conn = self.create_airflow_connection(dag_id=dag_id)
                    # Call Databricks API to get Workflow ID Number
                    job_id = self.get_databricks_job_by_name(step.ExecutableGroupName)
                    
                    # Set Databricks Operator
                    workflow_task = DatabricksRunNowOperator(
                        task_id = f"Databricks_Job_{step.ExecutableGroupName}",
                        databricks_conn_id = conn.conn_id,
                        job_id = job_id,
                        python_params = python_params,
                        dag = dag
                    )

                    previous_task >> workflow_task 
                    previous_task = workflow_task

                    # Execution of Airflow Databricks task
                    workflow_task = EndMLPipelineStepOperator(
                        task_id = f"EndPipelineStep_{step.ExecutableName}",
                        client=self.client,
                        target_workflow_step=step,
                        dag = dag
                    )

                    previous_task >> workflow_task 
                    previous_task = workflow_task

            # Post processing execution
            for step in self.ml_metadata_postprocess:
                if step.ExecutableType == self.STOREDPROCEDURE_STEP_TYPE:
                    workflow_task = ExecutePostMLStoredProcedureStepOperator(
                            task_id = step.ExecutableName,
                            client=self.client,
                            target_workflow_step=step,
                            dag = dag
                        )

                    previous_task >> workflow_task 
                    previous_task = workflow_task

            previous_task >> end_pipeline

        except Exception as ex:
            raise PipelineBuildException(f"An issue occurred when attempting to construct a 'LoadMachineLearningPipeline' pipeline for {self.client.client_code} - {str(ex)}") from ex
        
        return dag

    def get_databricks_job_by_name(self, job_name:str) -> str:
        # Getting the id of a databricks job by name

        host, token = self.parse_databricks_connection()
        
        endpoint = f"{host}/api/2.0/jobs/list"
        headers = {"Authorization": f"Bearer {token}"}
        try:
            result = requests.get(endpoint, headers=headers, timeout=99)
        except requests.exceptions.RequestException as ex:
            # catastrophic error. bail.
            raise SystemExit(ex) from ex

        # Parse JSON to get list of job name/job ID
        job_json = result.json()
        if job_json == {}: # Empty list
            return None
        else:
            jobs_list = job_json["jobs"]
            job = list(filter(lambda x:x["settings"]["name"] == job_name, jobs_list))
            return job

    def parse_databricks_connection(self) -> str:
        # Parsing Databricks Connection String
        conn_string = self.client.databricks_connection_string
        host_loc = 13
        token_loc = conn_string.find("?token=")
        host = conn_string[host_loc:token_loc]
        token = conn_string[token_loc + 7:]
        
        return host, token
    
    def create_airflow_connection(self, dag_id:str) -> Connection:

        host, token = self.parse_databricks_connection()
        
        conn = Connection(
        conn_id=dag_id,
        conn_type="Databricks",
        host=host,
        extra="{\"token\": \"" + token + "\", \"host\": \"" + host + "\"}"
        ) 

        # Validate 
        session = settings.Session() # get the session
        conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

        # Cleanup
        if str(conn_name) == str(conn.conn_id):
            session.execute(Connection.__table__.delete().where(Connection.conn_id == conn.conn_id))
        
        # Create a connection 
        session.add(conn)
        session.commit()                    
