from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataTransformer,
    JobRunStatus,
    DataTransformerRun,
)
from typing import Dict, Any
from datetime import datetime
from odd_collector_aws.adapters.dms import (
    _keys_to_include_task,
    _METADATA_SCHEMA_URL_PREFIX,
)
from oddrn_generator.generators import DmsGenerator
from .metadata import create_metadata_extension_list
from json import loads
from .tables import EntitiesExtractor
from .endpoints import engines_factory
import os
from yaml import safe_load

DMS_TASK_STATUSES: Dict[str, JobRunStatus] = {
    "creating": JobRunStatus.UNKNOWN,
    "running": JobRunStatus.RUNNING,
    "stopped": JobRunStatus.ABORTED,
    "stopping": JobRunStatus.UNKNOWN,
    "deleting": JobRunStatus.UNKNOWN,
    "failed": JobRunStatus.FAILED,
    "starting": JobRunStatus.UNKNOWN,
    "ready": JobRunStatus.UNKNOWN,
    "modifying": JobRunStatus.UNKNOWN,
    "moving": JobRunStatus.UNKNOWN,
    "failed-move": JobRunStatus.BROKEN,
}


def get_platform_host_url() -> str:
    config_file_name = "collector_config.yaml"
    path = os.path.dirname(os.path.abspath(config_file_name)) + '/' + config_file_name
    with open(path) as f:
        config_dict: Dict[str, Any] = safe_load(f)
    return config_dict['platform_host_url']


def map_dms_task(
        raw_job_data: Dict[str, Any], mapper_args: Dict[str, Any]
) -> DataEntity:
    oddrn_generator: DmsGenerator = mapper_args["oddrn_generator"]
    endpoints_arn_dict: Dict[str, Dict[str, Any]] = mapper_args["endpoints_arn_dict"]
    rules_nodes = loads(raw_job_data['TableMappings'])['rules']
    input_endpoint_node = endpoints_arn_dict.get(raw_job_data.get("SourceEndpointArn"))
    input_engine_name = input_endpoint_node.get('EngineName')
    input_endpoint_engine_cls = engines_factory.get(input_engine_name)
    if input_endpoint_engine_cls is None:
        inputs = []
    else:
        input_endpoint_engine = input_endpoint_engine_cls(input_endpoint_node)
        input_extractor = EntitiesExtractor(rules_nodes, get_platform_host_url(), input_endpoint_engine)
        inputs = input_extractor.get_oddrns_list()
    # output_endpoint_entity = endpoints_arn_dict.get(raw_job_data.get("TargetEndpointArn"))
    trans = DataTransformer(
        inputs=inputs,
        outputs=[]
    )
    data_entity_task = DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path(
            "tasks", raw_job_data["ReplicationTaskIdentifier"]
        ),
        name=raw_job_data["ReplicationTaskIdentifier"],
        owner=None,
        type=DataEntityType.JOB,
        created_at=raw_job_data.get("ReplicationTaskCreationDate"),
    )
    data_entity_task.data_transformer = trans
    data_entity_task.metadata = create_metadata_extension_list(
        _METADATA_SCHEMA_URL_PREFIX, raw_job_data, _keys_to_include_task
    )
    return data_entity_task


def map_dms_task_run(
        raw_job_data: Dict[str, Any], mapper_args: Dict[str, Any]
) -> DataEntity:
    oddrn_generator: DmsGenerator = mapper_args["oddrn_generator"]
    oddrn_generator.get_oddrn_by_path(
        "tasks", raw_job_data["ReplicationTaskIdentifier"]
    )
    status = DMS_TASK_STATUSES.get(raw_job_data["Status"], JobRunStatus.UNKNOWN)
    data_entity_task_run = DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("runs", "run"),
        name=raw_job_data["ReplicationTaskIdentifier"] + "_run",
        owner=None,
        type=DataEntityType.JOB_RUN,
        created_at=raw_job_data.get("ReplicationTaskCreationDate"),
        data_transformer_run=DataTransformerRun(
            # start_time=raw_job_data.get('ReplicationTaskStartDate'),
            start_time=datetime(2022, 9, 24),
            end_time=datetime(2022, 9, 25),
            transformer_oddrn=oddrn_generator.get_oddrn_by_path("tasks"),
            status_reason=raw_job_data.get("StopReason")
            if status == "failed"
            else None,
            status=status,
        ),
    )
    return data_entity_task_run
