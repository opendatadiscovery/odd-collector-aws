from odd_models.models import DataEntity, DataEntityType, DataTransformer, JobRunStatus, DataTransformerRun
from typing import Dict, Any, List
from datetime import datetime
from oddrn_generator.generators import DmsGenerator

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
    "failed-move": JobRunStatus.BROKEN

}


def map_dms_task(
        raw_job_data: Dict[str, Any], mapper_args: Dict[str, Any]
) -> List[DataEntity]:
    oddrn_generator: DmsGenerator = mapper_args["oddrn_generator"]
    endpoints_arn_dict: Dict[str, DataEntity] = mapper_args["endpoints_arn_dict"]
    trans = DataTransformer(
        inputs=[endpoints_arn_dict.get(raw_job_data.get('SourceEndpointArn')).oddrn],

        outputs=[endpoints_arn_dict.get(raw_job_data.get('TargetEndpointArn')).oddrn],

    )
    data_entity_task = DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("tasks", raw_job_data['ReplicationTaskIdentifier']),
        name=raw_job_data['ReplicationTaskIdentifier'],
        owner=None,
        type=DataEntityType.JOB,
        created_at=raw_job_data.get('ReplicationTaskCreationDate')
    )
    status = DMS_TASK_STATUSES.get(
        raw_job_data["Status"], JobRunStatus.UNKNOWN
    )
    data_entity_task_run = DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("runs", "run"),
        name=raw_job_data['ReplicationTaskIdentifier'] + '_run',
        owner=None,
        type=DataEntityType.JOB_RUN,
        created_at=raw_job_data.get('ReplicationTaskCreationDate'),
        data_transformer_run=DataTransformerRun(
            # start_time=raw_job_data.get('ReplicationTaskStartDate'),
            start_time=datetime(2022, 9, 24),
            end_time=datetime(2022, 9, 25),
            transformer_oddrn=oddrn_generator.get_oddrn_by_path("tasks"),
            status_reason=raw_job_data.get('StopReason')
            if status == "failed"
            else None,
            status=status,
        ),
    )
    data_entity_task.data_transformer = trans
    return [data_entity_task, data_entity_task_run]
